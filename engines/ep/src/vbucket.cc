/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "config.h"

#include "atomic.h"
#include "bgfetcher.h"
#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "conflict_resolution.h"
#include "durability_monitor.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ep_types.h"
#include "failover-table.h"
#include "flusher.h"
#include "hash_table.h"
#include "pre_link_document_context.h"
#include "statwriter.h"
#include "stored_value_factories.h"
#include "vbucket.h"
#include "vbucketdeletiontask.h"

#include <memcached/protocol_binary.h>
#include <memcached/server_document_iface.h>
#include <platform/compress.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <logtags.h>
#include <functional>
#include <list>
#include <set>
#include <string>
#include <vector>

/* Macros */
const auto MIN_CHK_FLUSH_TIMEOUT = std::chrono::seconds(10);
const auto MAX_CHK_FLUSH_TIMEOUT = std::chrono::seconds(30);

/* Statics definitions */
cb::AtomicDuration VBucket::chkFlushTimeout(MIN_CHK_FLUSH_TIMEOUT);
double VBucket::mutationMemThreshold = 0.9;

VBucketFilter VBucketFilter::filter_diff(const VBucketFilter &other) const {
    std::vector<Vbid> tmp(acceptable.size() + other.size());
    std::vector<Vbid>::iterator end;
    end = std::set_symmetric_difference(acceptable.begin(),
                                        acceptable.end(),
                                        other.acceptable.begin(),
                                        other.acceptable.end(),
                                        tmp.begin());
    return VBucketFilter(std::vector<Vbid>(tmp.begin(), end));
}

VBucketFilter VBucketFilter::filter_intersection(const VBucketFilter &other)
                                                                        const {
    std::vector<Vbid> tmp(acceptable.size() + other.size());
    std::vector<Vbid>::iterator end;

    end = std::set_intersection(acceptable.begin(), acceptable.end(),
                                other.acceptable.begin(),
                                other.acceptable.end(),
                                tmp.begin());
    return VBucketFilter(std::vector<Vbid>(tmp.begin(), end));
}

static bool isRange(std::set<Vbid>::const_iterator it,
                    const std::set<Vbid>::const_iterator& end,
                    size_t& length) {
    length = 0;
    for (Vbid val = *it; it != end && Vbid(val.get() + length) == *it;
         ++it, ++length) {
        // empty
    }

    --length;

    return length > 1;
}

std::ostream& operator <<(std::ostream &out, const VBucketFilter &filter)
{
    std::set<Vbid>::const_iterator it;

    if (filter.acceptable.empty()) {
        out << "{ empty }";
    } else {
        bool needcomma = false;
        out << "{ ";
        for (it = filter.acceptable.begin();
             it != filter.acceptable.end();
             ++it) {
            if (needcomma) {
                out << ", ";
            }

            size_t length;
            if (isRange(it, filter.acceptable.end(), length)) {
                std::set<Vbid>::iterator last = it;
                for (size_t i = 0; i < length; ++i) {
                    ++last;
                }
                out << "[" << *it << "," << *last << "]";
                it = last;
            } else {
                out << *it;
            }
            needcomma = true;
        }
        out << " }";
    }

    return out;
}

const vbucket_state_t VBucket::ACTIVE =
                     static_cast<vbucket_state_t>(htonl(vbucket_state_active));
const vbucket_state_t VBucket::REPLICA =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_replica));
const vbucket_state_t VBucket::PENDING =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_pending));
const vbucket_state_t VBucket::DEAD =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_dead));

VBucket::VBucket(Vbid i,
                 vbucket_state_t newState,
                 EPStats& st,
                 CheckpointConfig& chkConfig,
                 int64_t lastSeqno,
                 uint64_t lastSnapStart,
                 uint64_t lastSnapEnd,
                 std::unique_ptr<FailoverTable> table,
                 std::shared_ptr<Callback<Vbid>> flusherCb,
                 std::unique_ptr<AbstractStoredValueFactory> valFact,
                 NewSeqnoCallback newSeqnoCb,
                 SyncWriteCompleteCallback syncWriteCb,
                 Configuration& config,
                 item_eviction_policy_t evictionPolicy,
                 vbucket_state_t initState,
                 uint64_t purgeSeqno,
                 uint64_t maxCas,
                 int64_t hlcEpochSeqno,
                 bool mightContainXattrs,
                 const Collections::VB::PersistedManifest& collectionsManifest)
    : ht(st, std::move(valFact), config.getHtSize(), config.getHtLocks()),
      checkpointManager(std::make_unique<CheckpointManager>(st,
                                                            i,
                                                            chkConfig,
                                                            lastSeqno,
                                                            lastSnapStart,
                                                            lastSnapEnd,
                                                            flusherCb)),
      failovers(std::move(table)),
      opsCreate(0),
      opsDelete(0),
      opsGet(0),
      opsReject(0),
      opsUpdate(0),
      dirtyQueueSize(0),
      dirtyQueueMem(0),
      dirtyQueueFill(0),
      dirtyQueueDrain(0),
      dirtyQueueAge(0),
      dirtyQueuePendingWrites(0),
      metaDataDisk(0),
      numExpiredItems(0),
      eviction(evictionPolicy),
      stats(st),
      persistenceSeqno(0),
      numHpVBReqs(0),
      id(i),
      state(newState),
      initialState(initState),
      purge_seqno(purgeSeqno),
      takeover_backed_up(false),
      persisted_snapshot_start(lastSnapStart),
      persisted_snapshot_end(lastSnapEnd),
      receivingInitialDiskSnapshot(false),
      rollbackItemCount(0),
      hlc(maxCas,
          hlcEpochSeqno,
          std::chrono::microseconds(config.getHlcDriftAheadThresholdUs()),
          std::chrono::microseconds(config.getHlcDriftBehindThresholdUs())),
      statPrefix("vb_" + std::to_string(i.get())),
      persistenceCheckpointId(0),
      bucketCreation(false),
      deferredDeletion(false),
      deferredDeletionCookie(nullptr),
      newSeqnoCb(std::move(newSeqnoCb)),
      syncWriteCompleteCb(syncWriteCb),
      manifest(
              std::make_unique<Collections::VB::Manifest>(collectionsManifest)),
      mayContainXattrs(mightContainXattrs),
      durabilityMonitor(std::make_unique<DurabilityMonitor>(*this)) {
    if (config.getConflictResolutionType().compare("lww") == 0) {
        conflictResolver.reset(new LastWriteWinsResolution());
    } else {
        conflictResolver.reset(new RevisionSeqnoResolution());
    }

    backfill.isBackfillPhase = false;
    pendingOpsStart = std::chrono::steady_clock::time_point();
    stats.coreLocal.get()->memOverhead.fetch_add(
            sizeof(VBucket) + ht.memorySize() + sizeof(CheckpointManager));
    EP_LOG_INFO(
            "VBucket: created {} with state:{} "
            "initialState:{} lastSeqno:{} lastSnapshot:{{{},{}}} "
            "persisted_snapshot:{{{},{}}} max_cas:{} uuid:{}",
            id,
            VBucket::toString(state),
            VBucket::toString(initialState),
            lastSeqno,
            lastSnapStart,
            lastSnapEnd,
            persisted_snapshot_start,
            persisted_snapshot_end,
            getMaxCas(),
            failovers ? std::to_string(failovers->getLatestUUID()) : "<>");

    // @todo-durability: Register the replication chain via the new
    // SET_VBUCKET_STATE message instead of here.
    durabilityMonitor->registerReplicationChain({{"replica"}});
}

VBucket::~VBucket() {
    if (!pendingOps.empty()) {
        EP_LOG_WARN("~VBucket(): {} has {} pending ops", id, pendingOps.size());
    }

    stats.diskQueueSize.fetch_sub(dirtyQueueSize.load());
    stats.vbBackfillQueueSize.fetch_sub(getBackfillSize());

    // Clear out the bloomfilter(s)
    clearFilter();

    stats.coreLocal.get()->memOverhead.fetch_sub(
            sizeof(VBucket) + ht.memorySize() + sizeof(CheckpointManager));

    EP_LOG_INFO("Destroying {}", id);
}

int64_t VBucket::getHighSeqno() const {
    return checkpointManager->getHighSeqno();
}

size_t VBucket::getChkMgrMemUsage() const {
    return checkpointManager->getMemoryUsage();
}

size_t VBucket::getChkMgrMemUsageOfUnrefCheckpoints() const {
    return checkpointManager->getMemoryUsageOfUnrefCheckpoints();
}

size_t VBucket::getChkMgrMemUsageOverhead() const {
    return checkpointManager->getMemoryOverhead();
}

void VBucket::fireAllOps(EventuallyPersistentEngine &engine,
                         ENGINE_ERROR_CODE code) {
    std::unique_lock<std::mutex> lh(pendingOpLock);

    if (pendingOpsStart > std::chrono::steady_clock::time_point()) {
        auto now = std::chrono::steady_clock::now();
        if (now > pendingOpsStart) {
            auto d = std::chrono::duration_cast<std::chrono::microseconds>(
                    now - pendingOpsStart);
            stats.pendingOpsHisto.add(d);
            atomic_setIfBigger(stats.pendingOpsMaxDuration,
                               std::make_unsigned<hrtime_t>::type(d.count()));
        }
    } else {
        return;
    }

    pendingOpsStart = std::chrono::steady_clock::time_point();
    stats.pendingOps.fetch_sub(pendingOps.size());
    atomic_setIfBigger(stats.pendingOpsMax, pendingOps.size());

    while (!pendingOps.empty()) {
        const void *pendingOperation = pendingOps.back();
        pendingOps.pop_back();
        // We don't want to hold the pendingOpLock when
        // calling notifyIOComplete.
        lh.unlock();
        engine.notifyIOComplete(pendingOperation, code);
        lh.lock();
    }

    EP_LOG_DEBUG("Fired pendings ops for {} in state {}",
                 id,
                 VBucket::toString(state));
}

void VBucket::fireAllOps(EventuallyPersistentEngine &engine) {

    if (state == vbucket_state_active) {
        fireAllOps(engine, ENGINE_SUCCESS);
    } else if (state == vbucket_state_pending) {
        // Nothing
    } else {
        fireAllOps(engine, ENGINE_NOT_MY_VBUCKET);
    }
}

VBucket::ItemsToFlush VBucket::getItemsToPersist(size_t approxLimit) {
    // Fetch up to approxLimit items from rejectQueue, backfill items and
    // checkpointManager (in that order); then check if we obtained everything
    // which is available.
    ItemsToFlush result;

    // First add any items from the rejectQueue.
    while (result.items.size() < approxLimit && !rejectQueue.empty()) {
        result.items.push_back(rejectQueue.front());
        rejectQueue.pop();
    }

    // Append any 'backfill' items (mutations added by a DCP stream).
    bool backfillEmpty;
    {
        LockHolder lh(backfill.mutex);
        size_t num_items = 0;
        while (result.items.size() < approxLimit && !backfill.items.empty()) {
            result.items.push_back(backfill.items.front());
            backfill.items.pop();
            num_items++;
        }
        backfillEmpty = backfill.items.empty();
        stats.vbBackfillQueueSize.fetch_sub(num_items);
        stats.coreLocal.get()->memOverhead.fetch_sub(num_items *
                                                     sizeof(queued_item));
    }

    // Append up to approxLimit checkpoint items outstanding for the persistence
    // cursor, if we haven't yet hit the limit.
    // Note that it is only valid to queue a complete checkpoint - this is where
    // the "approx" in the limit comes from.
    const auto ckptMgrLimit = approxLimit - result.items.size();
    CheckpointManager::ItemsForCursor ckptItems;
    if (ckptMgrLimit > 0) {
        auto _begin_ = std::chrono::steady_clock::now();
        ckptItems = checkpointManager->getItemsForPersistence(result.items,
                                                              ckptMgrLimit);
        result.range = ckptItems.range;
        stats.persistenceCursorGetItemsHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - _begin_));
    } else {
        // We haven't got sufficient remaining capacity to read items from
        // CheckpoitnManager, therefore we must assume that there /could/
        // more data to follow.
        ckptItems.moreAvailable = true;
    }

    // Check if there's any more items remaining.
    result.moreAvailable =
            !rejectQueue.empty() || !backfillEmpty || ckptItems.moreAvailable;

    return result;
}

void VBucket::setState(vbucket_state_t to) {
    WriterLockHolder wlh(stateLock);
    setState_UNLOCKED(to, wlh);
}

void VBucket::setState_UNLOCKED(vbucket_state_t to,
                                WriterLockHolder &vbStateLock) {
    vbucket_state_t oldstate = state;

    if (to == vbucket_state_active &&
        checkpointManager->getOpenCheckpointId() < 2) {
        checkpointManager->setOpenCheckpointId(2);
    }

    EP_LOG_INFO("VBucket::setState: transitioning {} from:{} to:{}",
                id,
                VBucket::toString(oldstate),
                VBucket::toString(to));

    state = to;
}

vbucket_state VBucket::getVBucketState() const {
     auto persisted_range = getPersistedSnapshot();

     return vbucket_state{getState(),
                          getPersistenceCheckpointId(),
                          0,
                          getHighSeqno(),
                          getPurgeSeqno(),
                          persisted_range.start,
                          persisted_range.end,
                          getMaxCas(),
                          hlc.getEpochSeqno(),
                          mightContainXattrs(),
                          failovers->toJSON(),
                          true /*will only be written if collections enabled*/};
}



void VBucket::doStatsForQueueing(const Item& qi, size_t itemBytes)
{
    ++dirtyQueueSize;
    dirtyQueueMem.fetch_add(sizeof(Item));
    ++dirtyQueueFill;
    dirtyQueueAge.fetch_add(qi.getQueuedTime());
    dirtyQueuePendingWrites.fetch_add(itemBytes);
}

void VBucket::doStatsForFlushing(const Item& qi, size_t itemBytes) {
    --dirtyQueueSize;
    decrDirtyQueueMem(sizeof(Item));
    ++dirtyQueueDrain;
    decrDirtyQueueAge(qi.getQueuedTime());
    decrDirtyQueuePendingWrites(itemBytes);
}

void VBucket::incrMetaDataDisk(const Item& qi) {
    metaDataDisk.fetch_add(qi.getKey().size() + sizeof(ItemMetaData));
}

void VBucket::decrMetaDataDisk(const Item& qi) {
    // assume couchstore remove approx this much data from disk
    metaDataDisk.fetch_sub((qi.getKey().size() + sizeof(ItemMetaData)));
}

void VBucket::resetStats() {
    opsCreate.store(0);
    opsDelete.store(0);
    opsGet.store(0);
    opsReject.store(0);
    opsUpdate.store(0);

    stats.diskQueueSize.fetch_sub(dirtyQueueSize.exchange(0));
    dirtyQueueMem.store(0);
    dirtyQueueFill.store(0);
    dirtyQueueAge.store(0);
    dirtyQueuePendingWrites.store(0);
    dirtyQueueDrain.store(0);

    hlc.resetStats();
}

uint64_t VBucket::getQueueAge() {
    uint64_t currDirtyQueueAge = dirtyQueueAge.load(std::memory_order_relaxed);
    rel_time_t currentAge = ep_current_time() * dirtyQueueSize;
    if (currentAge < currDirtyQueueAge) {
        return 0;
    }
    return (currentAge - currDirtyQueueAge) * 1000;
}

template <typename T>
void VBucket::addStat(const char *nm, const T &val, ADD_STAT add_stat,
                      const void *c) {
    std::string stat = statPrefix;
    if (nm != NULL) {
        add_prefixed_stat(statPrefix, nm, val, add_stat, c);
    } else {
        add_casted_stat(statPrefix.data(), val, add_stat, c);
    }
}

void VBucket::handlePreExpiry(const HashTable::HashBucketLock& hbl,
                              StoredValue& v) {
    // Pending items should not be subject to expiry
    if (v.getCommitted() == CommittedState::Pending) {
        std::stringstream ss;
        ss << v;
        throw std::invalid_argument(
                "VBucket::handlePreExpiry: Cannot expire pending "
                "StoredValues:" +
                cb::UserDataView(ss.str()).getSanitizedValue());
    }

    value_t value = v.getValue();
    if (value) {
        std::unique_ptr<Item> itm(v.toItem(false, id));
        item_info itm_info;
        EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();
        itm_info =
                itm->toItemInfo(failovers->getLatestUUID(), getHLCEpochSeqno());
        value_t new_val(Blob::Copy(*value));
        itm->replaceValue(new_val.get());
        itm->setDataType(v.getDatatype());

        SERVER_HANDLE_V1* sapi = engine->getServerApi();
        /* TODO: In order to minimize allocations, the callback needs to
         * allocate an item whose value size will be exactly the size of the
         * value after pre-expiry is performed.
         */
        if (sapi->document->pre_expiry(itm_info)) {
            Item new_item(v.getKey(),
                          v.getFlags(),
                          v.getExptime(),
                          itm_info.value[0].iov_base,
                          itm_info.value[0].iov_len,
                          itm_info.datatype,
                          v.getCas(),
                          v.getBySeqno(),
                          id,
                          v.getRevSeqno());

            new_item.setNRUValue(v.getNRUValue());
            new_item.setFreqCounterValue(v.getFreqCounterValue());
            new_item.setDeleted(DeleteSource::TTL);
            ht.unlocked_updateStoredValue(hbl, v, new_item);
        }
    }
}

ENGINE_ERROR_CODE VBucket::commit(const DocKey& key,
                                  uint64_t pendingSeqno,
                                  boost::optional<int64_t> commitSeqno) {
    auto htRes = ht.findForWrite(key);
    if (!htRes.storedValue) {
        // If we are committing we /should/ always find the pending item.
        EP_LOG_WARN(
                "VBucket::commit ({}) failed as no HashTable item found with "
                "key:{}",
                id,
                cb::UserDataView(cb::const_char_buffer(key)));
        return ENGINE_KEY_ENOENT;
    }

    if (htRes.storedValue->getCommitted() != CommittedState::Pending) {
        // We should always find a pending item when committing; if not
        // this is a logic error...
        std::stringstream ss;
        ss << *htRes.storedValue;
        EP_LOG_WARN(
                "VBucket::commit ({}) failed as HashTable value is not "
                "CommittedState::Pending - {}",
                id,
                cb::UserData(ss.str()));
        return ENGINE_EINVAL;
    }

    VBQueueItemCtx queueItmCtx;
    if (commitSeqno) {
        queueItmCtx.genBySeqno = GenerateBySeqno::No;
    }
    auto notify = commitStoredValue(
            htRes.lock, *htRes.storedValue, queueItmCtx, commitSeqno);

    notifyNewSeqno(notify);

    return ENGINE_SUCCESS;
}

void VBucket::notifyClientOfCommit(const void* cookie) {
    EP_LOG_DEBUG("VBucket::notifyClientOfCommit ({}) cookie:{}", id, cookie);

    syncWriteCompleteCb(cookie, ENGINE_SUCCESS);
}

bool VBucket::addPendingOp(const void* cookie) {
    LockHolder lh(pendingOpLock);
    if (state != vbucket_state_pending) {
        // State transitioned while we were waiting.
        return false;
    }
    // Start a timer when enqueuing the first client.
    if (pendingOps.empty()) {
        pendingOpsStart = std::chrono::steady_clock::now();
    }
    pendingOps.push_back(cookie);
    ++stats.pendingOps;
    ++stats.pendingOpsTotal;
    return true;
}

uint64_t VBucket::getPersistenceCheckpointId() const {
    return persistenceCheckpointId.load();
}

void VBucket::setPersistenceCheckpointId(uint64_t checkpointId) {
    persistenceCheckpointId.store(checkpointId);
}

void VBucket::markDirty(const DocKey& key) {
    auto htRes = ht.findForWrite(key);
    if (htRes.storedValue) {
        htRes.storedValue->markDirty();
    } else {
        EP_LOG_WARN(
                "VBucket::markDirty: Error marking dirty, a key is "
                "missing from {}",
                id);
    }
}

bool VBucket::isResidentRatioUnderThreshold(float threshold) {
    if (eviction != FULL_EVICTION) {
        throw std::invalid_argument("VBucket::isResidentRatioUnderThreshold: "
                "policy (which is " + std::to_string(eviction) +
                ") must be FULL_EVICTION");
    }
    size_t num_items = getNumItems();
    size_t num_non_resident_items = getNumNonResidentItems();
    float ratio =
            num_items
                    ? ((float)(num_items - num_non_resident_items) / num_items)
                    : 0.0;
    if (threshold >= ratio) {
        return true;
    } else {
        return false;
    }
}

void VBucket::createFilter(size_t key_count, double probability) {
    // Create the actual bloom filter upon vbucket creation during
    // scenarios:
    //      - Bucket creation
    //      - Rebalance
    LockHolder lh(bfMutex);
    if (bFilter == nullptr && tempFilter == nullptr) {
        bFilter = std::make_unique<BloomFilter>(key_count, probability,
                                        BFILTER_ENABLED);
    } else {
        EP_LOG_WARN("({}) Bloom filter / Temp filter already exist!", id);
    }
}

void VBucket::initTempFilter(size_t key_count, double probability) {
    // Create a temp bloom filter with status as COMPACTING,
    // if the main filter is found to exist, set its state to
    // COMPACTING as well.
    LockHolder lh(bfMutex);
    tempFilter = std::make_unique<BloomFilter>(key_count, probability,
                                     BFILTER_COMPACTING);
    if (bFilter) {
        bFilter->setStatus(BFILTER_COMPACTING);
    }
}

void VBucket::addToFilter(const DocKey& key) {
    LockHolder lh(bfMutex);
    if (bFilter) {
        bFilter->addKey(key);
    }

    // If the temp bloom filter is not found to be NULL,
    // it means that compaction is running on the particular
    // vbucket. Therefore add the key to the temp filter as
    // well, as once compaction completes the temp filter
    // will replace the main bloom filter.
    if (tempFilter) {
        tempFilter->addKey(key);
    }
}

bool VBucket::maybeKeyExistsInFilter(const DocKey& key) {
    LockHolder lh(bfMutex);
    if (bFilter) {
        return bFilter->maybeKeyExists(key);
    } else {
        // If filter doesn't exist, allow the BgFetch to go through.
        return true;
    }
}

bool VBucket::isTempFilterAvailable() {
    LockHolder lh(bfMutex);
    if (tempFilter &&
        (tempFilter->getStatus() == BFILTER_COMPACTING ||
         tempFilter->getStatus() == BFILTER_ENABLED)) {
        return true;
    } else {
        return false;
    }
}

void VBucket::addToTempFilter(const DocKey& key) {
    // Keys will be added to only the temp filter during
    // compaction.
    LockHolder lh(bfMutex);
    if (tempFilter) {
        tempFilter->addKey(key);
    }
}

void VBucket::swapFilter() {
    // Delete the main bloom filter and replace it with
    // the temp filter that was populated during compaction,
    // only if the temp filter's state is found to be either at
    // COMPACTING or ENABLED (if in the case the user enables
    // bloomfilters for some reason while compaction was running).
    // Otherwise, it indicates that the filter's state was
    // possibly disabled during compaction, therefore clear out
    // the temp filter. If it gets enabled at some point, a new
    // bloom filter will be made available after the next
    // compaction.

    LockHolder lh(bfMutex);
    if (tempFilter) {
        bFilter.reset();

        if (tempFilter->getStatus() == BFILTER_COMPACTING ||
             tempFilter->getStatus() == BFILTER_ENABLED) {
            bFilter = std::move(tempFilter);
            bFilter->setStatus(BFILTER_ENABLED);
        }
        tempFilter.reset();
    }
}

void VBucket::clearFilter() {
    LockHolder lh(bfMutex);
    bFilter.reset();
    tempFilter.reset();
}

void VBucket::setFilterStatus(bfilter_status_t to) {
    LockHolder lh(bfMutex);
    if (bFilter) {
        bFilter->setStatus(to);
    }
    if (tempFilter) {
        tempFilter->setStatus(to);
    }
}

std::string VBucket::getFilterStatusString() {
    LockHolder lh(bfMutex);
    if (bFilter) {
        return bFilter->getStatusString();
    } else if (tempFilter) {
        return tempFilter->getStatusString();
    } else {
        return "DOESN'T EXIST";
    }
}

size_t VBucket::getFilterSize() {
    LockHolder lh(bfMutex);
    if (bFilter) {
        return bFilter->getFilterSize();
    } else {
        return 0;
    }
}

size_t VBucket::getNumOfKeysInFilter() {
    LockHolder lh(bfMutex);
    if (bFilter) {
        return bFilter->getNumOfKeysInFilter();
    } else {
        return 0;
    }
}

VBNotifyCtx VBucket::queueDirty(
        StoredValue& v,
        const GenerateBySeqno generateBySeqno,
        const GenerateCas generateCas,
        const bool isBackfillItem,
        boost::optional<DurabilityItemCtx> durabilityCtx,
        PreLinkDocumentContext* preLinkDocumentContext) {
    VBNotifyCtx notifyCtx;

    // If we are queuing a SyncWrite StoredValue; extract the durability
    // requirements to use to create the Item.
    boost::optional<cb::durability::Requirements> durabilityReqs;
    if (durabilityCtx) {
        durabilityReqs = durabilityCtx->requirements;
    }
    queued_item qi(v.toItem(false, getId(), durabilityReqs));

    // MB-27457: Timestamp deletes only when they don't already have a timestamp
    // assigned. This is here to ensure all deleted items have a timestamp which
    // our tombstone purger can use to determine which tombstones to purge. A
    // DCP replicated or deleteWithMeta created delete may already have a time
    // assigned to it.
    if (qi->isDeleted() && qi->getDeleteTime() == 0) {
        qi->setExpTime(ep_real_time());
    }

    if (!mightContainXattrs() && mcbp::datatype::is_xattr(v.getDatatype())) {
        setMightContainXattrs();
    }

    if (isBackfillItem) {
        queueBackfillItem(qi, generateBySeqno);
        notifyCtx.notifyFlusher = true;
        /* During backfill on a TAP receiver we need to update the snapshot
         range in the checkpoint. Has to be done here because in case of TAP
         backfill, above, we use vb.queueBackfillItem() instead of
         vb.checkpointManager->queueDirty() */
        if (generateBySeqno == GenerateBySeqno::Yes) {
            checkpointManager->resetSnapshotRange();
        }
    } else {
        notifyCtx.notifyFlusher =
                checkpointManager->queueDirty(*this,
                                              qi,
                                              generateBySeqno,
                                              generateCas,
                                              preLinkDocumentContext);
        notifyCtx.notifyReplication = true;
        if (GenerateCas::Yes == generateCas) {
            v.setCas(qi->getCas());
        }
    }

    v.setBySeqno(qi->getBySeqno());
    notifyCtx.bySeqno = qi->getBySeqno();

    if (qi->getCommitted() == CommittedState::Pending) {
        // Register this mutation with the durability monitor.
        const auto cookie = durabilityCtx->cookie;
        durabilityMonitor->addSyncWrite(cookie, qi);
    }

    return notifyCtx;
}

StoredValue* VBucket::fetchValidValue(
        HashTable::HashBucketLock& hbl,
        const DocKey& key,
        WantsDeleted wantsDeleted,
        TrackReference trackReference,
        QueueExpired queueExpired,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    if (!hbl.getHTLock()) {
        throw std::logic_error(
                "Hash bucket lock not held in "
                "VBucket::fetchValidValue() for hash bucket: " +
                std::to_string(hbl.getBucketNum()) + "for key: " +
                std::string(reinterpret_cast<const char*>(key.data()),
                            key.size()));
    }
    if (queueExpired == QueueExpired::Yes && !cHandle.valid()) {
        throw std::invalid_argument(
                "VBucket::fetchValidValue cannot queue "
                "expired items for invalid collection");
    }
    StoredValue* v = ht.unlocked_find(
            key, hbl.getBucketNum(), wantsDeleted, trackReference);
    if (v && !v->isDeleted() && !v->isTempItem()) {
        // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            if (getState() != vbucket_state_active) {
                return wantsDeleted == WantsDeleted::Yes ? v : NULL;
            }

            // queueDirty only allowed on active VB
            if (queueExpired == QueueExpired::Yes &&
                getState() == vbucket_state_active) {
                incExpirationStat(ExpireBy::Access);
                handlePreExpiry(hbl, *v);
                VBNotifyCtx notifyCtx;
                std::tie(std::ignore, v, notifyCtx) =
                        processExpiredItem(hbl, *v, cHandle);
                notifyNewSeqno(notifyCtx);
            }
            return wantsDeleted == WantsDeleted::Yes ? v : NULL;
        }
    }
    return v;
}

void VBucket::incExpirationStat(const ExpireBy source) {
    switch (source) {
    case ExpireBy::Pager:
        ++stats.expired_pager;
        break;
    case ExpireBy::Compactor:
        ++stats.expired_compactor;
        break;
    case ExpireBy::Access:
        ++stats.expired_access;
        break;
    }
    ++numExpiredItems;
}

MutationStatus VBucket::setFromInternal(Item& itm) {
    if (!hasMemoryForStoredValue(stats, itm, UseActiveVBMemThreshold::Yes)) {
        return MutationStatus::NoMem;
    }
    return ht.set(itm);
}

cb::StoreIfStatus VBucket::callPredicate(cb::StoreIfPredicate predicate,
                                         StoredValue* v) {
    cb::StoreIfStatus storeIfStatus = cb::StoreIfStatus::Continue;
    if (v) {
        auto info = v->getItemInfo(failovers->getLatestUUID());
        storeIfStatus = predicate(info, getInfo());
        // No no, you can't ask for it again
        if (storeIfStatus == cb::StoreIfStatus::GetItemInfo &&
            info.is_initialized()) {
            throw std::logic_error(
                    "VBucket::callPredicate invalid result of GetItemInfo");
        }
    } else {
        storeIfStatus = predicate({/*no info*/}, getInfo());
    }

    if (storeIfStatus == cb::StoreIfStatus::GetItemInfo &&
        eviction == VALUE_ONLY) {
        // We're VE, if we don't have, we don't have it.
        storeIfStatus = cb::StoreIfStatus::Continue;
    }

    return storeIfStatus;
}

ENGINE_ERROR_CODE VBucket::set(Item& itm,
                               const void* cookie,
                               EventuallyPersistentEngine& engine,
                               cb::StoreIfPredicate predicate) {
    bool cas_op = (itm.getCas() != 0);
    auto htRes = ht.findForWrite(itm.getKey());
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    cb::StoreIfStatus storeIfStatus = cb::StoreIfStatus::Continue;
    if (predicate &&
        (storeIfStatus = callPredicate(predicate, v)) ==
                cb::StoreIfStatus::Fail) {
        return ENGINE_PREDICATE_FAILED;
    }

    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    bool maybeKeyExists = true;
    // If we didn't find a valid item then check the bloom filter, but only
    // if we're full-eviction with a CAS operation or a have a predicate that
    // requires the item's info
    if ((v == nullptr || v->isTempInitialItem()) &&
        (eviction == FULL_EVICTION) &&
        ((itm.getCas() != 0) ||
         storeIfStatus == cb::StoreIfStatus::GetItemInfo)) {
        // Check Bloomfilter's prediction
        if (!maybeKeyExistsInFilter(itm.getKey())) {
            maybeKeyExists = false;
        }
    }

    PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
    VBQueueItemCtx queueItmCtx;
    if (itm.getCommitted() == CommittedState::Pending) {
        queueItmCtx.durability =
                DurabilityItemCtx{itm.getDurabilityReqs(), cookie};
    }
    queueItmCtx.preLinkDocumentContext = &preLinkDocumentContext;
    MutationStatus status;
    boost::optional<VBNotifyCtx> notifyCtx;
    std::tie(status, notifyCtx) = processSet(hbl,
                                             v,
                                             itm,
                                             itm.getCas(),
                                             /*allowExisting*/ true,
                                             /*hashMetaData*/ false,
                                             queueItmCtx,
                                             storeIfStatus,
                                             maybeKeyExists);

    // For pending SyncWrites we initially return EWOULDBLOCK; will notify
    // client when request is committed / aborted later.
    ENGINE_ERROR_CODE ret = (itm.getCommitted() == CommittedState::Pending)
                                    ? ENGINE_EWOULDBLOCK
                                    : ENGINE_SUCCESS;
    switch (status) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::IsLocked:
        ret = ENGINE_LOCKED;
        break;
    case MutationStatus::NotFound:
        if (cas_op) {
            ret = ENGINE_KEY_ENOENT;
            break;
        }
    // FALLTHROUGH
    case MutationStatus::WasDirty:
    // Even if the item was dirty, push it into the vbucket's open
    // checkpoint.
    case MutationStatus::WasClean:
        notifyNewSeqno(*notifyCtx);

        itm.setBySeqno(v->getBySeqno());
        itm.setCas(v->getCas());
        break;
    case MutationStatus::NeedBgFetch: { // CAS operation with non-resident item
        // +
        // full eviction.
        if (v) {
            // temp item is already created. Simply schedule a bg fetch job
            hbl.getHTLock().unlock();
            bgFetch(itm.getKey(), cookie, engine, true);
            return ENGINE_EWOULDBLOCK;
        }
        ret = addTempItemAndBGFetch(hbl, itm.getKey(), cookie, engine, true);
        break;
    }

    case MutationStatus::IsPendingSyncWrite:
        ret = ENGINE_SYNC_WRITE_IN_PROGRESS;
        break;
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::replace(
        Item& itm,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        cb::StoreIfPredicate predicate,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto htRes = ht.findForWrite(itm.getKey());
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    cb::StoreIfStatus storeIfStatus = cb::StoreIfStatus::Continue;
    if (predicate &&
        (storeIfStatus = callPredicate(predicate, v)) ==
                cb::StoreIfStatus::Fail) {
        return ENGINE_PREDICATE_FAILED;
    }

    if (v) {
        if (isLogicallyNonExistent(*v, cHandle)) {
            ht.cleanupIfTemporaryItem(hbl, *v);
            return ENGINE_KEY_ENOENT;
        }

        MutationStatus mtype;
        boost::optional<VBNotifyCtx> notifyCtx;
        if (eviction == FULL_EVICTION && v->isTempInitialItem()) {
            mtype = MutationStatus::NeedBgFetch;
        } else {
            PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
            VBQueueItemCtx queueItmCtx;
            queueItmCtx.preLinkDocumentContext = &preLinkDocumentContext;
            if (itm.getCommitted() == CommittedState::Pending) {
                queueItmCtx.durability =
                        DurabilityItemCtx{itm.getDurabilityReqs(), cookie};
            }
            std::tie(mtype, notifyCtx) = processSet(hbl,
                                                    v,
                                                    itm,
                                                    0,
                                                    /*allowExisting*/ true,
                                                    /*hasMetaData*/ false,
                                                    queueItmCtx,
                                                    storeIfStatus);
        }

        // For pending SyncWrites we initially return EWOULDBLOCK; will notify
        // client when request is committed / aborted later.
        ENGINE_ERROR_CODE ret = (itm.getCommitted() == CommittedState::Pending)
                                        ? ENGINE_EWOULDBLOCK
                                        : ENGINE_SUCCESS;
        switch (mtype) {
        case MutationStatus::NoMem:
            ret = ENGINE_ENOMEM;
            break;
        case MutationStatus::IsLocked:
            ret = ENGINE_LOCKED;
            break;
        case MutationStatus::InvalidCas:
        case MutationStatus::NotFound:
            ret = ENGINE_NOT_STORED;
            break;
        // FALLTHROUGH
        case MutationStatus::WasDirty:
        // Even if the item was dirty, push it into the vbucket's open
        // checkpoint.
        case MutationStatus::WasClean:
            notifyNewSeqno(*notifyCtx);

            itm.setBySeqno(v->getBySeqno());
            itm.setCas(v->getCas());
            break;
        case MutationStatus::NeedBgFetch: {
            // temp item is already created. Simply schedule a bg fetch job
            hbl.getHTLock().unlock();
            bgFetch(itm.getKey(), cookie, engine, true);
            ret = ENGINE_EWOULDBLOCK;
            break;
        }
        case MutationStatus::IsPendingSyncWrite:
            ret = ENGINE_SYNC_WRITE_IN_PROGRESS;
            break;
        }

        return ret;
    } else {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        }

        if (maybeKeyExistsInFilter(itm.getKey())) {
            return addTempItemAndBGFetch(
                    hbl, itm.getKey(), cookie, engine, false);
        } else {
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT for replace().
            return ENGINE_KEY_ENOENT;
        }
    }
}

ENGINE_ERROR_CODE VBucket::addBackfillItem(Item& itm) {
    auto htRes = ht.findForWrite(itm.getKey());
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    // Note that this function is only called on replica or pending vbuckets.
    if (v && v->isLocked(ep_current_time())) {
        v->unlock();
    }

    VBQueueItemCtx queueItmCtx{
            GenerateBySeqno::No,
            GenerateCas::No,
            TrackCasDrift::No,
            /*isBackfillItem*/ true,
            DurabilityItemCtx{itm.getDurabilityReqs(), nullptr},
            nullptr /* No pre link should happen */};
    MutationStatus status;
    boost::optional<VBNotifyCtx> notifyCtx;
    std::tie(status, notifyCtx) = processSet(hbl,
                                             v,
                                             itm,
                                             0,
                                             /*allowExisting*/ true,
                                             /*hasMetaData*/ true,
                                             queueItmCtx,
                                             {/*no predicate*/});

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (status) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
    case MutationStatus::IsLocked:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::WasDirty:
    // FALLTHROUGH, to ensure the bySeqno for the hashTable item is
    // set correctly, and also the sequence numbers are ordered correctly.
    // (MB-14003)
    case MutationStatus::NotFound:
    // FALLTHROUGH
    case MutationStatus::WasClean: {
        if (v == nullptr) {
            // Scan build thinks v could be nullptr - check to suppress warning
            throw std::logic_error(
                    "VBucket::addBackfillItem: "
                    "StoredValue should not be null if status WasClean");
        }
        setMaxCas(v->getCas());
        // we unlock ht lock here because we want to avoid potential lock
        // inversions arising from notifyNewSeqno() call
        hbl.getHTLock().unlock();
        notifyNewSeqno(*notifyCtx);
    } break;
    case MutationStatus::NeedBgFetch:
        throw std::logic_error(
                "VBucket::addBackfillItem: "
                "SET on a non-active vbucket should not require a "
                "bg_metadata_fetch.");

    case MutationStatus::IsPendingSyncWrite:
        throw std::logic_error(
                "VBucket::addBackfillItem: SET on a non-active vbucket should "
                "not encounter a Pending Sync Write");
    }

    return ret;
}

void VBucket::addDurabilityMonitorStats(ADD_STAT addStat,
                                        const void* cookie) const {
    durabilityMonitor->addStats(addStat, cookie);
}

ENGINE_ERROR_CODE VBucket::setWithMeta(
        Item& itm,
        uint64_t cas,
        uint64_t* seqno,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        CheckConflicts checkConflicts,
        bool allowExisting,
        GenerateBySeqno genBySeqno,
        GenerateCas genCas,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto htRes = ht.findForWrite(itm.getKey());
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;
    bool maybeKeyExists = true;

    // Effectively ignore logically deleted keys, they cannot stop the op
    if (v && cHandle.isLogicallyDeleted(v->getBySeqno())) {
        // v is not really here, operate like it's not and skip conflict checks
        checkConflicts = CheckConflicts::No;
        // And ensure ADD_W_META works like SET_W_META, just overwrite existing
        allowExisting = true;
    }

    if (checkConflicts == CheckConflicts::Yes) {
        if (v) {
            if (v->isTempInitialItem()) {
                bgFetch(itm.getKey(), cookie, engine, true);
                return ENGINE_EWOULDBLOCK;
            }

            if (!(conflictResolver->resolve(*v,
                                            itm.getMetaData(),
                                            itm.getDataType(),
                                            itm.isDeleted()))) {
                ++stats.numOpsSetMetaResolutionFailed;
                // If the existing item happens to be a temporary item,
                // delete the item to save memory in the hash table
                if (v->isTempItem()) {
                    deleteStoredValue(hbl, *v);
                }
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            if (maybeKeyExistsInFilter(itm.getKey())) {
                return addTempItemAndBGFetch(
                        hbl, itm.getKey(), cookie, engine, true);
            } else {
                maybeKeyExists = false;
            }
        }
    } else {
        if (eviction == FULL_EVICTION) {
            // Check Bloomfilter's prediction
            if (!maybeKeyExistsInFilter(itm.getKey())) {
                maybeKeyExists = false;
            }
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    VBQueueItemCtx queueItmCtx{
            genBySeqno,
            genCas,
            TrackCasDrift::Yes,
            /*isBackfillItem*/ false,
            DurabilityItemCtx{itm.getDurabilityReqs(), cookie},
            nullptr /* No pre link step needed */};
    MutationStatus status;
    boost::optional<VBNotifyCtx> notifyCtx;
    std::tie(status, notifyCtx) = processSet(hbl,
                                             v,
                                             itm,
                                             cas,
                                             allowExisting,
                                             true,
                                             queueItmCtx,
                                             {/*no predicate*/},
                                             maybeKeyExists);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (status) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::IsLocked:
        ret = ENGINE_LOCKED;
        break;
    case MutationStatus::WasDirty:
    case MutationStatus::WasClean: {
        if (v == nullptr) {
            // Scan build thinks v could be nullptr - check to suppress warning
            throw std::logic_error(
                    "VBucket::setWithMeta: "
                    "StoredValue should not be null if status WasClean");
        }
        if (seqno) {
            *seqno = static_cast<uint64_t>(v->getBySeqno());
        }
        // we unlock ht lock here because we want to avoid potential lock
        // inversions arising from notifyNewSeqno() call
        hbl.getHTLock().unlock();
        notifyNewSeqno(*notifyCtx);
    } break;
    case MutationStatus::NotFound:
        ret = ENGINE_KEY_ENOENT;
        break;
    case MutationStatus::NeedBgFetch: { // CAS operation with non-resident item
        // + full eviction.
        if (v) { // temp item is already created. Simply schedule a
            hbl.getHTLock().unlock(); // bg fetch job.
            bgFetch(itm.getKey(), cookie, engine, true);
            return ENGINE_EWOULDBLOCK;
        }
        ret = addTempItemAndBGFetch(hbl, itm.getKey(), cookie, engine, true);
        break;
    }
    case MutationStatus::IsPendingSyncWrite:
        ret = ENGINE_SYNC_WRITE_IN_PROGRESS;
        break;
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::deleteItem(
        uint64_t& cas,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        ItemMetaData* itemMeta,
        mutation_descr_t& mutInfo,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto htRes = ht.findForWrite(cHandle.getKey());
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    if (!v || v->isDeleted() || v->isTempItem() ||
        cHandle.isLogicallyDeleted(v->getBySeqno())) {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else { // Full eviction.
            if (!v) { // Item might be evicted from cache.
                if (maybeKeyExistsInFilter(cHandle.getKey())) {
                    return addTempItemAndBGFetch(
                            hbl, cHandle.getKey(), cookie, engine, true);
                } else {
                    // As bloomfilter predicted that item surely doesn't
                    // exist on disk, return ENOENT for deleteItem().
                    return ENGINE_KEY_ENOENT;
                }
            } else if (v->isTempInitialItem()) {
                hbl.getHTLock().unlock();
                bgFetch(cHandle.getKey(), cookie, engine, true);
                return ENGINE_EWOULDBLOCK;
            } else { // Non-existent or deleted key.
                if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
                    // Delete a temp non-existent item to ensure that
                    // if a delete were issued over an item that doesn't
                    // exist, then we don't preserve a temp item.
                    deleteStoredValue(hbl, *v);
                }
                return ENGINE_KEY_ENOENT;
            }
        }
    }

    if (v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    if (itemMeta != nullptr) {
        itemMeta->cas = v->getCas();
    }

    MutationStatus delrv;
    boost::optional<VBNotifyCtx> notifyCtx;
    if (v->isExpired(ep_real_time())) {
        std::tie(delrv, v, notifyCtx) = processExpiredItem(hbl, *v, cHandle);
    } else {
        ItemMetaData metadata;
        metadata.revSeqno = v->getRevSeqno() + 1;
        std::tie(delrv, v, notifyCtx) =
                processSoftDelete(hbl,
                                  *v,
                                  cas,
                                  metadata,
                                  VBQueueItemCtx{},
                                  /*use_meta*/ false,
                                  /*bySeqno*/ v->getBySeqno(),
                                  DeleteSource::Explicit);
    }

    uint64_t seqno = 0;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (delrv) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::IsLocked:
        ret = ENGINE_LOCKED_TMPFAIL;
        break;
    case MutationStatus::NotFound:
        ret = ENGINE_KEY_ENOENT;
    /* Fallthrough:
     * A NotFound return value at this point indicates that the
     * item has expired. But, a deletion still needs to be queued
     * for the item in order to persist it.
     */
    case MutationStatus::WasClean:
    case MutationStatus::WasDirty:
        if (itemMeta != nullptr) {
            itemMeta->revSeqno = v->getRevSeqno();
            itemMeta->flags = v->getFlags();
            itemMeta->exptime = v->getExptime();
        }

        notifyNewSeqno(*notifyCtx);
        seqno = static_cast<uint64_t>(v->getBySeqno());
        cas = v->getCas();

        if (delrv != MutationStatus::NotFound) {
            mutInfo.seqno = seqno;
            mutInfo.vbucket_uuid = failovers->getLatestUUID();
            if (itemMeta != nullptr) {
                itemMeta->cas = v->getCas();
            }
        }
        break;
    case MutationStatus::NeedBgFetch:
        // We already figured out if a bg fetch is requred for a full-evicted
        // item above.
        throw std::logic_error(
                "VBucket::deleteItem: "
                "Unexpected NEEDS_BG_FETCH from processSoftDelete");

    case MutationStatus::IsPendingSyncWrite:
        ret = ENGINE_SYNC_WRITE_IN_PROGRESS;
        break;
    }
    return ret;
}

ENGINE_ERROR_CODE VBucket::deleteWithMeta(
        uint64_t& cas,
        uint64_t* seqno,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        CheckConflicts checkConflicts,
        const ItemMetaData& itemMeta,
        bool backfill,
        GenerateBySeqno genBySeqno,
        GenerateCas generateCas,
        uint64_t bySeqno,
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        DeleteSource deleteSource) {
    const auto& key = cHandle.getKey();
    auto htRes = ht.findForWrite(key);
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    if (v && cHandle.isLogicallyDeleted(v->getBySeqno())) {
        return ENGINE_KEY_ENOENT;
    }

    // Need conflict resolution?
    if (checkConflicts == CheckConflicts::Yes) {
        if (v) {
            if (v->isTempInitialItem()) {
                bgFetch(key, cookie, engine, true);
                return ENGINE_EWOULDBLOCK;
            }

            if (!(conflictResolver->resolve(*v,
                                            itemMeta,
                                            PROTOCOL_BINARY_RAW_BYTES,
                                            true))) {
                ++stats.numOpsDelMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            // Item is 1) deleted or not existent in the value eviction case OR
            // 2) deleted or evicted in the full eviction.
            if (maybeKeyExistsInFilter(key)) {
                return addTempItemAndBGFetch(hbl, key, cookie, engine, true);
            } else {
                // Even though bloomfilter predicted that item doesn't exist
                // on disk, we must put this delete on disk if the cas is valid.
                TempAddStatus rv = addTempStoredValue(hbl, key);
                if (rv == TempAddStatus::NoMem) {
                    return ENGINE_ENOMEM;
                }
                v = ht.unlocked_find(key,
                                     hbl.getBucketNum(),
                                     WantsDeleted::Yes,
                                     TrackReference::No);
                v->setTempDeleted();
            }
        }
    } else {
        if (!v) {
            // We should always try to persist a delete here.
            TempAddStatus rv = addTempStoredValue(hbl, key);
            if (rv == TempAddStatus::NoMem) {
                return ENGINE_ENOMEM;
            }
            v = ht.unlocked_find(key,
                                 hbl.getBucketNum(),
                                 WantsDeleted::Yes,
                                 TrackReference::No);
            v->setTempDeleted();
            v->setCas(cas);
        } else if (v->isTempInitialItem()) {
            v->setTempDeleted();
            v->setCas(cas);
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    MutationStatus delrv;
    boost::optional<VBNotifyCtx> notifyCtx;
    bool metaBgFetch = true;
    if (!v) {
        if (eviction == FULL_EVICTION) {
            delrv = MutationStatus::NeedBgFetch;
        } else {
            delrv = MutationStatus::NotFound;
        }
    } else if (v->isTempDeletedItem() &&
               mcbp::datatype::is_xattr(v->getDatatype()) && !v->isResident()) {
        // MB-25671: A temp deleted xattr with no value must be fetched before
        // the deleteWithMeta can be applied.
        delrv = MutationStatus::NeedBgFetch;
        metaBgFetch = false;
    } else {
        VBQueueItemCtx queueItmCtx{genBySeqno,
                                   generateCas,
                                   TrackCasDrift::Yes,
                                   backfill,
                                   {},
                                   nullptr /* No pre link step needed */};

        std::unique_ptr<Item> itm;
        if (getState() == vbucket_state_active &&
            mcbp::datatype::is_xattr(v->getDatatype()) &&
            (itm = pruneXattrDocument(*v, itemMeta))) {
            // A new item has been generated and must be given a new seqno
            queueItmCtx.genBySeqno = GenerateBySeqno::Yes;

            std::tie(v, delrv, notifyCtx) =
                    updateStoredValue(hbl, *v, *itm, queueItmCtx);
        } else {
            // system xattrs must remain, however no need to prune xattrs if
            // this is a replication call (i.e. not to an active vbucket),
            // the active has done this and we must just store what we're
            // given.
            std::tie(delrv, v, notifyCtx) = processSoftDelete(hbl,
                                                              *v,
                                                              cas,
                                                              itemMeta,
                                                              queueItmCtx,
                                                              /*use_meta*/ true,
                                                              bySeqno,
                                                              deleteSource);
        }
    }
    cas = v ? v->getCas() : 0;

    switch (delrv) {
    case MutationStatus::NoMem:
        return ENGINE_ENOMEM;
    case MutationStatus::InvalidCas:
        return ENGINE_KEY_EEXISTS;
    case MutationStatus::IsLocked:
        return ENGINE_LOCKED_TMPFAIL;
    case MutationStatus::NotFound:
        return ENGINE_KEY_ENOENT;
    case MutationStatus::WasDirty:
    case MutationStatus::WasClean: {
        if (v == nullptr) {
            // Scan build thinks v could be nullptr - check to suppress warning
            throw std::logic_error(
                    "VBucket::addBackfillItem: "
                    "StoredValue should not be null if status WasClean");
        }
        if (seqno) {
            *seqno = static_cast<uint64_t>(v->getBySeqno());
        }
        // we unlock ht lock here because we want to avoid potential lock
        // inversions arising from notifyNewSeqno() call
        hbl.getHTLock().unlock();
        notifyNewSeqno(*notifyCtx);
        break;
    }
    case MutationStatus::NeedBgFetch:
        hbl.getHTLock().unlock();
        bgFetch(key, cookie, engine, metaBgFetch);
        return ENGINE_EWOULDBLOCK;

    case MutationStatus::IsPendingSyncWrite:
        return ENGINE_SYNC_WRITE_IN_PROGRESS;
    }
    return ENGINE_SUCCESS;
}

void VBucket::deleteExpiredItem(const Item& it,
                                time_t startTime,
                                ExpireBy source) {
    // Pending items should not be subject to expiry
    if (it.getCommitted() == CommittedState::Pending) {
        std::stringstream ss;
        ss << it;
        throw std::invalid_argument(
                "VBucket::deleteExpiredItem: Cannot expire pending item:" +
                cb::UserDataView(ss.str()).getSanitizedValue());
    }

    const DocKey& key = it.getKey();

    // Must obtain collection handle and hold it to ensure any queued item is
    // interlocked with collection membership changes.
    auto cHandle = manifest->lock(key);
    if (!cHandle.valid()) {
        // The collection has now been dropped, no action required
        return;
    }

    // The item is correctly trimmed (by the caller). Fetch the one in the
    // hashtable and replace it if the CAS match (same item; no race).
    // If not found in the hashtable we should add it as a deleted item
    auto htRes = ht.findForWrite(key);
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    if (v) {
        if (v->getCas() != it.getCas()) {
            return;
        }

        if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            bool deleted = deleteStoredValue(hbl, *v);
            if (!deleted) {
                throw std::logic_error(
                        "VBucket::deleteExpiredItem: "
                        "Failed to delete seqno:" +
                        std::to_string(v->getBySeqno()) + " from bucket " +
                        std::to_string(hbl.getBucketNum()));
            }
        } else if (v->isExpired(startTime) && !v->isDeleted()) {
            VBNotifyCtx notifyCtx;
            auto result = ht.unlocked_updateStoredValue(hbl, *v, it);
            std::tie(std::ignore, std::ignore, notifyCtx) =
                    processExpiredItem(hbl, *result.storedValue, cHandle);
            // we unlock ht lock here because we want to avoid potential lock
            // inversions arising from notifyNewSeqno() call
            hbl.getHTLock().unlock();
            notifyNewSeqno(notifyCtx);
        }
    } else {
        if (eviction == FULL_EVICTION) {
            // Create a temp item and delete and push it
            // into the checkpoint queue, only if the bloomfilter
            // predicts that the item may exist on disk.
            if (maybeKeyExistsInFilter(key)) {
                TempAddStatus rv = addTempStoredValue(hbl, key);
                if (rv == TempAddStatus::NoMem) {
                    return;
                }
                v = ht.unlocked_find(key,
                                     hbl.getBucketNum(),
                                     WantsDeleted::Yes,
                                     TrackReference::No);
                v->setTempDeleted();
                v->setRevSeqno(it.getRevSeqno());
                auto result = ht.unlocked_updateStoredValue(hbl, *v, it);
                VBNotifyCtx notifyCtx;
                std::tie(std::ignore, std::ignore, notifyCtx) =
                        processExpiredItem(hbl, *result.storedValue, cHandle);
                // we unlock ht lock here because we want to avoid potential
                // lock inversions arising from notifyNewSeqno() call
                hbl.getHTLock().unlock();
                notifyNewSeqno(notifyCtx);
            }
        }
    }
    incExpirationStat(source);
}

ENGINE_ERROR_CODE VBucket::add(
        Item& itm,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto htRes = ht.findForWrite(itm.getKey());
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    bool maybeKeyExists = true;
    if ((v == nullptr || v->isTempInitialItem()) &&
        (eviction == FULL_EVICTION)) {
        // Check bloomfilter's prediction
        if (!maybeKeyExistsInFilter(itm.getKey())) {
            maybeKeyExists = false;
        }
    }

    PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
    VBQueueItemCtx queueItmCtx;
    queueItmCtx.preLinkDocumentContext = &preLinkDocumentContext;
    if (itm.getCommitted() == CommittedState::Pending) {
        queueItmCtx.durability =
                DurabilityItemCtx{itm.getDurabilityReqs(), cookie};
    }
    AddStatus status;
    boost::optional<VBNotifyCtx> notifyCtx;
    std::tie(status, notifyCtx) =
            processAdd(hbl, v, itm, maybeKeyExists, queueItmCtx, cHandle);

    switch (status) {
    case AddStatus::NoMem:
        return ENGINE_ENOMEM;
    case AddStatus::Exists:
        return ENGINE_NOT_STORED;
    case AddStatus::AddTmpAndBgFetch:
        return addTempItemAndBGFetch(hbl, itm.getKey(), cookie, engine, true);
    case AddStatus::BgFetch:
        hbl.getHTLock().unlock();
        bgFetch(itm.getKey(), cookie, engine, true);
        return ENGINE_EWOULDBLOCK;
    case AddStatus::Success:
    case AddStatus::UnDel:
        notifyNewSeqno(*notifyCtx);
        itm.setBySeqno(v->getBySeqno());
        itm.setCas(v->getCas());
        break;
    }

    // For pending SyncWrites we initially return EWOULDBLOCK; will notify
    // client when request is committed / aborted later.
    return (itm.getCommitted() == CommittedState::Pending) ? ENGINE_EWOULDBLOCK
                                                           : ENGINE_SUCCESS;
}

std::pair<MutationStatus, GetValue> VBucket::processGetAndUpdateTtl(
        HashTable::HashBucketLock& hbl,
        StoredValue* v,
        time_t exptime,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    if (v) {
        if (isLogicallyNonExistent(*v, cHandle)) {
            ht.cleanupIfTemporaryItem(hbl, *v);
            return {MutationStatus::NotFound, GetValue()};
        }

        if (!v->isResident()) {
            return {MutationStatus::NeedBgFetch, GetValue()};
        }

        if (v->isLocked(ep_current_time())) {
            return {MutationStatus::IsLocked,
                    GetValue(nullptr, ENGINE_KEY_EEXISTS, 0)};
        }

        const bool exptime_mutated = exptime != v->getExptime();
        auto bySeqNo = v->getBySeqno();
        if (exptime_mutated) {
            v->markDirty();
            v->setExptime(exptime);
            v->setRevSeqno(v->getRevSeqno() + 1);
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), getId()),
                    ENGINE_SUCCESS,
                    bySeqNo);

        if (exptime_mutated) {
            VBQueueItemCtx qItemCtx;
            VBNotifyCtx notifyCtx;
            std::tie(v, std::ignore, notifyCtx) =
                    updateStoredValue(hbl, *v, *rv.item, qItemCtx, true);
            rv.item->setCas(v->getCas());
            // we unlock ht lock here because we want to avoid potential lock
            // inversions arising from notifyNewSeqno() call
            hbl.getHTLock().unlock();
            notifyNewSeqno(notifyCtx);
        }

        return {MutationStatus::WasClean, std::move(rv)};
    } else {
        if (eviction == VALUE_ONLY) {
            return {MutationStatus::NotFound, GetValue()};
        } else {
            if (maybeKeyExistsInFilter(cHandle.getKey())) {
                return {MutationStatus::NeedBgFetch, GetValue()};
            } else {
                // As bloomfilter predicted that item surely doesn't exist
                // on disk, return ENOENT for getAndUpdateTtl().
                return {MutationStatus::NotFound, GetValue()};
            }
        }
    }
}

GetValue VBucket::getAndUpdateTtl(
        const void* cookie,
        EventuallyPersistentEngine& engine,
        time_t exptime,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto hbl = ht.getLockedBucket(cHandle.getKey());
    StoredValue* v = fetchValidValue(hbl,
                                     cHandle.getKey(),
                                     WantsDeleted::Yes,
                                     TrackReference::Yes,
                                     QueueExpired::Yes,
                                     cHandle);
    GetValue gv;
    MutationStatus status;
    std::tie(status, gv) = processGetAndUpdateTtl(hbl, v, exptime, cHandle);

    if (status == MutationStatus::NeedBgFetch) {
        if (v) {
            bgFetch(cHandle.getKey(), cookie, engine);
            return GetValue(nullptr, ENGINE_EWOULDBLOCK, v->getBySeqno());
        } else {
            ENGINE_ERROR_CODE ec = addTempItemAndBGFetch(
                    hbl, cHandle.getKey(), cookie, engine, false);
            return GetValue(NULL, ec, -1, true);
        }
    }

    return gv;
}

GetValue VBucket::getInternal(
        const void* cookie,
        EventuallyPersistentEngine& engine,
        get_options_t options,
        bool diskFlushAll,
        GetKeyOnly getKeyOnly,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    const TrackReference trackReference = (options & TRACK_REFERENCE)
                                                  ? TrackReference::Yes
                                                  : TrackReference::No;
    const bool metadataOnly = (options & ALLOW_META_ONLY);
    const bool getDeletedValue = (options & GET_DELETED_VALUE);
    const bool bgFetchRequired = (options & QUEUE_BG_FETCH);
    auto hbl = ht.getLockedBucket(cHandle.getKey());
    StoredValue* v = fetchValidValue(hbl,
                                     cHandle.getKey(),
                                     WantsDeleted::Yes,
                                     trackReference,
                                     QueueExpired::Yes,
                                     cHandle);
    if (v) {
        // 1 If SV is deleted and user didn't request deleted items
        // 2 (or) If collection says this key is gone.
        // then return ENOENT.
        if ((v->isDeleted() && !getDeletedValue) ||
            cHandle.isLogicallyDeleted(v->getBySeqno())) {
            return GetValue();
        }

        // If SV is a temp deleted item (i.e. marker added after a BgFetch to
        // note that the item has been deleted), *but* the user requested
        // full deleted items, then we need to fetch the complete deleted item
        // (including body) from disk.
        if (v->isTempDeletedItem() && getDeletedValue && !metadataOnly) {
            const auto queueBgFetch =
                    (bgFetchRequired) ? QueueBgFetch::Yes : QueueBgFetch::No;
            return getInternalNonResident(
                    cHandle.getKey(), cookie, engine, queueBgFetch, *v);
        }

        // If SV is otherwise a temp non-existent (i.e. a marker added after a
        // BgFetch to note that no such item exists) or temp deleted, then we
        // should cleanup the SV (if requested) before returning ENOENT (so we
        // don't keep temp items in HT).
        if (v->isTempDeletedItem() || v->isTempNonExistentItem()) {
            if (options & DELETE_TEMP) {
                deleteStoredValue(hbl, *v);
            }
            return GetValue();
        }

        // If the value is not resident (and it was requested), wait for it...
        if (!v->isResident() && !metadataOnly) {
            auto queueBgFetch = (bgFetchRequired) ?
                    QueueBgFetch::Yes :
                    QueueBgFetch::No;
            return getInternalNonResident(
                    cHandle.getKey(), cookie, engine, queueBgFetch, *v);
        }

        // Should we hide (return -1) for the items' CAS?
        const bool hideCas =
                (options & HIDE_LOCKED_CAS) && v->isLocked(ep_current_time());
        std::unique_ptr<Item> item;
        if (getKeyOnly == GetKeyOnly::Yes) {
            item = v->toItemKeyOnly(getId());
        } else {
            item = v->toItem(hideCas, getId());
        }

        if (options & TRACK_STATISTICS) {
            opsGet++;
        }

        return GetValue(std::move(item),
                        ENGINE_SUCCESS,
                        v->getBySeqno(),
                        !v->isResident(),
                        v->getNRUValue());
    } else {
        if (!getDeletedValue && (eviction == VALUE_ONLY || diskFlushAll)) {
            return GetValue();
        }

        if (maybeKeyExistsInFilter(cHandle.getKey())) {
            ENGINE_ERROR_CODE ec = ENGINE_EWOULDBLOCK;
            if (bgFetchRequired) { // Full eviction and need a bg fetch.
                ec = addTempItemAndBGFetch(
                        hbl, cHandle.getKey(), cookie, engine, metadataOnly);
            }
            return GetValue(NULL, ec, -1, true);
        } else {
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT, for getInternal().
            return GetValue();
        }
    }
}

ENGINE_ERROR_CODE VBucket::getMetaData(
        const void* cookie,
        EventuallyPersistentEngine& engine,
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        ItemMetaData& metadata,
        uint32_t& deleted,
        uint8_t& datatype) {
    deleted = 0;
    auto htRes = ht.findForWrite(cHandle.getKey());
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    if (v) {
        stats.numOpsGetMeta++;
        if (v->isTempInitialItem()) {
            // Need bg meta fetch.
            bgFetch(cHandle.getKey(), cookie, engine, true);
            return ENGINE_EWOULDBLOCK;
        } else if (v->isTempNonExistentItem()) {
            metadata.cas = v->getCas();
            return ENGINE_KEY_ENOENT;
        } else if (cHandle.isLogicallyDeleted(v->getBySeqno())) {
            return ENGINE_KEY_ENOENT;
        } else {
            if (v->isTempDeletedItem() || v->isDeleted() ||
                v->isExpired(ep_real_time())) {
                deleted |= GET_META_ITEM_DELETED_FLAG;
            }

            if (v->isLocked(ep_current_time())) {
                metadata.cas = static_cast<uint64_t>(-1);
            } else {
                metadata.cas = v->getCas();
            }
            metadata.flags = v->getFlags();
            metadata.exptime = v->getExptime();
            metadata.revSeqno = v->getRevSeqno();
            datatype = v->getDatatype();

            return ENGINE_SUCCESS;
        }
    } else {
        // The key wasn't found. However, this may be because it was previously
        // deleted or evicted with the full eviction strategy.
        // So, add a temporary item corresponding to the key to the hash table
        // and schedule a background fetch for its metadata from the persistent
        // store. The item's state will be updated after the fetch completes.
        //
        // Schedule this bgFetch only if the key is predicted to be may-be
        // existent on disk by the bloomfilter.

        if (maybeKeyExistsInFilter(cHandle.getKey())) {
            return addTempItemAndBGFetch(
                    hbl, cHandle.getKey(), cookie, engine, true);
        } else {
            stats.numOpsGetMeta++;
            return ENGINE_KEY_ENOENT;
        }
    }
}

ENGINE_ERROR_CODE VBucket::getKeyStats(
        const void* cookie,
        EventuallyPersistentEngine& engine,
        struct key_stats& kstats,
        WantsDeleted wantsDeleted,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto hbl = ht.getLockedBucket(cHandle.getKey());
    StoredValue* v = fetchValidValue(hbl,
                                     cHandle.getKey(),
                                     WantsDeleted::Yes,
                                     TrackReference::Yes,
                                     QueueExpired::Yes,
                                     cHandle);

    if (v) {
        if ((v->isDeleted() || cHandle.isLogicallyDeleted(v->getBySeqno())) &&
            wantsDeleted == WantsDeleted::No) {
            return ENGINE_KEY_ENOENT;
        }

        if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            deleteStoredValue(hbl, *v);
            return ENGINE_KEY_ENOENT;
        }
        if (eviction == FULL_EVICTION && v->isTempInitialItem()) {
            hbl.getHTLock().unlock();
            bgFetch(cHandle.getKey(), cookie, engine, true);
            return ENGINE_EWOULDBLOCK;
        }
        kstats.logically_deleted =
                v->isDeleted() || cHandle.isLogicallyDeleted(v->getBySeqno());
        kstats.dirty = v->isDirty();
        kstats.exptime = v->getExptime();
        kstats.flags = v->getFlags();
        kstats.cas = v->getCas();
        kstats.vb_state = getState();
        kstats.resident = v->isResident();

        return ENGINE_SUCCESS;
    } else {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else {
            if (maybeKeyExistsInFilter(cHandle.getKey())) {
                return addTempItemAndBGFetch(
                        hbl, cHandle.getKey(), cookie, engine, true);
            } else {
                // If bgFetch were false, or bloomfilter predicted that
                // item surely doesn't exist on disk, return ENOENT for
                // getKeyStats().
                return ENGINE_KEY_ENOENT;
            }
        }
    }
}

GetValue VBucket::getLocked(
        rel_time_t currentTime,
        uint32_t lockTimeout,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto hbl = ht.getLockedBucket(cHandle.getKey());
    StoredValue* v = fetchValidValue(hbl,
                                     cHandle.getKey(),
                                     WantsDeleted::Yes,
                                     TrackReference::Yes,
                                     QueueExpired::Yes,
                                     cHandle);

    if (v) {
        if (isLogicallyNonExistent(*v, cHandle)) {
            ht.cleanupIfTemporaryItem(hbl, *v);
            return GetValue(NULL, ENGINE_KEY_ENOENT);
        }

        // if v is locked return error
        if (v->isLocked(currentTime)) {
            return GetValue(NULL, ENGINE_LOCKED_TMPFAIL);
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (cookie) {
                bgFetch(cHandle.getKey(), cookie, engine);
            }
            return GetValue(NULL, ENGINE_EWOULDBLOCK, -1, true);
        }

        // acquire lock and increment cas value
        v->lock(currentTime + lockTimeout);

        auto it = v->toItem(false, getId());
        it->setCas(nextHLCCas());
        v->setCas(it->getCas());

        return GetValue(std::move(it));

    } else {
        // No value found in the hashtable.
        switch (eviction) {
        case VALUE_ONLY:
            return GetValue(NULL, ENGINE_KEY_ENOENT);

        case FULL_EVICTION:
            if (maybeKeyExistsInFilter(cHandle.getKey())) {
                ENGINE_ERROR_CODE ec = addTempItemAndBGFetch(
                        hbl, cHandle.getKey(), cookie, engine, false);
                return GetValue(NULL, ec, -1, true);
            } else {
                // As bloomfilter predicted that item surely doesn't exist
                // on disk, return ENOENT for getLocked().
                return GetValue(NULL, ENGINE_KEY_ENOENT);
            }
        }
        return GetValue(); // just to prevent compiler warning
    }
}

void VBucket::deletedOnDiskCbk(const Item& queuedItem, bool deleted) {
    auto handle = manifest->lock(queuedItem.getKey());
    auto hbl = ht.getLockedBucket(queuedItem.getKey());
    StoredValue* v = nullptr;

    v = fetchValidValue(hbl,
                        queuedItem.getKey(),
                        WantsDeleted::Yes,
                        TrackReference::No,
                        handle.valid() ? QueueExpired::Yes : QueueExpired::No,
                        handle);

    // Delete the item in the hash table iff:
    //  1. Item is existent in hashtable, and deleted flag is true
    //  2. rev seqno of queued item matches rev seqno of hash table item
    if (v && v->isDeleted() && (queuedItem.getRevSeqno() == v->getRevSeqno())) {
        bool isDeleted = deleteStoredValue(hbl, *v);
        if (!isDeleted) {
            throw std::logic_error(
                    "deletedOnDiskCbk:callback: "
                    "Failed to delete key with seqno:" +
                    std::to_string(v->getBySeqno()) + "' from bucket " +
                    std::to_string(hbl.getBucketNum()));
        }

        /**
         * Deleted items are to be added to the bloomfilter,
         * in either eviction policy.
         */
        addToFilter(queuedItem.getKey());
    }

    if (deleted) {
        ++stats.totalPersisted;
        ++opsDelete;

        /**
         * MB-30137: Decrement the total number of on-disk items. This needs to be
         * done to ensure that the item count is accurate in the case of full
         * eviction
         */
        if (v) {
            decrNumTotalItems();
        }
    }
    doStatsForFlushing(queuedItem, queuedItem.size());
    --stats.diskQueueSize;
    decrMetaDataDisk(queuedItem);
}

bool VBucket::deleteKey(const DocKey& key) {
    auto htRes = ht.findForWrite(key);
    if (!htRes.storedValue) {
        return false;
    }
    return deleteStoredValue(htRes.lock, *htRes.storedValue);
}

void VBucket::postProcessRollback(const RollbackResult& rollbackResult,
                                  uint64_t prevHighSeqno) {
    failovers->pruneEntries(rollbackResult.highSeqno);
    checkpointManager->clear(*this, rollbackResult.highSeqno);
    setPersistedSnapshot(rollbackResult.snapStartSeqno,
                         rollbackResult.snapEndSeqno);
    incrRollbackItemCount(prevHighSeqno - rollbackResult.highSeqno);
    checkpointManager->setOpenCheckpointId(1);
    setReceivingInitialDiskSnapshot(false);
}

void VBucket::collectionsRolledBack(KVStore& kvstore) {
    manifest = std::make_unique<Collections::VB::Manifest>(
            kvstore.getCollectionsManifest(getId()));
    auto kvstoreContext = kvstore.makeFileHandle(getId());
    auto wh = manifest->wlock();
    // For each collection in the VB, reload the stats to the point before
    // the rollback seqno
    for (auto& collection : wh) {
        collection.second.setDiskCount(kvstore.getCollectionStats(
                *kvstoreContext, collection.first).itemCount);
        collection.second.resetPersistedHighSeqno(kvstore.getCollectionStats(
                *kvstoreContext, collection.first).highSeqno);
    }
}

void VBucket::dump() const {
    std::cerr << "VBucket[" << this << "] with state: " << toString(getState())
              << " numItems:" << getNumItems()
              << " numNonResident:" << getNumNonResidentItems()
              << " ht: " << std::endl << "  " << ht << std::endl
              << "]" << std::endl;
}

void VBucket::setMutationMemoryThreshold(double memThreshold) {
    if (memThreshold > 0.0 && memThreshold <= 1.0) {
        mutationMemThreshold = memThreshold;
    }
}

bool VBucket::hasMemoryForStoredValue(
        EPStats& st,
        const Item& item,
        UseActiveVBMemThreshold useActiveVBMemThreshold) {
    double newSize = static_cast<double>(estimateNewMemoryUsage(st, item));
    double maxSize = static_cast<double>(st.getMaxDataSize());
    if (useActiveVBMemThreshold == UseActiveVBMemThreshold::Yes ||
        getState() == vbucket_state_active) {
        return newSize <= (maxSize * mutationMemThreshold);
    } else {
        return newSize <= (maxSize * st.replicationThrottleThreshold);
    }
}

void VBucket::_addStats(bool details, ADD_STAT add_stat, const void* c) {
    addStat(NULL, toString(state), add_stat, c);
    if (details) {
        size_t numItems = getNumItems();
        size_t tempItems = getNumTempItems();
        addStat("num_items", numItems, add_stat, c);
        addStat("num_temp_items", tempItems, add_stat, c);
        addStat("num_non_resident", getNumNonResidentItems(), add_stat, c);
        addStat("ht_memory", ht.memorySize(), add_stat, c);
        addStat("ht_item_memory", ht.getItemMemory(), add_stat, c);
        addStat("ht_item_memory_uncompressed",
                ht.getUncompressedItemMemory(),
                add_stat,
                c);
        addStat("ht_cache_size", ht.getCacheSize(), add_stat, c);
        addStat("ht_size", ht.getSize(), add_stat, c);
        addStat("num_ejects", ht.getNumEjects(), add_stat, c);
        addStat("ops_create", opsCreate.load(), add_stat, c);
	addStat("ops_delete", opsDelete.load(), add_stat, c);
        addStat("ops_get", opsGet.load(), add_stat, c);
        addStat("ops_reject", opsReject.load(), add_stat, c);
        addStat("ops_update", opsUpdate.load(), add_stat, c);
        addStat("queue_size", dirtyQueueSize.load(), add_stat, c);
        addStat("backfill_queue_size", getBackfillSize(), add_stat, c);
        addStat("queue_memory", dirtyQueueMem.load(), add_stat, c);
        addStat("queue_fill", dirtyQueueFill.load(), add_stat, c);
        addStat("queue_drain", dirtyQueueDrain.load(), add_stat, c);
        addStat("queue_age", getQueueAge(), add_stat, c);
        addStat("pending_writes", dirtyQueuePendingWrites.load(), add_stat, c);

        addStat("high_seqno", getHighSeqno(), add_stat, c);
        addStat("uuid", failovers->getLatestUUID(), add_stat, c);
        addStat("purge_seqno", getPurgeSeqno(), add_stat, c);
        addStat("bloom_filter", getFilterStatusString().data(),
                add_stat, c);
        addStat("bloom_filter_size", getFilterSize(), add_stat, c);
        addStat("bloom_filter_key_count", getNumOfKeysInFilter(), add_stat, c);
        addStat("rollback_item_count", getRollbackItemCount(), add_stat, c);
        addStat("hp_vb_req_size", getHighPriorityChkSize(), add_stat, c);
        addStat("might_contain_xattrs", mightContainXattrs(), add_stat, c);
        addStat("max_deleted_revid", ht.getMaxDeletedRevSeqno(), add_stat, c);
        hlc.addStats(statPrefix, add_stat, c);
    }
}

void VBucket::decrDirtyQueueMem(size_t decrementBy)
{
    size_t oldVal, newVal;
    do {
        oldVal = dirtyQueueMem.load(std::memory_order_relaxed);
        if (oldVal < decrementBy) {
            newVal = 0;
        } else {
            newVal = oldVal - decrementBy;
        }
    } while (!dirtyQueueMem.compare_exchange_strong(oldVal, newVal));
}

void VBucket::decrDirtyQueueAge(uint32_t decrementBy)
{
    uint64_t oldVal, newVal;
    do {
        oldVal = dirtyQueueAge.load(std::memory_order_relaxed);
        if (oldVal < decrementBy) {
            newVal = 0;
        } else {
            newVal = oldVal - decrementBy;
        }
    } while (!dirtyQueueAge.compare_exchange_strong(oldVal, newVal));
}

void VBucket::decrDirtyQueuePendingWrites(size_t decrementBy)
{
    size_t oldVal, newVal;
    do {
        oldVal = dirtyQueuePendingWrites.load(std::memory_order_relaxed);
        if (oldVal < decrementBy) {
            newVal = 0;
        } else {
            newVal = oldVal - decrementBy;
        }
    } while (!dirtyQueuePendingWrites.compare_exchange_strong(oldVal, newVal));
}

std::pair<MutationStatus, boost::optional<VBNotifyCtx>> VBucket::processSet(
        const HashTable::HashBucketLock& hbl,
        StoredValue*& v,
        Item& itm,
        uint64_t cas,
        bool allowExisting,
        bool hasMetaData,
        const VBQueueItemCtx& queueItmCtx,
        cb::StoreIfStatus storeIfStatus,
        bool maybeKeyExists) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::processSet: htLock not held for " +
                getId().to_string());
    }

    if (!hasMemoryForStoredValue(stats, itm)) {
        return {MutationStatus::NoMem, {}};
    }

    if (v == nullptr && itm.isDeleted() && cas &&
        !areDeletedItemsAlwaysResident()) {
        // Request to perform a CAS operation on a deleted body which may
        // not be resident. Need a bg_fetch to be able to perform this request.
        return {MutationStatus::NeedBgFetch, VBNotifyCtx()};
    }

    // bgFetch only in FE, only if the bloom-filter thinks the key may exist.
    // But only for cas operations or if a store_if is requiring the item_info.
    if (eviction == FULL_EVICTION && maybeKeyExists &&
        (cas || storeIfStatus == cb::StoreIfStatus::GetItemInfo)) {
        if (!v || v->isTempInitialItem()) {
            return {MutationStatus::NeedBgFetch, {}};
        }
    }

    /*
     * prior to checking for the lock, we should check if this object
     * has expired. If so, then check if CAS value has been provided
     * for this set op. In this case the operation should be denied since
     * a cas operation for a key that doesn't exist is not a very cool
     * thing to do. See MB 3252
     */
    if (v && v->isExpired(ep_real_time()) && !hasMetaData && !itm.isDeleted()) {
        if (v->isLocked(ep_current_time())) {
            v->unlock();
        }
        if (cas) {
            /* item has expired and cas value provided. Deny ! */
            return {MutationStatus::NotFound, {}};
        }
    }

    if (v) {
        if (!allowExisting && !v->isTempItem() && !v->isDeleted()) {
            return {MutationStatus::InvalidCas, {}};
        }
        if (v->isLocked(ep_current_time())) {
            /*
             * item is locked, deny if there is cas value mismatch
             * or no cas value is provided by the user
             */
            if (cas != v->getCas()) {
                return {MutationStatus::IsLocked, {}};
            }
            /* allow operation*/
            v->unlock();
        } else if (cas && cas != v->getCas()) {
            if (v->isTempNonExistentItem()) {
                // This is a temporary item which marks a key as non-existent;
                // therefore specifying a non-matching CAS should be exposed
                // as item not existing.
                return {MutationStatus::NotFound, {}};
            }
            if ((v->isTempDeletedItem() || v->isDeleted()) && !itm.isDeleted()) {
                // Existing item is deleted, and we are not replacing it with
                // a (different) deleted value - return not existing.
                return {MutationStatus::NotFound, {}};
            }
            // None of the above special cases; the existing item cannot be
            // modified with the specified CAS.
            return {MutationStatus::InvalidCas, {}};
        }
        if (!hasMetaData) {
            itm.setRevSeqno(v->getRevSeqno() + 1);
            /* MB-23530: We must ensure that a replace operation (i.e.
             * set with a CAS) /fails/ if the old document is deleted; it
             * logically "doesn't exist". However, if the new value is deleted
             * this op is a /delete/ with a CAS and we must permit a
             * deleted -> deleted transition for Deleted Bodies.
             */
            if (cas && (v->isDeleted() || v->isTempDeletedItem()) &&
                !itm.isDeleted()) {
                return {MutationStatus::NotFound, {}};
            }
        }

        MutationStatus status;
        VBNotifyCtx notifyCtx;
        std::tie(v, status, notifyCtx) =
                updateStoredValue(hbl, *v, itm, queueItmCtx);
        return {status, notifyCtx};
    } else if (cas != 0) {
        return {MutationStatus::NotFound, {}};
    } else {
        VBNotifyCtx notifyCtx;
        auto genRevSeqno = hasMetaData ? GenerateRevSeqno::No :
                           GenerateRevSeqno::Yes;
        std::tie(v, notifyCtx) =
                addNewStoredValue(hbl, itm, queueItmCtx, genRevSeqno);
        itm.setRevSeqno(v->getRevSeqno());
        return {MutationStatus::WasClean, notifyCtx};
    }
}

std::pair<AddStatus, boost::optional<VBNotifyCtx>> VBucket::processAdd(
        const HashTable::HashBucketLock& hbl,
        StoredValue*& v,
        Item& itm,
        bool maybeKeyExists,
        const VBQueueItemCtx& queueItmCtx,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::processAdd: htLock not held for " +
                getId().to_string());
    }
    if (v && !v->isDeleted() && !v->isExpired(ep_real_time()) &&
        !v->isTempItem() && !cHandle.isLogicallyDeleted(v->getBySeqno())) {
        return {AddStatus::Exists, {}};
    }
    if (!hasMemoryForStoredValue(stats, itm)) {
        return {AddStatus::NoMem, {}};
    }

    std::pair<AddStatus, VBNotifyCtx> rv = {AddStatus::Success, {}};

    if (v) {
        if (v->isTempInitialItem() && eviction == FULL_EVICTION &&
            maybeKeyExists) {
            // Need to figure out if an item exists on disk
            return {AddStatus::BgFetch, {}};
        }

        rv.first = (v->isDeleted() || v->isExpired(ep_real_time()))
                           ? AddStatus::UnDel
                           : AddStatus::Success;

        if (v->isTempDeletedItem()) {
            itm.setRevSeqno(v->getRevSeqno() + 1);
        } else {
            itm.setRevSeqno(ht.getMaxDeletedRevSeqno() + 1);
        }

        if (!v->isTempItem()) {
            itm.setRevSeqno(v->getRevSeqno() + 1);
        }

        std::tie(v, std::ignore, rv.second) =
                updateStoredValue(hbl, *v, itm, queueItmCtx);
    } else {
        if (itm.getBySeqno() != StoredValue::state_temp_init) {
            if (eviction == FULL_EVICTION && maybeKeyExists) {
                return {AddStatus::AddTmpAndBgFetch, VBNotifyCtx()};
            }
        }

        if (itm.getBySeqno() == StoredValue::state_temp_init) {
            /* A 'temp initial item' is just added to the hash table. It is
             not put on checkpoint manager or sequence list */
            v = ht.unlocked_addNewStoredValue(hbl, itm);
            updateRevSeqNoOfNewStoredValue(*v);
        } else {
            std::tie(v, rv.second) = addNewStoredValue(
                    hbl, itm, queueItmCtx, GenerateRevSeqno::Yes);
        }

        itm.setRevSeqno(v->getRevSeqno());

        if (v->isTempItem()) {
            rv.first = AddStatus::BgFetch;
        }
    }

    if (v->isTempItem()) {
        v->setNRUValue(MAX_NRU_VALUE);
    }
    return rv;
}

std::tuple<MutationStatus, StoredValue*, boost::optional<VBNotifyCtx>>
VBucket::processSoftDelete(const HashTable::HashBucketLock& hbl,
                           StoredValue& v,
                           uint64_t cas,
                           const ItemMetaData& metadata,
                           const VBQueueItemCtx& queueItmCtx,
                           bool use_meta,
                           uint64_t bySeqno,
                           DeleteSource deleteSource) {
    boost::optional<VBNotifyCtx> empty;
    if (v.isTempInitialItem() && eviction == FULL_EVICTION) {
        return std::make_tuple(MutationStatus::NeedBgFetch, &v, empty);
    }

    if (v.isLocked(ep_current_time())) {
        if (cas != v.getCas()) {
            return std::make_tuple(MutationStatus::IsLocked, &v, empty);
        }
        v.unlock();
    }

    if (cas != 0 && cas != v.getCas()) {
        return std::make_tuple(MutationStatus::InvalidCas, &v, empty);
    }

    /* allow operation */
    v.unlock();

    MutationStatus rv =
            v.isDirty() ? MutationStatus::WasDirty : MutationStatus::WasClean;

    if (use_meta) {
        v.setCas(metadata.cas);
        v.setFlags(metadata.flags);
        v.setExptime(metadata.exptime);
    }

    v.setRevSeqno(metadata.revSeqno);
    VBNotifyCtx notifyCtx;
    StoredValue* newSv;
    std::tie(newSv, notifyCtx) =
            softDeleteStoredValue(hbl,
                                  v,
                                  /*onlyMarkDeleted*/ false,
                                  queueItmCtx,
                                  bySeqno,
                                  deleteSource);
    ht.updateMaxDeletedRevSeqno(metadata.revSeqno);
    return std::make_tuple(rv, newSv, notifyCtx);
}

std::tuple<MutationStatus, StoredValue*, VBNotifyCtx>
VBucket::processExpiredItem(
        const HashTable::HashBucketLock& hbl,
        StoredValue& v,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::processExpiredItem: htLock not held for " +
                getId().to_string());
    }

    if (v.isTempInitialItem() && eviction == FULL_EVICTION) {
        return std::make_tuple(MutationStatus::NeedBgFetch,
                               &v,
                               queueDirty(v,
                                          GenerateBySeqno::Yes,
                                          GenerateCas::Yes,
                                          /*isBackfillItem*/ false,
                                          {},
                                          /*preLinkDocumentContext*/ nullptr));
    }

    /* If the datatype is XATTR, mark the item as deleted
     * but don't delete the value as system xattrs can
     * still be queried by mobile clients even after
     * deletion.
     * TODO: The current implementation is inefficient
     * but functionally correct and for performance reasons
     * only the system xattrs need to be stored.
     */
    value_t value = v.getValue();
    bool onlyMarkDeleted = value && mcbp::datatype::is_xattr(v.getDatatype());
    v.setRevSeqno(v.getRevSeqno() + 1);
    VBNotifyCtx notifyCtx;
    StoredValue* newSv;
    std::tie(newSv, notifyCtx) =
            softDeleteStoredValue(hbl,
                                  v,
                                  onlyMarkDeleted,
                                  VBQueueItemCtx{},
                                  v.getBySeqno(),
                                  DeleteSource::TTL);
    ht.updateMaxDeletedRevSeqno(newSv->getRevSeqno() + 1);
    return std::make_tuple(MutationStatus::NotFound, newSv, notifyCtx);
}

bool VBucket::deleteStoredValue(const HashTable::HashBucketLock& hbl,
                                StoredValue& v) {
    if (!v.isDeleted() && v.isLocked(ep_current_time())) {
        return false;
    }

    /* StoredValue deleted here. If any other in-memory data structures are
       using the StoredValue intrusively then they must have handled the delete
       by this point */
    ht.unlocked_del(hbl, v.getKey());
    return true;
}

TempAddStatus VBucket::addTempStoredValue(const HashTable::HashBucketLock& hbl,
                                          const DocKey& key) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::addTempStoredValue: htLock not held for " +
                getId().to_string());
    }

    Item itm(key,
             /*flags*/ 0,
             /*exp*/ 0,
             /*data*/ NULL,
             /*size*/ 0,
             PROTOCOL_BINARY_RAW_BYTES,
             0,
             StoredValue::state_temp_init);

    if (!hasMemoryForStoredValue(stats, itm)) {
        return TempAddStatus::NoMem;
    }

    /* A 'temp initial item' is just added to the hash table. It is
       not put on checkpoint manager or sequence list */
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

    updateRevSeqNoOfNewStoredValue(*v);
    itm.setRevSeqno(v->getRevSeqno());
    v->setNRUValue(MAX_NRU_VALUE);

    return TempAddStatus::BgFetch;
}

void VBucket::notifyNewSeqno(const VBNotifyCtx& notifyCtx) {
    if (newSeqnoCb) {
        newSeqnoCb->callback(getId(), notifyCtx);
    }
}

VBNotifyCtx VBucket::queueDirty(StoredValue& v,
                                const VBQueueItemCtx& queueItmCtx) {
    if (queueItmCtx.trackCasDrift == TrackCasDrift::Yes) {
        setMaxCasAndTrackDrift(v.getCas());
    }
    return queueDirty(v,
                      queueItmCtx.genBySeqno,
                      queueItmCtx.genCas,
                      queueItmCtx.isBackfillItem,
                      queueItmCtx.durability,
                      queueItmCtx.preLinkDocumentContext);
}

void VBucket::updateRevSeqNoOfNewStoredValue(StoredValue& v) {
    /**
     * Possibly, this item is being recreated. Conservatively assign it
     * a seqno that is greater than the greatest seqno of all deleted
     * items seen so far.
     */
    uint64_t seqno = ht.getMaxDeletedRevSeqno();
    if (!v.isTempItem()) {
        ++seqno;
    }
    v.setRevSeqno(seqno);
}

void VBucket::addHighPriorityVBEntry(uint64_t seqnoOrChkId,
                                     const void* cookie,
                                     HighPriorityVBNotify reqType) {
    std::unique_lock<std::mutex> lh(hpVBReqsMutex);
    hpVBReqs.push_back(HighPriorityVBEntry(cookie, seqnoOrChkId, reqType));
    numHpVBReqs.store(hpVBReqs.size());

    EP_LOG_INFO(
            "Added high priority async request {} for {}, Check for:{}, "
            "Persisted upto:{}, cookie:{}",
            to_string(reqType),
            getId(),
            seqnoOrChkId,
            getPersistenceSeqno(),
            cookie);
}

std::map<const void*, ENGINE_ERROR_CODE> VBucket::getHighPriorityNotifications(
        EventuallyPersistentEngine& engine,
        uint64_t idNum,
        HighPriorityVBNotify notifyType) {
    std::unique_lock<std::mutex> lh(hpVBReqsMutex);
    std::map<const void*, ENGINE_ERROR_CODE> toNotify;

    auto entry = hpVBReqs.begin();

    while (entry != hpVBReqs.end()) {
        if (notifyType != entry->reqType) {
            ++entry;
            continue;
        }

        std::string logStr(to_string(notifyType));

        auto wall_time = std::chrono::steady_clock::now() - entry->start;
        auto spent =
                std::chrono::duration_cast<std::chrono::seconds>(wall_time);
        if (entry->id <= idNum) {
            toNotify[entry->cookie] = ENGINE_SUCCESS;
            stats.chkPersistenceHisto.add(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            wall_time));
            adjustCheckpointFlushTimeout(spent);
            EP_LOG_INFO(
                    "Notified the completion of {} for {} Check for: {}, "
                    "Persisted upto: {}, cookie {}",
                    logStr,
                    getId(),
                    entry->id,
                    idNum,
                    entry->cookie);
            entry = hpVBReqs.erase(entry);
        } else if (spent > getCheckpointFlushTimeout()) {
            adjustCheckpointFlushTimeout(spent);
            engine.storeEngineSpecific(entry->cookie, NULL);
            toNotify[entry->cookie] = ENGINE_TMPFAIL;
            EP_LOG_WARN(
                    "Notified the timeout on {} for {} Check for: {}, "
                    "Persisted upto: {}, cookie {}",
                    logStr,
                    getId(),
                    entry->id,
                    idNum,
                    entry->cookie);
            entry = hpVBReqs.erase(entry);
        } else {
            ++entry;
        }
    }
    numHpVBReqs.store(hpVBReqs.size());
    return toNotify;
}

std::map<const void*, ENGINE_ERROR_CODE> VBucket::tmpFailAndGetAllHpNotifies(
        EventuallyPersistentEngine& engine) {
    std::map<const void*, ENGINE_ERROR_CODE> toNotify;

    LockHolder lh(hpVBReqsMutex);

    for (auto& entry : hpVBReqs) {
        toNotify[entry.cookie] = ENGINE_TMPFAIL;
        engine.storeEngineSpecific(entry.cookie, NULL);
    }
    hpVBReqs.clear();

    return toNotify;
}

void VBucket::adjustCheckpointFlushTimeout(std::chrono::seconds wall_time) {
    auto middle = (MIN_CHK_FLUSH_TIMEOUT + MAX_CHK_FLUSH_TIMEOUT) / 2;

    if (wall_time <= MIN_CHK_FLUSH_TIMEOUT) {
        chkFlushTimeout = MIN_CHK_FLUSH_TIMEOUT;
    } else if (wall_time <= middle) {
        chkFlushTimeout = middle;
    } else {
        chkFlushTimeout = MAX_CHK_FLUSH_TIMEOUT;
    }
}

std::chrono::seconds VBucket::getCheckpointFlushTimeout() {
    return std::chrono::duration_cast<std::chrono::seconds>(
            chkFlushTimeout.load());
}

std::unique_ptr<Item> VBucket::pruneXattrDocument(
        StoredValue& v, const ItemMetaData& itemMeta) {
    // Need to take a copy of the value, prune it, and add it back

    // Create work-space document
    std::vector<char> workspace(
            v.getValue()->getData(),
            v.getValue()->getData() + v.getValue()->valueSize());

    // Now attach to the XATTRs in the document
    cb::xattr::Blob xattr({workspace.data(), workspace.size()},
                          mcbp::datatype::is_snappy(v.getDatatype()));
    xattr.prune_user_keys();

    auto prunedXattrs = xattr.finalize();

    if (prunedXattrs.size()) {
        // Something remains - Create a Blob and copy-in just the XATTRs
        auto newValue =
                Blob::New(reinterpret_cast<const char*>(prunedXattrs.data()),
                          prunedXattrs.size());
        auto rv = v.toItem(false, getId());
        rv->setCas(itemMeta.cas);
        rv->setFlags(itemMeta.flags);
        rv->setExpTime(itemMeta.exptime);
        rv->setRevSeqno(itemMeta.revSeqno);
        rv->replaceValue(newValue);
        rv->setDataType(PROTOCOL_BINARY_DATATYPE_XATTR);
        return rv;
    } else {
        return {};
    }
}

bool VBucket::isLogicallyNonExistent(
        const StoredValue& v,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    return v.isDeleted() || v.isTempDeletedItem() ||
           v.isTempNonExistentItem() ||
           cHandle.isLogicallyDeleted(v.getBySeqno());
}

ENGINE_ERROR_CODE VBucket::seqnoAcknowledged(const std::string& replicaId,
                                             uint64_t inMemorySeqno,
                                             uint64_t onDiskSeqno) {
    return durabilityMonitor->seqnoAckReceived(replicaId, inMemorySeqno);
}

void VBucket::DeferredDeleter::operator()(VBucket* vb) const {
    // If the vbucket is marked as deleting then we must schedule task to
    // perform the resource destruction (memory/disk).
    if (vb->isDeletionDeferred()) {
        vb->scheduleDeferredDeletion(engine);
        return;
    }
    delete vb;
}

void VBucket::setFreqSaturatedCallback(std::function<void()> callbackFunction) {
    ht.setFreqSaturatedCallback(callbackFunction);
}
