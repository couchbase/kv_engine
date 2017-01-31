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

#include <functional>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "atomic.h"
#include "bgfetcher.h"
#include "ep_engine.h"

#define STATWRITER_NAMESPACE vbucket
#include "statwriter.h"
#undef STATWRITER_NAMESPACE

#include "flusher.h"
#include "vbucket.h"

VBucketFilter VBucketFilter::filter_diff(const VBucketFilter &other) const {
    std::vector<uint16_t> tmp(acceptable.size() + other.size());
    std::vector<uint16_t>::iterator end;
    end = std::set_symmetric_difference(acceptable.begin(),
                                        acceptable.end(),
                                        other.acceptable.begin(),
                                        other.acceptable.end(),
                                        tmp.begin());
    return VBucketFilter(std::vector<uint16_t>(tmp.begin(), end));
}

VBucketFilter VBucketFilter::filter_intersection(const VBucketFilter &other)
                                                                        const {
    std::vector<uint16_t> tmp(acceptable.size() + other.size());
    std::vector<uint16_t>::iterator end;

    end = std::set_intersection(acceptable.begin(), acceptable.end(),
                                other.acceptable.begin(),
                                other.acceptable.end(),
                                tmp.begin());
    return VBucketFilter(std::vector<uint16_t>(tmp.begin(), end));
}

static bool isRange(std::set<uint16_t>::const_iterator it,
                    const std::set<uint16_t>::const_iterator &end,
                    size_t &length)
{
    length = 0;
    for (uint16_t val = *it;
         it != end && (val + length) == *it;
         ++it, ++length) {
        // empty
    }

    --length;

    return length > 1;
}

std::ostream& operator <<(std::ostream &out, const VBucketFilter &filter)
{
    std::set<uint16_t>::const_iterator it;

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
                std::set<uint16_t>::iterator last = it;
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

std::atomic<size_t> VBucket::chkFlushTimeout(MIN_CHK_FLUSH_TIMEOUT);

const vbucket_state_t VBucket::ACTIVE =
                     static_cast<vbucket_state_t>(htonl(vbucket_state_active));
const vbucket_state_t VBucket::REPLICA =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_replica));
const vbucket_state_t VBucket::PENDING =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_pending));
const vbucket_state_t VBucket::DEAD =
                    static_cast<vbucket_state_t>(htonl(vbucket_state_dead));

VBucket::VBucket(id_type i,
                 vbucket_state_t newState,
                 EPStats& st,
                 CheckpointConfig& chkConfig,
                 KVShard* kvshard,
                 int64_t lastSeqno,
                 uint64_t lastSnapStart,
                 uint64_t lastSnapEnd,
                 std::unique_ptr<FailoverTable> table,
                 std::shared_ptr<Callback<id_type> > flusherCb,
                 NewSeqnoCallback newSeqnoCb,
                 Configuration& config,
                 item_eviction_policy_t evictionPolicy,
                 vbucket_state_t initState,
                 uint64_t purgeSeqno,
                 uint64_t maxCas)
    : ht(st),
      checkpointManager(st,
                        i,
                        chkConfig,
                        lastSeqno,
                        lastSnapStart,
                        lastSnapEnd,
                        flusherCb),
      failovers(std::move(table)),
      opsCreate(0),
      opsUpdate(0),
      opsDelete(0),
      opsReject(0),
      dirtyQueueSize(0),
      dirtyQueueMem(0),
      dirtyQueueFill(0),
      dirtyQueueDrain(0),
      dirtyQueueAge(0),
      dirtyQueuePendingWrites(0),
      metaDataDisk(0),
      numExpiredItems(0),
      id(i),
      state(newState),
      initialState(initState),
      stats(st),
      purge_seqno(purgeSeqno),
      takeover_backed_up(false),
      persisted_snapshot_start(lastSnapStart),
      persisted_snapshot_end(lastSnapEnd),
      numHpChks(0),
      shard(kvshard),
      rollbackItemCount(0),
      hlc(maxCas,
          std::chrono::microseconds(config.getHlcDriftAheadThresholdUs()),
          std::chrono::microseconds(config.getHlcDriftBehindThresholdUs())),
      statPrefix("vb_" + std::to_string(i)),
      persistenceCheckpointId(0),
      bucketCreation(false),
      bucketDeletion(false),
      persistenceSeqno(0),
      newSeqnoCb(std::move(newSeqnoCb)),
      eviction(evictionPolicy),
      multiBGFetchEnabled(kvshard
                                  ? kvshard->getROUnderlying()
                                            ->getStorageProperties()
                                            .hasEfficientGet()
                                  : false) {
    if (config.getConflictResolutionType().compare("lww") == 0) {
        conflictResolver.reset(new LastWriteWinsResolution());
    } else {
        conflictResolver.reset(new RevisionSeqnoResolution());
    }

    backfill.isBackfillPhase = false;
    pendingOpsStart = 0;
    stats.memOverhead->fetch_add(sizeof(VBucket)
                                + ht.memorySize() + sizeof(CheckpointManager));
    LOG(EXTENSION_LOG_NOTICE,
        "VBucket: created vbucket:%" PRIu16 " with state:%s "
                "initialState:%s "
                "lastSeqno:%" PRIu64 " "
                "lastSnapshot:{%" PRIu64 ",%" PRIu64 "} "
                "persisted_snapshot:{%" PRIu64 ",%" PRIu64 "} "
                "max_cas:%" PRIu64,
        id, VBucket::toString(state), VBucket::toString(initialState),
        lastSeqno, lastSnapStart, lastSnapEnd,
        persisted_snapshot_start, persisted_snapshot_end,
        getMaxCas());
}

VBucket::~VBucket() {
    if (!pendingOps.empty() || !pendingBGFetches.empty()) {
        LOG(EXTENSION_LOG_WARNING,
            "Have %ld pending ops and %ld pending reads "
            "while destroying vbucket\n",
            pendingOps.size(), pendingBGFetches.size());
    }

    stats.decrDiskQueueSize(dirtyQueueSize.load());

    // Clear out the bloomfilter(s)
    clearFilter();

    stats.memOverhead->fetch_sub(sizeof(VBucket) + ht.memorySize() +
                                sizeof(CheckpointManager));

    LOG(EXTENSION_LOG_INFO, "Destroying vbucket %d\n", id);
}

void VBucket::fireAllOps(EventuallyPersistentEngine &engine,
                         ENGINE_ERROR_CODE code) {
    std::unique_lock<std::mutex> lh(pendingOpLock);

    if (pendingOpsStart > 0) {
        hrtime_t now = gethrtime();
        if (now > pendingOpsStart) {
            hrtime_t d = (now - pendingOpsStart) / 1000;
            stats.pendingOpsHisto.add(d);
            atomic_setIfBigger(stats.pendingOpsMaxDuration, d);
        }
    } else {
        return;
    }

    pendingOpsStart = 0;
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

    LOG(EXTENSION_LOG_INFO,
        "Fired pendings ops for vbucket %" PRIu16 " in state %s\n",
        id, VBucket::toString(state));
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

void VBucket::setState(vbucket_state_t to) {
    vbucket_state_t oldstate;
    {
        WriterLockHolder wlh(stateLock);
        oldstate = state;

        if (to == vbucket_state_active &&
            checkpointManager.getOpenCheckpointId() < 2) {
            checkpointManager.setOpenCheckpointId(2);
        }

        LOG(EXTENSION_LOG_NOTICE,
            "VBucket::setState: transitioning vbucket:%" PRIu16 " from:%s to:%s",
            id, VBucket::toString(oldstate), VBucket::toString(to));

        state = to;
    }
}

vbucket_state VBucket::getVBucketState() const {
     auto persisted_range = getPersistedSnapshot();

     return vbucket_state{getState(),
                          getPersistenceCheckpointId(), 0, getHighSeqno(),
                          getPurgeSeqno(),
                          persisted_range.start, persisted_range.end,
                          getMaxCas(), failovers->toJSON()};
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
    decrDirtyQueueSize(1);
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
    opsUpdate.store(0);
    opsDelete.store(0);
    opsReject.store(0);

    stats.decrDiskQueueSize(dirtyQueueSize.load());
    dirtyQueueSize.store(0);
    dirtyQueueMem.store(0);
    dirtyQueueFill.store(0);
    dirtyQueueAge.store(0);
    dirtyQueuePendingWrites.store(0);
    dirtyQueueDrain.store(0);

    hlc.resetStats();
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

size_t VBucket::queueBGFetchItem(const DocKey& key,
                                 std::unique_ptr<VBucketBGFetchItem> fetch,
                                 BgFetcher* bgFetcher) {
    LockHolder lh(pendingBGFetchesLock);
    vb_bgfetch_item_ctx_t& bgfetch_itm_ctx =
        pendingBGFetches[key];

    if (bgfetch_itm_ctx.bgfetched_list.empty()) {
        bgfetch_itm_ctx.isMetaOnly = true;
    }

    if (!fetch->metaDataOnly) {
        bgfetch_itm_ctx.isMetaOnly = false;
    }

    bgfetch_itm_ctx.bgfetched_list.push_back(std::move(fetch));

    bgFetcher->addPendingVB(id);
    return pendingBGFetches.size();
}

vb_bgfetch_queue_t VBucket::getBGFetchItems() {
    vb_bgfetch_queue_t fetches;
    LockHolder lh(pendingBGFetchesLock);
    fetches.swap(pendingBGFetches);
    return fetches;
}

void VBucket::addHighPriorityVBEntry(uint64_t id, const void *cookie,
                                     bool isBySeqno) {
    LockHolder lh(hpChksMutex);
    if (shard) {
        ++shard->highPriorityCount;
    }
    hpChks.push_back(HighPriorityVBEntry(cookie, id, isBySeqno));
    numHpChks.store(hpChks.size());
}

void VBucket::notifyOnPersistence(EventuallyPersistentEngine &e,
                                  uint64_t idNum,
                                  bool isBySeqno) {
    std::unique_lock<std::mutex> lh(hpChksMutex);
    std::map<const void*, ENGINE_ERROR_CODE> toNotify;
    std::list<HighPriorityVBEntry>::iterator entry = hpChks.begin();

    std::string logStr(isBySeqno
                       ? "seqno persistence"
                       : "checkpoint persistence");

    while (entry != hpChks.end()) {
        if (isBySeqno != entry->isBySeqno_) {
            ++entry;
            continue;
        }

        std::string logStr(isBySeqno ?
                           "seqno persistence" :
                           "checkpoint persistence");

        hrtime_t wall_time(gethrtime() - entry->start);
        size_t spent = wall_time / 1000000000;
        if (entry->id <= idNum) {
            toNotify[entry->cookie] = ENGINE_SUCCESS;
            stats.chkPersistenceHisto.add(wall_time / 1000);
            adjustCheckpointFlushTimeout(wall_time / 1000000000);
            LOG(EXTENSION_LOG_NOTICE, "Notified the completion of %s "
                "for vbucket %" PRIu16 ", Check for: %" PRIu64 ", "
                "Persisted upto: %" PRIu64 ", cookie %p",
                logStr.c_str(), id, entry->id, idNum, entry->cookie);
            entry = hpChks.erase(entry);
            if (shard) {
                --shard->highPriorityCount;
            }
        } else if (spent > getCheckpointFlushTimeout()) {
            adjustCheckpointFlushTimeout(spent);
            e.storeEngineSpecific(entry->cookie, NULL);
            toNotify[entry->cookie] = ENGINE_TMPFAIL;
            LOG(EXTENSION_LOG_WARNING, "Notified the timeout on %s "
                "for vbucket %" PRIu16 ", Check for: %" PRIu64 ", "
                "Persisted upto: %" PRIu64 ", cookie %p",
                logStr.c_str(), id, entry->id, idNum, entry->cookie);
            entry = hpChks.erase(entry);
            if (shard) {
                --shard->highPriorityCount;
            }
        } else {
            ++entry;
        }
    }
    numHpChks.store(hpChks.size());
    lh.unlock();

    std::map<const void*, ENGINE_ERROR_CODE>::iterator itr = toNotify.begin();
    for (; itr != toNotify.end(); ++itr) {
        e.notifyIOComplete(itr->first, itr->second);
    }

}

void VBucket::notifyAllPendingConnsFailed(EventuallyPersistentEngine &e) {
    std::map<const void*, ENGINE_ERROR_CODE> toNotify;
    {
        LockHolder lh(hpChksMutex);
        std::list<HighPriorityVBEntry>::iterator entry = hpChks.begin();
        while (entry != hpChks.end()) {
            toNotify[entry->cookie] = ENGINE_TMPFAIL;
            e.storeEngineSpecific(entry->cookie, NULL);
            entry = hpChks.erase(entry);
            if (shard) {
                --shard->highPriorityCount;
            }
        }
    }

    // Add all the pendingBGFetches to the toNotify map
    {
        LockHolder lh(pendingBGFetchesLock);
        size_t num_of_deleted_pending_fetches = 0;
        for (auto& bgf : pendingBGFetches) {
            vb_bgfetch_item_ctx_t& bg_itm_ctx = bgf.second;
            for (auto& bgitem : bg_itm_ctx.bgfetched_list) {
                toNotify[bgitem->cookie] = ENGINE_NOT_MY_VBUCKET;
                e.storeEngineSpecific(bgitem->cookie, nullptr);
                ++num_of_deleted_pending_fetches;
            }
        }
        stats.numRemainingBgItems.fetch_sub(num_of_deleted_pending_fetches);
        pendingBGFetches.clear();
    }

    std::map<const void*, ENGINE_ERROR_CODE>::iterator itr = toNotify.begin();
    for (; itr != toNotify.end(); ++itr) {
        e.notifyIOComplete(itr->first, itr->second);
    }

    fireAllOps(e);
}

void VBucket::adjustCheckpointFlushTimeout(size_t wall_time) {
    size_t middle = (MIN_CHK_FLUSH_TIMEOUT + MAX_CHK_FLUSH_TIMEOUT) / 2;

    if (wall_time <= MIN_CHK_FLUSH_TIMEOUT) {
        chkFlushTimeout = MIN_CHK_FLUSH_TIMEOUT;
    } else if (wall_time <= middle) {
        chkFlushTimeout = middle;
    } else {
        chkFlushTimeout = MAX_CHK_FLUSH_TIMEOUT;
    }
}

size_t VBucket::getHighPriorityChkSize() {
    return numHpChks.load();
}

size_t VBucket::getCheckpointFlushTimeout() {
    return chkFlushTimeout;
}

size_t VBucket::getNumItems(item_eviction_policy_t policy) {
    if (policy == VALUE_ONLY) {
        return ht.getNumInMemoryItems();
    } else {
        return ht.getNumItems();
    }
}

size_t VBucket::getNumNonResidentItems(item_eviction_policy_t policy) {
    if (policy == VALUE_ONLY) {
        return ht.getNumInMemoryNonResItems();
    } else {
        size_t num_items = ht.getNumItems();
        size_t num_res_items = ht.getNumInMemoryItems() -
                               ht.getNumInMemoryNonResItems();
        return num_items > num_res_items ? (num_items - num_res_items) : 0;
    }
}


uint64_t VBucket::getPersistenceCheckpointId() const {
    return persistenceCheckpointId.load();
}

void VBucket::setPersistenceCheckpointId(uint64_t checkpointId) {
    persistenceCheckpointId.store(checkpointId);
}

bool VBucket::isResidentRatioUnderThreshold(float threshold,
                                            item_eviction_policy_t policy) {
    if (policy != FULL_EVICTION) {
        throw std::invalid_argument("VBucket::isResidentRatioUnderThreshold: "
                "policy (which is " + std::to_string(policy) +
                ") must be FULL_EVICTION");
    }
    size_t num_items = getNumItems(policy);
    size_t num_non_resident_items = getNumNonResidentItems(policy);
    if (threshold >= ((float)(num_items - num_non_resident_items) /
                                                                num_items)) {
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
        LOG(EXTENSION_LOG_WARNING, "(vb %" PRIu16 ") Bloom filter / Temp filter"
            " already exist!", id);
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

/* [TBD]: Get rid of pHtLh */
uint64_t VBucket::queueDirty(StoredValue& v,
                             std::unique_lock<std::mutex>* pHtLh,
                             const GenerateBySeqno generateBySeqno,
                             const GenerateCas generateCas,
                             const bool isBackfillItem) {
    VBNotifyCtx notifyCtx;
    queued_item qi(v.toItem(false, getId()));

    if (isBackfillItem) {
        notifyCtx.notifyFlusher = queueBackfillItem(qi, generateBySeqno);
        /* During backfill on a TAP receiver we need to update the snapshot
         range in the checkpoint. Has to be done here because in case of TAP
         backfill, above, we use vb.queueBackfillItem() instead of
         vb.checkpointManager.queueDirty() */
        if (generateBySeqno == GenerateBySeqno::Yes) {
            checkpointManager.resetSnapshotRange();
        }
    } else {
        notifyCtx.notifyFlusher = checkpointManager.queueDirty(
                *this, qi, generateBySeqno, generateCas);
        notifyCtx.bySeqno = qi->getBySeqno();
        notifyCtx.notifyReplication = true;
        if (GenerateCas::Yes == generateCas) {
            v.setCas(qi->getCas());
        }
    }

    v.setBySeqno(qi->getBySeqno());

    if (pHtLh) {
        pHtLh->unlock();
    }

    if (newSeqnoCb) {
        newSeqnoCb->callback(getId(), notifyCtx);
    }

    return qi->getBySeqno();
}

StoredValue* VBucket::fetchValidValue(std::unique_lock<std::mutex>& lh,
                                      const DocKey& key,
                                      const int bucket_num,
                                      const bool wantsDeleted,
                                      const bool trackReference,
                                      const bool queueExpired) {
    if (!lh) {
        throw std::logic_error(
                "Hash bucket lock not held in "
                "VBucket::fetchValidValue() for hash bucket: " +
                std::to_string(bucket_num) + "for key: " +
                std::string(reinterpret_cast<const char*>(key.data()),
                            key.size()));
    }
    StoredValue* v =
            ht.unlocked_find(key, bucket_num, wantsDeleted, trackReference);
    if (v && !v->isDeleted() && !v->isTempItem()) {
        // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            if (getState() != vbucket_state_active) {
                return wantsDeleted ? v : NULL;
            }

            // queueDirty only allowed on active VB
            if (queueExpired && getState() == vbucket_state_active) {
                incExpirationStat(ExpireBy::Access);
                ht.unlocked_softDelete(v, 0, eviction);
                queueDirty(*v);
            }
            return wantsDeleted ? v : NULL;
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

void VBucket::bgFetch(const DocKey& key,
                      const void* cookie,
                      EventuallyPersistentEngine& engine,
                      const int bgFetchDelay,
                      const bool isMeta) {
    if (multiBGFetchEnabled) {
        // schedule to the current batch of background fetch of the given
        // vbucket
        size_t bgfetch_size = queueBGFetchItem(
                key,
                std::make_unique<VBucketBGFetchItem>(cookie, isMeta),
                shard->getBgFetcher());
        if (shard) {
            shard->getBgFetcher()->notifyBGEvent();
        }
        LOG(EXTENSION_LOG_DEBUG,
            "Queued a background fetch, now at %" PRIu64,
            uint64_t(bgfetch_size));
    } else {
        ++stats.numRemainingBgJobs;
        stats.maxRemainingBgJobs.store(
                std::max(stats.maxRemainingBgJobs.load(),
                         stats.numRemainingBgJobs.load()));
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = new SingleBGFetcherTask(
                &engine, key, getId(), cookie, isMeta, bgFetchDelay, false);
        iom->schedule(task, READER_TASK_IDX);
        LOG(EXTENSION_LOG_DEBUG,
            "Queued a background fetch, now at %" PRIu64,
            uint64_t(stats.numRemainingBgJobs.load()));
    }
}

ENGINE_ERROR_CODE VBucket::completeBGFetchForSingleItem(
        const DocKey& key,
        const VBucketBGFetchItem& fetched_item,
        const hrtime_t startTime) {
    ENGINE_ERROR_CODE status = fetched_item.value.getStatus();
    Item* fetchedValue = fetched_item.value.getValue();
    { // locking scope
        ReaderLockHolder rlh(getStateLock());
        int bucket = 0;
        auto blh = ht.getLockedBucket(key, &bucket);
        StoredValue* v = fetchValidValue(blh, key, bucket, eviction, true);

        if (fetched_item.metaDataOnly) {
            if (status == ENGINE_SUCCESS) {
                if (v && v->isTempInitialItem()) {
                    ht.unlocked_restoreMeta(blh, *fetchedValue, *v);
                }
            } else if (status == ENGINE_KEY_ENOENT) {
                if (v && v->isTempInitialItem()) {
                    v->setNonExistent();
                }
                /* If ENGINE_KEY_ENOENT is the status from storage and the temp
                   key is removed from hash table by the time bgfetch returns
                   (in case multiple bgfetch is scheduled for a key), we still
                   need to return ENGINE_SUCCESS to the memcached worker thread,
                   so that the worker thread can visit the ep-engine and figure
                   out the correct flow */
                status = ENGINE_SUCCESS;
            } else {
                if (v && !v->isTempInitialItem()) {
                    status = ENGINE_SUCCESS;
                }
            }
        } else {
            bool restore = false;
            if (v && v->isResident()) {
                status = ENGINE_SUCCESS;
            } else {
                switch (eviction) {
                case VALUE_ONLY:
                    if (v && !v->isResident()) {
                        restore = true;
                    }
                    break;
                case FULL_EVICTION:
                    if (v) {
                        if (v->isTempInitialItem() || !v->isResident()) {
                            restore = true;
                        }
                    }
                    break;
                default:
                    throw std::logic_error("Unknown eviction policy");
                }
            }

            if (restore) {
                if (status == ENGINE_SUCCESS) {
                    ht.unlocked_restoreValue(blh, *fetchedValue, *v);
                    if (!v->isResident()) {
                        throw std::logic_error(
                                "VBucket::completeBGFetchForSingleItem: "
                                "storedvalue (which has seqno " +
                                std::to_string(v->getBySeqno()) +
                                ") should be resident after calling "
                                "restoreValue()");
                    }
                } else if (status == ENGINE_KEY_ENOENT) {
                    v->setNonExistent();
                    if (eviction == FULL_EVICTION) {
                        // For the full eviction, we should notify
                        // ENGINE_SUCCESS to the memcached worker thread,
                        // so that the worker thread can visit the
                        // ep-engine and figure out the correct error
                        // code.
                        status = ENGINE_SUCCESS;
                    }
                } else {
                    // underlying kvstore couldn't fetch requested data
                    // log returned error and notify TMPFAIL to client
                    LOG(EXTENSION_LOG_WARNING,
                        "Failed background fetch for vb:%" PRIu16
                        ", seqno:%" PRIu64,
                        getId(),
                        v->getBySeqno());
                    status = ENGINE_TMPFAIL;
                }
            }
        }
    } // locked scope ends

    if (fetched_item.metaDataOnly) {
        ++stats.bg_meta_fetched;
    } else {
        ++stats.bg_fetched;
    }

    updateBGStats(fetched_item.initTime, startTime, gethrtime());
    return status;
}

/* [TBD]: Get rid of std::unique_lock<std::mutex> lock */
ENGINE_ERROR_CODE VBucket::addTempItemAndBGFetch(
        std::unique_lock<std::mutex>& lock,
        const int bucket_num,
        const DocKey& key,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        const int bgFetchDelay,
        const bool metadataOnly,
        const bool isReplication) {
    AddStatus rv = addTempStoredValue(lock, bucket_num, key, isReplication);
    switch (rv) {
    case AddStatus::NoMem:
        return ENGINE_ENOMEM;

    case AddStatus::Exists:
    case AddStatus::UnDel:
    case AddStatus::Success:
    case AddStatus::AddTmpAndBgFetch:
        // Since the hashtable bucket is locked, we shouldn't get here
        throw std::logic_error(
                "VBucket::addTempItemAndBGFetch: "
                "Invalid result from addTempItem: " +
                std::to_string(static_cast<uint16_t>(rv)));

    case AddStatus::BgFetch:
        lock.unlock();
        bgFetch(key, cookie, engine, bgFetchDelay, metadataOnly);
    }
    return ENGINE_EWOULDBLOCK;
}

ENGINE_ERROR_CODE VBucket::statsVKey(const DocKey& key,
                                     const void* cookie,
                                     EventuallyPersistentEngine& engine,
                                     const int bgFetchDelay) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = fetchValidValue(lh, key, bucket_num, true);

    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return ENGINE_KEY_ENOENT;
        }
        ++stats.numRemainingBgJobs;
        ExecutorPool* iom = ExecutorPool::get();
        ExTask task = new VKeyStatBGFetchTask(&engine,
                                              key,
                                              getId(),
                                              v->getBySeqno(),
                                              cookie,
                                              bgFetchDelay,
                                              false);
        iom->schedule(task, READER_TASK_IDX);
        return ENGINE_EWOULDBLOCK;
    } else {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else {
            AddStatus rv = addTempStoredValue(lh, bucket_num, key);
            switch (rv) {
            case AddStatus::NoMem:
                return ENGINE_ENOMEM;
            case AddStatus::Exists:
            case AddStatus::UnDel:
            case AddStatus::Success:
            case AddStatus::AddTmpAndBgFetch:
                // Since the hashtable bucket is locked, we shouldn't get here
                throw std::logic_error(
                        "VBucket::statsVKey: "
                        "Invalid result from unlocked_addTempItem (" +
                        std::to_string(static_cast<uint16_t>(rv)) + ")");

            case AddStatus::BgFetch: {
                ++stats.numRemainingBgJobs;
                ExecutorPool* iom = ExecutorPool::get();
                ExTask task = new VKeyStatBGFetchTask(
                        &engine, key, getId(), -1, cookie, bgFetchDelay, false);
                iom->schedule(task, READER_TASK_IDX);
            }
            }
            return ENGINE_EWOULDBLOCK;
        }
    }
}

void VBucket::completeStatsVKey(const DocKey& key,
                                const RememberingCallback<GetValue>& gcb) {
    int bucket_num(0);
    auto hlh = ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = fetchValidValue(hlh, key, bucket_num, eviction, true);

    if (v && v->isTempInitialItem()) {
        if (gcb.val.getStatus() == ENGINE_SUCCESS) {
            ht.unlocked_restoreValue(hlh, *(gcb.val.getValue()), *v);
            if (!v->isResident()) {
                throw std::logic_error(
                        "VBucket::completeStatsVKey: "
                        "storedvalue (which has seqno:" +
                        std::to_string(v->getBySeqno()) +
                        ") should be resident after calling restoreValue()");
            }
        } else if (gcb.val.getStatus() == ENGINE_KEY_ENOENT) {
            v->setNonExistent();
        } else {
            // underlying kvstore couldn't fetch requested data
            // log returned error and notify TMPFAIL to client
            LOG(EXTENSION_LOG_WARNING,
                "VBucket::completeStatsVKey: "
                "Failed background fetch for vb:%" PRIu16 ", seqno:%" PRIu64,
                getId(),
                v->getBySeqno());
        }
    }
}

MutationStatus VBucket::setFromInternal(Item& itm, const bool hasMetaData) {
    const PreserveRevSeqno preserveRevSeqno =
            hasMetaData ? PreserveRevSeqno::Yes : PreserveRevSeqno::No;
    return ht.set(itm, preserveRevSeqno);
}

ENGINE_ERROR_CODE VBucket::set(Item& itm,
                               const void* cookie,
                               EventuallyPersistentEngine& engine,
                               const int bgFetchDelay) {
    bool cas_op = (itm.getCas() != 0);
    int bucket_num(0);
    auto lh = ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue* v = ht.unlocked_find(itm.getKey(),
                                      bucket_num,
                                      /*wantsDeleted*/ true,
                                      /*trackReference*/ false);
    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    bool maybeKeyExists = true;
    // If we didn't find a valid item, check Bloomfilter's prediction if in
    // full eviction policy and for a CAS operation.
    if ((v == nullptr || v->isTempInitialItem()) &&
        (eviction == FULL_EVICTION) && (itm.getCas() != 0)) {
        // Check Bloomfilter's prediction
        if (!maybeKeyExistsInFilter(itm.getKey())) {
            maybeKeyExists = false;
        }
    }

    MutationStatus mtype =
            processSet(lh, v, itm, itm.getCas(), true, false, maybeKeyExists);

    uint64_t seqno = 0;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (mtype) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
    case MutationStatus::IsLocked:
        ret = ENGINE_KEY_EEXISTS;
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
        // We keep lh held as we need to do v->getCas()
        seqno = queueDirty(*v, nullptr);

        itm.setBySeqno(seqno);
        itm.setCas(v->getCas());
        break;
    case MutationStatus::NeedBgFetch: { // CAS operation with non-resident item
        // +
        // full eviction.
        if (v) {
            // temp item is already created. Simply schedule a bg fetch job
            lh.unlock();
            bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
            return ENGINE_EWOULDBLOCK;
        }
        ret = addTempItemAndBGFetch(lh,
                                    bucket_num,
                                    itm.getKey(),
                                    cookie,
                                    engine,
                                    bgFetchDelay,
                                    true);
        break;
    }
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::replace(Item& itm,
                                   const void* cookie,
                                   EventuallyPersistentEngine& engine,
                                   const int bgFetchDelay) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue* v = ht.unlocked_find(itm.getKey(), bucket_num, true, false);
    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return ENGINE_KEY_ENOENT;
        }

        MutationStatus mtype;
        if (eviction == FULL_EVICTION && v->isTempInitialItem()) {
            mtype = MutationStatus::NeedBgFetch;
        } else {
            mtype = processSet(lh, v, itm, 0, true, false);
        }

        uint64_t seqno = 0;
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        switch (mtype) {
        case MutationStatus::NoMem:
            ret = ENGINE_ENOMEM;
            break;
        case MutationStatus::IsLocked:
            ret = ENGINE_KEY_EEXISTS;
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
            // We need to keep lh as we will do v->getCas()
            seqno = queueDirty(*v, nullptr);

            itm.setBySeqno(seqno);
            itm.setCas(v->getCas());
            break;
        case MutationStatus::NeedBgFetch: {
            // temp item is already created. Simply schedule a bg fetch job
            lh.unlock();
            bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
            ret = ENGINE_EWOULDBLOCK;
            break;
        }
        }

        return ret;
    } else {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        }

        if (maybeKeyExistsInFilter(itm.getKey())) {
            return addTempItemAndBGFetch(lh,
                                         bucket_num,
                                         itm.getKey(),
                                         cookie,
                                         engine,
                                         bgFetchDelay,
                                         false);
        } else {
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT for replace().
            return ENGINE_KEY_ENOENT;
        }
    }
}

ENGINE_ERROR_CODE VBucket::addBackfillItem(Item& itm, const bool genBySeqno) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue* v = ht.unlocked_find(itm.getKey(), bucket_num, true, false);

    // Note that this function is only called on replica or pending vbuckets.
    if (v && v->isLocked(ep_current_time())) {
        v->unlock();
    }
    MutationStatus mtype = processSet(lh, v, itm, 0, true, true);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (mtype) {
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
    case MutationStatus::WasClean:
        setMaxCas(v->getCas());
        queueDirty(*v,
                   &lh,
                   genBySeqno ? GenerateBySeqno::Yes : GenerateBySeqno::No,
                   GenerateCas::No,
                   true);
        break;
    case MutationStatus::NeedBgFetch:
        throw std::logic_error(
                "VBucket::addBackfillItem: "
                "SET on a non-active vbucket should not require a "
                "bg_metadata_fetch.");
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::setWithMeta(Item& itm,
                                       const uint64_t cas,
                                       uint64_t* seqno,
                                       const void* cookie,
                                       EventuallyPersistentEngine& engine,
                                       const int bgFetchDelay,
                                       const bool force,
                                       const bool allowExisting,
                                       const GenerateBySeqno genBySeqno,
                                       const GenerateCas genCas,
                                       const bool isReplication) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue* v = ht.unlocked_find(itm.getKey(), bucket_num, true, false);

    bool maybeKeyExists = true;
    if (!force) {
        if (v) {
            if (v->isTempInitialItem()) {
                bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
                return ENGINE_EWOULDBLOCK;
            }

            if (!(conflictResolver->resolve(*v, itm.getMetaData(), false))) {
                ++stats.numOpsSetMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            if (maybeKeyExistsInFilter(itm.getKey())) {
                return addTempItemAndBGFetch(lh,
                                             bucket_num,
                                             itm.getKey(),
                                             cookie,
                                             engine,
                                             bgFetchDelay,
                                             true,
                                             isReplication);
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

    MutationStatus mtype = processSet(lh,
                                      v,
                                      itm,
                                      cas,
                                      allowExisting,
                                      true,
                                      maybeKeyExists,
                                      isReplication);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (mtype) {
    case MutationStatus::NoMem:
        ret = ENGINE_ENOMEM;
        break;
    case MutationStatus::InvalidCas:
    case MutationStatus::IsLocked:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case MutationStatus::WasDirty:
    case MutationStatus::WasClean: {
        setMaxCasAndTrackDrift(v->getCas());
        auto queued_seqno = queueDirty(*v, &lh, genBySeqno, genCas);
        if (nullptr != seqno) {
            *seqno = queued_seqno;
        }
    } break;
    case MutationStatus::NotFound:
        ret = ENGINE_KEY_ENOENT;
        break;
    case MutationStatus::NeedBgFetch: { // CAS operation with non-resident item
        // + full eviction.
        if (v) { // temp item is already created. Simply schedule a
            lh.unlock(); // bg fetch job.
            bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
            return ENGINE_EWOULDBLOCK;
        }
        ret = addTempItemAndBGFetch(lh,
                                    bucket_num,
                                    itm.getKey(),
                                    cookie,
                                    engine,
                                    bgFetchDelay,
                                    true,
                                    isReplication);
    }
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::deleteItem(const DocKey& key,
                                      uint64_t& cas,
                                      const void* cookie,
                                      EventuallyPersistentEngine& engine,
                                      const int bgFetchDelay,
                                      bool force,
                                      Item* itm,
                                      ItemMetaData* itemMeta,
                                      mutation_descr_t* mutInfo) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = ht.unlocked_find(key, bucket_num, true, false);
    if (!v || v->isDeleted() || v->isTempItem()) {
        if (eviction == VALUE_ONLY) {
            return ENGINE_KEY_ENOENT;
        } else { // Full eviction.
            if (!force) {
                if (!v) { // Item might be evicted from cache.
                    if (maybeKeyExistsInFilter(key)) {
                        return addTempItemAndBGFetch(lh,
                                                     bucket_num,
                                                     key,
                                                     cookie,
                                                     engine,
                                                     bgFetchDelay,
                                                     true);
                    } else {
                        // As bloomfilter predicted that item surely doesn't
                        // exist on disk, return ENOENT for deleteItem().
                        return ENGINE_KEY_ENOENT;
                    }
                } else if (v->isTempInitialItem()) {
                    lh.unlock();
                    bgFetch(key, cookie, engine, bgFetchDelay, true);
                    return ENGINE_EWOULDBLOCK;
                } else { // Non-existent or deleted key.
                    if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
                        // Delete a temp non-existent item to ensure that
                        // if a delete were issued over an item that doesn't
                        // exist, then we don't preserve a temp item.
                        ht.unlocked_del(key, bucket_num);
                    }
                    return ENGINE_KEY_ENOENT;
                }
            } else {
                if (!v) { // Item might be evicted from cache.
                    // Create a temp item and delete it below as it is a
                    // force deletion, only if bloomfilter predicts that
                    // item may exist on disk.
                    if (maybeKeyExistsInFilter(key)) {
                        AddStatus rv = addTempStoredValue(lh, bucket_num, key);
                        if (rv == AddStatus::NoMem) {
                            return ENGINE_ENOMEM;
                        }
                        v = ht.unlocked_find(key, bucket_num, true, false);
                        v->setDeleted();
                    } else {
                        return ENGINE_KEY_ENOENT;
                    }
                } else if (v->isTempInitialItem()) {
                    v->setDeleted();
                } else { // Non-existent or deleted key.
                    if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
                        // Delete a temp non-existent item to ensure that
                        // if a delete were issued over an item that doesn't
                        // exist, then we don't preserve a temp item.
                        ht.unlocked_del(key, bucket_num);
                    }
                    return ENGINE_KEY_ENOENT;
                }
            }
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }
    MutationStatus delrv;

    /* if an item containing a deleted value is present, set the value
     * as part of the stored value so that a value is set as part of the
     * delete.
     */
    if (itm && v) {
        v->setValue(*itm, ht, PreserveRevSeqno::Yes);
    }
    delrv = ht.unlocked_softDelete(v, cas, eviction);
    if (v && (delrv == MutationStatus::NotFound ||
              delrv == MutationStatus::WasDirty ||
              delrv == MutationStatus::WasClean)) {
        if (itemMeta != nullptr) {
            itemMeta->revSeqno = v->getRevSeqno();
            itemMeta->cas = v->getCas();
            itemMeta->flags = v->getFlags();
            itemMeta->exptime = v->getExptime();
        }
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
        ret = ENGINE_TMPFAIL;
        break;
    case MutationStatus::NotFound:
        ret = ENGINE_KEY_ENOENT;
    case MutationStatus::WasClean:
    case MutationStatus::WasDirty:
        if (v) {
            // We need to keep lh as we will do v->getCas()
            seqno = queueDirty(*v, nullptr);
            cas = v->getCas();
        }

        if (delrv != MutationStatus::NotFound) {
            if (mutInfo) {
                mutInfo->seqno = seqno;
                mutInfo->vbucket_uuid = failovers->getLatestUUID();
            }
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
                "Unexpected NEEDS_BG_FETCH from unlocked_softDelete");
    }
    return ret;
}

ENGINE_ERROR_CODE VBucket::deleteWithMeta(const DocKey& key,
                                          uint64_t& cas,
                                          uint64_t* seqno,
                                          const void* cookie,
                                          EventuallyPersistentEngine& engine,
                                          const int bgFetchDelay,
                                          const bool force,
                                          const ItemMetaData& itemMeta,
                                          const bool backfill,
                                          const GenerateBySeqno genBySeqno,
                                          const GenerateCas generateCas,
                                          const uint64_t bySeqno,
                                          const bool isReplication) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = ht.unlocked_find(key, bucket_num, true, false);
    if (!force) { // Need conflict resolution.
        if (v) {
            if (v->isTempInitialItem()) {
                bgFetch(key, cookie, engine, bgFetchDelay, true);
                return ENGINE_EWOULDBLOCK;
            }

            if (!(conflictResolver->resolve(*v, itemMeta, true))) {
                ++stats.numOpsDelMetaResolutionFailed;
                return ENGINE_KEY_EEXISTS;
            }
        } else {
            // Item is 1) deleted or not existent in the value eviction case OR
            // 2) deleted or evicted in the full eviction.
            if (maybeKeyExistsInFilter(key)) {
                return addTempItemAndBGFetch(lh,
                                             bucket_num,
                                             key,
                                             cookie,
                                             engine,
                                             bgFetchDelay,
                                             true,
                                             isReplication);
            } else {
                // Even though bloomfilter predicted that item doesn't exist
                // on disk, we must put this delete on disk if the cas is valid.
                AddStatus rv =
                        addTempStoredValue(lh, bucket_num, key, isReplication);
                if (rv == AddStatus::NoMem) {
                    return ENGINE_ENOMEM;
                }
                v = ht.unlocked_find(key, bucket_num, true, false);
                v->setDeleted();
            }
        }
    } else {
        if (!v) {
            // We should always try to persist a delete here.
            AddStatus rv =
                    addTempStoredValue(lh, bucket_num, key, isReplication);
            if (rv == AddStatus::NoMem) {
                return ENGINE_ENOMEM;
            }
            v = ht.unlocked_find(key, bucket_num, true, false);
            v->setDeleted();
            v->setCas(cas);
        } else if (v->isTempInitialItem()) {
            v->setDeleted();
            v->setCas(cas);
        }
    }

    if (v && v->isLocked(ep_current_time()) &&
        (getState() == vbucket_state_replica ||
         getState() == vbucket_state_pending)) {
        v->unlock();
    }

    MutationStatus delrv =
            ht.unlocked_softDelete(v, cas, itemMeta, eviction, true);
    cas = v ? v->getCas() : 0;

    switch (delrv) {
    case MutationStatus::NoMem:
        return ENGINE_ENOMEM;
    case MutationStatus::InvalidCas:
        return ENGINE_KEY_EEXISTS;
    case MutationStatus::IsLocked:
        return ENGINE_TMPFAIL;
    case MutationStatus::NotFound:
        return ENGINE_KEY_ENOENT;
    case MutationStatus::WasDirty:
    case MutationStatus::WasClean: {
        if (genBySeqno == GenerateBySeqno::No) {
            v->setBySeqno(bySeqno);
        }

        setMaxCasAndTrackDrift(v->getCas());
        auto queued_seqno =
                queueDirty(*v, &lh, genBySeqno, generateCas, backfill);
        if (seqno) {
            *seqno = queued_seqno;
        }
        break;
    }
    case MutationStatus::NeedBgFetch:
        lh.unlock();
        bgFetch(key, cookie, engine, bgFetchDelay, true);
        return ENGINE_EWOULDBLOCK;
    }
    return ENGINE_SUCCESS;
}

void VBucket::deleteExpiredItem(const DocKey& key,
                                time_t startTime,
                                uint64_t revSeqno,
                                ExpireBy source) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = ht.unlocked_find(key, bucket_num, true, false);
    if (v) {
        if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            // This is a temporary item whose background fetch for metadata
            // has completed.
            bool deleted = ht.unlocked_del(key, bucket_num);
            if (!deleted) {
                throw std::logic_error(
                        "VBucket::deleteExpiredItem: "
                        "Failed to delete seqno:" +
                        std::to_string(v->getBySeqno()) + " from bucket " +
                        std::to_string(bucket_num));
            }
        } else if (v->isExpired(startTime) && !v->isDeleted()) {
            ht.unlocked_softDelete(v, 0, eviction);
            queueDirty(*v, &lh);
        }
    } else {
        if (eviction == FULL_EVICTION) {
            // Create a temp item and delete and push it
            // into the checkpoint queue, only if the bloomfilter
            // predicts that the item may exist on disk.
            if (maybeKeyExistsInFilter(key)) {
                AddStatus rv = addTempStoredValue(lh, bucket_num, key);
                if (rv == AddStatus::NoMem) {
                    return;
                }
                v = ht.unlocked_find(key, bucket_num, true, false);
                v->setDeleted();
                v->setRevSeqno(revSeqno);
                ht.unlocked_softDelete(v, 0, eviction);
                queueDirty(*v, &lh);
            }
        }
    }
    incExpirationStat(source);
}

ENGINE_ERROR_CODE VBucket::add(Item& itm,
                               const void* cookie,
                               EventuallyPersistentEngine& engine,
                               const int bgFetchDelay) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue* v = ht.unlocked_find(itm.getKey(), bucket_num, true, false);

    bool maybeKeyExists = true;
    if ((v == nullptr || v->isTempInitialItem()) &&
        (eviction == FULL_EVICTION)) {
        // Check bloomfilter's prediction
        if (!maybeKeyExistsInFilter(itm.getKey())) {
            maybeKeyExists = false;
        }
    }

    AddStatus atype = processAdd(lh,
                                 v,
                                 itm,
                                 maybeKeyExists,
                                 /*isReplication*/ false);

    uint64_t seqno = 0;
    switch (atype) {
    case AddStatus::NoMem:
        return ENGINE_ENOMEM;
    case AddStatus::Exists:
        return ENGINE_NOT_STORED;
    case AddStatus::AddTmpAndBgFetch:
        return addTempItemAndBGFetch(lh,
                                     bucket_num,
                                     itm.getKey(),
                                     cookie,
                                     engine,
                                     bgFetchDelay,
                                     true);
    case AddStatus::BgFetch:
        lh.unlock();
        bgFetch(itm.getKey(), cookie, engine, bgFetchDelay, true);
        return ENGINE_EWOULDBLOCK;
    case AddStatus::Success:
    case AddStatus::UnDel:
        // We need to keep lh as we will do v->getCas()
        seqno = queueDirty(*v, nullptr);
        itm.setBySeqno(seqno);
        itm.setCas(v->getCas());
        break;
    }
    return ENGINE_SUCCESS;
}

GetValue VBucket::getAndUpdateTtl(const DocKey& key,
                                  const void* cookie,
                                  EventuallyPersistentEngine& engine,
                                  int bgFetchDelay,
                                  time_t exptime) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = fetchValidValue(lh, key, bucket_num, true);

    if (v) {
        if (v->isDeleted() || v->isTempDeletedItem() ||
            v->isTempNonExistentItem()) {
            return {};
        }

        if (!v->isResident()) {
            bgFetch(key, cookie, engine, bgFetchDelay);
            return GetValue(nullptr, ENGINE_EWOULDBLOCK, v->getBySeqno());
        }
        if (v->isLocked(ep_current_time())) {
            return GetValue(nullptr, ENGINE_KEY_EEXISTS, 0);
        }

        const bool exptime_mutated = exptime != v->getExptime();
        if (exptime_mutated) {
            v->markDirty();
            v->setExptime(exptime);
            v->setRevSeqno(v->getRevSeqno() + 1);
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), getId()),
                    ENGINE_SUCCESS,
                    v->getBySeqno());

        if (exptime_mutated) {
            queueDirty(*v, &lh);
        }

        return rv;
    } else {
        if (eviction == VALUE_ONLY) {
            return {};
        } else {
            if (maybeKeyExistsInFilter(key)) {
                ENGINE_ERROR_CODE ec = addTempItemAndBGFetch(lh,
                                                             bucket_num,
                                                             key,
                                                             cookie,
                                                             engine,
                                                             bgFetchDelay,
                                                             false);
                return GetValue(NULL, ec, -1, true);
            } else {
                // As bloomfilter predicted that item surely doesn't exist
                // on disk, return ENOENT for getAndUpdateTtl().
                return {};
            }
        }
    }
}

MutationStatus VBucket::insertFromWarmup(Item& itm,
                                         bool eject,
                                         bool keyMetaDataOnly) {
    if (!StoredValue::hasAvailableSpace(stats, itm)) {
        return MutationStatus::NoMem;
    }

    int bucket_num(0);
    auto lh = ht.getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue* v = ht.unlocked_find(itm.getKey(), bucket_num, true, false);

    if (v == NULL) {
        v = addNewStoredValue(lh, itm, PreserveRevSeqno::Yes);
        if (keyMetaDataOnly) {
            v->markNotResident();
            /* For now ht stats are updated from outside ht. This seems to be
               a better option for now than passing a flag to
               addNewStoredValue() just for this func */
            ++(ht.numNonResidentItems);
        }
        /* For now ht stats are updated from outside ht. This seems to be
           a better option for now than passing a flag to
           addNewStoredValue() just for this func.
           We need to decrNumTotalItems because ht.numTotalItems is already
           set by warmup when it estimated the item count from disk */
        ht.decrNumTotalItems();
        v->setNewCacheItem(false);
    } else {
        if (keyMetaDataOnly) {
            // We don't have a better error code ;)
            return MutationStatus::InvalidCas;
        }

        // Verify that the CAS isn't changed
        if (v->getCas() != itm.getCas()) {
            if (v->getCas() == 0) {
                v->setCas(itm.getCas());
                v->setFlags(itm.getFlags());
                v->setExptime(itm.getExptime());
                v->setRevSeqno(itm.getRevSeqno());
            } else {
                return MutationStatus::InvalidCas;
            }
        }
        updateStoredValue(lh, *v, itm, PreserveRevSeqno::Yes);
    }

    v->markClean();

    if (eject && !keyMetaDataOnly) {
        ht.unlocked_ejectItem(v, eviction);
    }

    return MutationStatus::NotFound;
}

GetValue VBucket::getInternal(const DocKey& key,
                              const void* cookie,
                              EventuallyPersistentEngine& engine,
                              int bgFetchDelay,
                              get_options_t options,
                              bool diskFlushAll) {
    const bool trackReference = (options & TRACK_REFERENCE);
    const bool getDeletedValue = (options & GET_DELETED_VALUE);
    int bucket_num(0);
    auto lh = ht.getLockedBucket(key, &bucket_num);
    StoredValue* v = fetchValidValue(
            lh, key, bucket_num, /*wantsDeleted*/ true, trackReference);
    if (v) {
        if (v->isDeleted() && !getDeletedValue) {
            return GetValue();
        }
        if (v->isTempDeletedItem() || v->isTempNonExistentItem()) {
            // Delete a temp non-existent item to ensure that
            // if the get were issued over an item that doesn't
            // exist, then we dont preserve a temp item.
            if (options & DELETE_TEMP) {
                ht.unlocked_del(key, bucket_num);
            }
            return GetValue();
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (options & QUEUE_BG_FETCH) {
                bgFetch(key, cookie, engine, bgFetchDelay);
            }
            return GetValue(NULL,
                            ENGINE_EWOULDBLOCK,
                            v->getBySeqno(),
                            true,
                            v->getNRUValue());
        }

        // Should we hide (return -1) for the items' CAS?
        const bool hide_cas =
                (options & HIDE_LOCKED_CAS) && v->isLocked(ep_current_time());
        return GetValue(v->toItem(hide_cas, getId()),
                        ENGINE_SUCCESS,
                        v->getBySeqno(),
                        false,
                        v->getNRUValue());
    } else {
        if (!getDeletedValue && (eviction == VALUE_ONLY || diskFlushAll)) {
            return GetValue();
        }

        if (maybeKeyExistsInFilter(key)) {
            ENGINE_ERROR_CODE ec = ENGINE_EWOULDBLOCK;
            if (options &
                QUEUE_BG_FETCH) { // Full eviction and need a bg fetch.
                ec = addTempItemAndBGFetch(lh,
                                           bucket_num,
                                           key,
                                           cookie,
                                           engine,
                                           bgFetchDelay,
                                           false);
            }
            return GetValue(NULL, ec, -1, true);
        } else {
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT, for getInternal().
            return GetValue();
        }
    }
}

void VBucket::deletedOnDiskCbk(const Item& queuedItem, bool deleted) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(queuedItem.getKey(), &bucket_num);
    StoredValue* v = fetchValidValue(lh,
                                     queuedItem.getKey(),
                                     bucket_num,
                                     /*wantsDeleted*/ true,
                                     /*trackReference*/ false);
    // Delete the item in the hash table iff:
    //  1. Item is existent in hashtable, and deleted flag is true
    //  2. rev seqno of queued item matches rev seqno of hash table item
    if (v && v->isDeleted() && (queuedItem.getRevSeqno() == v->getRevSeqno())) {
        bool isDeleted = ht.unlocked_del(queuedItem.getKey(), bucket_num);
        if (!isDeleted) {
            throw std::logic_error(
                    "deletedOnDiskCbk:callback: "
                    "Failed to delete key with seqno:" +
                    std::to_string(v->getBySeqno()) + "' from bucket " +
                    std::to_string(bucket_num));
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
    }
    doStatsForFlushing(queuedItem, queuedItem.size());
    stats.decrDiskQueueSize(1);
    decrMetaDataDisk(queuedItem);
}

bool VBucket::deleteKey(const DocKey& key) {
    int bucket_num(0);
    auto lh = ht.getLockedBucket(key, &bucket_num);
    return ht.unlocked_del(key, bucket_num);
}

void VBucket::addStats(bool details, ADD_STAT add_stat, const void *c,
                       item_eviction_policy_t policy) {
    addStat(NULL, toString(state), add_stat, c);
    if (details) {
        size_t numItems = getNumItems(policy);
        size_t tempItems = getNumTempItems();
        addStat("num_items", numItems, add_stat, c);
        addStat("num_temp_items", tempItems, add_stat, c);
        addStat("num_non_resident", getNumNonResidentItems(policy),
                add_stat, c);
        addStat("ht_memory", ht.memorySize(), add_stat, c);
        addStat("ht_item_memory", ht.getItemMemory(), add_stat, c);
        addStat("ht_cache_size", ht.cacheSize.load(), add_stat, c);
        addStat("num_ejects", ht.getNumEjects(), add_stat, c);
        addStat("ops_create", opsCreate.load(), add_stat, c);
        addStat("ops_update", opsUpdate.load(), add_stat, c);
        addStat("ops_delete", opsDelete.load(), add_stat, c);
        addStat("ops_reject", opsReject.load(), add_stat, c);
        addStat("queue_size", dirtyQueueSize.load(), add_stat, c);
        addStat("queue_memory", dirtyQueueMem.load(), add_stat, c);
        addStat("queue_fill", dirtyQueueFill.load(), add_stat, c);
        addStat("queue_drain", dirtyQueueDrain.load(), add_stat, c);
        addStat("queue_age", getQueueAge(), add_stat, c);
        addStat("pending_writes", dirtyQueuePendingWrites.load(), add_stat, c);

        try {
            DBFileInfo fileInfo = shard->getRWUnderlying()->getDbFileInfo(getId());
            addStat("db_data_size", fileInfo.spaceUsed, add_stat, c);
            addStat("db_file_size", fileInfo.fileSize, add_stat, c);
        } catch (std::runtime_error& e) {
            LOG(EXTENSION_LOG_WARNING,
                "VBucket::addStats: Exception caught during getDbFileInfo "
                "for vb:%" PRIu16 " - what(): %s", getId(), e.what());
        }

        addStat("high_seqno", getHighSeqno(), add_stat, c);
        addStat("uuid", failovers->getLatestUUID(), add_stat, c);
        addStat("purge_seqno", getPurgeSeqno(), add_stat, c);
        addStat("bloom_filter", getFilterStatusString().data(),
                add_stat, c);
        addStat("bloom_filter_size", getFilterSize(), add_stat, c);
        addStat("bloom_filter_key_count", getNumOfKeysInFilter(), add_stat, c);
        addStat("rollback_item_count", getRollbackItemCount(), add_stat, c);
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

void VBucket::updateBGStats(const hrtime_t init,
                            const hrtime_t start,
                            const hrtime_t stop) {
    if (stop >= start && start >= init) {
        // skip the measurement if the counter wrapped...
        ++stats.bgNumOperations;
        hrtime_t w = (start - init) / 1000;
        BlockTimer::log(start - init, "bgwait", stats.timingLog);
        stats.bgWaitHisto.add(w);
        stats.bgWait.fetch_add(w);
        atomic_setIfLess(stats.bgMinWait, w);
        atomic_setIfBigger(stats.bgMaxWait, w);

        hrtime_t l = (stop - start) / 1000;
        BlockTimer::log(stop - start, "bgload", stats.timingLog);
        stats.bgLoadHisto.add(l);
        stats.bgLoad.fetch_add(l);
        atomic_setIfLess(stats.bgMinLoad, l);
        atomic_setIfBigger(stats.bgMaxLoad, l);
    }
}

MutationStatus VBucket::processSet(const std::unique_lock<std::mutex>& htLock,
                                   StoredValue*& v,
                                   Item& itm,
                                   const uint64_t cas,
                                   const bool allowExisting,
                                   const bool hasMetaData,
                                   const bool maybeKeyExists,
                                   const bool isReplication) {
    if (!htLock) {
        throw std::invalid_argument(
                "VBucket::processSet: htLock not held for "
                "VBucket " +
                std::to_string(getId()));
    }

    if (!StoredValue::hasAvailableSpace(stats, itm, isReplication)) {
        return MutationStatus::NoMem;
    }

    if (cas && eviction == FULL_EVICTION && maybeKeyExists) {
        if (!v || v->isTempInitialItem()) {
            return MutationStatus::NeedBgFetch;
        }
    }

    /*
     * prior to checking for the lock, we should check if this object
     * has expired. If so, then check if CAS value has been provided
     * for this set op. In this case the operation should be denied since
     * a cas operation for a key that doesn't exist is not a very cool
     * thing to do. See MB 3252
     */
    if (v && v->isExpired(ep_real_time()) && !hasMetaData) {
        if (v->isLocked(ep_current_time())) {
            v->unlock();
        }
        if (cas) {
            /* item has expired and cas value provided. Deny ! */
            return MutationStatus::NotFound;
        }
    }

    const PreserveRevSeqno preserveRevSeqno =
            hasMetaData ? PreserveRevSeqno::Yes : PreserveRevSeqno::No;
    if (v) {
        if (!allowExisting && !v->isTempItem()) {
            return MutationStatus::InvalidCas;
        }
        if (v->isLocked(ep_current_time())) {
            /*
             * item is locked, deny if there is cas value mismatch
             * or no cas value is provided by the user
             */
            if (cas != v->getCas()) {
                return MutationStatus::IsLocked;
            }
            /* allow operation*/
            v->unlock();
        } else if (cas && cas != v->getCas()) {
            if (v->isTempDeletedItem() || v->isTempNonExistentItem() ||
                v->isDeleted()) {
                return MutationStatus::NotFound;
            }
            return MutationStatus::InvalidCas;
        }
        return updateStoredValue(htLock, *v, itm, preserveRevSeqno);
    } else if (cas != 0) {
        return MutationStatus::NotFound;
    } else {
        v = addNewStoredValue(htLock, itm, preserveRevSeqno);
        return MutationStatus::WasClean;
    }
}

AddStatus VBucket::processAdd(const std::unique_lock<std::mutex>& htLock,
                              StoredValue*& v,
                              Item& itm,
                              bool maybeKeyExists,
                              bool isReplication) {
    if (!htLock) {
        throw std::invalid_argument(
                "VBucket::processAdd: htLock not held for "
                "VBucket " +
                std::to_string(getId()));
    }

    if (v && !v->isDeleted() && !v->isExpired(ep_real_time()) &&
        !v->isTempItem()) {
        return AddStatus::Exists;
    }
    if (!StoredValue::hasAvailableSpace(stats, itm, isReplication)) {
        return AddStatus::NoMem;
    }

    AddStatus rv = AddStatus::Success;

    if (v) {
        if (v->isTempInitialItem() && eviction == FULL_EVICTION &&
            maybeKeyExists) {
            // Need to figure out if an item exists on disk
            return AddStatus::BgFetch;
        }

        rv = (v->isDeleted() || v->isExpired(ep_real_time()))
                     ? AddStatus::UnDel
                     : AddStatus::Success;

        if (v->isTempDeletedItem()) {
            itm.setRevSeqno(v->getRevSeqno() + 1);
        } else {
            itm.setRevSeqno(ht.getMaxDeletedRevSeqno() + 1);
        }

        updateStoredValue(
                htLock,
                *v,
                itm,
                v->isTempItem() ? PreserveRevSeqno::Yes : PreserveRevSeqno::No);
    } else {
        if (itm.getBySeqno() != StoredValue::state_temp_init) {
            if (eviction == FULL_EVICTION && maybeKeyExists) {
                return AddStatus::AddTmpAndBgFetch;
            }
        }
        v = addNewStoredValue(htLock, itm, PreserveRevSeqno::No);
        if (v->isTempItem()) {
            rv = AddStatus::BgFetch;
        }
    }

    if (v->isTempItem()) {
        v->setNRUValue(MAX_NRU_VALUE);
    }
    return rv;
}

MutationStatus VBucket::updateStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        StoredValue& v,
        Item& itm,
        const PreserveRevSeqno preserveRevSeqno) {
    return ht.unlocked_updateStoredValue(htLock, v, itm, preserveRevSeqno);
}

StoredValue* VBucket::addNewStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        Item& itm,
        const PreserveRevSeqno preserveRevSeqno) {
    StoredValue* v = ht.unlocked_addNewStoredValue(htLock, itm);

    if (preserveRevSeqno == PreserveRevSeqno::No) {
        /**
         * Possibly, this item is being recreated. Conservatively assign it
         * a seqno that is greater than the greatest seqno of all deleted
         * items seen so far.
         */
        uint64_t seqno = ht.getMaxDeletedRevSeqno();
        if (!v->isTempItem()) {
            ++seqno;
        }
        v->setRevSeqno(seqno);
        itm.setRevSeqno(seqno);
    }
    return v;
}

AddStatus VBucket::addTempStoredValue(
        const std::unique_lock<std::mutex>& htLock,
        int bucket_num,
        const DocKey& key,
        bool isReplication) {
    uint8_t ext_meta[EXT_META_LEN] = {PROTOCOL_BINARY_RAW_BYTES};
    static_assert(sizeof(ext_meta) == 1,
                  "VBucket::addTempStoredValue(): expected "
                  "EXT_META_LEN to be 1");
    Item itm(key,
             /*flags*/ 0,
             /*exp*/ 0,
             /*data*/ NULL,
             /*size*/ 0,
             ext_meta,
             sizeof(ext_meta),
             0,
             StoredValue::state_temp_init);

    /* if a temp item for a possibly deleted, set it non-resident by resetting
       the value cuz normally a new item added is considered resident which
       does not apply for temp item. */
    StoredValue* v = nullptr;
    return processAdd(htLock,
                      v,
                      itm,
                      /*maybeKeyExists*/ true,
                      isReplication);
}
