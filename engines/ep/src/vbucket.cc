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

#include "vbucket.h"
#include "atomic.h"
#include "bgfetcher.h"
#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "conflict_resolution.h"
#include "dcp/dcpconnmap.h"
#include "durability/active_durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ep_types.h"
#include "failover-table.h"
#include "hash_table.h"
#include "hash_table_stat_visitor.h"
#include "kvstore.h"
#include "pre_link_document_context.h"
#include "rollback_result.h"
#include "statwriter.h"
#include "stored_value_factories.h"
#include "vb_filter.h"
#include "vbucket_queue_item_ctx.h"
#include "vbucket_state.h"
#include "vbucketdeletiontask.h"

#include <boost/optional/optional_io.hpp>
#include <folly/lang/Assume.h>
#include <memcached/protocol_binary.h>
#include <memcached/server_document_iface.h>
#include <platform/compress.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <gsl.h>
#include <logtags.h>
#include <functional>
#include <list>
#include <set>
#include <string>
#include <vector>

using namespace std::string_literals;

/* Macros */
const auto MIN_CHK_FLUSH_TIMEOUT = std::chrono::seconds(10);
const auto MAX_CHK_FLUSH_TIMEOUT = std::chrono::seconds(30);

/* Statics definitions */
cb::AtomicDuration<> VBucket::chkFlushTimeout(MIN_CHK_FLUSH_TIMEOUT);
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

#if defined(linux) || defined(__linux__) || defined(__linux)
// One of the CV build fails due to htonl is defined as a macro:
// error: statement expression not allowed at file scope
#undef htonl
#endif

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
                 SyncWriteResolvedCallback syncWriteResolvedCb,
                 SyncWriteCompleteCallback syncWriteCb,
                 SeqnoAckCallback seqnoAckCb,
                 Configuration& config,
                 EvictionPolicy evictionPolicy,
                 std::unique_ptr<Collections::VB::Manifest> manifest,
                 vbucket_state_t initState,
                 uint64_t purgeSeqno,
                 uint64_t maxCas,
                 int64_t hlcEpochSeqno,
                 bool mightContainXattrs,
                 const nlohmann::json& replTopology,
                 uint64_t maxVisibleSeqno)
    : ht(st, std::move(valFact), config.getHtSize(), config.getHtLocks()),
      checkpointManager(std::make_unique<CheckpointManager>(st,
                                                            i,
                                                            chkConfig,
                                                            lastSeqno,
                                                            lastSnapStart,
                                                            lastSnapEnd,
                                                            maxVisibleSeqno,
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
      maxAllowedReplicasForSyncWrites(config.getSyncWritesMaxAllowedReplicas()),
      eviction(evictionPolicy),
      stats(st),
      persistenceSeqno(0),
      numHpVBReqs(0),
      id(i),
      state(newState),
      initialState(initState),
      purge_seqno(purgeSeqno),
      takeover_backed_up(false),
      persistedRange(lastSnapStart, lastSnapEnd),
      receivingInitialDiskSnapshot(false),
      rollbackItemCount(0),
      hlc(maxCas,
          hlcEpochSeqno,
          std::chrono::microseconds(config.getHlcDriftAheadThresholdUs()),
          std::chrono::microseconds(config.getHlcDriftBehindThresholdUs())),
      statPrefix("vb_" + std::to_string(i.get())),
      bucketCreation(false),
      deferredDeletion(false),
      deferredDeletionCookie(nullptr),
      newSeqnoCb(std::move(newSeqnoCb)),
      syncWriteResolvedCb(syncWriteResolvedCb),
      syncWriteCompleteCb(syncWriteCb),
      seqnoAckCb(seqnoAckCb),
      manifest(std::move(manifest)),
      mayContainXattrs(mightContainXattrs) {
    if (config.getConflictResolutionType().compare("lww") == 0) {
        conflictResolver.reset(new LastWriteWinsResolution());
    } else {
        conflictResolver.reset(new RevisionSeqnoResolution());
    }

    pendingOpsStart = std::chrono::steady_clock::time_point();
    stats.coreLocal.get()->memOverhead.fetch_add(
            sizeof(VBucket) + ht.memorySize() + sizeof(CheckpointManager));

    setupSyncReplication(replTopology);

    EP_LOG_INFO(
            "VBucket: created {} with state:{} initialState:{} lastSeqno:{} "
            "persistedRange:{{{},{}}} max_cas:{} uuid:{} topology:{}",
            id,
            VBucket::toString(state),
            VBucket::toString(initialState),
            lastSeqno,
            persistedRange.getStart(),
            persistedRange.getEnd(),
            getMaxCas(),
            failovers ? std::to_string(failovers->getLatestUUID()) : "<>",
            replicationTopology.rlock()->dump());
}

VBucket::~VBucket() {
    EP_LOG_INFO("~VBucket(): {}", id);

    if (!pendingOps.empty()) {
        EP_LOG_WARN("~VBucket(): {} has {} pending ops", id, pendingOps.size());
    }

    stats.diskQueueSize.fetch_sub(dirtyQueueSize.load());

    // Clear out the bloomfilter(s)
    clearFilter();

    stats.coreLocal.get()->memOverhead.fetch_sub(
            sizeof(VBucket) + ht.memorySize() + sizeof(CheckpointManager));

}

int64_t VBucket::getHighSeqno() const {
    return checkpointManager->getHighSeqno();
}

int64_t VBucket::getHighPreparedSeqno() const {
    if (!durabilityMonitor) {
        return -1;
    }
    return durabilityMonitor->getHighPreparedSeqno();
}

int64_t VBucket::getHighCompletedSeqno() const {
    if (!durabilityMonitor) {
        return -1;
    }
    return durabilityMonitor->getHighCompletedSeqno();
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

size_t VBucket::getSyncWriteAcceptedCount() const {
    folly::SharedMutex::ReadHolder lh(stateLock);
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getNumAccepted();
}

size_t VBucket::getSyncWriteCommittedCount() const {
    folly::SharedMutex::ReadHolder lh(stateLock);
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getNumCommitted();
}

size_t VBucket::getSyncWriteAbortedCount() const {
    folly::SharedMutex::ReadHolder lh(stateLock);
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getNumAborted();
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

std::vector<const void*> VBucket::getCookiesForInFlightSyncWrites() {
    return getActiveDM().getCookiesForInFlightSyncWrites();
}

std::vector<const void*> VBucket::prepareTransitionAwayFromActive() {
    return getActiveDM().prepareTransitionAwayFromActive();
}

size_t VBucket::size() {
    HashTableDepthStatVisitor v;
    ht.visitDepth(v);
    return v.size;
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

    // Append up to approxLimit checkpoint items outstanding for the persistence
    // cursor, if we haven't yet hit the limit.
    // Note that it is only valid to queue a complete checkpoint - this is where
    // the "approx" in the limit comes from.
    const auto ckptMgrLimit = approxLimit - result.items.size();
    bool ckptItemsAvailable = true;
    if (ckptMgrLimit > 0) {
        auto _begin_ = std::chrono::steady_clock::now();
        auto ckptItems = checkpointManager->getItemsForPersistence(
                result.items, ckptMgrLimit);
        result.ranges = std::move(ckptItems.ranges);
        result.maxDeletedRevSeqno = ckptItems.maxDeletedRevSeqno;
        result.checkpointType = ckptItems.checkpointType;
        ckptItemsAvailable = ckptItems.moreAvailable;
        stats.persistenceCursorGetItemsHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - _begin_));
    } // else result.ranges is empty, all items from rejectQueue

    // Check if there's any more items remaining.
    result.moreAvailable = !rejectQueue.empty() || ckptItemsAvailable;

    return result;
}

const char* VBucket::toString(vbucket_state_t s) {
    switch (s) {
    case vbucket_state_active:
        return "active";
    case vbucket_state_replica:
        return "replica";
    case vbucket_state_pending:
        return "pending";
    case vbucket_state_dead:
        return "dead";
    }
    return "unknown";
}

vbucket_state_t VBucket::fromString(const char* state) {
    if (strcmp(state, "active") == 0) {
        return vbucket_state_active;
    } else if (strcmp(state, "replica") == 0) {
        return vbucket_state_replica;
    } else if (strcmp(state, "pending") == 0) {
        return vbucket_state_pending;
    } else {
        return vbucket_state_dead;
    }
}

void VBucket::setState(vbucket_state_t to, const nlohmann::json& meta) {
    folly::SharedMutex::WriteHolder wlh(getStateLock());
    setState_UNLOCKED(to, meta, wlh);
}

std::string VBucket::validateReplicationTopology(
        const nlohmann::json& topology) {
    // Topology must be an array with 1..2 chain elements; and
    // each chain is an array of 1..4 nodes.
    //   [[<active>, <replica>, ...], [<active>, <replica>, ...]]
    //
    // - The first node (active) must always be a string representing
    //   the node name.
    // - The subsequent nodes (replicas) can either be strings
    //   indicating a defined replica, or Null indicating an undefined
    //   replica.
    if (!topology.is_array()) {
        return "'topology' must be an array, found:"s + topology.dump();
    }
    if ((topology.size() < 1) || (topology.size() > 2)) {
        return "'topology' must contain 1..2 elements, found:"s +
               topology.dump();
    }
    for (const auto& chain : topology.items()) {
        const auto& chainId = chain.key();
        const auto& nodes = chain.value();
        if (!nodes.is_array()) {
            return "'topology' chain["s + chainId +
                   "] must be an array, found:" + nodes.dump();
        }
        if ((nodes.size() < 1) || (nodes.size() > 4)) {
            return "'topology' chain["s + chainId +
                   "] must contain 1..4 nodes, found:" + nodes.dump();
        }
        for (const auto& node : nodes.items()) {
            switch (node.value().type()) {
            case nlohmann::json::value_t::string:
                break;
            case nlohmann::json::value_t::null:
                // Null not permitted for active (first) node.
                if (node.key() == "0") {
                    return "'topology' chain[" + chainId + "] node[" +
                           node.key() + "] (active) cannot be null";
                }
                break;
            default:
                return "'topology' chain[" + chainId + "] node[" + node.key() +
                       "] must be a string, found:" + node.value().dump();
            }
        }
    }
    return {};
}

std::string VBucket::validateSetStateMeta(const nlohmann::json& meta) {
    if (!meta.is_object()) {
        return "'meta' must be an object if specified, found:"s + meta.dump();
    }
    for (const auto& el : meta.items()) {
        if (el.key() == "topology") {
            return validateReplicationTopology(el.value());
        } else {
            return "'topology' contains unsupported key:"s + el.key() +
                   " with value:" + el.value().dump();
        }
    }
    return {};
}

void VBucket::setState_UNLOCKED(
        vbucket_state_t to,
        const nlohmann::json& meta,
        const folly::SharedMutex::WriteHolder& vbStateLock) {
    vbucket_state_t oldstate = state;

    // Validate (optional) meta content.
    if (!meta.is_null()) {
        if (to != vbucket_state_active) {
            throw std::invalid_argument(
                    "VBucket::setState: meta only permitted for state:active, "
                    "found state:"s +
                    VBucket::toString(to) + " meta:" + meta.dump());
        }
        auto error = validateSetStateMeta(meta);
        if (!error.empty()) {
            throw std::invalid_argument("VBucket::setState: " + error);
        }
    }

    if (to == vbucket_state_active &&
        checkpointManager->getOpenCheckpointId() < 2) {
        checkpointManager->setOpenCheckpointId(2);
    }

    EP_LOG_INFO(
            "VBucket::setState: transitioning {} with high seqno:{} from:{} "
            "to:{}{}",
            id,
            getHighSeqno(),
            VBucket::toString(oldstate),
            VBucket::toString(to),
            meta.is_null() ? ""s : (" meta:"s + meta.dump()));

    state = to;

    setupSyncReplication(meta.is_null() ? nlohmann::json{}
                                        : meta.at("topology"));
}

vbucket_transition_state VBucket::getTransitionState() const {
    nlohmann::json topology;
    if (getState() == vbucket_state_active) {
        topology = getReplicationTopology();
    }

    return vbucket_transition_state{failovers->toJSON(), topology, getState()};
}

nlohmann::json VBucket::getReplicationTopology() const {
    return *replicationTopology.rlock();
}

void VBucket::setupSyncReplication(const nlohmann::json& topology) {
    // First, update the Replication Topology in VBucket
    if (!topology.is_null()) {
        if (state != vbucket_state_active) {
            throw std::invalid_argument(
                    "VBucket::setupSyncReplication: Topology only valid for "
                    "vbucket_state_active");
        }
        auto error = validateReplicationTopology(topology);
        if (!error.empty()) {
            throw std::invalid_argument(
                    "VBucket::setupSyncReplication: Invalid replication "
                    "topology: " +
                    error);
        }
        replicationTopology = topology;
    } else {
        *replicationTopology.wlock() = {};
    }

    // Then, initialize the DM and propagate the new topology if necessary
    auto* currentPassiveDM =
            dynamic_cast<PassiveDurabilityMonitor*>(durabilityMonitor.get());

    // If we are transitioning away from active then we need to mark all of our
    // prepares as PreparedMaybeVisible to avoid exposing them
    std::vector<queued_item> trackedWrites;
    if (!currentPassiveDM && durabilityMonitor &&
        state != vbucket_state_active) {
        auto& adm = dynamic_cast<ActiveDurabilityMonitor&>(*durabilityMonitor);
        trackedWrites = adm.getTrackedWrites();
        for (auto& write : trackedWrites) {
            auto htRes = ht.findOnlyPrepared(write->getKey());
            Expects(htRes.storedValue);
            Expects(htRes.storedValue->isPending() ||
                    htRes.storedValue->isCompleted());
            htRes.storedValue->setCommitted(
                    CommittedState::PreparedMaybeVisible);
            write->setPreparedMaybeVisible();
        }
    }

    switch (state) {
    case vbucket_state_active: {
        if (currentPassiveDM) {
            // Change to active from passive durability monitor - construct
            // an ActiveDM from the PassiveDM, maintaining any in-flight
            // SyncWrites.
            durabilityMonitor = std::make_unique<ActiveDurabilityMonitor>(
                    stats, std::move(*currentPassiveDM));
        } else if (!durabilityMonitor) {
            // Change to Active from no previous DurabilityMonitor - create
            // one.
            durabilityMonitor =
                    std::make_unique<ActiveDurabilityMonitor>(stats, *this);
        } else {
            // Already have an (active?) DurabilityMonitor and just changing
            // topology.
            if (!dynamic_cast<ActiveDurabilityMonitor*>(
                        durabilityMonitor.get())) {
                throw std::logic_error(
                        "VBucket::setupSyncReplication: A durabilityMonitor "
                        "already exists but it is neither Active nor Passive!");
            }
        }

        // @todo: We want to support empty-topology in ActiveDM, that's for
        //     Warmup. Deferred to dedicated patch (tracked under MB-33186).
        if (!topology.is_null()) {
            getActiveDM().setReplicationTopology(*replicationTopology.rlock());
        }
        return;
    }
    case vbucket_state_replica:
    case vbucket_state_pending:
        if (currentPassiveDM) {
            // Already have a PassiveDM - given topology changes are not
            // applicable to PassiveDM, nothing to do here.
            return;
        }
        // Current DM (if exists) is not Passive; replace it with a Passive one.
        if (durabilityMonitor) {
            durabilityMonitor = std::make_unique<PassiveDurabilityMonitor>(
                    *this,
                    std::move(dynamic_cast<ActiveDurabilityMonitor&>(
                            *durabilityMonitor.get())));
        } else {
            durabilityMonitor =
                    std::make_unique<PassiveDurabilityMonitor>(*this);
        }
        return;
    case vbucket_state_dead:
        // No DM in dead state.
        return;
    }
    folly::assume_unreachable();
}

ActiveDurabilityMonitor& VBucket::getActiveDM() {
    Expects(state == vbucket_state_active);
    return dynamic_cast<ActiveDurabilityMonitor&>(*durabilityMonitor);
}

PassiveDurabilityMonitor& VBucket::getPassiveDM() {
    Expects(state == vbucket_state_replica || state == vbucket_state_pending);
    return dynamic_cast<PassiveDurabilityMonitor&>(*durabilityMonitor);
}

void VBucket::processDurabilityTimeout(
        const std::chrono::steady_clock::time_point asOf) {
    folly::SharedMutex::ReadHolder lh(stateLock);
    // @todo-durability: Add support for DurabilityMonitor at Replica
    if (getState() != vbucket_state_active) {
        return;
    }
    getActiveDM().processTimeout(asOf);
}

void VBucket::notifySyncWritesPendingCompletion() {
    syncWriteResolvedCb(getId());
}

void VBucket::processResolvedSyncWrites() {
    // Acquire shared access on stateLock as need to ensure the vbucket is
    // active (and we have an ActiveDM).
    folly::SharedMutex::ReadHolder rlh(getStateLock());
    if (getState() != vbucket_state_active) {
        return;
    }
    getActiveDM().processCompletedSyncWriteQueue();
}

void VBucket::doStatsForQueueing(const Item& qi, size_t itemBytes)
{
    ++dirtyQueueSize;
    dirtyQueueMem.fetch_add(sizeof(Item));
    ++dirtyQueueFill;
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
            qi.getQueuedTime().time_since_epoch());
    dirtyQueueAge.fetch_add(us.count());
    dirtyQueuePendingWrites.fetch_add(itemBytes);
}

void VBucket::doStatsForFlushing(const Item& qi, size_t itemBytes) {
    --dirtyQueueSize;
    decrDirtyQueueMem(sizeof(Item));
    ++dirtyQueueDrain;
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
            qi.getQueuedTime().time_since_epoch());
    decrDirtyQueueAge(us.count());
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
    // dirtyQueue size is 0, so the queueAge is 0.
    if (currDirtyQueueAge == 0) {
        return 0;
    }

    // Get time now multiplied by the queue size. We need to subtract
    // dirtyQueueAge from this to offset time_since_epoch.
    auto currentAge =
            std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count() *
            dirtyQueueSize;

    // Return the time in milliseconds
    return (currentAge - currDirtyQueueAge) / 1000;
}

template <typename T>
void VBucket::addStat(const char* nm,
                      const T& val,
                      const AddStatFn& add_stat,
                      const void* c) {
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
    if (v.isPending()) {
        std::stringstream ss;
        ss << v;
        throw std::invalid_argument(
                "VBucket::handlePreExpiry: Cannot expire pending "
                "StoredValues:" +
                cb::UserDataView(ss.str()).getSanitizedValue());
    }

    value_t value = v.getValue();
    if (value) {
        std::unique_ptr<Item> itm(v.toItem(id));
        item_info itm_info;
        EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();
        itm_info =
                itm->toItemInfo(failovers->getLatestUUID(), getHLCEpochSeqno());

        SERVER_HANDLE_V1* sapi = engine->getServerApi();
        /* TODO: In order to minimize allocations, the callback needs to
         * allocate an item whose value size will be exactly the size of the
         * value after pre-expiry is performed.
         */
        auto result = sapi->document->pre_expiry(itm_info);
        // The API states only uncompressed xattr values are returned
        auto datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
        if (!result.empty()) {
            Item new_item(v.getKey(),
                          v.getFlags(),
                          v.getExptime(),
                          result.data(),
                          result.size(),
                          datatype,
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

ENGINE_ERROR_CODE VBucket::commit(
        const DocKey& key,
        uint64_t prepareSeqno,
        boost::optional<int64_t> commitSeqno,
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        const void* cookie) {
    auto res = ht.findForUpdate(key);
    if (!res.pending) {
        // If we are committing we /should/ always find the pending item.
        EP_LOG_ERR(
                "VBucket::commit ({}) failed as no HashTable item found with "
                "key:{} prepare_seqno:{}, commit_seqno:{}",
                id,
                cb::UserDataView(key.to_string()),
                prepareSeqno,
                commitSeqno);
        return ENGINE_KEY_ENOENT;
    }

    Expects(prepareSeqno);

    // Value for Pending must never be ejected
    Expects(res.pending->isResident());

    // If prepare seqno is not the same as our stored seqno then we should be
    // a replica and have missed a completion and a prepare due to de-dupe.
    if (prepareSeqno != static_cast<uint64_t>(res.pending->getBySeqno())) {
        Expects(getState() != vbucket_state_active);
        Expects(isReceivingDiskSnapshot());
        Expects(prepareSeqno >= checkpointManager->getOpenSnapshotStartSeqno());
    }

    VBQueueItemCtx queueItmCtx;
    if (commitSeqno) {
        queueItmCtx.genBySeqno = GenerateBySeqno::No;
    }
    // Never generate a new cas. We should always take the existing cas because
    // it may have been set explicitly.
    queueItmCtx.genCas = GenerateCas::No;

    queueItmCtx.durability =
            DurabilityItemCtx{res.pending->getBySeqno(), nullptr /*cookie*/};

    auto notify =
            commitStoredValue(res, prepareSeqno, queueItmCtx, commitSeqno);

    notifyNewSeqno(notify);
    doCollectionsStats(cHandle, notify);

    // Cookie representing the client connection, provided only at Active
    if (cookie) {
        notifyClientOfSyncWriteComplete(cookie, ENGINE_SUCCESS);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE VBucket::abort(
        const DocKey& key,
        uint64_t prepareSeqno,
        boost::optional<int64_t> abortSeqno,
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        const void* cookie) {
    auto htRes = ht.findForUpdate(key);

    // This block handles the case where at Replica we receive an Abort but we
    // do not have any in-flight Prepare in the HT. That is possible when
    // Replica receives a Backfill (Disk) Snapshot (for both EP and Ephemeral
    // bucket).
    // Note that, while for EP we just expect no-pending in the HT, for
    // Ephemeral we may have no-pending or a pre-existing completed (Committed
    // or Aborted) Prepare in the HT.
    if (!htRes.pending || (htRes.pending && htRes.pending->isCompleted())) {
        // Active should always find the pending item.
        if (getState() == vbucket_state_active) {
            if (htRes.committed) {
                std::stringstream ss;
                ss << *htRes.committed;
                EP_LOG_ERR(
                        "VBucket::abort ({}) - active failed as HashTable "
                        "value is not "
                        "CommittedState::Pending - {}",
                        id,
                        cb::UserData(ss.str()));
                return ENGINE_EINVAL;
            } else {
                EP_LOG_ERR(
                        "VBucket::abort ({}) - active failed as no HashTable"
                        "item found with key:{}",
                        id,
                        cb::UserDataView(key.to_string()));
                return ENGINE_KEY_ENOENT;
            }
        }

        // If we did not find the corresponding prepare for this abort then we
        // must be receiving a disk snapshot.
        if (!isReceivingDiskSnapshot()) {
            EP_LOG_ERR(
                    "VBucket::abort ({}) - replica - failed as we received "
                    "an abort for a prepare that does not exist and we are not "
                    "currently receiving a disk snapshot. Prepare seqno: {} "
                    "Abort seqno: {}",
                    id,
                    prepareSeqno,
                    abortSeqno);
            return ENGINE_EINVAL;
        }

        // If we did not find the corresponding prepare for this abort then the
        // prepare seqno of the abort must be greater than or equal to the
        // snapshot start seqno.
        if (static_cast<uint64_t>(prepareSeqno) <
            checkpointManager->getOpenSnapshotStartSeqno()) {
            EP_LOG_ERR(
                    "VBucket::abort ({}) - replica - failed as we received "
                    "an abort for a prepare that does not exist and the "
                    "prepare seqno is less than the current snapshot start: {}",
                    id,
                    prepareSeqno,
                    checkpointManager->getOpenSnapshotStartSeqno());
            return ENGINE_EINVAL;
        }

        // Replica is receiving a legal Abort but we do not have any in-flight
        // Prepare in the HT.
        // 1) If we do not have any pending in the HT, then we just proceed to
        //     creating a new Abort item
        // 2) Else, if we have a Completed pending in the HT (possible only at
        //     Ephemeral) then we have to convert/update the existing pending
        //     into a new PersistedAborted item
        VBNotifyCtx ctx;
        if (!htRes.pending) {
            ctx = addNewAbort(
                    htRes.pending.getHBL(), key, prepareSeqno, *abortSeqno);
        } else {
            // This code path can be reached only at Ephemeral
            Expects(htRes.pending->isCompleted());
            ctx = abortStoredValue(htRes.pending.getHBL(),
                                   *htRes.pending.release(),
                                   prepareSeqno,
                                   *abortSeqno);
        }

        notifyNewSeqno(ctx);
        doCollectionsStats(cHandle, ctx);

        return ENGINE_SUCCESS;
    }

    // If prepare seqno is not the same as our stored seqno then we should be
    // a replica and have missed a completion and a prepare due to de-dupe.
    if (prepareSeqno != static_cast<uint64_t>(htRes.pending->getBySeqno())) {
        Expects(getState() != vbucket_state_active);
        Expects(isReceivingDiskSnapshot());
        Expects(prepareSeqno >= checkpointManager->getOpenSnapshotStartSeqno());
    }

    // abortStoredValue deallocates the pending SV, releasing here so
    // ~StoredValueProxy will not attempt to update stats and lead to
    // use-after-free
    auto notify = abortStoredValue(htRes.pending.getHBL(),
                                   *htRes.pending.release(),
                                   prepareSeqno,
                                   abortSeqno);

    notifyNewSeqno(notify);
    doCollectionsStats(cHandle, notify);

    // Cookie representing the client connection, provided only at Active
    if (cookie) {
        notifyClientOfSyncWriteComplete(cookie, ENGINE_SYNC_WRITE_AMBIGUOUS);
    }

    return ENGINE_SUCCESS;
}

void VBucket::notifyActiveDMOfLocalSyncWrite() {
    getActiveDM().checkForCommit();
}

void VBucket::notifyClientOfSyncWriteComplete(const void* cookie,
                                              ENGINE_ERROR_CODE result) {
    EP_LOG_DEBUG(
            "VBucket::notifyClientOfSyncWriteComplete ({}) cookie:{} result:{}",
            id,
            cookie,
            result);
    Expects(cookie);
    syncWriteCompleteCb(cookie, result);
}

void VBucket::notifyPassiveDMOfSnapEndReceived(uint64_t snapEnd) {
    getPassiveDM().notifySnapshotEndReceived(snapEnd);
}

void VBucket::sendSeqnoAck(int64_t seqno) {
    Expects(state == vbucket_state_replica || state == vbucket_state_pending);
    seqnoAckCb(getId(), seqno);
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
    if (eviction != EvictionPolicy::Full) {
        throw std::invalid_argument(
                "VBucket::isResidentRatioUnderThreshold: "
                "policy (which is " +
                to_string(eviction) + ") must be EvictionPolicy::Full");
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

VBNotifyCtx VBucket::queueItem(queued_item& item, const VBQueueItemCtx& ctx) {
    // Ensure that durable writes are queued with the same seqno-order in both
    // Backfill/CheckpointManager Queues and DurabilityMonitor. Note that
    // bySeqno may be generated by Queues when the item is queued.
    // Lock only for durable writes to minimize front-end thread contention.
    std::unique_lock<std::mutex> durLock(dmQueueMutex, std::defer_lock);
    if (item->isPending()) {
        durLock.lock();
    }

    VBNotifyCtx notifyCtx;
        notifyCtx.notifyFlusher =
                checkpointManager->queueDirty(*this,
                                              item,
                                              ctx.genBySeqno,
                                              ctx.genCas,
                                              ctx.preLinkDocumentContext);
        notifyCtx.notifyReplication = true;
    notifyCtx.bySeqno = item->getBySeqno();
    notifyCtx.syncWrite = item->isPending() ? SyncWriteOperation::Yes
                                            : SyncWriteOperation::No;

    // Process Durability items (notify the DurabilityMonitor of
    // Prepare/Commit/Abort)
    switch (state) {
    case vbucket_state_active:
        if (item->isPending()) {
            Expects(ctx.durability.is_initialized());
            getActiveDM().addSyncWrite(ctx.durability->cookie, item);
        }
        break;
    case vbucket_state_replica:
    case vbucket_state_pending:
    case vbucket_state_dead:
        auto& pdm = getPassiveDM();
        if (item->isPending()) {
            pdm.addSyncWrite(item, ctx.overwritingPrepareSeqno);
        } else if (item->isCommitSyncWrite()) {
            pdm.completeSyncWrite(
                    item->getKey(),
                    PassiveDurabilityMonitor::Resolution::Commit,
                    boost::get<int64_t>(
                            ctx.durability->requirementsOrPreparedSeqno));
        } else if (item->isAbort()) {
            pdm.completeSyncWrite(item->getKey(),
                                  PassiveDurabilityMonitor::Resolution::Abort,
                                  {} /* no prepareSeqno*/);
        }
        break;
    }

    return notifyCtx;
}

VBNotifyCtx VBucket::queueDirty(const HashTable::HashBucketLock& hbl,
                                StoredValue& v,
                                const VBQueueItemCtx& ctx) {
    if (ctx.trackCasDrift == TrackCasDrift::Yes) {
        setMaxCasAndTrackDrift(v.getCas());
    }

    // If we are queueing a SyncWrite StoredValue; extract the durability
    // requirements to use to create the Item.
    boost::optional<cb::durability::Requirements> durabilityReqs;
    if (v.isPending()) {
        Expects(ctx.durability.is_initialized());
        durabilityReqs = boost::get<cb::durability::Requirements>(
                ctx.durability->requirementsOrPreparedSeqno);
    }

    queued_item qi(v.toItem(getId(),
                            StoredValue::HideLockedCas::No,
                            StoredValue::IncludeValue::Yes,
                            durabilityReqs));

    // Set queue time to now. Why not in the ctor of the Item? We only need to
    // do this in certain places for new items as it's used to determine how
    // long it took an item to get flushed.
    qi->setQueuedTime();

    if (qi->isCommitSyncWrite()) {
        Expects(ctx.durability.is_initialized());
        qi->setPrepareSeqno(boost::get<int64_t>(
                ctx.durability->requirementsOrPreparedSeqno));
    }

    // MB-27457: Timestamp deletes only when they don't already have a timestamp
    // assigned. This is here to ensure all deleted items have a timestamp which
    // our tombstone purger can use to determine which tombstones to purge. A
    // DCP replicated or deleteWithMeta created delete may already have a time
    // assigned to it.
    if (qi->isDeleted() && (ctx.generateDeleteTime == GenerateDeleteTime::Yes ||
                            qi->getExptime() == 0)) {
        qi->setExpTime(ep_real_time());
    }

    if (!mightContainXattrs() && mcbp::datatype::is_xattr(v.getDatatype())) {
        setMightContainXattrs();
    }

    // Enqueue the item for persistence and replication
    VBNotifyCtx notifyCtx = queueItem(qi, ctx);

    // Some StoredValue adjustments now..
    if (ctx.genCas == GenerateCas::Yes) {
        v.setCas(qi->getCas());
        }
    if (ctx.genBySeqno == GenerateBySeqno::Yes) {
        v.setBySeqno(qi->getBySeqno());
    }

    return notifyCtx;
}

VBNotifyCtx VBucket::queueAbort(const HashTable::HashBucketLock& hbl,
                                const StoredValue& v,
                                int64_t prepareSeqno,
                                const VBQueueItemCtx& ctx) {
    if (ctx.trackCasDrift == TrackCasDrift::Yes) {
        setMaxCasAndTrackDrift(v.getCas());
    }

    queued_item item(v.toItemAbort(getId()));
    item->setPrepareSeqno(prepareSeqno);
    item->setExpTime(ep_real_time());

    Expects(item->isAbort());
    Expects(item->isDeleted());

    return queueItem(item, ctx);
}

VBNotifyCtx VBucket::queueAbortForUnseenPrepare(queued_item item,
                                                const VBQueueItemCtx& ctx) {
    item->setExpTime(ep_real_time());

    Expects(item->isAbort());
    Expects(item->isDeleted());
    Expects(item->getPrepareSeqno());

    return queueItem(item, ctx);
}

queued_item VBucket::createNewAbortedItem(const DocKey& key,
                                          int64_t prepareSeqno,
                                          int64_t abortSeqno) {
    queued_item item = std::make_unique<Item>(key,
                                              0 /*flags*/,
                                              ep_real_time() /*exp*/,
                                              value_t{},
                                              PROTOCOL_BINARY_RAW_BYTES,
                                              0,
                                              abortSeqno,
                                              getId());

    item->setAbortSyncWrite();
    item->setDeleted();

    item->setPrepareSeqno(prepareSeqno);

    return item;
}

HashTable::FindResult VBucket::fetchValidValue(
        WantsDeleted wantsDeleted,
        TrackReference trackReference,
        QueueExpired queueExpired,
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        const ForGetReplicaOp fetchRequestedForReplicaItem) {
    if (queueExpired == QueueExpired::Yes && !cHandle.valid()) {
        throw std::invalid_argument(
                "VBucket::fetchValidValue cannot queue "
                "expired items for invalid collection");
    }
    const auto& key = cHandle.getKey();
    auto res = ht.findForRead(
            key, trackReference, wantsDeleted, fetchRequestedForReplicaItem);
    // Temp: The following const_cast<> is needed because unfortunately this
    // function does need a non-const SV to be able to perform expiry, and also
    // because various existing callers of this method expect a non-const SV.
    // I prefer to have HashTable::findForRead() return a const SV so at least
    // other use-cases _can_ be const-correct, even if we cannot be here.
    // Ideally this should be removed; but that requires that findForRead() is
    // refactored to return a proxy object which while logically const, *does*
    // allow some "physically" const methods (like ht.unlocked_restore) to be
    // called on it.
    auto* v = const_cast<StoredValue*>(res.storedValue);
    if (v && !v->isDeleted() && !v->isTempItem()) {
        // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            if (getState() != vbucket_state_active) {
                return {(wantsDeleted == WantsDeleted::Yes) ? v : nullptr,
                        std::move(res.lock)};
            }

            // queueDirty only allowed on active VB
            if (queueExpired == QueueExpired::Yes &&
                getState() == vbucket_state_active) {
                incExpirationStat(ExpireBy::Access);
                handlePreExpiry(res.lock, *v);
                VBNotifyCtx notifyCtx;
                std::tie(std::ignore, v, notifyCtx) =
                        processExpiredItem(res.lock, *v, cHandle);
                notifyNewSeqno(notifyCtx);
                doCollectionsStats(cHandle, notifyCtx);
            }
            return {(wantsDeleted == WantsDeleted::Yes) ? v : nullptr,
                    std::move(res.lock)};
        }
    }
    return {v, std::move(res.lock)};
}

VBucket::FetchForWriteResult VBucket::fetchValueForWrite(
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        QueueExpired queueExpired) {
    if (queueExpired == QueueExpired::Yes && !cHandle.valid()) {
        throw std::invalid_argument(
                "VBucket::fetchValueForWrite cannot queue "
                "expired items for invalid collection");
    }

    auto res = ht.findForWrite(cHandle.getKey(), WantsDeleted::Yes);
    if (!res.storedValue) {
        // No item found.
        return {FetchForWriteResult::Status::OkVacant, {}, std::move(res.lock)};
    }

    auto* sv = res.storedValue;

    if (sv->isPending()) {
        // Attempted to write and found a Pending SyncWrite. Cannot write until
        // the in-flight one has completed.
        return {FetchForWriteResult::Status::ESyncWriteInProgress, nullptr, {}};
    }

    if (sv->isDeleted()) {
        // If we got a deleted value then can return directly (and skip
        // expiration checks because deleted items are not subject
        // to expiration).
        return {FetchForWriteResult::Status::OkFound, sv, std::move(res.lock)};
    }

    if (sv->isTempItem()) {
        // No expiry check needed for temps.
        return {FetchForWriteResult::Status::OkFound, sv, std::move(res.lock)};
    }

    if (!sv->isExpired(ep_real_time())) {
        // Not expired, good to return as-is.
        return {FetchForWriteResult::Status::OkFound, sv, std::move(res.lock)};
    }

    // Expired - but queueDirty only allowed on active VB
    if ((getState() == vbucket_state_active) &&
        (queueExpired == QueueExpired::Yes)) {
        incExpirationStat(ExpireBy::Access);
        handlePreExpiry(res.lock, *sv);
        VBNotifyCtx notifyCtx;
        std::tie(std::ignore, sv, notifyCtx) =
                processExpiredItem(res.lock, *sv, cHandle);
        notifyNewSeqno(notifyCtx);
        doCollectionsStats(cHandle, notifyCtx);
    }

    // Item found (but now deleted via expiration).
    return {FetchForWriteResult::Status::OkFound, sv, std::move(res.lock)};
}

HashTable::FindResult VBucket::fetchPreparedValue(
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto res = ht.findForWrite(cHandle.getKey(), WantsDeleted::Yes);
    if (res.storedValue && res.storedValue->isPending()) {
        return res;
    }
    return {nullptr, std::move(res.lock)};
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

MutationStatus VBucket::setFromInternal(const Item& itm) {
    if (!hasMemoryForStoredValue(stats, itm, UseActiveVBMemThreshold::Yes)) {
        return MutationStatus::NoMem;
    }
    ht.rollbackItem(itm);
    return MutationStatus::WasClean;
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
        eviction == EvictionPolicy::Value) {
        // We're VE, if we don't have, we don't have it.
        storeIfStatus = cb::StoreIfStatus::Continue;
    }

    return storeIfStatus;
}

ENGINE_ERROR_CODE VBucket::set(
        Item& itm,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        cb::StoreIfPredicate predicate,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto ret = checkDurabilityRequirements(itm);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    bool cas_op = (itm.getCas() != 0);

    { // HashBucketLock scope
        auto htRes = ht.findForUpdate(itm.getKey());
        auto* v = htRes.selectSVToModify(itm);
        auto& hbl = htRes.getHBL();

        cb::StoreIfStatus storeIfStatus = cb::StoreIfStatus::Continue;
        if (predicate && (storeIfStatus = callPredicate(predicate, v)) ==
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
        // if we're full-eviction with a CAS operation or a have a predicate
        // that requires the item's info
        if ((v == nullptr || v->isTempInitialItem()) &&
            (eviction == EvictionPolicy::Full) &&
            (cas_op || storeIfStatus == cb::StoreIfStatus::GetItemInfo)) {
            // Check Bloomfilter's prediction
            if (!maybeKeyExistsInFilter(itm.getKey())) {
                maybeKeyExists = false;
            }
        }

        PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
        VBQueueItemCtx queueItmCtx;
        if (itm.isPending()) {
            queueItmCtx.durability =
                    DurabilityItemCtx{itm.getDurabilityReqs(), cookie};
        }
        queueItmCtx.preLinkDocumentContext = &preLinkDocumentContext;
        MutationStatus status;
        boost::optional<VBNotifyCtx> notifyCtx;
        std::tie(status, notifyCtx) = processSet(htRes,
                                                 v,
                                                 itm,
                                                 itm.getCas(),
                                                 /*allowExisting*/ true,
                                                 /*hashMetaData*/ false,
                                                 queueItmCtx,
                                                 storeIfStatus,
                                                 maybeKeyExists);

        // For pending SyncWrites we initially return ENGINE_SYNC_WRITE_PENDING;
        // will notify client when request is committed / aborted later. This is
        // effectively EWOULDBLOCK, but needs to be distinguishable by the
        // ep-engine caller (storeIfInner) from EWOULDBLOCK for bg-fetch
        ret = itm.isPending() ? ENGINE_SYNC_WRITE_PENDING : ENGINE_SUCCESS;
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
            doCollectionsStats(cHandle, *notifyCtx);

            itm.setBySeqno(v->getBySeqno());
            itm.setCas(v->getCas());
            break;
        case MutationStatus::NeedBgFetch: { // CAS operation with non-resident
                                            // item
            // +
            // full eviction.
            if (v) {
                // temp item is already created. Simply schedule a bg fetch job
                hbl.getHTLock().unlock();
                bgFetch(itm.getKey(), cookie, engine, true);
                return ENGINE_EWOULDBLOCK;
            }
            ret = addTempItemAndBGFetch(
                    hbl, itm.getKey(), cookie, engine, true);
            break;
        }

        case MutationStatus::IsPendingSyncWrite:
            ret = ENGINE_SYNC_WRITE_IN_PROGRESS;
            break;
        }
    }

    return ret;
}

ENGINE_ERROR_CODE VBucket::replace(
        Item& itm,
        const void* cookie,
        EventuallyPersistentEngine& engine,
        cb::StoreIfPredicate predicate,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    auto ret = checkDurabilityRequirements(itm);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    { // HashBucketLock scope
        auto htRes = ht.findForUpdate(itm.getKey());
        auto& hbl = htRes.getHBL();

        // If a pending SV was found and it's not yet complete, then we cannot
        // perform another mutation while the first is in progress.
        if (htRes.pending && !htRes.pending->isCompleted()) {
            return ENGINE_SYNC_WRITE_IN_PROGRESS;
        }

        cb::StoreIfStatus storeIfStatus = cb::StoreIfStatus::Continue;
        if (predicate &&
            (storeIfStatus = callPredicate(predicate, htRes.committed)) ==
                    cb::StoreIfStatus::Fail) {
            return ENGINE_PREDICATE_FAILED;
        }

        if (htRes.committed) {
            if (isLogicallyNonExistent(*htRes.committed, cHandle) ||
                htRes.committed->isExpired(ep_real_time())) {
                ht.cleanupIfTemporaryItem(hbl, *htRes.committed);
                return ENGINE_KEY_ENOENT;
            }

            auto* v = htRes.selectSVToModify(itm);
            MutationStatus mtype;
            boost::optional<VBNotifyCtx> notifyCtx;
            if (eviction == EvictionPolicy::Full &&
                htRes.committed->isTempInitialItem()) {
                mtype = MutationStatus::NeedBgFetch;
            } else {
                PreLinkDocumentContext preLinkDocumentContext(
                        engine, cookie, &itm);
                VBQueueItemCtx queueItmCtx;
                queueItmCtx.preLinkDocumentContext = &preLinkDocumentContext;
                if (itm.isPending()) {
                    queueItmCtx.durability =
                            DurabilityItemCtx{itm.getDurabilityReqs(), cookie};
                }
                std::tie(mtype, notifyCtx) = processSet(htRes,
                                                        v,
                                                        itm,
                                                        0,
                                                        /*allowExisting*/ true,
                                                        /*hasMetaData*/ false,
                                                        queueItmCtx,
                                                        storeIfStatus);
            }

            // For pending SyncWrites we initially return
            // ENGINE_SYNC_WRITE_PENDING; will notify client when request is
            // committed / aborted later. This is effectively EWOULDBLOCK, but
            // needs to be distinguishable by the ep-engine caller
            // (storeIfInner) from EWOULDBLOCK for bg-fetch
            ret = itm.isPending() ? ENGINE_SYNC_WRITE_PENDING : ENGINE_SUCCESS;
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
                doCollectionsStats(cHandle, *notifyCtx);

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
        } else {
            if (eviction == EvictionPolicy::Value) {
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

    return ret;
}

void VBucket::addDurabilityMonitorStats(const AddStatFn& addStat,
                                        const void* cookie) const {
    durabilityMonitor->addStats(addStat, cookie);
}

void VBucket::dumpDurabilityMonitor(std::ostream& os) const {
    os << *durabilityMonitor;
}

ENGINE_ERROR_CODE VBucket::prepare(
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
    auto htRes = ht.findForUpdate(itm.getKey());
    auto* v = htRes.pending.getSV();
    auto& hbl = htRes.getHBL();
    bool maybeKeyExists = true;
    MutationStatus status;
    boost::optional<VBNotifyCtx> notifyCtx;
    VBQueueItemCtx queueItmCtx{
            genBySeqno,
            genCas,
            GenerateDeleteTime::No,
            TrackCasDrift::Yes,
            DurabilityItemCtx{itm.getDurabilityReqs(), cookie},
            nullptr /* No pre link step needed */,
            {} /*overwritingPrepareSeqno*/};

    if (v && v->getBySeqno() <= allowedDuplicatePrepareThreshold) {
        // Valid duplicate prepare - call processSetInner and skip the
        // SyncWrite checks.
        queueItmCtx.overwritingPrepareSeqno = v->getBySeqno();
        std::tie(status, notifyCtx) = processSetInner(htRes,
                                                      v,
                                                      itm,
                                                      cas,
                                                      allowExisting,
                                                      true,
                                                      queueItmCtx,
                                                      {/*no predicate*/},
                                                      maybeKeyExists);
    } else {
        // Not a valid duplicate prepare, call processSet and hit the SyncWrite
        // checks.
        std::tie(status, notifyCtx) = processSet(htRes,
                                                 v,
                                                 itm,
                                                 cas,
                                                 allowExisting,
                                                 true,
                                                 queueItmCtx,
                                                 {},
                                                 maybeKeyExists);
    }

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
                    "VBucket::prepare: "
                    "StoredValue should not be null if status WasClean");
        }
        if (seqno) {
            *seqno = static_cast<uint64_t>(v->getBySeqno());
        }
        // we unlock ht lock here because we want to avoid potential lock
        // inversions arising from notifyNewSeqno() call
        hbl.getHTLock().unlock();
        notifyNewSeqno(*notifyCtx);
        doCollectionsStats(cHandle, *notifyCtx);
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
    auto htRes = ht.findForUpdate(itm.getKey());
    auto* v = htRes.selectSVToModify(itm);
    auto& hbl = htRes.getHBL();
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
        if (eviction == EvictionPolicy::Full) {
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

    // MB-33919: Do not generate the delete-time - delete's can come through
    // this path and the delete time from the input should be used (unless it
    // is 0, where it must be regenerated)
    VBQueueItemCtx queueItmCtx{
            genBySeqno,
            genCas,
            GenerateDeleteTime::No,
            TrackCasDrift::Yes,
            DurabilityItemCtx{itm.getDurabilityReqs(), cookie},
            nullptr /* No pre link step needed */,
            {} /*overwritingPrepareSeqno*/};

    MutationStatus status;
    boost::optional<VBNotifyCtx> notifyCtx;
    std::tie(status, notifyCtx) = processSet(htRes,
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
        doCollectionsStats(cHandle, *notifyCtx);
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
        boost::optional<cb::durability::Requirements> durability,
        ItemMetaData* itemMeta,
        mutation_descr_t& mutInfo,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    if (durability && durability->isValid()) {
        if (!isValidDurabilityLevel(durability->getLevel())) {
            return ENGINE_DURABILITY_INVALID_LEVEL;
        }

        if (!getActiveDM().isDurabilityPossible()) {
            return ENGINE_DURABILITY_IMPOSSIBLE;
        }
    }

    // For pending SyncDeletes we initially return ENGINE_SYNC_WRITE_PENDING;
    // will notify client when request is committed / aborted later. This is
    // effectively EWOULDBLOCK, but needs to be distinguishable by the
    // ep-engine caller (itemDelete) from EWOULDBLOCK for bg-fetch
    auto ret = durability ? ENGINE_SYNC_WRITE_PENDING : ENGINE_SUCCESS;

    { // HashBucketLock scope
        auto htRes = ht.findForUpdate(cHandle.getKey());
        auto& hbl = htRes.getHBL();

        if (htRes.pending && htRes.pending->isPending()) {
            // Existing item is an in-flight SyncWrite
            return ENGINE_SYNC_WRITE_IN_PROGRESS;
        }

        // When deleting an item, always use the StoredValue which is committed
        // for the various logic checks.
        if (!htRes.committed || htRes.committed->isTempInitialItem() ||
            isLogicallyNonExistent(*htRes.committed, cHandle)) {
            if (eviction == EvictionPolicy::Value) {
                return ENGINE_KEY_ENOENT;
            } else { // Full eviction.
                if (!htRes.committed) { // Item might be evicted from cache.
                    if (maybeKeyExistsInFilter(cHandle.getKey())) {
                        return addTempItemAndBGFetch(
                                hbl, cHandle.getKey(), cookie, engine, true);
                    } else {
                        // As bloomfilter predicted that item surely doesn't
                        // exist on disk, return ENOENT for deleteItem().
                        return ENGINE_KEY_ENOENT;
                    }
                } else if (htRes.committed->isTempInitialItem()) {
                    hbl.getHTLock().unlock();
                    bgFetch(cHandle.getKey(), cookie, engine, true);
                    return ENGINE_EWOULDBLOCK;
                } else { // Non-existent or deleted key.
                    if (htRes.committed->isTempNonExistentItem() ||
                        htRes.committed->isTempDeletedItem()) {
                        // Delete a temp non-existent item to ensure that
                        // if a delete were issued over an item that doesn't
                        // exist, then we don't preserve a temp item.
                        deleteStoredValue(hbl, *htRes.committed);
                    }
                    return ENGINE_KEY_ENOENT;
                }
            }
        }

        if (htRes.committed->isLocked(ep_current_time()) &&
            (getState() == vbucket_state_replica ||
             getState() == vbucket_state_pending)) {
            htRes.committed->unlock();
        }

        if (itemMeta != nullptr) {
            itemMeta->cas = htRes.committed->getCas();
        }

        MutationStatus delrv;
        boost::optional<VBNotifyCtx> notifyCtx;

        // Determine which of committed / prepared SV to modify.
        auto* v = htRes.selectSVToModify(durability.is_initialized());

        if (htRes.committed->isExpired(ep_real_time())) {
            std::tie(delrv, v, notifyCtx) =
                    processExpiredItem(hbl, *htRes.committed, cHandle);
        } else {
            ItemMetaData metadata;
            metadata.revSeqno = htRes.committed->getRevSeqno() + 1;
            VBQueueItemCtx queueItmCtx;
            if (durability) {
                queueItmCtx.durability = DurabilityItemCtx{*durability, cookie};
            }
            std::tie(delrv, v, notifyCtx) =
                    processSoftDelete(htRes,
                                      *v,
                                      cas,
                                      metadata,
                                      queueItmCtx,
                                      /*use_meta*/ false,
                                      /*bySeqno*/ v->getBySeqno(),
                                      DeleteSource::Explicit);
        }

        uint64_t seqno = 0;

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
            doCollectionsStats(cHandle, *notifyCtx);
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
            // We already figured out if a bg fetch is requred for a
            // full-evicted item above.
            throw std::logic_error(
                    "VBucket::deleteItem: "
                    "Unexpected NEEDS_BG_FETCH from processSoftDelete");

        case MutationStatus::IsPendingSyncWrite:
            ret = ENGINE_SYNC_WRITE_IN_PROGRESS;
            break;
        }
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
        GenerateBySeqno genBySeqno,
        GenerateCas generateCas,
        uint64_t bySeqno,
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        DeleteSource deleteSource) {
    const auto& key = cHandle.getKey();
    auto htRes = ht.findForUpdate(key);
    auto* v = htRes.selectSVToModify(false);
    auto& hbl = htRes.pending.getHBL();

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
                auto rv = addTempStoredValue(hbl, key);
                if (rv.status == TempAddStatus::NoMem) {
                    return ENGINE_ENOMEM;
                }
                v = rv.storedValue;
                v->setTempDeleted();
            }
        }
    } else {
        if (!v) {
            // We should always try to persist a delete here.
            auto rv = addTempStoredValue(hbl, key);
            if (rv.status == TempAddStatus::NoMem) {
                return ENGINE_ENOMEM;
            }
            v = rv.storedValue;
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
        if (eviction == EvictionPolicy::Full) {
            delrv = MutationStatus::NeedBgFetch;
        } else {
            delrv = MutationStatus::NotFound;
        }
    } else if (mcbp::datatype::is_xattr(v->getDatatype()) && !v->isResident()) {
        // MB-25671: A temp deleted xattr with no value must be fetched before
        // the deleteWithMeta can be applied.
        // MB-36087: Any non-resident value
        delrv = MutationStatus::NeedBgFetch;
        metaBgFetch = false;
    } else {
        // MB-33919: The incoming meta.exptime should be used as the delete-time
        // so request GenerateDeleteTime::No, if the incoming value is 0, a new
        // delete-time will be generated.
        VBQueueItemCtx queueItmCtx{genBySeqno,
                                   generateCas,
                                   GenerateDeleteTime::No,
                                   TrackCasDrift::Yes,
                                   {},
                                   nullptr /* No pre link step needed */,
                                   {} /*overwritingPrepareSeqno*/};

        std::unique_ptr<Item> itm;
        if (getState() == vbucket_state_active &&
            mcbp::datatype::is_xattr(v->getDatatype()) &&
            (itm = pruneXattrDocument(*v, itemMeta))) {
            // A new item has been generated and must be given a new seqno
            queueItmCtx.genBySeqno = GenerateBySeqno::Yes;

            // MB-36101: The result should always be a deleted item
            itm->setDeleted();
            std::tie(v, delrv, notifyCtx) =
                    updateStoredValue(hbl, *v, *itm, queueItmCtx);
        } else {
            // system xattrs must remain, however no need to prune xattrs if
            // this is a replication call (i.e. not to an active vbucket),
            // the active has done this and we must just store what we're
            // given.
            std::tie(delrv, v, notifyCtx) = processSoftDelete(htRes,
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
        doCollectionsStats(cHandle, *notifyCtx);
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
    if (it.isPending()) {
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
            doCollectionsStats(cHandle, notifyCtx);
        }
    } else {
        if (eviction == EvictionPolicy::Full) {
            // Create a temp item and delete and push it
            // into the checkpoint queue, only if the bloomfilter
            // predicts that the item may exist on disk.
            if (maybeKeyExistsInFilter(key)) {
                auto addTemp = addTempStoredValue(hbl, key);
                if (addTemp.status == TempAddStatus::NoMem) {
                    return;
                }
                v = addTemp.storedValue;
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
                doCollectionsStats(cHandle, notifyCtx);
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
    auto ret = checkDurabilityRequirements(itm);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    { // HashBucketLock scope
        auto htRes = ht.findForUpdate(itm.getKey());
        auto* v = htRes.selectSVToModify(itm);
        auto& hbl = htRes.getHBL();

        if (htRes.pending && htRes.pending->isPending()) {
            // If an existing item was found and it is prepared, then cannot
            // (yet) perform an Add (Add would only succeed if prepared
            // SyncWrite was subsequently aborted).
            return ENGINE_SYNC_WRITE_IN_PROGRESS;
        }

        bool maybeKeyExists = true;
        if ((v == nullptr || v->isTempInitialItem()) &&
            (eviction == EvictionPolicy::Full)) {
            // Check bloomfilter's prediction
            if (!maybeKeyExistsInFilter(itm.getKey())) {
                maybeKeyExists = false;
            }
        }

        PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
        VBQueueItemCtx queueItmCtx;
        queueItmCtx.preLinkDocumentContext = &preLinkDocumentContext;
        if (itm.isPending()) {
            queueItmCtx.durability =
                    DurabilityItemCtx{itm.getDurabilityReqs(), cookie};
        }
        AddStatus status;
        boost::optional<VBNotifyCtx> notifyCtx;
        std::tie(status, notifyCtx) =
                processAdd(htRes, v, itm, maybeKeyExists, queueItmCtx, cHandle);

        switch (status) {
        case AddStatus::NoMem:
            return ENGINE_ENOMEM;
        case AddStatus::Exists:
            return ENGINE_NOT_STORED;
        case AddStatus::AddTmpAndBgFetch:
            return addTempItemAndBGFetch(
                    hbl, itm.getKey(), cookie, engine, true);
        case AddStatus::BgFetch:
            hbl.getHTLock().unlock();
            bgFetch(itm.getKey(), cookie, engine, true);
            return ENGINE_EWOULDBLOCK;
        case AddStatus::Success:
        case AddStatus::UnDel:
            Expects(v &&
                    "VBucket::add: Expect a non-null StoredValue upon Success "
                    "or Undel result");
            notifyNewSeqno(*notifyCtx);
            doCollectionsStats(cHandle, *notifyCtx);
            itm.setBySeqno(v->getBySeqno());
            itm.setCas(v->getCas());
            break;
        }
    }

    // For pending SyncWrites we initially return ENGINE_SYNC_WRITE_PENDING;
    // will notify client when request is committed / aborted later. This is
    // effectively EWOULDBLOCK, but needs to be distinguishable by the
    // ep-engine caller (storeIfInner) from EWOULDBLOCK for bg-fetch
    return itm.isPending() ? ENGINE_SYNC_WRITE_PENDING : ENGINE_SUCCESS;
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

            auto committedState = v->getCommitted();

            Expects(committedState == CommittedState::CommittedViaMutation ||
                    committedState == CommittedState::CommittedViaPrepare);

            if (committedState == CommittedState::CommittedViaPrepare) {
                // we are updating an item which was set through a sync write
                // we should not queueDirty a queue_op::commit_sync_write
                // because this touch op is *not* a sync write, and doesn't
                // even support durability. queueDirty expects durability reqs
                // for a commit, as a real commit would have a prepareSeqno.
                // Change the committed state to reflect that the new
                // value is from a non-sync write op.
                v->setCommitted(CommittedState::CommittedViaMutation);
            }
        }

        const auto hideLockedCas = (v->isLocked(ep_current_time())
                                            ? StoredValue::HideLockedCas::Yes
                                            : StoredValue::HideLockedCas::No);
        GetValue rv(v->toItem(getId(), hideLockedCas), ENGINE_SUCCESS, bySeqNo);

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
            doCollectionsStats(cHandle, notifyCtx);
        }

        return {MutationStatus::WasClean, std::move(rv)};
    } else {
        if (eviction == EvictionPolicy::Value) {
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
    auto res = fetchValueForWrite(cHandle, QueueExpired::Yes);
    switch (res.status) {
    case FetchForWriteResult::Status::OkFound:
    case FetchForWriteResult::Status::OkVacant: {
        // In both OkFound and OkVacent, call processGetAndUpdateTtl - even
        // if currently vacant it might exist after bgfetch.
        GetValue gv;
        MutationStatus status;
        std::tie(status, gv) = processGetAndUpdateTtl(
                res.lock, res.storedValue, exptime, cHandle);

        if (status == MutationStatus::NeedBgFetch) {
            if (res.storedValue) {
                bgFetch(cHandle.getKey(), cookie, engine);
                return GetValue(nullptr,
                                ENGINE_EWOULDBLOCK,
                                res.storedValue->getBySeqno());
            } else {
                ENGINE_ERROR_CODE ec = addTempItemAndBGFetch(
                        res.lock, cHandle.getKey(), cookie, engine, false);
                return GetValue(NULL, ec, -1, true);
            }
        }
        return gv;
    }
    case FetchForWriteResult::Status::ESyncWriteInProgress:
        return GetValue(nullptr, ENGINE_SYNC_WRITE_IN_PROGRESS);
    }
    folly::assume_unreachable();
}

GetValue VBucket::getInternal(
        const void* cookie,
        EventuallyPersistentEngine& engine,
        get_options_t options,
        GetKeyOnly getKeyOnly,
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        const ForGetReplicaOp getReplicaItem) {
    const TrackReference trackReference = (options & TRACK_REFERENCE)
                                                  ? TrackReference::Yes
                                                  : TrackReference::No;
    const bool metadataOnly = (options & ALLOW_META_ONLY);
    const bool getDeletedValue = (options & GET_DELETED_VALUE);
    const bool bgFetchRequired = (options & QUEUE_BG_FETCH);

    auto res = fetchValidValue(WantsDeleted::Yes,
                               trackReference,
                               QueueExpired::Yes,
                               cHandle,
                               getReplicaItem);

    auto* v = res.storedValue;
    if (v) {
        // If the fetched value is a Prepared SyncWrite which may already have
        // been made visible to clients, then we cannot yet report _any_
        // value for this key until the Prepare has bee re-committed.
        if (v->isPreparedMaybeVisible()) {
            return GetValue(nullptr, ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS);
        }

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
                deleteStoredValue(res.lock, *v);
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

        std::unique_ptr<Item> item;
        if (getKeyOnly == GetKeyOnly::Yes) {
            item = v->toItem(getId(),
                             StoredValue::HideLockedCas::No,
                             StoredValue::IncludeValue::No);
        } else {
            const auto hideLockedCas =
                    ((options & HIDE_LOCKED_CAS) &&
                                     v->isLocked(ep_current_time())
                             ? StoredValue::HideLockedCas::Yes
                             : StoredValue::HideLockedCas::No);
            item = v->toItem(getId(), hideLockedCas);
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
        if (!getDeletedValue && (eviction == EvictionPolicy::Value)) {
            return GetValue();
        }

        if (maybeKeyExistsInFilter(cHandle.getKey())) {
            ENGINE_ERROR_CODE ec = ENGINE_EWOULDBLOCK;
            if (bgFetchRequired) { // Full eviction and need a bg fetch.
                ec = addTempItemAndBGFetch(res.lock,
                                           cHandle.getKey(),
                                           cookie,
                                           engine,
                                           metadataOnly);
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
    auto htRes = ht.findForRead(
            cHandle.getKey(), TrackReference::Yes, WantsDeleted::Yes);
    auto* v = htRes.storedValue;
    auto& hbl = htRes.lock;

    if (v) {
        if (v->isPreparedMaybeVisible()) {
            return ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS;
        }
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
    auto res = fetchValidValue(
            WantsDeleted::Yes, TrackReference::Yes, QueueExpired::Yes, cHandle);
    auto* v = res.storedValue;

    if (v) {
        if ((v->isDeleted() || cHandle.isLogicallyDeleted(v->getBySeqno())) &&
            wantsDeleted == WantsDeleted::No) {
            return ENGINE_KEY_ENOENT;
        }

        if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            deleteStoredValue(res.lock, *v);
            return ENGINE_KEY_ENOENT;
        }
        if (eviction == EvictionPolicy::Full && v->isTempInitialItem()) {
            res.lock.getHTLock().unlock();
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
        if (eviction == EvictionPolicy::Value) {
            return ENGINE_KEY_ENOENT;
        } else {
            if (maybeKeyExistsInFilter(cHandle.getKey())) {
                return addTempItemAndBGFetch(
                        res.lock, cHandle.getKey(), cookie, engine, true);
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
    auto res = fetchValueForWrite(cHandle, QueueExpired::Yes);
    switch (res.status) {
    case FetchForWriteResult::Status::OkFound: {
        auto* v = res.storedValue;
        if (isLogicallyNonExistent(*v, cHandle)) {
            ht.cleanupIfTemporaryItem(res.lock, *v);
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

        auto it = v->toItem(getId());
        it->setCas(nextHLCCas());
        v->setCas(it->getCas());

        return GetValue(std::move(it));
    }
    case FetchForWriteResult::Status::OkVacant:
        // No value found in the hashtable.
        switch (eviction) {
        case EvictionPolicy::Value:
            return GetValue(NULL, ENGINE_KEY_ENOENT);

        case EvictionPolicy::Full:
            if (maybeKeyExistsInFilter(cHandle.getKey())) {
                ENGINE_ERROR_CODE ec = addTempItemAndBGFetch(
                        res.lock, cHandle.getKey(), cookie, engine, false);
                return GetValue(NULL, ec, -1, true);
            } else {
                // As bloomfilter predicted that item surely doesn't exist
                // on disk, return ENOENT for getLocked().
                return GetValue(NULL, ENGINE_KEY_ENOENT);
            }
        }
        folly::assume_unreachable();
    case FetchForWriteResult::Status::ESyncWriteInProgress:
        return GetValue(nullptr, ENGINE_SYNC_WRITE_IN_PROGRESS);
    }
    folly::assume_unreachable();
}

void VBucket::deletedOnDiskCbk(const Item& queuedItem, bool deleted) {
    auto res = ht.findItem(queuedItem);
    auto* v = res.storedValue;

    // Delete the item in the hash table iff:
    //  1. Item is existent in hashtable, and deleted flag is true
    //  2. rev seqno of queued item matches rev seqno of hash table item
    if (v && v->isDeleted() && (queuedItem.getRevSeqno() == v->getRevSeqno())) {
        bool isDeleted = deleteStoredValue(res.lock, *v);
        if (!isDeleted) {
            throw std::logic_error(
                    "deletedOnDiskCbk:callback: "
                    "Failed to delete key with seqno:" +
                    std::to_string(v->getBySeqno()) + "' from bucket " +
                    std::to_string(res.lock.getBucketNum()));
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
         * MB-30137: Decrement the total number of on-disk items. This needs to
         * be done to ensure that the item count is accurate in the case of full
         * eviction. We should only decrement the counter for committed (via
         * mutation or commit) items as we only increment for these.
         */
        if (v && queuedItem.isCommitted()) {
            decrNumTotalItems();
        }
    }
    doStatsForFlushing(queuedItem, queuedItem.size());
    --stats.diskQueueSize;
    decrMetaDataDisk(queuedItem);
}

bool VBucket::removeItemFromMemory(const Item& item) {
    auto htRes = ht.findItem(item);
    if (!htRes.storedValue) {
        return false;
    }
    return deleteStoredValue(htRes.lock, *htRes.storedValue);
}

void VBucket::postProcessRollback(const RollbackResult& rollbackResult,
                                  uint64_t prevHighSeqno) {
    failovers->pruneEntries(rollbackResult.highSeqno);
    checkpointManager->clear(*this, rollbackResult.highSeqno);
    setPersistedSnapshot(
            {rollbackResult.snapStartSeqno, rollbackResult.snapEndSeqno});
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
        auto stats =
                kvstore.getCollectionStats(*kvstoreContext, collection.first);
        collection.second.setDiskCount(stats.itemCount);
        collection.second.resetPersistedHighSeqno(stats.highSeqno);
        collection.second.resetHighSeqno(
                collection.second.getPersistedHighSeqno());
    }
}

void VBucket::dump() const {
    std::cerr << "VBucket[" << this << "] with state: " << toString(getState())
              << " numItems:" << getNumItems()
              << " numNonResident:" << getNumNonResidentItems()
              << " ht: " << std::endl << "  " << ht << std::endl
              << "]" << std::endl;
}

void VBucket::setMutationMemoryThreshold(size_t memThreshold) {
    if (memThreshold > 0 && memThreshold <= 100) {
        mutationMemThreshold = static_cast<double>(memThreshold) / 100.0;
    } else {
        throw std::invalid_argument(
                "VBucket::setMutationMemoryThreshold invalid memThreshold:" +
                std::to_string(memThreshold));
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

void VBucket::_addStats(VBucketStatsDetailLevel detail,
                        const AddStatFn& add_stat,
                        const void* c) {
    switch (detail) {
    case VBucketStatsDetailLevel::Full: {
        size_t numItems = getNumItems();
        size_t tempItems = getNumTempItems();
        addStat("num_items", numItems, add_stat, c);
        addStat("num_temp_items", tempItems, add_stat, c);
        addStat("num_non_resident", getNumNonResidentItems(), add_stat, c);
        addStat("num_prepared_sync_writes",
                ht.getNumPreparedSyncWrites(),
                add_stat,
                c);
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
        addStat("queue_memory", dirtyQueueMem.load(), add_stat, c);
        addStat("queue_fill", dirtyQueueFill.load(), add_stat, c);
        addStat("queue_drain", dirtyQueueDrain.load(), add_stat, c);
        addStat("queue_age", getQueueAge(), add_stat, c);
        addStat("pending_writes", dirtyQueuePendingWrites.load(), add_stat, c);

        addStat("uuid", failovers->getLatestUUID(), add_stat, c);
        addStat("purge_seqno", getPurgeSeqno(), add_stat, c);
        addStat("bloom_filter", getFilterStatusString().data(), add_stat, c);
        addStat("bloom_filter_size", getFilterSize(), add_stat, c);
        addStat("bloom_filter_key_count", getNumOfKeysInFilter(), add_stat, c);
        addStat("rollback_item_count", getRollbackItemCount(), add_stat, c);
        addStat("hp_vb_req_size", getHighPriorityChkSize(), add_stat, c);
        addStat("might_contain_xattrs", mightContainXattrs(), add_stat, c);
        addStat("max_deleted_revid", ht.getMaxDeletedRevSeqno(), add_stat, c);

        addStat("high_completed_seqno", getHighCompletedSeqno(), add_stat, c);
        addStat("sync_write_accepted_count",
                getSyncWriteAcceptedCount(),
                add_stat,
                c);
        addStat("sync_write_committed_count",
                getSyncWriteCommittedCount(),
                add_stat,
                c);
        addStat("sync_write_aborted_count",
                getSyncWriteAbortedCount(),
                add_stat,
                c);

        hlc.addStats(statPrefix, add_stat, c);
    }
        // fallthrough
    case VBucketStatsDetailLevel::Durability:
        addStat("high_seqno", getHighSeqno(), add_stat, c);
        addStat("topology", getReplicationTopology().dump(), add_stat, c);
        addStat("high_prepared_seqno", getHighPreparedSeqno(), add_stat, c);
        // fallthrough
    case VBucketStatsDetailLevel::State:
        // adds the vbucket state stat (unnamed stat)
        addStat(NULL, toString(state), add_stat, c);
        break;
    case VBucketStatsDetailLevel::PreviousState:
        throw std::invalid_argument(
                "VBucket::_addStats: unexpected detail level");
        break;
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
        HashTable::FindUpdateResult& htRes,
        StoredValue*& v,
        Item& itm,
        uint64_t cas,
        bool allowExisting,
        bool hasMetaData,
        const VBQueueItemCtx& queueItmCtx,
        cb::StoreIfStatus storeIfStatus,
        bool maybeKeyExists) {
    if (v && v->isPending()) {
        // It is not valid for an active vBucket to attempt to overwrite an
        // in flight SyncWrite. If this vBucket is not active, we are
        // allowed to overwrite an in flight SyncWrite iff we are receiving
        // a disk snapshot. This is due to disk based de-dupe that allows
        // only 1 value per key. In this case, the active node may send a
        // mutation instead of a commit if it knows that this replica may be
        // missing a prepare. This code allows this mutation to be accepted
        // and overwrites the existing prepare.
        if (getState() == vbucket_state_active || !isReceivingDiskSnapshot()) {
            return {MutationStatus::IsPendingSyncWrite, {}};
        }

        Expects(itm.isCommitted());
        getPassiveDM().completeSyncWrite(
                itm.getKey(),
                PassiveDurabilityMonitor::Resolution::Commit,
                v->getBySeqno() /* prepareSeqno */);

        // Deal with the already existing prepare
        processImplicitlyCompletedPrepare(htRes.pending);

        // Add a new or overwrite the existing mutation
        return processSetInner(htRes,
                               htRes.committed,
                               itm,
                               cas,
                               allowExisting,
                               hasMetaData,
                               queueItmCtx,
                               storeIfStatus,
                               maybeKeyExists);
    }

    return processSetInner(htRes,
                           v,
                           itm,
                           cas,
                           allowExisting,
                           hasMetaData,
                           queueItmCtx,
                           storeIfStatus,
                           maybeKeyExists);
}

std::pair<MutationStatus, boost::optional<VBNotifyCtx>>
VBucket::processSetInner(HashTable::FindUpdateResult& htRes,
                         StoredValue*& v,
                         Item& itm,
                         uint64_t cas,
                         bool allowExisting,
                         bool hasMetaData,
                         const VBQueueItemCtx& queueItmCtx,
                         cb::StoreIfStatus storeIfStatus,
                         bool maybeKeyExists) {
    if (!htRes.getHBL().getHTLock()) {
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
    if (eviction == EvictionPolicy::Full && maybeKeyExists &&
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
    // need to test cas and locking against the committed value
    // explicitly, as v may be a completed prepare (to be modified)
    // with a cas, deleted status, expiry etc. different from the committed
    auto* committed = htRes.committed;
    if (committed && committed->isExpired(ep_real_time()) && !hasMetaData &&
        !itm.isDeleted()) {
        if (committed->isLocked(ep_current_time())) {
            committed->unlock();
        }
        if (cas) {
            /* item has expired and cas value provided. Deny ! */
            return {MutationStatus::NotFound, {}};
        }
    }

    if (committed) {
        if (!allowExisting && !committed->isTempItem() &&
            !committed->isDeleted()) {
            return {MutationStatus::InvalidCas, {}};
        }
        if (committed->isLocked(ep_current_time())) {
            /*
             * item is locked, deny if there is cas value mismatch
             * or no cas value is provided by the user
             */
            if (cas != committed->getCas()) {
                return {MutationStatus::IsLocked, {}};
            }
            /* allow operation*/
            committed->unlock();
        } else if (cas && cas != committed->getCas()) {
            if (committed->isTempNonExistentItem()) {
                // This is a temporary item which marks a key as non-existent;
                // therefore specifying a non-matching CAS should be exposed
                // as item not existing.
                return {MutationStatus::NotFound, {}};
            }
            if ((committed->isTempDeletedItem() || committed->isDeleted()) &&
                !itm.isDeleted()) {
                // Existing item is deleted, and we are not replacing it with
                // a (different) deleted value - return not existing.
                return {MutationStatus::NotFound, {}};
            }
            // None of the above special cases; the existing item cannot be
            // modified with the specified CAS.
            return {MutationStatus::InvalidCas, {}};
        }
        if (!hasMetaData) {
            itm.setRevSeqno(committed->getRevSeqno() + 1);
            /* MB-23530: We must ensure that a replace operation (i.e.
             * set with a CAS) /fails/ if the old document is deleted; it
             * logically "doesn't exist". However, if the new value is deleted
             * this op is a /delete/ with a CAS and we must permit a
             * deleted -> deleted transition for Deleted Bodies.
             */
            if (cas &&
                (committed->isDeleted() || committed->isTempDeletedItem()) &&
                !itm.isDeleted()) {
                return {MutationStatus::NotFound, {}};
            }
        }
    } else if (cas != 0) {
        // if a cas has been specified but there is no committed item
        // the op should fail
        return {MutationStatus::NotFound, {}};
    }

    MutationStatus status;
    VBNotifyCtx notifyCtx;
    if (v) {
        // This is a new SyncWrite, we just want to add a new prepare unless we
        // still have a completed prepare (Ephemeral) which we should replace
        // instead.
        if (v->isCommitted() && !v->isCompleted() && itm.isPending()) {
            std::tie(v, notifyCtx) = addNewStoredValue(
                    htRes.getHBL(), itm, queueItmCtx, GenerateRevSeqno::No);
            // Add should always be clean
            status = MutationStatus::WasClean;
        } else {
            std::tie(v, status, notifyCtx) =
                    updateStoredValue(htRes.getHBL(), *v, itm, queueItmCtx);
        }
    } else {
        auto genRevSeqno = hasMetaData ? GenerateRevSeqno::No :
                           GenerateRevSeqno::Yes;
        std::tie(v, notifyCtx) = addNewStoredValue(
                htRes.getHBL(), itm, queueItmCtx, genRevSeqno);
        itm.setRevSeqno(v->getRevSeqno());
        status = MutationStatus::WasClean;
    }
    return {status, notifyCtx};
}

std::pair<AddStatus, boost::optional<VBNotifyCtx>> VBucket::processAdd(
        HashTable::FindUpdateResult& htRes,
        StoredValue*& v,
        Item& itm,
        bool maybeKeyExists,
        const VBQueueItemCtx& queueItmCtx,
        const Collections::VB::Manifest::CachingReadHandle& cHandle) {
    if (!htRes.getHBL().getHTLock()) {
        throw std::invalid_argument(
                "VBucket::processAdd: htLock not held for " +
                getId().to_string());
    }
    auto* committed = htRes.committed;
    // must specifically check the committed item here rather than v
    // as v may be a completed prepare (only in ephemeral).
    if (committed && !committed->isDeleted() &&
        !committed->isExpired(ep_real_time()) && !committed->isTempItem() &&
        !cHandle.isLogicallyDeleted(committed->getBySeqno())) {
        return {AddStatus::Exists, {}};
    }
    if (!hasMemoryForStoredValue(stats, itm)) {
        return {AddStatus::NoMem, {}};
    }

    std::pair<AddStatus, VBNotifyCtx> rv = {AddStatus::Success, {}};

    if (v) {
        if (v->isTempInitialItem() && eviction == EvictionPolicy::Full &&
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
                updateStoredValue(htRes.getHBL(), *v, itm, queueItmCtx);
    } else {
        if (itm.getBySeqno() != StoredValue::state_temp_init) {
            if (eviction == EvictionPolicy::Full && maybeKeyExists) {
                return {AddStatus::AddTmpAndBgFetch, VBNotifyCtx()};
            }
        }

        if (itm.getBySeqno() == StoredValue::state_temp_init) {
            /* A 'temp initial item' is just added to the hash table. It is
             not put on checkpoint manager or sequence list */
            v = ht.unlocked_addNewStoredValue(htRes.getHBL(), itm);
            updateRevSeqNoOfNewStoredValue(*v);
        } else {
            std::tie(v, rv.second) = addNewStoredValue(
                    htRes.getHBL(), itm, queueItmCtx, GenerateRevSeqno::Yes);
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
VBucket::processSoftDelete(HashTable::FindUpdateResult& htRes,
                           StoredValue& v,
                           uint64_t cas,
                           const ItemMetaData& metadata,
                           const VBQueueItemCtx& queueItmCtx,
                           bool use_meta,
                           uint64_t bySeqno,
                           DeleteSource deleteSource) {
    StoredValue* deleteValue = &v;
    if (v.isPending()) {
        // It is not valid for an active vBucket to attempt to overwrite an
        // in flight SyncWrite. If this vBucket is not active, we are
        // allowed to overwrite an in flight SyncWrite iff we are receiving
        // a disk snapshot. This is due to disk based de-dupe that allows
        // only 1 value per key. In this case, the active node may send a
        // mutation instead of a commit if it knows that this replica may be
        // missing a prepare. This code allows this mutation to be accepted
        // and overwrites the existing prepare.
        if (getState() == vbucket_state_active || !isReceivingDiskSnapshot()) {
            return {MutationStatus::IsPendingSyncWrite, &v, boost::none};
        }

        getPassiveDM().completeSyncWrite(
                StoredDocKey(v.getKey()),
                PassiveDurabilityMonitor::Resolution::Commit,
                v.getBySeqno() /* prepareSeqno */);

        if (htRes.committed) {
            // A committed value exists:
            // Firstly deal with the existing prepare
            processImplicitlyCompletedPrepare(htRes.pending);
            // Secondly proceed to delete the committed value
            deleteValue = htRes.committed;
        } else if (isReceivingDiskSnapshot()) {
            // No committed value, but we are processing a disk-snapshot
            // Must continue to create a delete so that we create an accurate
            // replica. htRes must no longer own the pending pointer as it's
            // going to be deleted below
            htRes.pending.release();
        }
    }

    return processSoftDeleteInner(htRes.getHBL(),
                                  *deleteValue,
                                  cas,
                                  metadata,
                                  queueItmCtx,
                                  use_meta,
                                  bySeqno,
                                  deleteSource);
}

std::tuple<MutationStatus, StoredValue*, boost::optional<VBNotifyCtx>>
VBucket::processSoftDeleteInner(const HashTable::HashBucketLock& hbl,
                                StoredValue& v,
                                uint64_t cas,
                                const ItemMetaData& metadata,
                                const VBQueueItemCtx& queueItmCtx,
                                bool use_meta,
                                uint64_t bySeqno,
                                DeleteSource deleteSource) {
    boost::optional<VBNotifyCtx> empty;
    if (v.isTempInitialItem() && eviction == EvictionPolicy::Full) {
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
    DeletionStatus delStatus;

    // SyncDeletes are special cases. We actually want to add a new prepare.
    if (queueItmCtx.durability) {
        auto requirements = boost::get<cb::durability::Requirements>(
                queueItmCtx.durability->requirementsOrPreparedSeqno);

        // @TODO potentially inefficient to recreate the item in the below
        // update/add cases. Could rework ht.unlocked_softDeleteStoredValue
        // to only mark CommittedViaPrepares as CommittedViaMutation and use
        // softDeletedStoredValue instead.
        if (v.isCompleted()) {
            auto itm = v.toItem(getId(),
                                StoredValue::HideLockedCas::No,
                                StoredValue::IncludeValue::No,
                                requirements);
            itm->setDeleted(DeleteSource::Explicit);
            // Empty, deleted value has datatype:RAW_BYTES
            itm->setDataType(PROTOCOL_BINARY_RAW_BYTES);
            // The StoredValue 'v' we are softDeleting could be an aborted
            // prepare - in which case we need to reset the item created to
            // be a pending (not aborted) SyncWrite.
            itm->setPendingSyncWrite(requirements);

            std::tie(newSv, std::ignore, notifyCtx) =
                    updateStoredValue(hbl, v, *itm, queueItmCtx);
            return std::make_tuple(rv, newSv, notifyCtx);
        }

        auto deletedPrepare =
                ht.unlocked_createSyncDeletePrepare(hbl, v, deleteSource);
        auto itm = deletedPrepare->toItem(getId(),
                                          StoredValue::HideLockedCas::No,
                                          StoredValue::IncludeValue::Yes,
                                          requirements);
        std::tie(newSv, notifyCtx) =
                addNewStoredValue(hbl, *itm, queueItmCtx, GenerateRevSeqno::No);
        return std::make_tuple(rv, newSv, notifyCtx);
    }

    std::tie(newSv, delStatus, notifyCtx) =
            softDeleteStoredValue(hbl,
                                  v,
                                  /*onlyMarkDeleted*/ false,
                                  queueItmCtx,
                                  bySeqno,
                                  deleteSource);

    switch (delStatus) {
    case DeletionStatus::Success:
        ht.updateMaxDeletedRevSeqno(metadata.revSeqno);
        return std::make_tuple(rv, newSv, notifyCtx);

    case DeletionStatus::IsPendingSyncWrite:
        return std::make_tuple(MutationStatus::IsPendingSyncWrite, &v, empty);
    }
    folly::assume_unreachable();
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

    if (v.isTempInitialItem() && eviction == EvictionPolicy::Full) {
        return std::make_tuple(MutationStatus::NeedBgFetch,
                               &v,
                               queueDirty(hbl, v, {} /*VBQueueItemCtx*/));
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
    DeletionStatus delStatus;
    std::tie(newSv, delStatus, notifyCtx) =
            softDeleteStoredValue(hbl,
                                  v,
                                  onlyMarkDeleted,
                                  VBQueueItemCtx{},
                                  v.getBySeqno(),
                                  DeleteSource::TTL);
    switch (delStatus) {
    case DeletionStatus::Success:
        ht.updateMaxDeletedRevSeqno(newSv->getRevSeqno() + 1);
        return std::make_tuple(MutationStatus::NotFound, newSv, notifyCtx);

    case DeletionStatus::IsPendingSyncWrite:
        return std::make_tuple(
                MutationStatus::IsPendingSyncWrite, newSv, VBNotifyCtx{});
    }
    folly::assume_unreachable();
}

bool VBucket::deleteStoredValue(const HashTable::HashBucketLock& hbl,
                                StoredValue& v) {
    if (!v.isDeleted() && v.isLocked(ep_current_time())) {
        return false;
    }

    /* StoredValue deleted here. If any other in-memory data structures are
       using the StoredValue intrusively then they must have handled the delete
       by this point */
    ht.unlocked_del(hbl, &v);
    return true;
}

VBucket::AddTempSVResult VBucket::addTempStoredValue(
        const HashTable::HashBucketLock& hbl, const DocKey& key) {
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
        return {TempAddStatus::NoMem, nullptr};
    }

    /* A 'temp initial item' is just added to the hash table. It is
       not put on checkpoint manager or sequence list */
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

    updateRevSeqNoOfNewStoredValue(*v);
    itm.setRevSeqno(v->getRevSeqno());
    v->setNRUValue(MAX_NRU_VALUE);

    return {TempAddStatus::BgFetch, v};
}

void VBucket::notifyNewSeqno(
        const VBNotifyCtx& notifyCtx) {
    if (newSeqnoCb) {
        newSeqnoCb->callback(getId(), notifyCtx);
    }
}

void VBucket::doCollectionsStats(
        const Collections::VB::Manifest::CachingReadHandle& cHandle,
        const VBNotifyCtx& notifyCtx) {
    cHandle.setHighSeqno(notifyCtx.bySeqno);

    if (notifyCtx.itemCountDifference == 1) {
        cHandle.incrementDiskCount();
    } else if (notifyCtx.itemCountDifference == -1) {
        cHandle.decrementDiskCount();
    }
}

void VBucket::doCollectionsStats(
        const Collections::VB::Manifest::ReadHandle& readHandle,
        CollectionID collection,
        const VBNotifyCtx& notifyCtx) {
    readHandle.setHighSeqno(collection, notifyCtx.bySeqno);

    if (notifyCtx.itemCountDifference == 1) {
        readHandle.incrementDiskCount(collection);
    } else if (notifyCtx.itemCountDifference == -1) {
        readHandle.decrementDiskCount(collection);
    }
}

void VBucket::doCollectionsStats(
        const Collections::VB::Manifest::WriteHandle& writeHandle,
        CollectionID collection,
        const VBNotifyCtx& notifyCtx) {
    writeHandle.setHighSeqno(collection, notifyCtx.bySeqno);
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
        auto rv = v.toItem(getId());
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
    Expects(v.isCommitted());
    return v.isDeleted() || v.isTempDeletedItem() ||
           v.isTempNonExistentItem() ||
           cHandle.isLogicallyDeleted(v.getBySeqno());
}

ENGINE_ERROR_CODE VBucket::seqnoAcknowledged(
        const folly::SharedMutex::ReadHolder& vbStateLock,
        const std::string& replicaId,
        uint64_t preparedSeqno) {
    // We may receive an ack after we have set a vBucket to dead during a
    // takeover; just ignore it.
    if (getState() == vbucket_state_dead) {
        return ENGINE_SUCCESS;
    }
    return getActiveDM().seqnoAckReceived(replicaId, preparedSeqno);
}

void VBucket::notifyPersistenceToDurabilityMonitor() {
    folly::SharedMutex::ReadHolder wlh(stateLock);

    if (state == vbucket_state_dead) {
        return;
    }

    durabilityMonitor->notifyLocalPersistence();
}

const DurabilityMonitor& VBucket::getDurabilityMonitor() const {
    return *durabilityMonitor;
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

ENGINE_ERROR_CODE VBucket::checkDurabilityRequirements(const Item& item) {
    if (item.isPending()) {
        if (!isValidDurabilityLevel(item.getDurabilityReqs().getLevel())) {
            return ENGINE_DURABILITY_INVALID_LEVEL;
        }

        if (!getActiveDM().isDurabilityPossible()) {
            return ENGINE_DURABILITY_IMPOSSIBLE;
        }
    }

    return ENGINE_SUCCESS;
}

void VBucket::removeAcksFromADM(const std::string& node) {
    if (state == vbucket_state_active) {
        getActiveDM().removedQueuedAck(node);
    }
}

void VBucket::removeAcksFromADM(
        const std::string& node,
        const folly::SharedMutex::WriteHolder& vbstateLock) {
    removeAcksFromADM(node);
}

void VBucket::removeAcksFromADM(
        const std::string& node,
        const folly::SharedMutex::ReadHolder& vbstateLock) {
    removeAcksFromADM(node);
}

void VBucket::setDuplicateSyncWriteWindow(uint64_t highSeqno) {
    setUpAllowedDuplicatePrepareThreshold();
}

void VBucket::setUpAllowedDuplicatePrepareThreshold() {
    const auto& pdm = getPassiveDM();
    // We should only see duplicates for prepares currently in trackedWrites
    // prepares which are not in trackedWrites are either
    //  - Completed: A new prepare would be valid anyway, as the item is
    //  completed
    //  - New: Added after this setup, and should not be followed by another
    //  prepare without an intervening Commit/Abort
    // As a sanity check, store the current highTrackedSeqno and assert that
    // any prepares attempting to replace an existing prepare have a seqno
    // less than or equal to this value.
    // If no SyncWrites are being tracked, nothing can be duplicated
    allowedDuplicatePrepareThreshold = pdm.getHighestTrackedSeqno();
}

void VBucket::addSyncWriteForRollback(const Item& item) {
    // Search the HashTable for an existing prepare, it should not exist.
    Expects(item.isPending());
    auto htRes = ht.findItem(item);
    Expects(!htRes.storedValue);
    ht.unlocked_addNewStoredValue(htRes.lock, item);
}

bool VBucket::isReceivingDiskSnapshot() const {
    return checkpointManager->isOpenCheckpointDisk();
}
