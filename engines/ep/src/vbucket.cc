/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vbucket.h"
#include "bgfetcher.h"
#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_filter.h"
#include "collections/vbucket_manifest_handles.h"
#include "conflict_resolution.h"
#include "dcp/dcpconnmap.h"
#include "doc_pre_expiry.h"
#include "durability/active_durability_monitor.h"
#include "durability/dead_durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ep_types.h"
#include "failover-table.h"
#include "hash_table.h"
#include "hash_table_stat_visitor.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "objectregistry.h"
#include "pre_link_document_context.h"
#include "rollback_result.h"
#include "stored_value_factories.h"
#include "vb_filter.h"
#include "vbucket_queue_item_ctx.h"
#include "vbucket_state.h"
#include <boost/range/adaptor/strided.hpp>
#include <folly/lang/Assume.h>
#include <gsl/gsl-lite.hpp>
#include <memcached/document_expired.h>
#include <memcached/protocol_binary.h>
#include <platform/atomic.h>
#include <platform/optional.h>
#include <statistics/cbstat_collector.h>
#include <utilities/logtags.h>
#include <vbucket_bgfetch_item.h>
#include <xattr/blob.h>
#include <functional>
#include <list>
#include <set>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

using namespace std::string_literals;

const SyncWriteTimeoutHandlerFactory NoopSyncWriteTimeoutFactory =
        [](VBucket&) {
            return std::make_unique<NoopEventDrivenDurabilityTimeout>();
        };

std::vector<VBucketFilter> VBucketFilter::split(size_t count) const {
    if (count == 0) {
        throw std::invalid_argument("VBucketFilter::split requires count != 0");
    }

    if (count == 1) {
        return {*this};
    }

    // Do not create more filters than there are acceptable vBuckets.
    count = std::min(acceptable.size(), count);

    std::vector<VBucketFilter> filters(count);

    auto filterIndex = 0;

    for (const Vbid& vbid : acceptable) {
        filters[filterIndex++].addVBucket(vbid);
        filterIndex %= count;
    }

    return filters;
}

VBucketFilter VBucketFilter::slice(size_t start, size_t stride) const {
    using namespace boost::adaptors;
    Expects(start < stride);
    Expects(start < acceptable.size());

    auto it = acceptable.begin();
    std::advance(it, start);

    VBucketFilter filter;
    for (auto vbid : std::make_pair(it, acceptable.end()) | strided(stride)) {
        filter.addVBucket(vbid);
    }
    return filter;
}

static bool isRange(std::set<Vbid>::const_iterator it,
                    const std::set<Vbid>::const_iterator& end,
                    size_t& length) {
    length = 0;
    for (Vbid val = *it;
         it != end &&
         Vbid(gsl::narrow_cast<Vbid::id_type>(val.get() + length)) == *it;
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
                auto last = it;
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

SeqnoPersistenceRequest::SeqnoPersistenceRequest(
        CookieIface* cookie, uint64_t seqno, std::chrono::milliseconds timeout)
    : cookie(cookie),
      seqno(seqno),
      start(cb::time::steady_clock::now()),
      timeout(timeout) {
}

SeqnoPersistenceRequest::~SeqnoPersistenceRequest() = default;

cb::time::steady_clock::duration SeqnoPersistenceRequest::getDuration(
        cb::time::steady_clock::time_point now) const {
    return start - now;
}

cb::time::steady_clock::time_point SeqnoPersistenceRequest::getDeadline()
        const {
    return start + timeout;
}

void SeqnoPersistenceRequest::expired() const {
    // do nothing
}

#if defined(linux) || defined(__linux__) || defined(__linux)
// One of the CV build fails due to htonl is defined as a macro:
// error: statement expression not allowed at file scope
#undef htonl
#endif

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
                 SyncWriteResolvedCallback syncWriteResolvedCb,
                 SyncWriteCompleteCallback syncWriteCb,
                 SyncWriteTimeoutHandlerFactory syncWriteTimeoutFactory,
                 SeqnoAckCallback seqnoAckCb,
                 Configuration& config,
                 EvictionPolicy evictionPolicy,
                 std::unique_ptr<Collections::VB::Manifest> manifest,
                 KVBucket* bucket,
                 vbucket_state_t initState,
                 uint64_t purgeSeqno,
                 uint64_t maxCas,
                 int64_t hlcEpochSeqno,
                 bool mightContainXattrs,
                 const nlohmann::json* replTopology,
                 uint64_t maxVisibleSeqno,
                 uint64_t maxPrepareSeqno)
    : ht(
              st,
              std::move(valFact),
              config.getHtSize(),
              config.getHtLocks(),
              config.getFreqCounterIncrementFactor(),
              config.getHtTempItemsAllowedPercent(),
              [this] {
                  return this->bucket ? this->bucket->getInitialMFU()
                                      : HashTable::defaultGetInitialMFU();
              },
              [this](const HashTable::HashBucketLock& lh,
                     const StoredValue& v) {
                  return this->isEligibleForEviction(lh, v);
              }),
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
      durabilityImpossibleFallback(config.getDurabilityImpossibleFallback()),
      eviction(evictionPolicy),
      stats(st),
      persistenceSeqno(0),
      numHpVBReqs(0),
      manifest(std::move(manifest)),
      state(newState),
      initialState(initState),
      bucket(bucket),
      id(i),
      checkpointManager(std::make_unique<CheckpointManager>(st,
                                                            *this,
                                                            chkConfig,
                                                            lastSeqno,
                                                            lastSnapStart,
                                                            lastSnapEnd,
                                                            maxVisibleSeqno,
                                                            maxPrepareSeqno,
                                                            flusherCb)),
      syncWriteTimeoutFactory(std::move(syncWriteTimeoutFactory)),
      replicationTopology(nlohmann::json().dump()),
      purge_seqno(purgeSeqno),
      takeover_backed_up(false),
      persistedRange(lastSnapStart, lastSnapEnd),
      receivingInitialDiskSnapshot(false),
      rollbackItemCount(0),
      hlc(maxCas,
          hlcEpochSeqno,
          std::chrono::microseconds(config.getHlcDriftAheadThresholdUs()),
          std::chrono::microseconds(config.getHlcDriftBehindThresholdUs()),
          std::chrono::microseconds(config.getHlcMaxFutureThresholdUs())),
      statPrefix("vb_" + std::to_string(i.get())),
      bucketCreation(false),
      deferredDeletion(false),
      deferredDeletionCookie(nullptr),
      syncWriteResolvedCb(std::move(syncWriteResolvedCb)),
      syncWriteCompleteCb(std::move(syncWriteCb)),
      seqnoAckCb(std::move(seqnoAckCb)),
      mayContainXattrs(mightContainXattrs) {
    ht.minimumSize = [bucket]() { return bucket->getMinimumHashTableSize(); };
    // There are tests where we just create vbuckets & bucket can be null -
    // get the value from the KVBucket only when it's not null.
    if (bucket) {
        ht.tempItemsAllowedPercent = [bucket]() {
            return bucket->getHtTempItemsAllowedPercent();
        };
    }
    ht.updateNumTempItemsAllowed();
    if (config.getConflictResolutionTypeString() == "seqno") {
        conflictResolver = std::make_unique<RevisionSeqnoResolution>();
    } else {
        // Both last-write-wins and custom conflict resolution are treated
        // as LWW from KV-Engine's pov.
        conflictResolver = std::make_unique<LastWriteWinsResolution>();
    }

    pendingOpsStart = cb::time::steady_clock::time_point();
    // HashTable accounts its own overhead
    stats.coreLocal.get()->memOverhead +=
            sizeof(VBucket) - sizeof(HashTable) + sizeof(CheckpointManager);

    setupSyncReplication(std::shared_lock(stateLock), replTopology);

    EP_LOG_INFO_CTX(
            "VBucket created",
            {"vb", id},
            {"state", VBucket::toString(state)},
            {"initial_state", VBucket::toString(initialState)},
            {"last_seqno", lastSeqno},
            {"persisted_range",
             {{"start", persistedRange.getStart()},
              {"end", persistedRange.getEnd()}}},
            {"purge_seqno", purge_seqno.load()},
            {"max_cas", getMaxCas()},
            {"uuid",
             failovers ? std::to_string(failovers->getLatestUUID()) : ""},
            {"topology", getReplicationTopology()});
}

VBucket::~VBucket() {
    EP_LOG_INFO_CTX("~VBucket()", {"vb", id});

    if (!pendingOps.empty()) {
        EP_LOG_WARN_CTX("~VBucket(): pending ops",
                        {"vb", id},
                        {"total", pendingOps.size()});
    }

    stats.getCoreLocalDiskQueueSize().fetch_sub(dirtyQueueSize.load());

    // Clear out the bloomfilter(s)
    clearFilter();

    stats.coreLocal.get()->memOverhead -=
            sizeof(VBucket) - sizeof(HashTable) + sizeof(CheckpointManager);
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

std::optional<uint64_t> VBucket::getHighSeqnoOfCollections(
        const Collections::VB::Filter& filter) const {
    if (filter.isPassThroughFilter()) {
        return {};
    }

    uint64_t maxHighSeqno = 0;
    for (auto& coll : filter) {
        auto handle = getManifest().lock(coll.first);
        if (!handle.valid()) {
            EP_LOG_WARN(
                    "VBucket::getHighSeqnoOfCollections {} could not lock "
                    "manifest for {}",
                    id,
                    coll.first);
            return {};
        }
        auto collHighSeqno = handle.getHighSeqno();
        maxHighSeqno = std::max(maxHighSeqno, collHighSeqno);
    }

    return {maxHighSeqno};
}

size_t VBucket::getChkMgrMemUsage() const {
    return checkpointManager->getMemUsage();
}

size_t VBucket::getCMQueuedItemsMemUsage() const {
    return checkpointManager->getQueuedItemsMemUsage();
}

size_t VBucket::getCMMemOverhead() const {
    return checkpointManager->getMemOverhead();
}

size_t VBucket::getCMMemOverheadQueue() const {
    return checkpointManager->getMemOverheadQueue();
}

size_t VBucket::getCMMemOverheadIndex() const {
    return checkpointManager->getMemOverheadIndex();
}

size_t VBucket::getCMMemFreedByItemExpel() const {
    return checkpointManager->getMemFreedByItemExpel();
}

size_t VBucket::getCMMemFreedByRemoval() const {
    return checkpointManager->getMemFreedByCheckpointRemoval();
}

static DurabilityMonitor::CommitStrategy getCommitStrategy(
        cb::config::DurabilityImpossibleFallback fallback) {
    using namespace cb::config;
    using Strategy = DurabilityMonitor::CommitStrategy;
    switch (fallback) {
    case DurabilityImpossibleFallback::Disabled:
        return Strategy::MajorityAck;
    case DurabilityImpossibleFallback::FallbackToMasterAck:
        return Strategy::MajorityAckFallbackToMasterAckOnly;
    }
    folly::assume_unreachable();
}

void VBucket::setDurabilityImpossibleFallback(
        VBucketStateLockRef vbStateLock,
        cb::config::DurabilityImpossibleFallback fallback) {
    durabilityImpossibleFallback = fallback;

    if (state != vbucket_state_active) {
        // No ADM on non-active vbs.
        return;
    }

    const auto newStrategy = getCommitStrategy(fallback);
    getActiveDM().setAndProcessCommitStrategy(newStrategy);
}

size_t VBucket::getSyncWriteAcceptedCount() const {
    std::shared_lock<folly::SharedMutex> lh(
            const_cast<folly::SharedMutex&>(stateLock));
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getNumAccepted();
}

size_t VBucket::getSyncWriteCommittedCount() const {
    std::shared_lock<folly::SharedMutex> lh(
            const_cast<folly::SharedMutex&>(stateLock));
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getNumCommitted();
}

size_t VBucket::getSyncWriteCommittedNotDurableCount() const {
    std::shared_lock<folly::SharedMutex> lh(
            const_cast<folly::SharedMutex&>(stateLock));
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getNumCommittedNotDurable();
}

size_t VBucket::getSyncWriteAbortedCount() const {
    std::shared_lock<folly::SharedMutex> lh(
            const_cast<folly::SharedMutex&>(stateLock));
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getNumAborted();
}

size_t VBucket::getDurabilityMonitorMemory() const {
    std::shared_lock<folly::SharedMutex> lh(
            const_cast<folly::SharedMutex&>(stateLock));
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getTotalMemoryUsed();
}

size_t VBucket::getDurabilityNumTracked() const {
    std::shared_lock<folly::SharedMutex> lh(
            const_cast<folly::SharedMutex&>(stateLock));
    if (!durabilityMonitor) {
        return 0;
    }
    return durabilityMonitor->getNumTracked();
}

void VBucket::fireAllOps(EventuallyPersistentEngine& engine,
                         cb::engine_errc code) {
    std::unique_lock<std::mutex> lh(pendingOpLock);

    if (pendingOpsStart > cb::time::steady_clock::time_point()) {
        auto now = cb::time::steady_clock::now();
        if (now > pendingOpsStart) {
            auto d = std::chrono::duration_cast<std::chrono::microseconds>(
                    now - pendingOpsStart);
            stats.pendingOpsHisto.add(d);
            atomic_setIfBigger(stats.pendingOpsMaxDuration,
                               std::make_unsigned_t<hrtime_t>(d.count()));
        }
    } else {
        return;
    }

    pendingOpsStart = cb::time::steady_clock::time_point();
    stats.pendingOps.fetch_sub(pendingOps.size());
    atomic_setIfBigger(stats.pendingOpsMax, pendingOps.size());

    while (!pendingOps.empty()) {
        auto* pendingOperation = pendingOps.back();
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
        fireAllOps(engine, cb::engine_errc::success);
    } else if (state == vbucket_state_pending) {
        // Nothing
    } else {
        fireAllOps(engine, cb::engine_errc::not_my_vbucket);
    }
}

std::vector<CookieIface*> VBucket::getCookiesForInFlightSyncWrites() {
    return getActiveDM().getCookiesForInFlightSyncWrites();
}

std::vector<CookieIface*> VBucket::prepareTransitionAwayFromActive() {
    return getActiveDM().prepareTransitionAwayFromActive();
}

ItemsToFlush VBucket::getItemsToPersist(size_t approxMaxItems,
                                        size_t approxMaxBytes) {
    ItemsToFlush result;

    if (approxMaxItems == 0) {
        throw std::invalid_argument("VBucket::getItemsToPersist: limit=0");
    }

    // Get up to approxLimit checkpoint items outstanding for the persistence
    // cursor. Note that it is only valid to queue a complete checkpoint, this
    // is where the "approx" in the limit comes from.

    const auto begin = cb::time::steady_clock::now();

    auto rangeInfo = checkpointManager->getItemsForPersistence(
            result.items, approxMaxItems, approxMaxBytes);

    if (result.items.size() > 0 &&
        rangeInfo.purgeSeqno > rangeInfo.ranges.front().getStart()) {
        // The purge-seqno is inside the snapshot range, we must expose this to
        // the flusher, but cap it to the end of the flushed range (which can
        // be a subset of the snapshot range)
        result.purgeSeqno =
                std::min(rangeInfo.purgeSeqno,
                         uint64_t(result.items.back()->getBySeqno()));
    }

    result.ranges = std::move(rangeInfo.ranges);
    result.maxDeletedRevSeqno = rangeInfo.maxDeletedRevSeqno;
    result.checkpointType = rangeInfo.checkpointType;
    result.historical = rangeInfo.historical;
    result.flushHandle = std::move(rangeInfo.flushHandle);
    result.moreAvailable = rangeInfo.moreAvailable;
    result.maxCas = rangeInfo.maxCas;

    stats.persistenceCursorGetItemsHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    cb::time::steady_clock::now() - begin));

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

vbucket_state_t VBucket::fromString(const std::string_view state) {
    using namespace std::string_view_literals;
    if (state == "active"sv) {
        return vbucket_state_active;
    }
    if (state == "replica"sv) {
        return vbucket_state_replica;
    }
    if (state == "pending"sv) {
        return vbucket_state_pending;
    }
    return vbucket_state_dead;
}

void VBucket::setState(vbucket_state_t to, const nlohmann::json* meta) {
    std::unique_lock wlh(getStateLock());
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
    if ((topology.empty()) || (topology.size() > 2)) {
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
        if ((nodes.empty()) || (nodes.size() > 4)) {
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
        }
        return "'topology' contains unsupported key:"s + el.key() +
               " with value:" + el.value().dump();
    }
    return {};
}

void VBucket::setState_UNLOCKED(
        vbucket_state_t to,
        const nlohmann::json* meta,
        const std::unique_lock<folly::SharedMutex>& vbStateLock) {
    vbucket_state_t oldstate = state;

    // Validate (optional) meta content.
    if (meta) {
        if (to != vbucket_state_active) {
            throw std::invalid_argument(
                    "VBucket::setState: meta only permitted for state:active, "
                    "found state:"s +
                    VBucket::toString(to) + " meta:" + meta->dump());
        }
        auto error = validateSetStateMeta(*meta);
        if (!error.empty()) {
            throw std::invalid_argument("VBucket::setState: " + error);
        }
    }

    if (meta) {
        EP_LOG_INFO_CTX("VBucket::setState: transitioning",
                        {"vb", id},
                        {"high_seqno", getHighSeqno()},
                        {"from", VBucket::toString(oldstate)},
                        {"to", VBucket::toString(to)},
                        {"meta", *meta});
    } else {
        EP_LOG_INFO_CTX("VBucket::setState: transitioning",
                        {"vb", id},
                        {"high_seqno", getHighSeqno()},
                        {"from", VBucket::toString(oldstate)},
                        {"to", VBucket::toString(to)});
    }
    state = to;

    if (state != vbucket_state_active) {
        // Transition to !active, the vbucket should never be left in
        // takeover-backup.
        setTakeoverBackedUpState(false);
    }

    setupSyncReplication(vbStateLock, meta ? &meta->at("topology") : nullptr);

    updateStatsForStateChange(oldstate, to);
}

vbucket_transition_state VBucket::getTransitionState() const {
    nlohmann::json topology;
    if (getState() == vbucket_state_active) {
        topology = nlohmann::json::parse(getReplicationTopology());
    }

    return {failovers->getJSON(), topology, getState()};
}

std::string VBucket::getReplicationTopology() const {
    return *replicationTopology.rlock();
}

void VBucket::setupSyncReplication(VBucketStateLockRef vbStateLock,
                                   const nlohmann::json* topology) {
    // First, update the Replication Topology in VBucket
    if (topology) {
        if (state != vbucket_state_active) {
            throw std::invalid_argument(
                    "VBucket::setupSyncReplication: Topology only valid for "
                    "vbucket_state_active");
        }
        auto error = validateReplicationTopology(*topology);
        if (!error.empty()) {
            throw std::invalid_argument(
                    "VBucket::setupSyncReplication: Invalid replication "
                    "topology: " +
                    error);
        }
        *replicationTopology.wlock() = topology->dump();
    } else {
        *replicationTopology.wlock() = nlohmann::json().dump();
    }

    // Then, initialize the DM and propagate the new topology if necessary
    auto* currentPassiveDM =
            dynamic_cast<PassiveDurabilityMonitor*>(durabilityMonitor.get());
    auto* currentActiveDM =
            dynamic_cast<ActiveDurabilityMonitor*>(durabilityMonitor.get());
    auto* currentDeadDM =
            dynamic_cast<DeadDurabilityMonitor*>(durabilityMonitor.get());

    // If we are transitioning away from active then we need to mark all of our
    // prepares as PreparedMaybeVisible to avoid exposing them
    if (currentActiveDM && durabilityMonitor && state != vbucket_state_active) {
        auto& adm = dynamic_cast<ActiveDurabilityMonitor&>(*durabilityMonitor);
        auto trackedWrites = adm.getTrackedKeys();
        for (auto& key : trackedWrites) {
            auto htRes = ht.findOnlyPrepared(key);
            Expects(htRes.storedValue);
            Expects(htRes.storedValue->isPending() ||
                    htRes.storedValue->isPrepareCompleted());
            htRes.storedValue->setCommitted(
                    CommittedState::PreparedMaybeVisible);
        }
    }

    switch (state) {
    case vbucket_state_active: {
        if (!durabilityMonitor) {
            // Change to Active from no previous DurabilityMonitor - create
            // one.
            durabilityMonitor = std::make_unique<ActiveDurabilityMonitor>(
                    stats, *this, syncWriteTimeoutFactory(*this));
        } else if (!currentActiveDM) {
            // Change to active from current DM
            durabilityMonitor = std::make_unique<ActiveDurabilityMonitor>(
                    stats,
                    *this,
                    std::move(*durabilityMonitor),
                    syncWriteTimeoutFactory(*this));
        }

        // @todo: We want to support empty-topology in ActiveDM, that's for
        //     Warmup. Deferred to dedicated patch (tracked under MB-33186).
        if (topology) {
            getActiveDM().setReplicationTopology(*topology);
        }
        setDurabilityImpossibleFallback(vbStateLock,
                                        durabilityImpossibleFallback);
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
                    *this, std::move(*durabilityMonitor));
        } else {
            durabilityMonitor =
                    std::make_unique<PassiveDurabilityMonitor>(*this);
        }
        return;
    case vbucket_state_dead:
        if (currentDeadDM) {
            // Already have a DeadDM - just return
            return;
        }
        if (durabilityMonitor) {
            durabilityMonitor = std::make_unique<DeadDurabilityMonitor>(
                    *this, std::move(*durabilityMonitor));
        } else {
            durabilityMonitor = std::make_unique<DeadDurabilityMonitor>(*this);
        }
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
        const cb::time::steady_clock::time_point asOf) {
    std::shared_lock lh(stateLock);
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
    std::shared_lock rlh(getStateLock());
    if (getState() != vbucket_state_active) {
        return;
    }
    getActiveDM().processCompletedSyncWriteQueue(rlh);
}

void VBucket::doStatsForQueueing(const Item& qi, size_t itemBytes)
{
    ++dirtyQueueSize;
    dirtyQueueMem.fetch_add(sizeof(Item));
    ++dirtyQueueFill;
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            qi.getQueuedTime().time_since_epoch());
    dirtyQueueAge.fetch_add(ms.count());
    dirtyQueuePendingWrites.fetch_add(itemBytes);
}

void AggregatedFlushStats::accountItem(const Item& item) {
    Expects(item.getQueuedTime().time_since_epoch().count() != 0);

    ++numItems;
    totalBytes += item.size();
    totalAgeInMilliseconds +=
            std::chrono::duration_cast<std::chrono::milliseconds>(
                    item.getQueuedTime().time_since_epoch())
                    .count();
}

void VBucket::doAggregatedFlushStats(const AggregatedFlushStats& aggStats) {
    const auto numItems = aggStats.getNumItems();
    stats.getCoreLocalDiskQueueSize() -= numItems;
    dirtyQueueSize -= numItems;
    decrDirtyQueueMem(sizeof(Item) * numItems);
    dirtyQueueDrain += numItems;
    decrDirtyQueueAge(aggStats.getTotalAgeInMilliseconds());
    decrDirtyQueuePendingWrites(aggStats.getTotalBytes());
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

    stats.getCoreLocalDiskQueueSize().fetch_sub(dirtyQueueSize.exchange(0));
    dirtyQueueMem.store(0);
    dirtyQueueFill.store(0);
    dirtyQueueAge.store(0);
    dirtyQueuePendingWrites.store(0);
    dirtyQueueDrain.store(0);

    hlc.resetStats();
}

uint64_t VBucket::getQueueAge() {
    uint64_t currDirtyQueueAge = dirtyQueueAge.load();
    // dirtyQueue size is 0, so the queueAge is 0.
    if (currDirtyQueueAge == 0) {
        return 0;
    }

    // Get time now multiplied by the queue size. We need to subtract
    // dirtyQueueAge from this to offset time_since_epoch.
    auto currentAge = std::chrono::duration_cast<std::chrono::milliseconds>(
                              cb::time::steady_clock::now().time_since_epoch())
                              .count() *
                      dirtyQueueSize;

    // Since the size and age are independent atomics, we might see an
    // inconsistent snapshot, such as age updated more than size. Avoid
    // potential overflow in that case.
    if (currentAge < currDirtyQueueAge) {
        return 0;
    }
    // Return the time in milliseconds
    return (currentAge - currDirtyQueueAge);
}

template <typename T>
void VBucket::addStat(const char* nm,
                      const T& val,
                      const AddStatFn& add_stat,
                      CookieIface& c) {
    std::string stat = statPrefix;
    if (nm != nullptr) {
        add_prefixed_stat(statPrefix, nm, val, add_stat, c);
    } else {
        add_casted_stat(statPrefix.data(), val, add_stat, c);
    }
}

// Force generation of the std::string version to fix ASAN build
template void VBucket::addStat<std::string>(const char* nm,
                                            const std::string& val,
                                            const AddStatFn& add_stat,
                                            CookieIface& c);

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
        /* TODO: In order to minimize allocations, the callback needs to
         * allocate an item whose value size will be exactly the size of the
         * value after pre-expiry is performed.
         */
        const auto result =
                document_pre_expiry(itm->getValueView(), itm->getDataType());

        // The API states only uncompressed xattr values are returned, but an
        // empty value has datatype:raw
        const auto datatype = result.empty() ? PROTOCOL_BINARY_RAW_BYTES
                                             : PROTOCOL_BINARY_DATATYPE_XATTR;
        std::unique_ptr<Blob> val(Blob::New(result.data(), result.size()));
        ht.unlocked_replaceValueAndDatatype(hbl, v, std::move(val), datatype);
    }
}

cb::engine_errc VBucket::commit(
        VBucketStateLockRef vbStateLock,
        const DocKeyView& key,
        uint64_t prepareSeqno,
        std::optional<int64_t> commitSeqno,
        CommitType commitType,
        const Collections::VB::CachingReadHandle& cHandle,
        CookieIface* cookie) {
    // Behaviour does not differ depending on the commit type. We return success
    // to clients.
    (void)commitType;
    Expects(cHandle.valid());
    auto res = ht.findForUpdate(key);
    if (!res.pending) {
        std::string committedItemInfo("null");
        if (res.committed) {
            committedItemInfo =
                    fmt::format("seqno:{}, isDeleted:{}, isResident:{}, cas:{}",
                                res.committed->getBySeqno(),
                                res.committed->isDeleted(),
                                res.committed->isResident(),
                                res.committed->getCas());
        }
        // If we are committing we /should/ always find the pending item.
        EP_LOG_ERR(
                "VBucket::commit ({}) failed as no pending HashTable item "
                "found with "
                "key:{}, committed item:[{}], prepare_seqno:{}, "
                "commit_seqno:{}, IsDiskSnapshot:{}, "
                "snapshotInfo:{}, HS:{}, HPS:{}, HCS:{}",
                id,
                cb::UserDataView(key.to_string()),
                committedItemInfo,
                prepareSeqno,
                to_string_or_none(commitSeqno),
                isReceivingDiskSnapshot(),
                checkpointManager->getSnapshotInfo(),
                getHighSeqno(),
                getHighPreparedSeqno(),
                getHighCompletedSeqno());
        return cb::engine_errc::no_such_key;
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

    VBQueueItemCtx queueItmCtx{cHandle.getCanDeduplicate()};
    if (commitSeqno) {
        queueItmCtx.genBySeqno = GenerateBySeqno::No;
    }
    // Never generate a new cas. We should always take the existing cas because
    // it may have been set explicitly.
    queueItmCtx.genCas = GenerateCas::No;

    queueItmCtx.durability =
            DurabilityItemCtx{res.pending->getBySeqno(), nullptr /*cookie*/};

    queueItmCtx.hcs = res.pending->getBySeqno();
    if (res.pending->isDeleted()) {
        // we are about to commit a sync delete, bump the maxDeletedRevSeqno
        // just as it would be for a non-sync delete
        ht.updateMaxDeletedRevSeqno(res.pending->getRevSeqno());
    }
    auto notify =
            commitStoredValue(res, prepareSeqno, queueItmCtx, commitSeqno);

    notifyNewSeqno(notify);
    doCollectionsStats(cHandle, notify);

    // Cookie representing the client connection, provided only at Active
    if (cookie) {
        notifyClientOfSyncWriteComplete(cookie, cb::engine_errc::success);
    }

    return cb::engine_errc::success;
}

cb::engine_errc VBucket::abort(
        VBucketStateLockRef vbStateLock,
        const DocKeyView& key,
        uint64_t prepareSeqno,
        std::optional<int64_t> abortSeqno,
        const Collections::VB::CachingReadHandle& cHandle,
        CookieIface* cookie) {
    Expects(cHandle.valid());
    auto htRes = ht.findForUpdate(key);

    // This block handles the case where at Replica we receive an Abort but we
    // do not have any in-flight Prepare in the HT. That is possible when
    // Replica receives a Backfill (Disk) Snapshot (for both EP and Ephemeral
    // bucket).
    // Note that, while for EP we just expect no-pending in the HT, for
    // Ephemeral we may have no-pending or a pre-existing completed (Committed
    // or Aborted) Prepare in the HT.
    if (!htRes.pending ||
        (htRes.pending && htRes.pending->isPrepareCompleted())) {
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
                return cb::engine_errc::invalid_arguments;
            }
            EP_LOG_ERR(
                    "VBucket::abort ({}) - active failed as no HashTable"
                    "item found with key:{}",
                    id,
                    cb::UserDataView(key.to_string()));
            return cb::engine_errc::no_such_key;
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
                    to_string_or_none(abortSeqno));
            return cb::engine_errc::invalid_arguments;
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
            ctx = addNewAbort(htRes.pending.getHBL(),
                              key,
                              prepareSeqno,
                              *abortSeqno,
                              cHandle);
        } else {
            // This code path can be reached only at Ephemeral
            Expects(htRes.pending->isPrepareCompleted());
            ctx = abortStoredValue(htRes.pending.getHBL(),
                                   *htRes.pending.release(),
                                   prepareSeqno,
                                   *abortSeqno,
                                   cHandle);
        }

        notifyNewSeqno(ctx);
        doCollectionsStats(cHandle, ctx);

        return cb::engine_errc::success;
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
                                   abortSeqno,
                                   cHandle);

    notifyNewSeqno(notify);
    doCollectionsStats(cHandle, notify);

    // Cookie representing the client connection, provided only at Active
    if (cookie) {
        notifyClientOfSyncWriteComplete(cookie,
                                        cb::engine_errc::sync_write_ambiguous);
    }

    return cb::engine_errc::success;
}

void VBucket::notifyActiveDMOfLocalSyncWrite() {
    getActiveDM().checkForCommit();
}

void VBucket::notifyClientOfSyncWriteComplete(CookieIface* cookie,
                                              cb::engine_errc result) {
    EP_LOG_DEBUG(
            "VBucket::notifyClientOfSyncWriteComplete ({}) cookie:{} result:{}",
            id,
            static_cast<const void*>(cookie),
            result);
    Expects(cookie);
    syncWriteCompleteCb(cookie, result);
}

void VBucket::notifyPassiveDMOfSnapEndReceived(uint64_t snapEnd,
                                               OptionalSeqno hps) {
    getPassiveDM().notifySnapshotEndReceived(snapEnd, hps);
}

void VBucket::sendSeqnoAck(int64_t seqno) {
    Expects(state == vbucket_state_replica || state == vbucket_state_pending);
    seqnoAckCb(getId(), seqno);
}

bool VBucket::addPendingOp(CookieIface* cookie) {
    std::lock_guard<std::mutex> lh(pendingOpLock);
    if (state != vbucket_state_pending) {
        // State transitioned while we were waiting.
        return false;
    }
    // Start a timer when enqueuing the first client.
    if (pendingOps.empty()) {
        pendingOpsStart = cb::time::steady_clock::now();
    }
    pendingOps.push_back(cookie);
    ++stats.pendingOps;
    ++stats.pendingOpsTotal;
    return true;
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
    }
    return false;
}

void VBucket::createFilter(size_t key_count, double probability) {
}

void VBucket::addToFilter(const DocKeyView& key) {
}

void VBucket::clearFilter() {
}

void VBucket::setFilterStatus(bfilter_status_t to) {
}

std::string VBucket::getFilterStatusString() {
    return "DOESN'T EXIST";
}

size_t VBucket::getFilterSize() {
    return 0;
}

size_t VBucket::getNumOfKeysInFilter() {
    return 0;
}

size_t VBucket::getFilterMemoryFootprint() {
    return 0;
}

/*
 * We shouldn't be adding anything here - left around for backward compat
 * & much be deprecated with a release-note.
 */
void VBucket::addBloomFilterStats(const AddStatFn& add_stat, CookieIface& c) {
    addStat("bloom_filter", getFilterStatusString(), add_stat, c);
    addStat("bloom_filter_size", getFilterSize(), add_stat, c);
    addStat("bloom_filter_key_count", getNumOfKeysInFilter(), add_stat, c);
    addStat("bloom_filter_memory", getFilterMemoryFootprint(), add_stat, c);
}

VBNotifyCtx VBucket::queueItem(queued_item& item, const VBQueueItemCtx& ctx) {
    if (item->isSystemEvent() && state == vbucket_state_active) {
        // MB-65281; Disable deduplication for all system events.
        // This check and set is required here for ephemeral vbuckets which
        // end up here for queueing system events, unlike EP buckets that
        // set this value and do the queueing within
        // EPBucket::addSystemEventItem.
        item->setCanDeduplicate(CanDeduplicate::No);
    } else if (bucket && bucket->isHistoryRetentionEnabled()) {
        item->setCanDeduplicate(ctx.deduplicate);
    }

    // Set queue time to now. Why not in the ctor of the Item? We only need to
    // do this in certain places for new items as it's used to determine how
    // long it took an item to get flushed.
    item->setQueuedTime();

    // Ensure that durable writes are queued with the same seqno-order in both
    // Backfill/CheckpointManager Queues and DurabilityMonitor. Note that
    // bySeqno may be generated by Queues when the item is queued.
    // Lock only for durable writes to minimize front-end thread contention.
    std::unique_lock<std::mutex> durLock(dmQueueMutex, std::defer_lock);
    if (item->isPending()) {
        durLock.lock();
    }

    const auto notifyFlusher = checkpointManager->queueDirty(
            item, ctx.genBySeqno, ctx.genCas, ctx.preLinkDocumentContext);

    const VBNotifyCtx notifyCtx(
            item->getBySeqno(), true, notifyFlusher, item->getOperation());

    // Process Durability items (notify the DurabilityMonitor of
    // Prepare/Commit/Abort)
    switch (state) {
    case vbucket_state_active:
        if (item->isPending()) {
            Expects(ctx.durability.has_value());
            getActiveDM().addSyncWrite(ctx.durability->cookie, item);
        }
        break;
    case vbucket_state_replica:
    case vbucket_state_pending: {
        auto& pdm = getPassiveDM();
        if (item->isPending()) {
            pdm.addSyncWrite(item, ctx.overwritingPrepareSeqno);
        } else if (item->isCommitSyncWrite()) {
            pdm.completeSyncWrite(
                    item->getKey(),
                    PassiveDurabilityMonitor::Resolution::Commit,
                    std::get<int64_t>(
                            ctx.durability->requirementsOrPreparedSeqno));
        } else if (item->isAbort()) {
            pdm.completeSyncWrite(item->getKey(),
                                  PassiveDurabilityMonitor::Resolution::Abort,
                                  {} /* no prepareSeqno*/);
        }
        break;
    }
    case vbucket_state_dead:
        // Do nothing, the vbucket is dead and the DM shouldn't be used
        break;
    }

    return notifyCtx;
}

VBNotifyCtx VBucket::queueDirty(const HashTable::HashBucketLock& hbl,
                                StoredValue& v,
                                const VBQueueItemCtx& ctx) {
    if (ctx.trackCasDrift == TrackCasDrift::Yes) {
        setOrForceMaxCasAndTrackDrift(v.getCas());
    }

    // If we are queueing a SyncWrite StoredValue; extract the durability
    // requirements to use to create the Item.
    std::optional<cb::durability::Requirements> durabilityReqs;
    if (v.isPending()) {
        Expects(ctx.durability.has_value());
        durabilityReqs = std::get<cb::durability::Requirements>(
                ctx.durability->requirementsOrPreparedSeqno);
    }

    queued_item qi(v.toItem(getId(),
                            StoredValue::HideLockedCas::No,
                            StoredValue::IncludeValue::Yes,
                            durabilityReqs));

    if (qi->isCommitSyncWrite()) {
        Expects(ctx.durability.has_value());
        qi->setPrepareSeqno(
                std::get<int64_t>(ctx.durability->requirementsOrPreparedSeqno));
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

    if (!mightContainXattrs() &&
        cb::mcbp::datatype::is_xattr(v.getDatatype())) {
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
        setOrForceMaxCasAndTrackDrift(v.getCas());
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

queued_item VBucket::createNewAbortedItem(const DocKeyView& key,
                                          int64_t prepareSeqno,
                                          int64_t abortSeqno) {
    auto item = make_STRCPtr<Item>(key,
                                   0 /*flags*/,
                                   ep_real_time() /*exp*/,
                                   value_t{},
                                   PROTOCOL_BINARY_RAW_BYTES,
                                   0,
                                   abortSeqno,
                                   getId());

    item->setAbortSyncWrite();
    item->setPrepareSeqno(prepareSeqno);

    return item;
}

HashTable::FindResult VBucket::fetchValidValue(
        VBucketStateLockRef vbStateLock,
        WantsDeleted wantsDeleted,
        TrackReference trackReference,
        const Collections::VB::CachingReadHandle& cHandle,
        const ForGetReplicaOp fetchRequestedForReplicaItem) {
    fetchValidValueHook(getStateLock());

    const auto& key = cHandle.getKey();

    // Whilst fetchValidValue is used for reads it also processes expiries which
    // means that it can update StoredValues
    auto res = ht.findForUpdate(key);
    auto* v = res.selectSVForRead(
            trackReference, wantsDeleted, fetchRequestedForReplicaItem);

    // We don't expire if:
    //  - the item is already deleted
    //  - it is a temp item
    //  - the item is a pending Prepare -> TTL will apply if/when the item is
    //    committed
    //  - it isn't logically expired
    if (!v || v->isDeleted() || v->isTempItem() || !v->isCommitted() ||
        !v->isExpired(ep_real_time())) {
        // Nothing to expire, just return the access result
        return {v, std::move(res.getHBL())};
    }

    // Expiry path

    // Note that only the master actively expires items, and only if the item's
    // collection is alive and there's mem available in checkpoints.
    const auto cmAvailable =
            bucket && !KVBucket::isCheckpointMemoryStateFull(
                              bucket->verifyCheckpointMemoryState());
    if (getState() == vbucket_state_active && cHandle.valid() && cmAvailable) {
        handlePreExpiry(res.getHBL(), *v);
        VBNotifyCtx notifyCtx;
        std::tie(std::ignore, v, notifyCtx) =
                processExpiredItem(res, cHandle, ExpireBy::Access);
        notifyNewSeqno(notifyCtx);
        doCollectionsStats(cHandle, notifyCtx);
    }

    // When reached here we know for sure that the item is logically expired, so
    // apply the WantsDeleted logic regardless of whether the fetch is for an
    // active or replica vbucket
    return {(wantsDeleted == WantsDeleted::Yes) ? v : nullptr,
            std::move(res.getHBL())};
}

VBucket::FetchForWriteResult VBucket::fetchValueForWrite(
        const Collections::VB::CachingReadHandle& cHandle) {
    Expects(getState() == vbucket_state_active);

    auto res = ht.findForUpdate(cHandle.getKey());
    auto* sv = res.selectSVToModify(false /*durability*/);

    if (!sv) {
        // No item found.
        return {FetchForWriteResult::Status::OkVacant,
                {},
                std::move(res.getHBL())};
    }

    if (sv->isPending()) {
        // Attempted to write and found a Pending SyncWrite. Cannot write until
        // the in-flight one has completed.
        return {FetchForWriteResult::Status::ESyncWriteInProgress, nullptr, {}};
    }

    if (sv->isDeleted()) {
        // If we got a deleted value then can return directly (and skip
        // expiration checks because deleted items are not subject
        // to expiration).
        return {FetchForWriteResult::Status::OkFound,
                sv,
                std::move(res.getHBL())};
    }

    if (sv->isTempItem()) {
        // No expiry check needed for temps.
        return {FetchForWriteResult::Status::OkFound,
                sv,
                std::move(res.getHBL())};
    }

    if (!sv->isExpired(ep_real_time())) {
        // Not expired, good to return as-is.
        return {FetchForWriteResult::Status::OkFound,
                sv,
                std::move(res.getHBL())};
    }

    // Expired - but queueDirty only allowed on active VB and only if the item's
    // collection is alive
    if (cHandle.valid()) {
        handlePreExpiry(res.getHBL(), *sv);
        VBNotifyCtx notifyCtx;
        std::tie(std::ignore, sv, notifyCtx) =
                processExpiredItem(res, cHandle, ExpireBy::Access);
        notifyNewSeqno(notifyCtx);
        doCollectionsStats(cHandle, notifyCtx);
    }

    // Item found (but now deleted via expiration).
    return {FetchForWriteResult::Status::OkFound, sv, std::move(res.getHBL())};
}

HashTable::FindResult VBucket::fetchPreparedValue(
        const Collections::VB::CachingReadHandle& cHandle) {
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
    if (!hasMemoryForStoredValue(itm)) {
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
            info.has_value()) {
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

cb::engine_errc VBucket::set(
        VBucketStateLockRef vbStateLock,
        Item& itm,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        cb::StoreIfPredicate predicate,
        const Collections::VB::CachingReadHandle& cHandle) {
    auto ret = checkDurabilityRequirements(itm);
    if (ret != cb::engine_errc::success) {
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
            return cb::engine_errc::predicate_failed;
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
            (cas_op || storeIfStatus == cb::StoreIfStatus::GetItemInfo ||
             itm.shouldPreserveTtl())) {
            // Check Bloomfilter's prediction
            if (!maybeKeyExistsInFilter(itm.getKey())) {
                maybeKeyExists = false;
            }
        }

        PreLinkDocumentContext preLinkDocumentContext(engine, cookie, &itm);
        VBQueueItemCtx queueItmCtx{cHandle.getCanDeduplicate()};
        if (itm.isPending()) {
            queueItmCtx.durability =
                    DurabilityItemCtx{itm.getDurabilityReqs(), cookie};
        }
        queueItmCtx.preLinkDocumentContext = &preLinkDocumentContext;
        auto [status, notifyCtx] = processSet(htRes,
                                              v,
                                              itm,
                                              itm.getCas(),
                                              /*allowExisting*/ true,
                                              /*hashMetaData*/ false,
                                              queueItmCtx,
                                              storeIfStatus,
                                              maybeKeyExists);

        // For pending SyncWrites we initially return
        // cb::engine_errc::sync_write_pending; will notify client when request
        // is committed / aborted later. This is effectively EWOULDBLOCK, but
        // needs to be distinguishable by the ep-engine caller (storeIfInner)
        // from EWOULDBLOCK for bg-fetch
        ret = itm.isPending() ? cb::engine_errc::sync_write_pending
                              : cb::engine_errc::success;
        switch (status) {
        case MutationStatus::NoMem:
            ret = cb::engine_errc::no_memory;
            break;
        case MutationStatus::InvalidCas:
            ret = cb::engine_errc::key_already_exists;
            break;
        case MutationStatus::IsLocked:
            ret = cb::engine_errc::locked;
            break;
        case MutationStatus::NotFound:
            if (cas_op) {
                ret = cb::engine_errc::no_such_key;
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
                return bgFetch(
                        std::move(hbl), itm.getKey(), *v, cookie, engine, true);
            }
            ret = addTempItemAndBGFetch(
                    std::move(hbl), itm.getKey(), cookie, engine, true);
            break;
        }

        case MutationStatus::IsPendingSyncWrite:
            ret = cb::engine_errc::sync_write_in_progress;
            break;
        }
    }

    return ret;
}

cb::engine_errc VBucket::replace(
        VBucketStateLockRef vbStateLock,
        Item& itm,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        cb::StoreIfPredicate predicate,
        const Collections::VB::CachingReadHandle& cHandle) {
    auto ret = checkDurabilityRequirements(itm);
    if (ret != cb::engine_errc::success) {
        return ret;
    }

    { // HashBucketLock scope
        auto htRes = ht.findForUpdate(itm.getKey());
        auto& hbl = htRes.getHBL();

        cb::StoreIfStatus storeIfStatus = cb::StoreIfStatus::Continue;
        if (predicate &&
            (storeIfStatus = callPredicate(predicate, htRes.committed)) ==
                    cb::StoreIfStatus::Fail) {
            return cb::engine_errc::predicate_failed;
        }

        if (htRes.committed) {
            if (isLogicallyNonExistent(*htRes.committed, cHandle) ||
                htRes.committed->isExpired(ep_real_time())) {
                ht.cleanupIfTemporaryItem(hbl, *htRes.committed);
                return cb::engine_errc::no_such_key;
            }

            auto* v = htRes.selectSVToModify(itm);
            MutationStatus mtype;
            std::optional<VBNotifyCtx> notifyCtx;
            if (eviction == EvictionPolicy::Full &&
                htRes.committed->isTempInitialItem()) {
                mtype = MutationStatus::NeedBgFetch;
            } else {
                // If a pending SV was found and it's not yet complete, then we
                // cannot perform another mutation while the first is in
                // progress.
                //
                // MB-37342: The semantic of replace must not change. Ie, we try
                //  the set only /after/ having verified that the doc exists,
                //  which is also the right time to check for any pending SW
                //  (and to reject the operation if a prepare is in-flight).
                if (htRes.pending && !htRes.pending->isPrepareCompleted()) {
                    return cb::engine_errc::sync_write_in_progress;
                }

                PreLinkDocumentContext preLinkDocumentContext(
                        engine, cookie, &itm);
                VBQueueItemCtx queueItmCtx{cHandle.getCanDeduplicate()};
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
            // cb::engine_errc::sync_write_pending; will notify client when
            // request is committed / aborted later. This is effectively
            // EWOULDBLOCK, but needs to be distinguishable by the ep-engine
            // caller (storeIfInner) from EWOULDBLOCK for bg-fetch
            ret = itm.isPending() ? cb::engine_errc::sync_write_pending
                                  : cb::engine_errc::success;
            switch (mtype) {
            case MutationStatus::NoMem:
                ret = cb::engine_errc::no_memory;
                break;
            case MutationStatus::IsLocked:
                ret = cb::engine_errc::locked;
                break;
            case MutationStatus::InvalidCas:
            case MutationStatus::NotFound:
                ret = cb::engine_errc::not_stored;
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
                return bgFetch(
                        std::move(hbl), itm.getKey(), *v, cookie, engine, true);
            }
            case MutationStatus::IsPendingSyncWrite:
                ret = cb::engine_errc::sync_write_in_progress;
                break;
            }
        } else {
            if (eviction == EvictionPolicy::Value) {
                return cb::engine_errc::no_such_key;
            }

            if (maybeKeyExistsInFilter(itm.getKey())) {
                return addTempItemAndBGFetch(
                        std::move(hbl), itm.getKey(), cookie, engine, false);
            }
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT for replace().
            return cb::engine_errc::no_such_key;
        }
    }

    return ret;
}

void VBucket::addDurabilityMonitorStats(const AddStatFn& addStat,
                                        CookieIface& cookie) const {
    durabilityMonitor->addStats(addStat, cookie);
}

void VBucket::dumpDurabilityMonitor(std::ostream& os) const {
    os << *durabilityMonitor;
}

Collections::VB::ReadHandle VBucket::lockCollections() const {
    return manifest->lock();
}

Collections::VB::CachingReadHandle VBucket::lockCollections(
        const DocKeyView& key) const {
    return manifest->lock(key);
}

Collections::VB::ManifestUpdateStatus VBucket::updateFromManifest(
        VBucketStateLockRef vbStateLock, const Collections::Manifest& m) {
    return manifest->update(vbStateLock, *this, m);
}

void VBucket::replicaBeginCollection(Collections::ManifestUid uid,
                                     ScopeCollectionPair identifiers,
                                     std::string_view collectionName,
                                     cb::ExpiryLimit maxTtl,
                                     Collections::Metered metered,
                                     CanDeduplicate canDeduplicate,
                                     Collections::ManifestUid flushUid,
                                     int64_t bySeqno) {
    // The state of the VBucket should not change here, because replicaCreate
    // will generate SystemEvent items.
    // NOTE: We kill all streams when changing the VBucket state and this
    // function is only called from PassiveStream, so the lock is not
    // technically required for now.
    std::shared_lock rlh(stateLock);
    manifest->wlock(rlh).replicaCreate(*this,
                                       uid,
                                       identifiers,
                                       collectionName,
                                       maxTtl,
                                       metered,
                                       canDeduplicate,
                                       flushUid,
                                       bySeqno);
}

void VBucket::replicaModifyCollection(Collections::ManifestUid uid,
                                      CollectionID cid,
                                      cb::ExpiryLimit maxTtl,
                                      Collections::Metered metered,
                                      CanDeduplicate canDeduplicate,
                                      int64_t bySeqno) {
    // The state of the VBucket must not change here, because
    // replicaModifyCollection will generate SystemEvent items.
    // NOTE: We kill all streams when changing the VBucket state and this
    // function is only called from PassiveStream, so the lock is not
    // technically required for now.
    std::shared_lock rlh(stateLock);
    manifest->wlock(rlh).replicaModifyCollection(
            *this, uid, cid, maxTtl, metered, canDeduplicate, bySeqno);
}

void VBucket::replicaDropCollection(Collections::ManifestUid uid,
                                    CollectionID cid,
                                    bool isSystemCollection,
                                    int64_t bySeqno) {
    // The state of the VBucket should not change here, because replicaDrop
    // will generate SystemEvent items.
    // NOTE: We kill all streams when changing the VBucket state and this
    // function is only called from PassiveStream, so the lock is not
    // technically required for now.
    std::shared_lock rlh(stateLock);
    manifest->wlock(rlh).replicaDrop(
            *this, uid, cid, isSystemCollection, bySeqno);
}

void VBucket::replicaCreateScope(Collections::ManifestUid uid,
                                 ScopeID sid,
                                 std::string_view scopeName,
                                 int64_t bySeqno) {
    // The state of the VBucket should not change here, because
    // replicaCreateScope will generate SystemEvent items.
    // NOTE: We kill all streams when changing the VBucket state and this
    // function is only called from PassiveStream, so the lock is not
    // technically required for now.
    std::shared_lock rlh(stateLock);
    manifest->wlock(rlh).replicaCreateScope(
            *this, uid, sid, scopeName, bySeqno);
}

void VBucket::replicaDropScope(Collections::ManifestUid uid,
                               ScopeID sid,
                               bool isSystemScope,
                               int64_t bySeqno) {
    // The state of the VBucket should not change here, because
    // replicaDropScope will generate SystemEvent items.
    // NOTE: We kill all streams when changing the VBucket state and this
    // function is only called from PassiveStream, so the lock is not
    // technically required for now.
    std::shared_lock rlh(stateLock);
    manifest->wlock(rlh).replicaDropScope(
            *this, uid, sid, isSystemScope, bySeqno);
}

cb::engine_errc VBucket::prepare(
        VBucketStateLockRef vbStateLock,
        Item& itm,
        uint64_t cas,
        uint64_t* seqno,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        CheckConflicts checkConflicts,
        bool allowExisting,
        GenerateBySeqno genBySeqno,
        GenerateCas genCas,
        const Collections::VB::CachingReadHandle& cHandle,
        EnforceMemCheck enforceMemCheck) {
    auto htRes = ht.findForUpdate(itm.getKey());
    auto* v = htRes.pending.getSV();
    auto& hbl = htRes.getHBL();
    bool maybeKeyExists = true;
    MutationStatus status;
    std::optional<VBNotifyCtx> notifyCtx;
    VBQueueItemCtx queueItmCtx{
            genBySeqno,
            genCas,
            GenerateDeleteTime::No,
            (genCas == GenerateCas::Yes) ? TrackCasDrift::No
                                         : TrackCasDrift::Yes,
            DurabilityItemCtx{itm.getDurabilityReqs(), cookie},
            nullptr /* No pre link step needed */,
            {} /*overwritingPrepareSeqno*/,
            cHandle.getCanDeduplicate(),
            enforceMemCheck};

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

    cb::engine_errc ret = cb::engine_errc::success;
    switch (status) {
    case MutationStatus::NoMem:
        ret = cb::engine_errc::no_memory;
        break;
    case MutationStatus::InvalidCas:
        ret = cb::engine_errc::key_already_exists;
        break;
    case MutationStatus::IsLocked:
        ret = cb::engine_errc::locked;
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
        ret = cb::engine_errc::no_such_key;
        break;
    case MutationStatus::NeedBgFetch: { // CAS operation with non-resident item
        // + full eviction.
        if (v) { // temp item is already created. Simply schedule a bg fetch job
            return bgFetch(
                    std::move(hbl), itm.getKey(), *v, cookie, engine, true);
        }
        ret = addTempItemAndBGFetch(
                std::move(hbl), itm.getKey(), cookie, engine, true);
        break;
    }
    case MutationStatus::IsPendingSyncWrite:
        ret = cb::engine_errc::sync_write_in_progress;
        break;
    }

    return ret;
}

cb::engine_errc VBucket::setWithMeta(
        VBucketStateLockRef vbStateLock,
        Item& itm,
        uint64_t cas,
        uint64_t* seqno,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        CheckConflicts checkConflicts,
        bool allowExisting,
        GenerateBySeqno genBySeqno,
        GenerateCas genCas,
        const Collections::VB::CachingReadHandle& cHandle,
        EnforceMemCheck enforceMemCheck) {
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
                return bgFetch(
                        std::move(hbl), itm.getKey(), *v, cookie, engine, true);
            }

            switch (conflictResolver->resolve(*v,
                                              itm.getMetaData(),
                                              itm.getDataType(),
                                              itm.isDeleted())) {
            case ConflictResolution::Result::RejectBehind:
                ++stats.numOpsSetMetaResolutionFailed;
                // If the existing item happens to be a temporary item,
                // delete the item to save memory in the hash table
                if (v->isTempItem()) {
                    deleteStoredValue(hbl, *v);
                }
                return cb::engine_errc::key_already_exists;
            case ConflictResolution::Result::RejectIdentical:
                ++stats.numOpsSetMetaResolutionFailedIdentical;
                if (v->isTempItem()) {
                    deleteStoredValue(hbl, *v);
                }
                return cb::engine_errc::key_already_exists;
            case ConflictResolution::Result::Accept:;
            }
        } else {
            if (maybeKeyExistsInFilter(itm.getKey())) {
                return addTempItemAndBGFetch(
                        std::move(hbl), itm.getKey(), cookie, engine, true);
            }
            maybeKeyExists = false;
        }
    } else {
        // Avoid the bloomfilter call when genBySeqno is GenerateBySeqno::No
        // (while processing a DCP mutation).
        if (eviction == EvictionPolicy::Full &&
            genBySeqno == GenerateBySeqno::Yes) {
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
            (genCas == GenerateCas::Yes) ? TrackCasDrift::No
                                         : TrackCasDrift::Yes,
            DurabilityItemCtx{itm.getDurabilityReqs(), cookie},
            nullptr /* No pre link step needed */,
            {} /*overwritingPrepareSeqno*/,
            cHandle.getCanDeduplicate(),
            enforceMemCheck};

    auto [status, notifyCtx] = processSet(htRes,
                                          v,
                                          itm,
                                          cas,
                                          allowExisting,
                                          true,
                                          queueItmCtx,
                                          {/*no predicate*/},
                                          maybeKeyExists);

    cb::engine_errc ret = cb::engine_errc::success;
    switch (status) {
    case MutationStatus::NoMem:
        ret = cb::engine_errc::no_memory;
        break;
    case MutationStatus::InvalidCas:
        ret = cb::engine_errc::key_already_exists;
        break;
    case MutationStatus::IsLocked:
        ret = cb::engine_errc::locked;
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
        ret = cb::engine_errc::no_such_key;
        break;
    case MutationStatus::NeedBgFetch: { // CAS operation with non-resident item
        // + full eviction.
        if (v) { // temp item is already created. Simply schedule a bg fetch job
            return bgFetch(
                    std::move(hbl), itm.getKey(), *v, cookie, engine, true);
        }
        ret = addTempItemAndBGFetch(
                std::move(hbl), itm.getKey(), cookie, engine, true);
        break;
    }
    case MutationStatus::IsPendingSyncWrite:
        ret = cb::engine_errc::sync_write_in_progress;
        break;
    }

    return ret;
}

cb::engine_errc VBucket::deleteItem(
        VBucketStateLockRef vbStateLock,
        uint64_t& cas,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        std::optional<cb::durability::Requirements> durability,
        ItemMetaData* itemMeta,
        mutation_descr_t& mutInfo,
        const Collections::VB::CachingReadHandle& cHandle) {
    if (durability && durability->isValid()) {
        auto ret = checkDurabilityRequirements(*durability);
        if (ret != cb::engine_errc::success) {
            return ret;
        }
    }

    // For pending SyncDeletes we initially return
    // cb::engine_errc::sync_write_pending; will notify client when request is
    // committed / aborted later. This is effectively EWOULDBLOCK, but needs to
    // be distinguishable by the ep-engine caller (removeInner) from EWOULDBLOCK
    // for bg-fetch
    auto ret = durability ? cb::engine_errc::sync_write_pending
                          : cb::engine_errc::success;

    { // HashBucketLock scope
        auto htRes = ht.findForUpdate(cHandle.getKey());
        auto& hbl = htRes.getHBL();

        if (htRes.pending && htRes.pending->isPending()) {
            // Existing item is an in-flight SyncWrite
            return cb::engine_errc::sync_write_in_progress;
        }

        // When deleting an item, always use the StoredValue which is committed
        // for the various logic checks.
        if (!htRes.committed || htRes.committed->isTempInitialItem() ||
            isLogicallyNonExistent(*htRes.committed, cHandle)) {
            if (eviction == EvictionPolicy::Value) {
                return cb::engine_errc::no_such_key;
            }
            // Full eviction.
            if (!htRes.committed) { // Item might be evicted from cache.
                if (maybeKeyExistsInFilter(cHandle.getKey())) {
                    return addTempItemAndBGFetch(std::move(hbl),
                                                 cHandle.getKey(),
                                                 cookie,
                                                 engine,
                                                 true);
                }
                // As bloomfilter predicted that item surely doesn't
                // exist on disk, return ENOENT for deleteItem().
                return cb::engine_errc::no_such_key;
            }

            if (htRes.committed->isTempInitialItem()) {
                return bgFetch(std::move(hbl),
                               cHandle.getKey(),
                               *htRes.committed,
                               cookie,
                               engine,
                               true);
            }

            // Non-existent or deleted key.
            if ((htRes.committed->isTempNonExistentItem() ||
                 htRes.committed->isTempDeletedItem()) &&
                ht.hasTooManyTempItems()) {
                // Delete a temp non-existent item to ensure that
                // if a delete were issued over an item that doesn't
                // exist, then we don't preserve a temp item.
                deleteStoredValue(hbl, *htRes.committed);
            }
            return cb::engine_errc::no_such_key;
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
        std::optional<VBNotifyCtx> notifyCtx;

        // Determine which of committed / prepared SV to modify.
        auto* v = htRes.selectSVToModify(durability.has_value());

        if (htRes.committed->isExpired(ep_real_time())) {
            handlePreExpiry(htRes.getHBL(), *v);
            std::tie(delrv, v, notifyCtx) =
                    processExpiredItem(htRes, cHandle, ExpireBy::Access);
        } else {
            ItemMetaData metadata;
            metadata.revSeqno = htRes.committed->getRevSeqno() + 1;
            VBQueueItemCtx queueItmCtx{cHandle.getCanDeduplicate()};
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
            ret = cb::engine_errc::no_memory;
            break;
        case MutationStatus::InvalidCas:
            ret = cb::engine_errc::key_already_exists;
            break;
        case MutationStatus::IsLocked:
            ret = cb::engine_errc::locked_tmpfail;
            break;
        case MutationStatus::NotFound:
            ret = cb::engine_errc::no_such_key;
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
            ret = cb::engine_errc::sync_write_in_progress;
            break;
        }
    }

    if (ret == cb::engine_errc::success) {
        cHandle.incrementOpsDelete();
    }

    return ret;
}

cb::engine_errc VBucket::deleteWithMeta(
        VBucketStateLockRef vbStateLock,
        uint64_t& cas,
        uint64_t* seqno,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        CheckConflicts checkConflicts,
        const ItemMetaData& itemMeta,
        GenerateBySeqno genBySeqno,
        GenerateCas generateCas,
        uint64_t bySeqno,
        const Collections::VB::CachingReadHandle& cHandle,
        DeleteSource deleteSource,
        EnforceMemCheck enforceMemCheck) {
    const auto& key = cHandle.getKey();
    auto htRes = ht.findForUpdate(key);
    auto* v = htRes.selectSVToModify(false);
    auto& hbl = htRes.pending.getHBL();

    if (v && cHandle.isLogicallyDeleted(v->getBySeqno())) {
        // v is not really here, operate like it's not and skip conflict checks
        checkConflicts = CheckConflicts::No;
    }

    // We will need to know the type of the value, as if it has xattrs, we
    // will need to preserve the system xattrs. This does not apply to
    // replication calls, since the active would have already done this.
    if (state == vbucket_state_active) {
        if (v && v->isTempInitialItem()) {
            return bgFetch(std::move(hbl), key, *v, cookie, engine, true);
        }
        if (!v) {
            // Item is 1) deleted or not existent in the value eviction case OR
            // 2) deleted or evicted in the full eviction.
            if (maybeKeyExistsInFilter(key)) {
                return addTempItemAndBGFetch(
                        std::move(hbl), key, cookie, engine, true);
            }
        }
    }
    // At this point, v might be nullptr if the bloom filter said it does not
    // exist on disk.

    // Need conflict resolution?
    if (checkConflicts == CheckConflicts::Yes) {
        if (v) {
            switch (conflictResolver->resolve(
                    *v, itemMeta, PROTOCOL_BINARY_RAW_BYTES, true)) {
            case ConflictResolution::Result::RejectBehind:
                ++stats.numOpsDelMetaResolutionFailed;
                return cb::engine_errc::key_already_exists;
            case ConflictResolution::Result::RejectIdentical:
                ++stats.numOpsDelMetaResolutionFailedIdentical;
                return cb::engine_errc::key_already_exists;
            case ConflictResolution::Result::Accept:;
                // continue
            }
        } else {
            // Even though bloomfilter predicted that item doesn't exist
            // on disk, we must put this delete on disk if the cas is valid.
            auto rv = addTempStoredValue(hbl, key, EnforceMemCheck::Yes);
            if (rv.status == TempAddStatus::NoMem) {
                return cb::engine_errc::no_memory;
            }
            v = rv.storedValue;
            // TODO(MB-63781): This should be set to non-existent, but since it
            // would change the return status code to no_such_key, it will be
            // done under a separate MB.
            v->setTempDeleted();
        }
    } else if (!v) {
        // We should always try to persist a delete here.
        auto rv = addTempStoredValue(hbl, key, enforceMemCheck);
        if (rv.status == TempAddStatus::NoMem) {
            return cb::engine_errc::no_memory;
        }
        v = rv.storedValue;
        // TODO(MB-63781): This should be set to non-existent, but since it
        // would change the return status code to no_such_key, it will be done
        // under a separate MB.
        v->setTempDeleted();
        v->setCas(cas);
    }

    Expects(v);

    if (v->isLocked(ep_current_time()) && state != vbucket_state_active) {
        v->unlock();
    }

    MutationStatus delrv;
    std::optional<VBNotifyCtx> notifyCtx;
    bool metaBgFetch = true;
    std::unique_ptr<Item> itm;
    // MB-33919: The incoming meta.exptime should be used as the delete-time
    // so request GenerateDeleteTime::No, if the incoming value is 0, a new
    // delete-time will be generated.
    VBQueueItemCtx queueItmCtx{genBySeqno,
                               generateCas,
                               GenerateDeleteTime::No,
                               (generateCas == GenerateCas::Yes)
                                       ? TrackCasDrift::No
                                       : TrackCasDrift::Yes,
                               {},
                               nullptr /* No pre link step needed */,
                               {} /*overwritingPrepareSeqno*/,
                               cHandle.getCanDeduplicate()};

    // Note: Inbound replication doesn't touch the payload, active manipulates
    // it, replica stores as it is
    const bool mayNeedXattrsPreserving =
            state == vbucket_state_active &&
            cb::mcbp::datatype::is_xattr(v->getDatatype()) &&
            !v->isTempNonExistentItem();

    if (mayNeedXattrsPreserving && !v->isResident()) {
        // MB-25671: A temp deleted xattr with no value must be fetched before
        // the deleteWithMeta can be applied.
        // MB-36087: Any non-resident value
        // MB-56970: Any non-resident value, which _actually exists_
        delrv = MutationStatus::NeedBgFetch;
        metaBgFetch = false;
    } else if (mayNeedXattrsPreserving &&
               (itm = pruneXattrDocument(*v, itemMeta))) {
        if (auto status = checkCasForWrite(*v, cas, true); status) {
            // CAS mismatch
            delrv = *status;
        } else {
            // A new item has been generated and must be given a new seqno
            queueItmCtx.genBySeqno = GenerateBySeqno::Yes;

            // MB-36101: The result should always be a deleted item
            itm->setDeleted();
            std::tie(v, delrv, notifyCtx) =
                    updateStoredValue(hbl, *v, *itm, queueItmCtx);
        }
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

    // Note: v reset to a new ptr by multiple paths, still !null expected
    Expects(v);
    cas = v->getCas();

    switch (delrv) {
    case MutationStatus::NoMem:
        return cb::engine_errc::no_memory;
    case MutationStatus::InvalidCas:
        return cb::engine_errc::key_already_exists;
    case MutationStatus::IsLocked:
        return cb::engine_errc::locked_tmpfail;
    case MutationStatus::NotFound:
        return cb::engine_errc::no_such_key;
    case MutationStatus::WasDirty:
    case MutationStatus::WasClean: {
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
        return bgFetch(std::move(hbl), key, *v, cookie, engine, metaBgFetch);

    case MutationStatus::IsPendingSyncWrite:
        return cb::engine_errc::sync_write_in_progress;
    }
    return cb::engine_errc::success;
}

std::unique_ptr<CompactionBGFetchItem> VBucket::processExpiredItem(
        const Item& it, time_t startTime, ExpireBy source) {
    // Pending items should not be subject to expiry
    if (it.isPending()) {
        std::stringstream ss;
        ss << it;
        throw std::invalid_argument(
                "VBucket::processExpiredItem: Cannot expire pending item:" +
                cb::UserDataView(ss.str()).getSanitizedValue());
    }

    const DocKeyView& key = it.getKey();

    // Must obtain collection handle and hold it to ensure any queued item is
    // interlocked with collection membership changes.
    auto cHandle = manifest->lock(key);
    if (!cHandle.valid()) {
        // The collection has now been dropped, no action required
        return nullptr;
    }

    // The item is correctly trimmed (by the caller). Fetch the one in the
    // hashtable and replace it if the CAS match (same item; no race).
    // If not found in the hashtable we should add it as a deleted item
    auto htRes = ht.findForUpdate(key);
    auto* v = htRes.selectSVToModify(false);
    auto& hbl = htRes.getHBL();

    if (v) {
        if (v->getCas() != it.getCas()) {
            return nullptr;
        }

        if (v->isPending()) {
            // If cas is the same (above statement) and the HashTable has
            // returned a prepare then we must have loaded a logically complete
            // prepare (as we remove them from the HashTable at completion) for
            // some reason. The prepare should be in a maybe visible state but
            // it probably isn't a good idea to assert that here. In this case
            // we must do nothing as we MUST commit any maybe visible prepares.
            return nullptr;
        }

        if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            bool deleted = deleteStoredValue(hbl, *v);
            if (!deleted) {
                throw std::logic_error(
                        "VBucket::processExpiredItem: "
                        "Failed to delete seqno:" +
                        std::to_string(v->getBySeqno()) + " from bucket " +
                        to_string(hbl.getPosition()));
            }
            incExpirationStat(source);
        } else if (v->isExpired(startTime) && !v->isDeleted()) {
            VBNotifyCtx notifyCtx;
            ht.unlocked_updateStoredValue(hbl, *v, it);
            std::tie(std::ignore, std::ignore, notifyCtx) =
                    processExpiredItem(htRes, cHandle, source);
            // we unlock ht lock here because we want to avoid potential lock
            // inversions arising from notifyNewSeqno() call
            hbl.getHTLock().unlock();
            notifyNewSeqno(notifyCtx);
            doCollectionsStats(cHandle, notifyCtx);
        }
    } else if (eviction == EvictionPolicy::Full) {
        // If this expiration is from the compactor then we need to perform a
        // BGFetch to see if we need to expire the item
        if (source == ExpireBy::Compactor) {
            // Need to bg fetch the latest copy from disk and only expire if it
            // is the same as this item (i.e. same cas). This is an issue as
            // magma background compaction can run whilst writes are happening
            // and call the expiry callback on an old item after we have already
            // ejected the newer version of it. See MB-36373 for more details.
            //
            // There's no point in checking the bloom filter here. We're getting
            // called back from compaction so we know that the Item currently
            // exists on disk in some form.

            if (bucket->isCompactionExpiryFetchInline()) {
                // fetches should be completed in this thread, "inline"
                // during compaction, rather than being queued separately
                // as bgfetches.
                // This avoids compaction expiry fetches monopolising
                // bgfetcher time and adding excessive frontend latency.
                // Allow the caller to complete the fetch.
                return createBgFetchForCompactionExpiry(hbl, key, it);
            }

            bgFetchForCompactionExpiry(hbl, key, it);

            // Early return, don't want to bump any expiration stats here as we
            // need to bg fetch our item in first.
            return nullptr;
        }

        if (maybeKeyExistsInFilter(key)) {
            auto addTemp = addTempStoredValue(hbl, key, EnforceMemCheck::Yes);
            if (addTemp.status == TempAddStatus::NoMem) {
                return nullptr;
            }
            v = addTemp.storedValue;
            v->setTempDeleted();
            v->setRevSeqno(it.getRevSeqno());

            // @TODO perf: Investigate if it is necessary to add this to the
            //  HashTable
            ht.unlocked_updateStoredValue(hbl, *v, it);
            VBNotifyCtx notifyCtx;

            // processExpiredItem expires the StoredValue at htRes.committed so
            // we must set it to our new StoredValue
            htRes.committed = v;

            std::tie(std::ignore, std::ignore, notifyCtx) =
                    processExpiredItem(htRes, cHandle, source);
            // we unlock ht lock here because we want to avoid potential
            // lock inversions arising from notifyNewSeqno() call
            hbl.getHTLock().unlock();
            notifyNewSeqno(notifyCtx);
            doCollectionsStats(cHandle, notifyCtx);
        }
    }
    return nullptr;
}

cb::engine_errc VBucket::add(
        VBucketStateLockRef vbStateLock,
        Item& itm,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        const Collections::VB::CachingReadHandle& cHandle) {
    auto ret = checkDurabilityRequirements(itm);
    if (ret != cb::engine_errc::success) {
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
            return cb::engine_errc::sync_write_in_progress;
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
        VBQueueItemCtx queueItmCtx{cHandle.getCanDeduplicate()};
        queueItmCtx.preLinkDocumentContext = &preLinkDocumentContext;
        if (itm.isPending()) {
            queueItmCtx.durability =
                    DurabilityItemCtx{itm.getDurabilityReqs(), cookie};
        }
        auto [status, notifyCtx] =
                processAdd(htRes, v, itm, maybeKeyExists, queueItmCtx, cHandle);

        switch (status) {
        case AddStatus::NoMem:
            return cb::engine_errc::no_memory;
        case AddStatus::Exists:
            return cb::engine_errc::not_stored;
        case AddStatus::AddTmpAndBgFetch:
            return addTempItemAndBGFetch(
                    std::move(hbl), itm.getKey(), cookie, engine, true);
        case AddStatus::BgFetch:
            Expects(v &&
                    "VBucket::add: Expect a non-null StoredValue upon BgFetch "
                    "result");
            return bgFetch(
                    std::move(hbl), itm.getKey(), *v, cookie, engine, true);
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

    // For pending SyncWrites we initially return
    // cb::engine_errc::sync_write_pending; will notify client when request is
    // committed / aborted later. This is effectively EWOULDBLOCK, but needs to
    // be distinguishable by the ep-engine caller (storeIfInner) from
    // EWOULDBLOCK for bg-fetch
    return itm.isPending() ? cb::engine_errc::sync_write_pending
                           : cb::engine_errc::success;
}

std::pair<MutationStatus, GetValue> VBucket::processGetAndUpdateTtl(
        HashTable::HashBucketLock& hbl,
        StoredValue* v,
        time_t exptime,
        const Collections::VB::CachingReadHandle& cHandle) {
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
                    GetValue(nullptr, cb::engine_errc::key_already_exists, 0)};
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
        GetValue rv(v->toItem(getId(), hideLockedCas),
                    cb::engine_errc::success,
                    bySeqNo);

        if (exptime_mutated) {
            VBQueueItemCtx qItemCtx{cHandle.getCanDeduplicate()};
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
    }
    if (eviction == EvictionPolicy::Value) {
        return {MutationStatus::NotFound, GetValue()};
    }
    if (maybeKeyExistsInFilter(cHandle.getKey())) {
        return {MutationStatus::NeedBgFetch, GetValue()};
    }
    // As bloomfilter predicted that item surely doesn't exist
    // on disk, return ENOENT for getAndUpdateTtl().
    return {MutationStatus::NotFound, GetValue()};
}

GetValue VBucket::getAndUpdateTtl(
        VBucketStateLockRef vbStateLock,
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        time_t exptime,
        const Collections::VB::CachingReadHandle& cHandle) {
    auto res = fetchValueForWrite(cHandle);
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
                cb::engine_errc ec = bgFetch(std::move(res.lock),
                                             cHandle.getKey(),
                                             *res.storedValue,
                                             cookie,
                                             engine);
                return GetValue(nullptr, ec, res.storedValue->getBySeqno());
            }
            auto ec = addTempItemAndBGFetch(std::move(res.lock),
                                            cHandle.getKey(),
                                            cookie,
                                            engine,
                                            false);
            return GetValue(nullptr, ec, -1, true);
        }
        return gv;
    }
    case FetchForWriteResult::Status::ESyncWriteInProgress:
        return GetValue(nullptr, cb::engine_errc::sync_write_in_progress);
    }
    folly::assume_unreachable();
}

GetValue VBucket::getInternal(VBucketStateLockRef vbStateLock,
                              CookieIface* cookie,
                              EventuallyPersistentEngine& engine,
                              get_options_t options,
                              GetKeyOnly getKeyOnly,
                              const Collections::VB::CachingReadHandle& cHandle,
                              const ForGetReplicaOp getReplicaItem) {
    const TrackReference trackReference = (options & TRACK_REFERENCE)
                                                  ? TrackReference::Yes
                                                  : TrackReference::No;
    const bool metadataOnly = (options & ALLOW_META_ONLY);
    const bool getDeletedValue = (options & GET_DELETED_VALUE);
    const bool bgFetchRequired = (options & QUEUE_BG_FETCH);

    auto res = fetchValidValue(vbStateLock,
                               WantsDeleted::Yes,
                               trackReference,
                               cHandle,
                               getReplicaItem);

    this->isCalledHook();
    auto* v = res.storedValue;
    if (v) {
        // If the fetched value is a Prepared SyncWrite which may already have
        // been made visible to clients, then we cannot yet report _any_
        // value for this key until the Prepare has bee re-committed.
        if (v->isPreparedMaybeVisible()) {
            return GetValue(nullptr,
                            cb::engine_errc::sync_write_re_commit_in_progress);
        }

        // 1 If SV is deleted or expired and user didn't request deleted items
        // 2 (or) If collection says this key is gone.
        // then return ENOENT.
        if (((v->isDeleted() || v->isExpired(ep_real_time())) &&
             !getDeletedValue) ||
            cHandle.isLogicallyDeleted(v->getBySeqno())) {
            return {};
        }

        // If SV is a temp deleted item (i.e. marker added after a BgFetch to
        // note that the item has been deleted), *but* the user requested
        // full deleted items, then we need to fetch the complete deleted item
        // (including body) from disk.
        if (v->isTempDeletedItem() && getDeletedValue && !metadataOnly) {
            const auto queueBgFetch =
                    (bgFetchRequired) ? QueueBgFetch::Yes : QueueBgFetch::No;
            return getInternalNonResident(std::move(res.lock),
                                          cHandle.getKey(),
                                          cookie,
                                          engine,
                                          queueBgFetch,
                                          *v);
        }

        // If SV is otherwise a temp non-existent (i.e. a marker added after a
        // BgFetch to note that no such item exists) or temp deleted, then we
        // should cleanup the SV (if requested) before returning ENOENT (so we
        // don't keep temp items in HT).
        if (v->isTempDeletedItem() || v->isTempNonExistentItem()) {
            if ((options & DELETE_TEMP) || ht.hasTooManyTempItems()) {
                deleteStoredValue(res.lock, *v);
            }
            return {};
        }

        // If the value is not resident (and it was requested), wait for it...
        if (!v->isResident() && !metadataOnly) {
            auto queueBgFetch = (bgFetchRequired) ?
                    QueueBgFetch::Yes :
                    QueueBgFetch::No;
            return getInternalNonResident(std::move(res.lock),
                                          cHandle.getKey(),
                                          cookie,
                                          engine,
                                          queueBgFetch,
                                          *v);
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
                        cb::engine_errc::success,
                        v->getBySeqno(),
                        !v->isResident());
    }
    if (!getDeletedValue && (eviction == EvictionPolicy::Value)) {
        return {};
    }

    // Full eviction and need a bg fetch.
    if (bgFetchRequired && maybeKeyExistsInFilter(cHandle.getKey())) {
        auto ec = addTempItemAndBGFetch(std::move(res.lock),
                                        cHandle.getKey(),
                                        cookie,
                                        engine,
                                        metadataOnly);
        return GetValue(nullptr, ec, -1, true);
    }

    // If a bgfetch wasn't requested or a bloomfilter predicted that item surely
    // doesn't exist on disk, return ENOENT, for getInternal().
    return {};
}

cb::engine_errc VBucket::getMetaData(
        CookieIface* cookie,
        EventuallyPersistentEngine& engine,
        const Collections::VB::CachingReadHandle& cHandle,
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
            return cb::engine_errc::sync_write_re_commit_in_progress;
        }
        stats.numOpsGetMeta++;
        if (v->isTempInitialItem()) {
            // Need bg meta fetch.
            return bgFetch(
                    std::move(hbl), cHandle.getKey(), *v, cookie, engine, true);
        }
        if (v->isTempNonExistentItem()) {
            metadata.cas = v->getCas();
            return cb::engine_errc::no_such_key;
        }
        if (cHandle.isLogicallyDeleted(v->getBySeqno())) {
            return cb::engine_errc::no_such_key;
        }
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

        return cb::engine_errc::success;
    }

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
                std::move(hbl), cHandle.getKey(), cookie, engine, true);
    }
    stats.numOpsGetMeta++;
    return cb::engine_errc::no_such_key;
}

cb::engine_errc VBucket::getKeyStats(
        VBucketStateLockRef vbStateLock,
        CookieIface& cookie,
        EventuallyPersistentEngine& engine,
        struct key_stats& kstats,
        WantsDeleted wantsDeleted,
        const Collections::VB::CachingReadHandle& cHandle) {
    auto res = fetchValidValue(
            vbStateLock, WantsDeleted::Yes, TrackReference::Yes, cHandle);
    auto* v = res.storedValue;

    if (v) {
        if (v->isPreparedMaybeVisible()) {
            return cb::engine_errc::sync_write_re_commit_in_progress;
        }
        if ((v->isDeleted() || cHandle.isLogicallyDeleted(v->getBySeqno())) &&
            wantsDeleted == WantsDeleted::No) {
            return cb::engine_errc::no_such_key;
        }

        if (v->isTempNonExistentItem() || v->isTempDeletedItem()) {
            if (ht.hasTooManyTempItems()) {
                deleteStoredValue(res.lock, *v);
            }
            return cb::engine_errc::no_such_key;
        }
        if (eviction == EvictionPolicy::Full && v->isTempInitialItem()) {
            return bgFetch(std::move(res.lock),
                           cHandle.getKey(),
                           *v,
                           &cookie,
                           engine,
                           true);
        }
        kstats.logically_deleted =
                v->isDeleted() || cHandle.isLogicallyDeleted(v->getBySeqno());
        kstats.dirty = v->isDirty();
        kstats.exptime = v->getExptime();
        kstats.datatype = v->getDatatype();
        kstats.flags = v->getFlags();
        kstats.cas = v->getCas();
        kstats.vb_state = getState();
        kstats.resident = v->isResident();

        return cb::engine_errc::success;
    }
    if (eviction == EvictionPolicy::Value) {
        return cb::engine_errc::no_such_key;
    }
    if (maybeKeyExistsInFilter(cHandle.getKey())) {
        return addTempItemAndBGFetch(
                std::move(res.lock), cHandle.getKey(), &cookie, engine, true);
    }
    // If bgFetch were false, or bloomfilter predicted that
    // item surely doesn't exist on disk, return ENOENT for
    // getKeyStats().
    return cb::engine_errc::no_such_key;
}

GetValue VBucket::getLocked(rel_time_t currentTime,
                            std::chrono::seconds lockTimeout,
                            CookieIface* cookie,
                            EventuallyPersistentEngine& engine,
                            const Collections::VB::CachingReadHandle& cHandle) {
    auto res = fetchValueForWrite(cHandle);
    switch (res.status) {
    case FetchForWriteResult::Status::OkFound: {
        auto* v = res.storedValue;
        if (isLogicallyNonExistent(*v, cHandle)) {
            ht.cleanupIfTemporaryItem(res.lock, *v);
            return GetValue(nullptr, cb::engine_errc::no_such_key);
        }

        // if v is locked return error
        if (v->isLocked(currentTime)) {
            return GetValue(nullptr, cb::engine_errc::locked_tmpfail);
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (cookie) {
                cb::engine_errc ec = bgFetch(std::move(res.lock),
                                             cHandle.getKey(),
                                             *v,
                                             cookie,
                                             engine);
                return GetValue(nullptr, ec, -1, true);
            }
            return GetValue(nullptr, cb::engine_errc::would_block, -1, true);
        }

        // acquire lock and increment cas value
        v->lock(currentTime + lockTimeout.count(), nextHLCCas());

        auto it = v->toItem(getId());
        it->setCas(v->getCasForWrite(currentTime));

        return GetValue(std::move(it));
    }
    case FetchForWriteResult::Status::OkVacant:
        // No value found in the hashtable.
        switch (eviction) {
        case EvictionPolicy::Value:
            return GetValue(nullptr, cb::engine_errc::no_such_key);

        case EvictionPolicy::Full:
            if (maybeKeyExistsInFilter(cHandle.getKey())) {
                cb::engine_errc ec = addTempItemAndBGFetch(std::move(res.lock),
                                                           cHandle.getKey(),
                                                           cookie,
                                                           engine,
                                                           false);
                return GetValue(nullptr, ec, -1, true);
            }
            // As bloomfilter predicted that item surely doesn't exist
            // on disk, return ENOENT for getLocked().
            return GetValue(nullptr, cb::engine_errc::no_such_key);
        }
        folly::assume_unreachable();
    case FetchForWriteResult::Status::ESyncWriteInProgress:
        return GetValue(nullptr, cb::engine_errc::sync_write_in_progress);
    }
    folly::assume_unreachable();
}

void VBucket::deletedOnDiskCbk(const Item& queuedItem, bool deleted) {
    auto res = ht.findItem(queuedItem);
    auto* v = res.storedValue;

    // Delete the item in the hash table iff:
    //  1. Item is existent in hashtable, and deleted flag is true
    //  2. seqno of queued item matches seqno of hash table item
    if (v && v->isDeleted() && queuedItem.getBySeqno() == v->getBySeqno()) {
        if (bucket && bucket->isWarmupLoadingData()) {
            // During warmup, we only mark the deletes as clean as they are
            // persisted - we cannot remove them from the HT. They will get
            // cleaned up by the expiry pager.
            ht.markSVClean(res.lock, *v);
            // Make immediate candidate for eviction.
            ht.setSVFreqCounter(res.lock, *v, 0);
        } else {
            bool isDeleted = deleteStoredValue(res.lock, *v);
            if (!isDeleted) {
                throw std::logic_error(
                        "deletedOnDiskCbk:callback: "
                        "Failed to delete key with seqno:" +
                        std::to_string(v->getBySeqno()) + "' from bucket " +
                        to_string(res.lock.getPosition()));
            }
        }

        /**
         * Deleted items are to be added to the bloomfilter,
         * in either eviction policy.
         */
        addToFilter(queuedItem.getKey());
    }

    if (deleted) {
        ++stats.totalPersisted;

        /**
         * MB-30137: Decrement the total number of on-disk items. This needs to
         * be done to ensure that the item count is accurate in the case of full
         * eviction. We should only decrement the counter for committed (via
         * mutation or commit) items as we only increment for these.
         */
        if (v && queuedItem.isCommitted()) {
            decrNumTotalItems();
            ++opsDelete;
        }
    }
    decrMetaDataDisk(queuedItem);
}

bool VBucket::removeItemFromMemory(const Item& item) {
    auto htRes = ht.findItem(item);
    if (!htRes.storedValue) {
        return false;
    }
    return deleteStoredValue(htRes.lock, *htRes.storedValue);
}

void VBucket::dump(std::ostream& ostream) const {
    ostream << "VBucket[" << this << "] " << getId()
            << " with state: " << toString(getState())
            << " numItems:" << getNumItems()
            << " numNonResident:" << getNumNonResidentItems()
            << " ht: " << std::endl
            << "  " << ht << std::endl
            << "]" << std::endl;
}

bool VBucket::hasMemoryForStoredValue(const Item& item) {
    if (!bucket) {
        // Code-path used during testing of the VBucket class, when bucket is
        // nullptr.
        return stats.getEstimatedTotalMemoryUsed() +
                       estimateRequiredMemory(item) <
               stats.getMaxDataSize();
    }

    size_t requiredMemory = estimateRequiredMemory(item);
    return bucket->getEPEngine().getMemoryTracker().isBelowMutationMemoryQuota(
            requiredMemory);
}

void VBucket::_addStats(VBucketStatsDetailLevel detail,
                        const AddStatFn& add_stat,
                        CookieIface& c) {
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
        addStat("ht_memory", ht.getMemoryOverhead(), add_stat, c);
        addStat("ht_num_items", ht.getNumItems(), add_stat, c);
        addStat("ht_num_deleted_items", ht.getNumDeletedItems(), add_stat, c);
        addStat("ht_num_in_memory_items",
                ht.getNumInMemoryItems(),
                add_stat,
                c);
        addStat("ht_num_in_memory_non_resident_items",
                ht.getNumInMemoryNonResItems(),
                add_stat,
                c);
        addStat("ht_num_temp_items", ht.getNumTempItems(), add_stat, c);
        addStat("ht_item_memory", ht.getItemMemory(), add_stat, c);
        addStat("ht_item_memory_uncompressed",
                ht.getUncompressedItemMemory(),
                add_stat,
                c);
        addStat("ht_cache_size", ht.getItemMemory(), add_stat, c);
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
        addBloomFilterStats(add_stat, c);
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
        addStat("sync_write_committed_not_durable_count",
                getSyncWriteCommittedNotDurableCount(),
                add_stat,
                c);
        addStat("sync_write_aborted_count",
                getSyncWriteAbortedCount(),
                add_stat,
                c);
        addStat("max_visible_seqno",
                checkpointManager->getMaxVisibleSeqno(),
                add_stat,
                c);
        addStat("persistence_seqno", getPersistenceSeqno(), add_stat, c);
        hlc.addStats(statPrefix, add_stat, c);
    }
        // fallthrough
    case VBucketStatsDetailLevel::Durability:
        addStat("high_seqno", getHighSeqno(), add_stat, c);
        addStat("topology", getReplicationTopology(), add_stat, c);
        addStat("high_prepared_seqno", getHighPreparedSeqno(), add_stat, c);
        // fallthrough
    case VBucketStatsDetailLevel::State:
        // adds the vbucket state stat (unnamed stat)
        addStat(nullptr, toString(state), add_stat, c);
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

void VBucket::decrDirtyQueueAge(size_t decrementBy) {
    dirtyQueueAge.fetch_sub(decrementBy);
}

void VBucket::decrDirtyQueuePendingWrites(size_t decrementBy) {
    dirtyQueuePendingWrites.fetch_sub(decrementBy);
}

std::optional<MutationStatus> VBucket::checkCasForWrite(StoredValue& v,
                                                        uint64_t cas,
                                                        bool isDelete) const {
    // Perform CAS check. If document is locked then the request cas must
    // match the locked CAS (i.e. the same CAS returned from the getLocked()
    // operation), if not locked then check against the normal cas (last
    // modified time) if CAS specified otherwise no CAS permitted.
    const auto now = ep_current_time();
    if (v.isLocked(now)) {
        if (cas != v.getCasForWrite(now)) {
            return MutationStatus::IsLocked;
        }
        return {};
    }

    if (cas == 0 || cas == v.getCas()) {
        // No CAS check required.
        return {};
    }
    // CAS mismatch - determine which status to return.

    if (v.isTempNonExistentItem()) {
        // This is a temporary item which marks a key as non-existent;
        // therefore specifying a non-matching CAS should be exposed
        // as item not existing.
        return MutationStatus::NotFound;
    }
    if ((v.isTempDeletedItem() || v.isDeleted()) && !isDelete) {
        // Existing item is deleted, and we are not replacing it with
        // a (different) deleted value - return not existing.
        return MutationStatus::NotFound;
    }
    // None of the above special cases; the existing item cannot be
    // modified with the specified CAS.
    return MutationStatus::InvalidCas;
}

std::pair<MutationStatus, std::optional<VBNotifyCtx>> VBucket::processSet(
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

        if (!itm.isCommitted()) {
            // We always expect that the item we are trying to store is in the
            // committed namespace here because we complete associated prepares.
            throw std::logic_error(
                    fmt::format("VBucket::processSet: {} expected a complete "
                                "item but the item is a prepare {} with "
                                "seqno:{}. Existing prepare has seqno:{}",
                                getId(),
                                cb::UserData(itm.getKey().to_string()),
                                itm.getBySeqno(),
                                v->getBySeqno()));
        }
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

std::pair<MutationStatus, std::optional<VBNotifyCtx>> VBucket::processSetInner(
        HashTable::FindUpdateResult& htRes,
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

    if (queueItmCtx.enforceMemCheck == EnforceMemCheck::Yes) {
        if (bucket && KVBucket::isCheckpointMemoryStateFull(
                              bucket->verifyCheckpointMemoryState())) {
            return {MutationStatus::NoMem, {}};
        }

        if (!hasMemoryForStoredValue(itm)) {
            return {MutationStatus::NoMem, {}};
        }
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
        (cas || storeIfStatus == cb::StoreIfStatus::GetItemInfo ||
         itm.shouldPreserveTtl())) {
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
        if (auto status = checkCasForWrite(*committed, cas, itm.isDeleted());
            status) {
            return {*status, {}};
        }
        /* allow operation*/
        committed->unlock();
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
        if (itm.shouldPreserveTtl() && !v->isDeleted() &&
            !v->isTempNonExistentItem()) {
            // copy the expiry time for the alive, non-temp item over.
            itm.setExpTime(v->getExptime());
        }

        // This is a new SyncWrite, we just want to add a new prepare unless we
        // still have a completed prepare (Ephemeral) which we should replace
        // instead.
        if (v->isCommitted() && !v->isPrepareCompleted() && itm.isPending()) {
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
    // cas was regenerated, update to return to client
    if (v->getCas() != itm.getCas()) {
        itm.setCas(v->getCas());
    }
    return {status, notifyCtx};
}

std::pair<AddStatus, std::optional<VBNotifyCtx>> VBucket::processAdd(
        HashTable::FindUpdateResult& htRes,
        StoredValue*& v,
        Item& itm,
        bool maybeKeyExists,
        const VBQueueItemCtx& queueItmCtx,
        const Collections::VB::CachingReadHandle& cHandle) {
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

    // If attempting to add a deleted item, then there cannot be _any_
    // StoredValue present - alive or deleted - including a tombstone.
    if (itm.isDeleted()) {
        if (!committed && !areDeletedItemsAlwaysResident()) {
            // No committed item resident, but tombstone could exist. Must
            // bgfetch to confirm / deny.
            return {AddStatus::AddTmpAndBgFetch, {}};
        }
        if (committed) {
            if (committed->isTempInitialItem()) {
                // We have a tempInitialItem - need to wait for bgFetch to
                // complete to see if a tombstone exists or not...
                return {AddStatus::BgFetch, {}};
            }
            if (!committed->isTempNonExistentItem()) {
                // .. anything apart from TempNonExistent (alive, deleted,
                // tempDeleted, ...) means there is an existing item of some
                // form (alive or deleted) and hence an add() of a Deleted item
                // must fail.
                return {AddStatus::Exists, {}};
            }
        }
    }

    if (bucket && KVBucket::isCheckpointMemoryStateFull(
                          bucket->verifyCheckpointMemoryState())) {
        return {AddStatus::NoMem, {}};
    }

    if (!hasMemoryForStoredValue(itm)) {
        return {AddStatus::NoMem, {}};
    }

    std::pair<AddStatus, VBNotifyCtx> rv = {AddStatus::Success, {}};

    // We cannot replace a committed SV with a pending one. If we were to do
    // so then a delete that has not yet been persisted could be replaced
    // with a prepare. A subsequent get could trigger a bg fetch that may
    // return the old (not deleted) document from disk if it runs before the
    // flusher. As such, we must keep the unpersisted delete and add a new
    // prepare for the SyncWrite. Any get will see the unpersisted delete
    // and return KEYNOENT.
    auto replacingCommittedItemWithPending =
            v && v == htRes.committed && itm.isPending();
    if (v && !replacingCommittedItemWithPending) {
        if (v->isTempInitialItem() && eviction == EvictionPolicy::Full &&
            maybeKeyExists) {
            // Need to figure out if an item exists on disk
            return {AddStatus::BgFetch, {}};
        }

        rv.first = (v->isDeleted() || v->isExpired(ep_real_time()))
                           ? AddStatus::UnDel
                           : AddStatus::Success;

        // this is an add operation; if we have reached this stage then
        // either:
        //  * there is an existing committed version of the item but it is:
        //    * deleted
        //    * temporary and deleted
        //    * logically deleted (collections)
        //    * expired
        // or:
        //  * there is no existing committed version of this item
        //
        // The presence or absence of a completed prepare (ephemeral) does not
        // change what revSeqno we should use. An incomplete prepare would
        // have blocked this operation already as sync write in progress.
        if (committed && !(committed->isTempNonExistentItem() ||
                           committed->isTempInitialItem())) {
            // the new item's revSeqno can be set to exactly one greater than
            // the old deleted value.
            itm.setRevSeqno(committed->getRevSeqno() + 1);
        } else {
            // This item may have previously existed and been deleted, but is
            // no longer available. To ensure rev seqno monotonicity, set the
            // rev seqno of the new item to one greater than any previously seen
            // delete.
            itm.setRevSeqno(ht.getMaxDeletedRevSeqno() + 1);
        }

        std::tie(v, std::ignore, rv.second) =
                updateStoredValue(htRes.getHBL(), *v, itm, queueItmCtx);
    } else {
        if (itm.getBySeqno() != StoredValue::state_temp_init &&
            !replacingCommittedItemWithPending) {
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
        } else if (replacingCommittedItemWithPending) {
            rv.first = AddStatus::UnDel;
        }
    }

    return rv;
}

std::tuple<MutationStatus, StoredValue*, std::optional<VBNotifyCtx>>
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
            return {MutationStatus::IsPendingSyncWrite, &v, std::nullopt};
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

std::tuple<MutationStatus, StoredValue*, std::optional<VBNotifyCtx>>
VBucket::processSoftDeleteInner(const HashTable::HashBucketLock& hbl,
                                StoredValue& v,
                                uint64_t cas,
                                const ItemMetaData& metadata,
                                const VBQueueItemCtx& queueItmCtx,
                                bool use_meta,
                                uint64_t bySeqno,
                                DeleteSource deleteSource) {
    std::optional<VBNotifyCtx> empty;
    if (v.isTempInitialItem() && eviction == EvictionPolicy::Full) {
        return std::make_tuple(MutationStatus::NeedBgFetch, &v, empty);
    }
    if (auto status = checkCasForWrite(v, cas, true); status) {
        return std::make_tuple(*status, &v, empty);
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
        auto requirements = std::get<cb::durability::Requirements>(
                queueItmCtx.durability->requirementsOrPreparedSeqno);

        // @TODO potentially inefficient to recreate the item in the below
        // update/add cases. Could rework ht.unlocked_softDeleteStoredValue
        // to only mark CommittedViaPrepares as CommittedViaMutation and use
        // softDeletedStoredValue instead.
        if (v.isPrepareCompleted()) {
            auto itm = v.toItem(getId(),
                                StoredValue::HideLockedCas::No,
                                StoredValue::IncludeValue::No,
                                requirements);
            itm->setDeleted(DeleteSource::Explicit);

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
VBucket::processExpiredItem(HashTable::FindUpdateResult& htRes,
                            const Collections::VB::CachingReadHandle& cHandle,
                            ExpireBy expirySource) {
    if (!htRes.getHBL().getHTLock()) {
        throw std::invalid_argument(
                "VBucket::processExpiredItem: htLock not held for " +
                getId().to_string());
    }

    if (!cHandle.valid()) {
        throw std::invalid_argument(
                "VBucket::processExpiredItem: cHandle not valid for cid:" +
                cHandle.getKey().getCollectionID().to_string());
    }

    if (htRes.pending && !htRes.pending->isPrepareCompleted()) {
        return std::make_tuple(MutationStatus::IsPendingSyncWrite,
                               htRes.committed,
                               VBNotifyCtx{});
    }

    // Callers should have ensured that v exists
    Expects(htRes.committed);
    auto& v = *htRes.committed;

    if (cHandle.isLogicallyDeleted(v.getBySeqno())) {
        return std::make_tuple(
                MutationStatus::NotFound, nullptr, VBNotifyCtx{});
    }

    if (v.isTempInitialItem() && eviction == EvictionPolicy::Full) {
        incExpirationStat(expirySource);
        return std::make_tuple(
                MutationStatus::NeedBgFetch,
                &v,
                queueDirty(htRes.getHBL(),
                           v,
                           VBQueueItemCtx{cHandle.getCanDeduplicate()}));
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
    bool onlyMarkDeleted =
            value && cb::mcbp::datatype::is_xattr(v.getDatatype());
    v.setRevSeqno(v.getRevSeqno() + 1);
    auto [newSv, delStatus, notifyCtx] =
            softDeleteStoredValue(htRes.getHBL(),
                                  v,
                                  onlyMarkDeleted,
                                  VBQueueItemCtx{cHandle.getCanDeduplicate()},
                                  v.getBySeqno(),
                                  DeleteSource::TTL);
    switch (delStatus) {
    case DeletionStatus::Success:
        ht.updateMaxDeletedRevSeqno(newSv->getRevSeqno() + 1);
        incExpirationStat(expirySource);
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
    ht.unlocked_del(hbl, v);
    return true;
}

VBucket::AddTempSVResult VBucket::addTempStoredValue(
        const HashTable::HashBucketLock& hbl,
        const DocKeyView& key,
        EnforceMemCheck enforceMemCheck) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "VBucket::addTempStoredValue: htLock not held for " +
                getId().to_string());
    }

    Item itm(key,
             /*flags*/ 0,
             /*exp*/ 0,
             /*data*/ nullptr,
             /*size*/ 0,
             PROTOCOL_BINARY_RAW_BYTES,
             0,
             StoredValue::state_temp_init);

    if (enforceMemCheck == EnforceMemCheck::Yes &&
        !hasMemoryForStoredValue(itm)) {
        return {TempAddStatus::NoMem, nullptr};
    }

    /* A 'temp initial item' is just added to the hash table. It is
       not put on checkpoint manager or sequence list */
    StoredValue* v = ht.unlocked_addNewStoredValue(hbl, itm);

    updateRevSeqNoOfNewStoredValue(*v);

    // MB-49207: We set the cas of temp items so that when we complete a BGFetch
    // we can check that we are replacing the correct temp item with the one
    // that triggered the BGFetch. If we didn't then we'd run the risk of
    // fetching older revisions of documents back into memory.
    v->setCas(nextHLCCas());

    return {TempAddStatus::BgFetch, v};
}

void VBucket::notifyNewSeqno(
        const VBNotifyCtx& notifyCtx) {
    if (bucket) {
        bucket->notifyNewSeqno(getId(), notifyCtx);
    }
}

void VBucket::doCollectionsStats(
        const Collections::VB::CachingReadHandle& cHandle,
        const VBNotifyCtx& notifyCtx) {
    cHandle.setHighSeqno(notifyCtx.getSeqno(),
                         isPrepareOrAbort(notifyCtx.getOp())
                                 ? Collections::VB::HighSeqnoType::PrepareAbort
                                 : Collections::VB::HighSeqnoType::Committed);

    if (notifyCtx.getItemCountDifference() == 1) {
        cHandle.incrementItemCount();
    } else if (notifyCtx.getItemCountDifference() == -1) {
        cHandle.decrementItemCount();
    }
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

cb::time::steady_clock::time_point VBucket::addHighPriorityVBEntry(
        std::unique_ptr<SeqnoPersistenceRequest> request) {
    std::unique_lock<std::mutex> lh(hpVBReqsMutex);
    hpVBReqs.emplace_back(std::move(request));
    numHpVBReqs.store(hpVBReqs.size());

    EP_LOG_INFO(
            "Added SeqnoPersistence request for {}, requested-seqno:{}, "
            "high-seqno: {}, persisted-seqno:{}, cookie:{}, timeout:{}",
            getId(),
            hpVBReqs.back()->seqno,
            getHighSeqno(),
            getPersistenceSeqno(),
            static_cast<const void*>(hpVBReqs.back()->cookie),
            hpVBReqs.back()->timeout.count());

    return hpVBReqs.back()->getDeadline();
}

SeqnoPersistenceRequestNotifications
VBucket::getSeqnoPersistenceRequestsToNotify(EventuallyPersistentEngine& engine,
                                             uint64_t seqno) {
    std::unique_lock<std::mutex> lh(hpVBReqsMutex);
    std::unordered_map<CookieIface*, cb::engine_errc> toNotify;

    auto itr = hpVBReqs.begin();
    std::optional<cb::time::steady_clock::time_point> nextDeadline;

    while (itr != hpVBReqs.end()) {
        Expects(*itr);
        const auto& req = *itr;
        const auto now = cb::time::steady_clock::now();
        if (req->seqno <= seqno) {
            toNotify[req->cookie] = cb::engine_errc::success;
            stats.seqnoPersistenceHisto.add(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            req->getDuration(now)));
            EP_LOG_INFO(
                    "Notified SeqnoPersistence completion for {} Check for: "
                    "{}, "
                    "Persisted upto: {}, cookie {}",
                    getId(),
                    req->seqno,
                    seqno,
                    static_cast<const void*>(req->cookie));
            itr = hpVBReqs.erase(itr);
        } else if (now >= req->getDeadline()) { // >= permits a 0 wait for tests
            toNotify[req->cookie] = cb::engine_errc::temporary_failure;
            EP_LOG_WARN(
                    "Notified SeqnoPersistence timeout for {} Check for: {}, "
                    "Persisted upto: {}, cookie {}",
                    getId(),
                    req->seqno,
                    seqno,
                    static_cast<const void*>(req->cookie));

            req->expired();

            itr = hpVBReqs.erase(itr);
        } else {
            if (!nextDeadline) {
                nextDeadline = req->getDeadline();
            } else {
                nextDeadline =
                        std::min(req->getDeadline(), nextDeadline.value());
            }
            ++itr;
        }
    }
    numHpVBReqs.store(hpVBReqs.size());
    return {toNotify, nextDeadline};
}

bool VBucket::doesSeqnoSatisfyAnySeqnoPersistenceRequest(uint64_t seqno) {
    std::unique_lock<std::mutex> lh(hpVBReqsMutex);
    for (const auto& req : hpVBReqs) {
        if (req->seqno <= seqno) {
            return true;
        }
    }
    return false;
}

std::optional<cb::time::steady_clock::time_point>
VBucket::notifyHighPriorityRequests(EventuallyPersistentEngine& engine,
                                    uint64_t seqno) {
    auto toNotify = getSeqnoPersistenceRequestsToNotify(engine, seqno);

    for (auto& notify : toNotify.notifications) {
        engine.notifyIOComplete(notify.first, notify.second);
    }

    return toNotify.nextDeadline;
}

std::map<CookieIface*, cb::engine_errc> VBucket::tmpFailAndGetAllHpNotifies(
        EventuallyPersistentEngine& engine) {
    std::map<CookieIface*, cb::engine_errc> toNotify;

    std::lock_guard<std::mutex> lh(hpVBReqsMutex);

    for (auto& entry : hpVBReqs) {
        toNotify[entry->cookie] = cb::engine_errc::temporary_failure;
        engine.clearEngineSpecific(*entry->cookie);
    }
    hpVBReqs.clear();

    return toNotify;
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
                          cb::mcbp::datatype::is_snappy(v.getDatatype()));
    xattr.prune_user_keys();

    auto prunedXattrs = xattr.finalize();

    if (prunedXattrs.empty()) {
        return {};
    }

    // Something remains - Create a Blob and copy-in just the XATTRs
    auto newValue = Blob::New(prunedXattrs.data(), prunedXattrs.size());
    auto rv = v.toItem(getId());
    rv->setCas(itemMeta.cas);
    rv->setFlags(itemMeta.flags);
    rv->setExpTime(itemMeta.exptime);
    rv->setRevSeqno(itemMeta.revSeqno);
    rv->replaceValue(TaggedPtr<Blob>(newValue, TaggedPtrBase::NoTagValue));
    rv->setDataType(PROTOCOL_BINARY_DATATYPE_XATTR);
    return rv;
}

bool VBucket::isLogicallyNonExistent(
        const StoredValue& v,
        const Collections::VB::CachingReadHandle& cHandle) {
    Expects(v.isCommitted());
    return v.isDeleted() || v.isTempDeletedItem() ||
           v.isTempNonExistentItem() ||
           cHandle.isLogicallyDeleted(v.getBySeqno());
}

cb::engine_errc VBucket::seqnoAcknowledged(
        const std::shared_lock<folly::SharedMutex>& vbStateLock,
        const std::string& replicaId,
        uint64_t preparedSeqno) {
    // We may receive an ack after we have set a vBucket to dead during a
    // takeover; just ignore it.
    if (getState() == vbucket_state_dead) {
        return cb::engine_errc::success;
    }
    return getActiveDM().seqnoAckReceived(replicaId, preparedSeqno);
}

void VBucket::notifyPersistenceToDurabilityMonitor() {
    std::shared_lock wlh(stateLock);

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

cb::engine_errc VBucket::checkDurabilityRequirements(const Item& item) {
    if (item.isPending()) {
        return checkDurabilityRequirements(item.getDurabilityReqs());
    }
    return cb::engine_errc::success;
}

cb::engine_errc VBucket::checkDurabilityRequirements(
        const cb::durability::Requirements& reqs) {
    if (!isValidDurabilityLevel(reqs.getLevel())) {
        return cb::engine_errc::durability_invalid_level;
    }
    if (!getActiveDM().isDurabilityPossible()) {
        return cb::engine_errc::durability_impossible;
    }
    return cb::engine_errc::success;
}

void VBucket::removeAcksFromADM(const std::string& node) {
    if (state == vbucket_state_active) {
        getActiveDM().removedQueuedAck(node);
    }
}

void VBucket::removeAcksFromADM(
        const std::string& node,
        const std::unique_lock<folly::SharedMutex>& vbstateLock) {
    removeAcksFromADM(node);
}

void VBucket::removeAcksFromADM(
        const std::string& node,
        const std::shared_lock<folly::SharedMutex>& vbstateLock) {
    removeAcksFromADM(node);
}

void VBucket::setDuplicatePrepareWindow() {
    const auto& pdm = getPassiveDM();
    // We should only see duplicates for prepares currently in trackedWrites;
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

bool VBucket::isReceivingDiskSnapshot() const {
    return checkpointManager->isOpenCheckpointDisk();
}

uint64_t VBucket::getMaxVisibleSeqno() const {
    return checkpointManager->getMaxVisibleSeqno();
}

void VBucket::dropPendingKey(const DocKeyView& key, int64_t seqno) {
    std::shared_lock vbStateLh(getStateLock());
    switch (state) {
    case vbucket_state_active:
        getActiveDM().eraseSyncWrite(key, seqno);
        return;
    case vbucket_state_replica:
    case vbucket_state_pending:
        getPassiveDM().eraseSyncWrite(key, seqno);
        return;
    case vbucket_state_dead:
        // Do nothing, the vbucket is dead and the DM shouldn't be used
        return;
    }
    folly::assume_unreachable();
}

void VBucket::scheduleDestruction(CheckpointList&& checkpoints) const {
    if (bucket) {
        bucket->scheduleDestruction(std::move(checkpoints), id);
    }
}

void VBucket::failAllSeqnoPersistenceReqs(EventuallyPersistentEngine& engine) {
    auto toNotify = tmpFailAndGetAllHpNotifies(engine);

    for (auto& notify : toNotify) {
        engine.notifyIOComplete(notify.first, notify.second);
    }
}

void VBucket::notifyReplication() {
    if (bucket) {
        bucket->notifyReplication(getId(), queue_op::empty);
    }
}

bool VBucket::isHistoryRetentionEnabled() const {
    if (!bucket) {
        // Test only path
        return true;
    }
    return bucket->isHistoryRetentionEnabled();
}

void VBucket::forceMaxCas(uint64_t cas) {
    hlc.forceMaxHLC(cas);
    checkpointManager->queueSetVBState();
}

void VBucket::setOrForceMaxCasAndTrackDrift(uint64_t cas) {
    // Active tracks max_cas, whilst a replica must track the active.
    if (state == vbucket_state_active) {
        hlc.setMaxHLCAndTrackDrift(cas);
    } else {
        hlc.forceMaxHLCAndTrackDrift(cas);
    }
}
