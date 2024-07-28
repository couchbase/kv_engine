/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/producer.h"

#include "backfill.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/manager.h"
#include "collections/vbucket_filter.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "connhandler_impl.h"
#include "dcp/active_stream.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/backfill-manager.h"
#include "dcp/dcpconnmap.h"
#include "dcp/response.h"
#include "ep_time.h"
#include "eviction_utils.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "learning_age_and_mfu_based_eviction.h"
#include "objectregistry.h"
#include "snappy-c.h"
#include "trace_helpers.h"
#include "vbucket.h"
#include <executor/executorpool.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <memcached/connection_iface.h>
#include <memcached/cookie_iface.h>
#include <memcached/tracer.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <platform/scope_timer.h>
#include <platform/timeutils.h>
#include <spdlog/fmt/fmt.h>
#include <statistics/cbstat_collector.h>
#include <numeric>

const std::chrono::seconds DcpProducer::defaultDcpNoopTxInterval(20);

/**
 * Construct a StreamsMap for the given maximum number of vBuckets this
 * Bucket could ever have.
 */
DcpProducer::StreamsMap::SmartPtr makeStreamsMap(
        size_t maxNumVBuckets) {
    // If we know the maximum number of vBuckets which will exist for this
    // Bucket (normally 1024), we can simply create a AtomicHashArray with
    // capacity == size (i.e. load factor of 1.0), given the key (Vbid) is
    // gauranteed to be unique and not collide with any other.
    DcpProducer::StreamsMap::Config config;
    config.maxLoadFactor = 1.0;
    return DcpProducer::StreamsMap::create(maxNumVBuckets, config);
}

DcpProducer::BufferLog::State DcpProducer::BufferLog::getState() const {
    if (isEnabled()) {
        if (isSpaceAvailable()) {
            return State::SpaceAvailable;
        }
        return State::Full;
    }
    return State::Disabled;
}

void DcpProducer::BufferLog::setBufferSize(size_t newMaxBytes) {
    maxBytes = newMaxBytes;
    if (newMaxBytes == 0) {
        bytesOutstanding = 0;
        ackedBytes.reset(0);
    }
}

bool DcpProducer::BufferLog::insert(size_t bytes) {
    bool inserted = false;
    // If the log is not enabled
    // or there is space, allow the insert
    if (!isEnabled() || !isFull()) {
        bytesOutstanding += bytes;
        inserted = true;
    }
    return inserted;
}

void DcpProducer::BufferLog::release(size_t bytes) {
    if (bytes > bytesOutstanding) {
        EP_LOG_WARN(
                "{} Attempting to release {} bytes which is greater than "
                "bytesOutstanding:{}",
                producer.logHeader(),
                uint64_t(bytes),
                uint64_t(bytesOutstanding));
    }

    bytesOutstanding -= bytes;
}

bool DcpProducer::BufferLog::pauseIfFull() {
    if (getState() == State::Full) {
        producer.pause(PausedReason::BufferLogFull);
        return true;
    }
    return false;
}

void DcpProducer::BufferLog::acknowledge(size_t bytes) {
    State state = getState();
    if (state != State::Disabled) {
        release(bytes);
        ackedBytes += bytes;
    }
}

void DcpProducer::BufferLog::addStats(const AddStatFn& add_stat,
                                      CookieIface& c) const {
    if (isEnabled()) {
        producer.addStat("max_buffer_bytes", maxBytes, add_stat, c);
        producer.addStat("unacked_bytes", bytesOutstanding, add_stat, c);
        producer.addStat("total_acked_bytes", ackedBytes, add_stat, c);
        producer.addStat("flow_control", "enabled", add_stat, c);
    } else {
        producer.addStat("flow_control", "disabled", add_stat, c);
    }
}

/// Decode IncludeValue from DCP producer flags.
static IncludeValue toIncludeValue(cb::mcbp::DcpOpenFlag flags) {
    using namespace cb::mcbp;
    if (isFlagSet(flags, DcpOpenFlag::NoValue)) {
        return IncludeValue::No;
    }
    if (isFlagSet(flags, DcpOpenFlag::NoValueWithUnderlyingDatatype)) {
        return IncludeValue::NoWithUnderlyingDatatype;
    }
    return IncludeValue::Yes;
}

DcpProducer::DcpProducer(EventuallyPersistentEngine& e,
                         CookieIface* cookie,
                         const std::string& name,
                         cb::mcbp::DcpOpenFlag flags,
                         bool startTask)
    : ConnHandler(e, cookie, name),
      sendStreamEndOnClientStreamClose(false),
      consumerSupportsHifiMfu(false),
      lastSendTime(ep_uptime_now()),
      log(BufferLog(*this)),
      backfillMgr(std::make_shared<BackfillManager>(
              *e.getKVBucket(),
              e.getKVBucket()->getKVStoreScanTracker(),
              name,
              e.getConfiguration())),
      ready(e.getConfiguration().getMaxVbuckets()),
      streams(makeStreamsMap(e.getConfiguration().getMaxVbuckets())),
      itemsSent(0),
      totalBytesSent(0),
      totalUncompressedDataSize(0),
      includeValue(toIncludeValue(flags)),
      includeXattrs(isFlagSet(flags, cb::mcbp::DcpOpenFlag::IncludeXattrs)
                            ? IncludeXattrs::Yes
                            : IncludeXattrs::No),
      includeDeleteTime(
              isFlagSet(flags, cb::mcbp::DcpOpenFlag::IncludeDeleteTimes)
                      ? IncludeDeleteTime::Yes
                      : IncludeDeleteTime::No),
      createChkPtProcessorTsk(startTask),
      connectionSupportsSnappy(
              cookie->isDatatypeSupported(PROTOCOL_BINARY_DATATYPE_SNAPPY)),
      collectionsEnabled(cookie->isCollectionsSupported()) {
    if (getName().find("secidx") != std::string::npos) {
        logBufferFullInterval = std::chrono::seconds{15};
#ifdef CB_DEVELOPMENT_ASSERTS
        logBufferAggregatedFullDuration = std::chrono::minutes{1};
#endif
    }

    nextLogBufferFull = logBufferFullInterval;
#ifdef CB_DEVELOPMENT_ASSERTS
    nextLogBufferAggregatedFull = logBufferAggregatedFullDuration;
#endif

    setSupportAck(true);
    pause(PausedReason::Initializing);
    setLogHeader("DCP (Producer) " + getName() + " -");

    // Reduce the minimum log level of view engine DCP streams as they are
    // extremely noisy due to creating new stream, per vbucket,per design doc
    // every ~10s.
    if (name.find("eq_dcpq:mapreduce_view") != std::string::npos ||
        name.find("eq_dcpq:spatial_view") != std::string::npos) {
        logger->set_level(spdlog::level::level_enum::warn);
        // Unregister this logger so that any changes in verbosity will not
        // be reflected in this logger. This prevents us from getting in a
        // state where we change the verbosity to a more verbose value, then
        // cannot return to the original state where this logger only prints
        // warning level messages and others print info level.
        logger->unregister();
    }

    getCookie()->getConnectionIface().setPriority(ConnectionPriority::Medium);

    // The consumer assigns opaques starting at 0 so lets have the producer
    //start using opaques at 10M to prevent any opaque conflicts.
    noopCtx.opaque = 10000000;
    noopCtx.sendTime = ep_uptime_now();

    // This is for backward compatibility with Couchbase 3.0. In 3.0 we set the
    // noop interval to 20 seconds by default, but in post 3.0 releases we set
    // it to be higher by default. Starting in 3.0.1 the DCP consumer sets the
    // noop interval of the producer when connecting so in an all 3.0.1+ cluster
    // this value will be overridden. In 3.0 however we do not set the noop
    // interval so setting this value will make sure we don't disconnect on
    // accident due to the producer and the consumer having a different noop
    // interval.
    noopCtx.dcpNoopTxInterval = defaultDcpNoopTxInterval;
    noopCtx.pendingRecv = false;
    noopCtx.enabled = false;

    forceValueCompression = false;
    enableExpiryOpcode = false;

    // Cursor dropping is disabled for replication connections by default,
    // but will be enabled through a control message to support backward
    // compatibility. For all other type of DCP connections, cursor dropping
    // will be enabled by default.
    if (name.find("replication") < name.length()) {
        supportsCursorDropping = false;
    } else {
        supportsCursorDropping = true;
    }

    includeDeletedUserXattrs =
            isFlagSet(flags, cb::mcbp::DcpOpenFlag::IncludeDeletedUserXattrs)
                    ? IncludeDeletedUserXattrs::Yes
                    : IncludeDeletedUserXattrs::No;
}

DcpProducer::~DcpProducer() {
    // Log runtime / pause information when we destruct.
    const auto now = ep_uptime_now();
    auto noopDescr = fmt::format("Noop enabled:{}", noopCtx.enabled);
    if (noopCtx.enabled) {
        noopDescr += fmt::format(
                ", txInterval:{}, pendingRecv:{} sendTime:{} s ago, "
                "recvTime:{} s ago.",
                noopCtx.dcpNoopTxInterval,
                noopCtx.pendingRecv,
                now - noopCtx.sendTime,
                now - noopCtx.recvTime);
    }
    logger->info(
            "Destroying connection. Created {} s ago. "
            "Sent {} bytes. {} {}",
            std::chrono::duration<double>(now - created).count(),
            totalBytesSent,
            noopDescr,
            getPausedDetailsDescription());

    backfillMgr.reset();
}

void DcpProducer::cancelCheckpointCreatorTask() {
    std::lock_guard<std::mutex> guard(checkpointCreator->mutex);
    if (checkpointCreator->task) {
        ExecutorPool::get()->cancel(checkpointCreator->task->getId());
        checkpointCreator->task.reset();
    }
}

cb::engine_errc DcpProducer::streamRequest(
        cb::mcbp::DcpAddStreamFlag flags,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint64_t vbucket_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        uint64_t* rollback_seqno,
        dcp_add_failover_log callback,
        std::optional<std::string_view> json) {
    StreamRequestInfo req{flags,
                          vbucket_uuid,
                          0 /* high_seqno */,
                          start_seqno,
                          end_seqno,
                          snap_start_seqno,
                          snap_end_seqno};
    auto ret = cb::engine_errc::failed;
    VBucketPtr vb;

    std::tie(ret, vb) = checkConditionsForStreamRequest(req, vbucket, json);
    if (ret != cb::engine_errc::success) {
        return ret;
    }
    Expects(vb);

    const auto maybeFilter = constructFilterForStreamRequest(*vb, json);
    if (std::holds_alternative<cb::engine_errc>(maybeFilter)) {
        return std::get<cb::engine_errc>(maybeFilter);
    }
    const auto& filter = std::get<Collections::VB::Filter>(maybeFilter);

    bool callAddVBConnByVBId = false;
    std::tie(ret, callAddVBConnByVBId) =
            shouldAddVBToProducerConnection(vbucket, filter);
    if (ret != cb::engine_errc::success) {
        return ret;
    }

    ret = checkStreamRequestNeedsRollback(req, *vb, filter, rollback_seqno);
    if (ret != cb::engine_errc::success) {
        return ret;
    }

    ret = adjustSeqnosForStreamRequest(req, *vb, filter);
    if (ret != cb::engine_errc::success) {
        return ret;
    }

    // Take copy of Filter's streamID, given it will be moved-from when
    // ActiveStream is constructed.
    const auto streamID = filter.getStreamId();

    if (!engine_.getMemoryTracker().isBelowBackfillThreshold() &&
        getAuthenticatedUser() != "@ns_server") {
        logger->warn(
                "({}) Stream request failed because "
                "memory usage is above quota",
                vbucket);
        return cb::engine_errc::no_memory;
    }

    std::shared_ptr<ActiveStream> stream;
    try {
        stream = std::make_shared<ActiveStream>(&engine_,
                                                shared_from_this(),
                                                getName(),
                                                req.flags,
                                                opaque,
                                                *vb,
                                                req.start_seqno,
                                                req.end_seqno,
                                                req.vbucket_uuid,
                                                req.snap_start_seqno,
                                                req.snap_end_seqno,
                                                includeValue,
                                                includeXattrs,
                                                includeDeleteTime,
                                                includeDeletedUserXattrs,
                                                std::move(filter));
    } catch (const cb::engine_error& e) {
        logger->warn(
                "({}) Stream request failed because "
                "the filter cannot be constructed, returning:{}",
                Vbid(vbucket),
                e.code().value());
        return cb::engine_errc(e.code().value());
    }

    return scheduleTasksForStreamRequest(std::move(stream),
                                         *vb,
                                         streamID,
                                         std::move(callback),
                                         callAddVBConnByVBId);
}

std::pair<cb::engine_errc, VBucketPtr>
DcpProducer::checkConditionsForStreamRequest(
        StreamRequestInfo& req,
        Vbid vbucket,
        std::optional<std::string_view> json) {
    lastReceiveTime = ep_uptime_now();
    if (doDisconnect()) {
        return {cb::engine_errc::disconnect, nullptr};
    }

    VBucketPtr vb = engine_.getVBucket(vbucket);
    if (!vb) {
        logger->warn(
                "({}) Stream request failed because "
                "this vbucket doesn't exist",
                vbucket);
        return {cb::engine_errc::not_my_vbucket, nullptr};
    }

    req.high_seqno = vb->getHighSeqno();
    if (isFlagSet(req.flags, cb::mcbp::DcpAddStreamFlag::FromLatest)) {
        req.start_seqno = req.snap_start_seqno = req.snap_end_seqno =
                req.high_seqno;
        if (req.vbucket_uuid == 0) {
            req.vbucket_uuid = vb->failovers->getLatestUUID();
        }
    }

    // check for mandatory noop
    if ((includeXattrs == IncludeXattrs::Yes) || json.has_value()) {
        if (!noopCtx.enabled &&
            engine_.getConfiguration().isDcpNoopMandatoryForV5Features()) {
            logger->warn(
                    "({}) noop is mandatory for v5 features like "
                    "xattrs and collections",
                    vbucket);
            return {cb::engine_errc::not_supported, nullptr};
        }
    }

    if (isFlagSet(req.flags, cb::mcbp::DcpAddStreamFlag::ActiveVbOnly) &&
        (vb->getState() != vbucket_state_active)) {
        logger->info(
                "({}) Stream request failed because "
                "the vbucket is in state:{}, only active vbuckets were "
                "requested",
                vbucket,
                vb->toString(vb->getState()));
        return {cb::engine_errc::not_my_vbucket, nullptr};
    }

    if (req.start_seqno > req.end_seqno) {
        EP_LOG_WARN(
                "{} ({}) Stream request failed because the start "
                "seqno ({}) is larger than the end seqno ({}); "
                "Incorrect params passed by the DCP client",
                logHeader(),
                vbucket,
                req.start_seqno,
                req.end_seqno);
        return {cb::engine_errc::out_of_range, nullptr};
    }

    if (!(req.snap_start_seqno <= req.start_seqno &&
          req.start_seqno <= req.snap_end_seqno)) {
        logger->warn(
                "({}) Stream request failed because "
                "the snap start seqno ({}) <= start seqno ({})"
                " <= snap end seqno ({}) is required",
                vbucket,
                req.snap_start_seqno,
                req.start_seqno,
                req.snap_end_seqno);
        return {cb::engine_errc::out_of_range, nullptr};
    }

    return {cb::engine_errc::success, std::move(vb)};
}

std::variant<Collections::VB::Filter, cb::engine_errc>
DcpProducer::constructFilterForStreamRequest(
        const VBucket& vb, std::optional<std::string_view> json) {
    const Vbid vbucket = vb.getId();
    // Construct the filter before rollback checks so we ensure the client view
    // of collections is compatible with the vbucket.
    Collections::VB::Filter filter(
            json, vb.getManifest(), *getCookie(), engine_);

    if (!filter.getStreamId() &&
        multipleStreamRequests == MultipleStreamRequests::Yes) {
        logger->warn(
                "Stream request for {} failed because a valid stream-ID is "
                "required.",
                vbucket);
        return cb::engine_errc::dcp_streamid_invalid;
    }
    if (filter.getStreamId() &&
        multipleStreamRequests == MultipleStreamRequests::No) {
        logger->warn(
                "Stream request for {} failed because a stream-ID:{} is "
                "present "
                "but not required.",
                vbucket,
                filter.getStreamId());
        return cb::engine_errc::dcp_streamid_invalid;
    }
    if (filter.isCollectionFilter() && isSyncWritesEnabled()) {
        // These two don't (or may not) quite work together (very little
        // coverage and never required)
        logger->warn(
                "({}) Stream request failed for filtered collections + "
                "sync-writes ",
                vbucket);
        return cb::engine_errc::not_supported;
    }

    return std::move(filter);
}

std::pair<cb::engine_errc, bool> DcpProducer::shouldAddVBToProducerConnection(
        Vbid vbucket, const Collections::VB::Filter& filter) {
    // Check if this vbid can be added to this producer connection, and if
    // the vb connection map needs updating (if this is a new VB).
    using cb::tracing::Code;
    ScopeTimer1<TracerStopwatch> timer(*getCookie(), Code::StreamFindMap);
    bool callAddVBConnByVBId = true;
    auto found = streams->find(vbucket.get());
    if (found != streams->end()) {
        // vbid is already mapped. found.second is a shared_ptr<StreamContainer>
        if (found->second) {
            auto handle = found->second->wlock();
            for (; !handle.end(); handle.next()) {
                auto& sp = handle.get(); // get the shared_ptr<Stream>
                if (sp->compareStreamId(filter.getStreamId())) {
                    // Error if found and active
                    if (sp->isActive()) {
                        logger->warn(
                                "({}) Stream ({}) request failed"
                                " because a stream already exists for this "
                                "vbucket",
                                vbucket,
                                filter.getStreamId().to_string());
                        return {cb::engine_errc::key_already_exists, false};
                    }
                    // Found a 'dead' stream which can be replaced.
                    handle.erase();

                    // Don't need to add an entry to vbucket-to-conns map
                    callAddVBConnByVBId = false;
                    break;
                }
            }
        }
    }
    return {cb::engine_errc::success, callAddVBConnByVBId};
}

cb::engine_errc DcpProducer::checkStreamRequestNeedsRollback(
        const StreamRequestInfo& req,
        VBucket& vb,
        const Collections::VB::Filter& filter,
        uint64_t* rollback_seqno) {
    // Timing for failover table as this has an internal mutex
    using cb::tracing::Code;
    ScopeTimer1<TracerStopwatch> timer(*getCookie(), Code::StreamCheckRollback);
    const Vbid vbucket = vb.getId();

    auto purgeSeqno = vb.getPurgeSeqno();
    if (isFlagSet(req.flags,
                  cb::mcbp::DcpAddStreamFlag::IgnorePurgedTombstones)) {
        // If the client does not care about purged tombstones, we issue
        // the request as-if the purgeSeqno is zero.
        purgeSeqno = 0;
    }

    const auto needsRollback = vb.failovers->needsRollback(
            req.start_seqno,
            req.high_seqno,
            req.vbucket_uuid,
            req.snap_start_seqno,
            req.snap_end_seqno,
            purgeSeqno,
            isFlagSet(req.flags, cb::mcbp::DcpAddStreamFlag::StrictVbUuid),
            getHighSeqnoOfCollections(filter, vb));

    if (needsRollback) {
        *rollback_seqno = needsRollback->rollbackSeqno;
        logger->warn(
                "({}) Stream request requires rollback to seqno:{} "
                "because {}. Client requested seqnos:{{{},{}}} "
                "snapshot:{{{},{}}} uuid:{}",
                vbucket,
                *rollback_seqno,
                needsRollback->rollbackReason,
                req.start_seqno,
                req.end_seqno,
                req.snap_start_seqno,
                req.snap_end_seqno,
                req.vbucket_uuid);
        return cb::engine_errc::rollback;
    }

    return cb::engine_errc::success;
}

cb::engine_errc DcpProducer::adjustSeqnosForStreamRequest(
        StreamRequestInfo& req,
        VBucket& vb,
        const Collections::VB::Filter& filter) {
    const Vbid vbucket = vb.getId();

    if (isFlagSet(req.flags, cb::mcbp::DcpAddStreamFlag::ToLatest)) {
        if (filter.isPassThroughFilter()) {
            req.end_seqno = req.high_seqno;
        } else {
            // If we are not passing through all collections then need to
            // stop at highest seqno of the collections included (this also
            // covers the legacy filter case where only the default collection
            // is streamed).
            auto highFilteredSeqno = getHighSeqnoOfCollections(filter, vb);
            if (!highFilteredSeqno) {
                // If this happens it means an unknown collection id was
                // specified in the filter ("race" where collection was dropped
                // between constructing filter above and requesting seqnos
                // here).
                logger->warn(
                        "Stream request for {} failed while calculating latest "
                        "seqno for filtered collections '{}'",
                        vbucket,
                        filter);
                return cb::engine_errc::unknown_collection;
            }
            req.end_seqno = highFilteredSeqno.value();
        }
    }

    if (isFlagSet(req.flags, cb::mcbp::DcpAddStreamFlag::DiskOnly)) {
        req.end_seqno = vb.getPersistenceSeqno();
    }

    if (req.start_seqno > req.end_seqno) {
        EP_LOG_WARN(
                "{} ({}) Stream request failed because "
                "the start seqno ({}) is larger than the end seqno ({}"
                "), stream request flags {}, vb_uuid {}, snapStartSeqno {}, "
                "snapEndSeqno {}; should have rolled back instead",
                logHeader(),
                vbucket,
                req.start_seqno,
                req.end_seqno,
                req.flags,
                req.vbucket_uuid,
                req.snap_start_seqno,
                req.snap_end_seqno);
        return cb::engine_errc::out_of_range;
    }

    if (req.start_seqno > req.high_seqno) {
        EP_LOG_WARN(
                "{} ({}) Stream request failed because "
                "the start seqno ({}) is larger than the vb highSeqno "
                "({}), stream request flags is {}, vb_uuid {}, snapStartSeqno "
                "{}, snapEndSeqno {}; should have rolled back instead",
                logHeader(),
                vbucket,
                req.start_seqno,
                req.high_seqno,
                req.flags,
                req.vbucket_uuid,
                req.snap_start_seqno,
                req.snap_end_seqno);
        return cb::engine_errc::out_of_range;
    }

    return cb::engine_errc::success;
}

cb::engine_errc DcpProducer::scheduleTasksForStreamRequest(
        std::shared_ptr<ActiveStream> s,
        VBucket& vb,
        const cb::mcbp::DcpStreamId streamID,
        const dcp_add_failover_log callback,
        const bool callAddVBConnByVBId) {
    const Vbid vbucket = vb.getId();

    /* We want to create the 'createCheckpointProcessorTask' here even if
       the stream creation fails later on in the func. The goal is to
       create the 'checkpointProcessorTask' before any valid active stream
       is created */
    if (createChkPtProcessorTsk && !checkpointCreator->task) {
        createCheckpointProcessorTask();
        scheduleCheckpointProcessorTask();
    }

    {
        folly::SharedMutex::ReadHolder rlh(vb.getStateLock());
        if (vb.getState() == vbucket_state_dead) {
            logger->warn(
                    "({}) Stream request failed because "
                    "this vbucket is in dead state",
                    vbucket);
            return cb::engine_errc::not_my_vbucket;
        }

        if (vb.isReceivingInitialDiskSnapshot()) {
            logger->info(
                    "({}) Stream request failed because this vbucket"
                    "is currently receiving its initial disk snapshot",
                    vbucket);
            return cb::engine_errc::temporary_failure;
        }

        // MB-19428: Only activate the stream if we are adding it to the
        // streams map.
        s->setActive();

        updateStreamsMap(vbucket, streamID, s);
    }

    const auto failoverEntries = vb.failovers->getFailoverLog();

    // See MB-25820:  Ensure that callback is called only after all other
    // possible error cases have been tested.  This is to ensure we do not
    // generate two responses for a single streamRequest.
    EventuallyPersistentEngine* epe =
            ObjectRegistry::onSwitchThread(nullptr, true);
    cb::engine_errc rv = callback(failoverEntries);
    ObjectRegistry::onSwitchThread(epe);
    if (rv != cb::engine_errc::success) {
        logger->warn(
                "({}) Couldn't add failover log to "
                "stream request due to error {}",
                vbucket,
                rv);
    }

    notifyStreamReady(vbucket);

    if (callAddVBConnByVBId) {
        engine_.getDcpConnMap().addVBConnByVBId(*this, vbucket);
    }

    return rv;
}

uint8_t DcpProducer::encodeItemHotness(const Item& item) const {
    auto freqCount =
            item.getFreqCounterValue().value_or(Item::initialFreqCount);
    if (consumerSupportsHifiMfu) {
        // The consumer supports the hifi_mfu eviction
        // policy, therefore use the frequency counter.
        return freqCount;
    }
    // The consumer does not support the hifi_mfu
    // eviction policy, therefore map from the 8-bit
    // probabilistic counter (256 states) to NRU (4 states).
    return cb::eviction::convertFreqCountToNRUValue(freqCount);
}

cb::unique_item_ptr DcpProducer::toUniqueItemPtr(
        std::unique_ptr<Item>&& item) const {
    return {item.release(), cb::ItemDeleter(&engine_)};
}

cb::engine_errc DcpProducer::step(bool throttled,
                                  DcpMessageProducersIface& producers) {
    if (doDisconnect()) {
        return cb::engine_errc::disconnect;
    }

    cb::engine_errc ret;
    if ((ret = maybeDisconnect()) != cb::engine_errc::failed) {
        return ret;
    }

    if ((ret = maybeSendNoop(producers)) != cb::engine_errc::failed) {
        return ret;
    }

    if (throttled) {
        return cb::engine_errc::throttled;
    }

    // If the BufferLog is full there isn't any point of trying to move the
    // stream forward. Ideally it should have been enough to just check if
    // the stream was pasued, but the log may be full without pause being
    // called.
    if (log.rlock()->isFull()) {
        // If the stream wasn't paused, pause the stream and return
        // that we're blocked (as we can't add more messages to the
        // stream
        if (!isPaused()) {
            pause(PausedReason::BufferLogFull);
            return cb::engine_errc::would_block;
        }

        auto details = getPausedDetails();
        const auto& duration = details.reasonDurations[static_cast<uint8_t>(
                PausedReason::BufferLogFull)];
#ifdef CB_DEVELOPMENT_ASSERTS
        if (nextLogBufferAggregatedFull < duration) {
            logger->info(
                    "Total wait time so far for the consumer to free up space "
                    "in the buffer window: {}",
                    cb::time2text(duration));
            ++nextLogBufferFull;
            ++nextLogBufferAggregatedFull;
        }
#endif

        if (details.reason == PausedReason::BufferLogFull) {
            using std::chrono::steady_clock;
            const auto now = steady_clock::now();
            if (details.lastPaused + nextLogBufferFull < now) {
                logger->warn(
                        "Waited {} for the consumer to free up space in the "
                        "buffer window",
                        cb::time2text(now - details.lastPaused));
                nextLogBufferFull += logBufferFullInterval;
            }
        }

        return cb::engine_errc::would_block;
    }
    nextLogBufferFull = logBufferFullInterval;

    std::unique_ptr<DcpResponse> resp;
    if (rejectResp) {
        resp = std::move(rejectResp);
    } else {
        resp = getNextItem();
        if (!resp) {
            return cb::engine_errc::would_block;
        }
    }

    std::unique_ptr<Item> itmCpy;
    totalUncompressedDataSize.fetch_add(resp->getMessageSize());

    MutationResponse* mutationResponse = nullptr;
    if (resp->isMutationResponse()) {
        mutationResponse = static_cast<MutationResponse*>(resp.get()); // NOLINT
        itmCpy = std::make_unique<Item>(*mutationResponse->getItem());
        if (isCompressionEnabled()) {
            /**
             * Retrieve the uncompressed length if the document is compressed.
             * This is to account for the total number of bytes if the data
             * was sent as uncompressed
             */
            if (cb::mcbp::datatype::is_snappy(itmCpy->getDataType())) {
                size_t inflated_length = 0;
                if (snappy_uncompressed_length(itmCpy->getData(), itmCpy->getNBytes(),
                                               &inflated_length) == SNAPPY_OK) {
                    totalUncompressedDataSize.fetch_add(inflated_length -
                                                        itmCpy->getNBytes());
                }
            }
        }
    }

    switch (resp->getEvent()) {
        case DcpResponse::Event::StreamEnd:
        {
            auto* se = static_cast<StreamEndResponse*>(resp.get());
            ret = producers.stream_end(
                    se->getOpaque(),
                    se->getVbucket(),
                    mapEndStreamStatus(getCookie(), se->getFlags()),
                    resp->getStreamId());

            // Stream has come to the end and is expected to be dead.
            // We must attempt to remove the stream, allowing it to free
            // resources. We do this here for all streams ending for any reason
            // other than close, e.g. stream ending because of a state-change.
            // For streams ending because of an client initiated 'close' we
            // may or may not need to remove the stream depending on the
            // control sendStreamEndOnClientStreamClose.
            // If sendStreamEndOnClientStreamClose is false and the stream was
            // closed by the client, the stream was released at the point of
            // handling the close-stream.
            if (ret == cb::engine_errc::success &&
                (sendStreamEndOnClientStreamClose ||
                 se->getFlags() != cb::mcbp::DcpStreamEndStatus::Closed)) {
                // We did not remove the ConnHandler earlier so we could wait to
                // send the streamEnd We have done that now, remove it.
                engine_.getDcpConnMap().removeVBConnByVBId(getCookie(),
                                                           se->getVbucket());
                auto [stream, vbFound] = closeStreamInner(
                        se->getVbucket(), resp->getStreamId(), true);
                if (!stream) {
                    throw std::logic_error(
                            "DcpProducer::step(StreamEnd): no stream was "
                            "found "
                            "for " +
                            se->getVbucket().to_string() + " " +
                            resp->getStreamId().to_string());
                }
                Expects(!stream->isActive());
            }
            break;
        }
        case DcpResponse::Event::Commit: {
            auto* csr = static_cast<CommitSyncWrite*>(resp.get());
            ret = producers.commit(csr->getOpaque(),
                                   csr->getVbucket(),
                                   csr->getKey(),
                                   csr->getPreparedSeqno(),
                                   csr->getCommitSeqno());
            break;
        }

        case DcpResponse::Event::Mutation:
        {
            if (itmCpy == nullptr) {
                throw std::logic_error(
                    "DcpProducer::step(Mutation): itmCpy must be != nullptr");
            }

            const uint8_t hotness =
                    encodeItemHotness(*mutationResponse->getItem());
            ret = producers.mutation(mutationResponse->getOpaque(),
                                     toUniqueItemPtr(std::move(itmCpy)),
                                     mutationResponse->getVBucket(),
                                     *mutationResponse->getBySeqno(),
                                     mutationResponse->getRevSeqno(),
                                     0 /* lock time */,
                                     hotness,
                                     mutationResponse->getStreamId());
            break;
        }
        case DcpResponse::Event::Deletion:
        {
            if (itmCpy == nullptr) {
                throw std::logic_error(
                    "DcpProducer::step(Deletion): itmCpy must be != nullptr");
            }
            ret = deletionV1OrV2(includeDeleteTime,
                                 *mutationResponse,
                                 producers,
                                 std::move(itmCpy),
                                 ret,
                                 mutationResponse->getStreamId());
            break;
        }
        case DcpResponse::Event::Expiration: {
            if (itmCpy == nullptr) {
                throw std::logic_error(
                        "DcpProducer::step(Expiration): itmCpy must be != "
                        "nullptr");
            }
            if (enableExpiryOpcode) {
                if (includeDeleteTime == IncludeDeleteTime::No) {
                    throw std::logic_error(
                            "DcpProducer::step(Expiration): If enabling Expiry "
                            "opcodes, you cannot disable delete_v2");
                }
                ret = producers.expiration(
                        mutationResponse->getOpaque(),
                        toUniqueItemPtr(std::move(itmCpy)),
                        mutationResponse->getVBucket(),
                        *mutationResponse->getBySeqno(),
                        mutationResponse->getRevSeqno(),
                        mutationResponse->getItem()->getExptime(),
                        resp->getStreamId());
            } else {
                ret = deletionV1OrV2(includeDeleteTime,
                                     *mutationResponse,
                                     producers,
                                     std::move(itmCpy),
                                     ret,
                                     resp->getStreamId());
            }
            break;
        }
        case DcpResponse::Event::Prepare: {
            if (itmCpy == nullptr) {
                throw std::logic_error(
                        "DcpProducer::step(Prepare): itmCpy must be != "
                        "nullptr");
            }

            const uint8_t hotness =
                    encodeItemHotness(*mutationResponse->getItem());
            const auto docState = mutationResponse->getItem()->isDeleted()
                                          ? DocumentState::Deleted
                                          : DocumentState::Alive;
            ret = producers.prepare(mutationResponse->getOpaque(),
                                    toUniqueItemPtr(std::move(itmCpy)),
                                    mutationResponse->getVBucket(),
                                    *mutationResponse->getBySeqno(),
                                    mutationResponse->getRevSeqno(),
                                    0 /* lock time */,
                                    hotness,
                                    docState,
                                    mutationResponse->getItem()
                                            ->getDurabilityReqs()
                                            .getLevel());
            break;
        }
        case DcpResponse::Event::Abort: {
            auto& abort = dynamic_cast<AbortSyncWrite&>(*resp);
            ret = producers.abort(abort.getOpaque(),
                                  abort.getVbucket(),
                                  abort.getKey(),
                                  abort.getPreparedSeqno(),
                                  abort.getAbortSeqno());
            break;
        }
        case DcpResponse::Event::SnapshotMarker:
        {
            auto* s = static_cast<SnapshotMarker*>(resp.get());
            ret = producers.marker(s->getOpaque(),
                                   s->getVBucket(),
                                   s->getStartSeqno(),
                                   s->getEndSeqno(),
                                   s->getFlags(),
                                   s->getHighCompletedSeqno(),
                                   s->getMaxVisibleSeqno(),
                                   s->getTimestamp(),
                                   resp->getStreamId());
            break;
        }
        case DcpResponse::Event::SetVbucket:
        {
            auto* s = static_cast<SetVBucketState*>(resp.get());
            ret = producers.set_vbucket_state(
                    s->getOpaque(), s->getVBucket(), s->getState());
            break;
        }
        case DcpResponse::Event::SystemEvent: {
            auto* s =
                    static_cast<SystemEventProducerMessage*>(resp.get());
            ret = producers.system_event(
                    s->getOpaque(),
                    s->getVBucket(),
                    s->getSystemEvent(),
                    *s->getBySeqno(),
                    s->getVersion(),
                    {reinterpret_cast<const uint8_t*>(s->getKey().data()),
                     s->getKey().size()},
                    {reinterpret_cast<const uint8_t*>(s->getEventData().data()),
                     s->getEventData().size()},
                    resp->getStreamId());
            break;
        }
        case DcpResponse::Event::OSOSnapshot: {
            auto& s = static_cast<OSOSnapshot&>(*resp);
            ret = producers.oso_snapshot(
                    s.getOpaque(),
                    s.getVBucket(),
                    s.isStart() ? uint32_t(cb::mcbp::request::
                                                   DcpOsoSnapshotFlags::Start)
                                : uint32_t(cb::mcbp::request::
                                                   DcpOsoSnapshotFlags::End),
                    resp->getStreamId());
            break;
        }
        case DcpResponse::Event::SeqnoAdvanced: {
            const auto& s = static_cast<SeqnoAdvanced&>(*resp);
            ret = producers.seqno_advanced(s.getOpaque(),
                                           s.getVBucket(),
                                           *s.getBySeqno(),
                                           resp->getStreamId());
            break;
        }
        default:
        {
            logger->warn(
                    "Unexpected dcp event ({}), "
                    "disconnecting",
                    resp->to_string());
            ret = cb::engine_errc::disconnect;
            break;
        }
    }

    const auto event = resp->getEvent();
    if (ret == cb::engine_errc::too_big) {
        rejectResp = std::move(resp);
    } else if (ret == cb::engine_errc::success) {
        switch (event) {
        case DcpResponse::Event::Abort:
        case DcpResponse::Event::Commit:
        case DcpResponse::Event::Deletion:
        case DcpResponse::Event::Expiration:
        case DcpResponse::Event::Mutation:
        case DcpResponse::Event::Prepare:
        case DcpResponse::Event::SystemEvent:
        case DcpResponse::Event::SeqnoAdvanced:
            itemsSent++;
            break;
        case DcpResponse::Event::AddStream:
        case DcpResponse::Event::SeqnoAcknowledgement:
        case DcpResponse::Event::SetVbucket:
        case DcpResponse::Event::SnapshotMarker:
        case DcpResponse::Event::StreamReq:
        case DcpResponse::Event::StreamEnd:
        case DcpResponse::Event::OSOSnapshot:
            break;
        }

        totalBytesSent.fetch_add(resp->getMessageSize());
    }

    lastSendTime = ep_uptime_now();
    return ret;
}

cb::engine_errc DcpProducer::bufferAcknowledgement(uint32_t opaque,
                                                   uint32_t buffer_bytes) {
    lastReceiveTime = ep_uptime_now();
    log.wlock()->acknowledge(buffer_bytes);
    return cb::engine_errc::success;
}

cb::engine_errc DcpProducer::deletionV1OrV2(IncludeDeleteTime incDeleteTime,
                                            MutationResponse& mutationResponse,
                                            DcpMessageProducersIface& producers,
                                            std::unique_ptr<Item> itmCpy,
                                            cb::engine_errc ret,
                                            cb::mcbp::DcpStreamId sid) {
    if (incDeleteTime == IncludeDeleteTime::Yes) {
        ret = producers.deletion_v2(mutationResponse.getOpaque(),
                                    toUniqueItemPtr(std::move(itmCpy)),
                                    mutationResponse.getVBucket(),
                                    *mutationResponse.getBySeqno(),
                                    mutationResponse.getRevSeqno(),
                                    mutationResponse.getItem()->getDeleteTime(),
                                    sid);
    } else {
        ret = producers.deletion(mutationResponse.getOpaque(),
                                 toUniqueItemPtr(std::move(itmCpy)),
                                 mutationResponse.getVBucket(),
                                 *mutationResponse.getBySeqno(),
                                 mutationResponse.getRevSeqno(),
                                 sid);
    }
    return ret;
}

cb::engine_errc DcpProducer::control(uint32_t opaque,
                                     std::string_view key,
                                     std::string_view value) {
    lastReceiveTime = ep_uptime_now();
    const char* param = key.data();
    std::string keyStr(key.data(), key.size());
    std::string valueStr(value.data(), value.size());

    if (strncmp(param, "backfill_order", key.size()) == 0) {
        using ScheduleOrder = BackfillManager::ScheduleOrder;
        if (valueStr == "round-robin") {
            backfillMgr->setBackfillOrder(ScheduleOrder::RoundRobin);
        } else if (valueStr == "sequential") {
            backfillMgr->setBackfillOrder(ScheduleOrder::Sequential);
        } else {
            engine_.setErrorContext(
                    *getCookie(),
                    "Unsupported value '" + keyStr +
                            "' for ctrl parameter 'backfill_order'");
            return cb::engine_errc::invalid_arguments;
        }
        return cb::engine_errc::success;
    }

    if (strncmp(param, "connection_buffer_size", key.size()) == 0) {
        uint32_t size;
        if (safe_strtoul(valueStr, size)) {
            /* Size 0 implies the client (DCP consumer) does not support
               flow control */
            log.wlock()->setBufferSize(size);
            NonBucketAllocationGuard guard;
            getCookie()->getConnectionIface().setDcpFlowControlBufferSize(size);
            return cb::engine_errc::success;
        }
    } else if (strncmp(param, "stream_buffer_size", key.size()) == 0) {
        logger->warn(
                "The ctrl parameter stream_buffer_size is"
                "not supported by this engine");
        return cb::engine_errc::not_supported;
    } else if (strncmp(param, "enable_noop", key.size()) == 0) {
        if (valueStr == "true") {
            noopCtx.enabled = true;
        } else {
            noopCtx.enabled = false;
        }
        return cb::engine_errc::success;
    } else if (strncmp(param, "force_value_compression", key.size()) == 0) {
        if (!isSnappyEnabled()) {
            engine_.setErrorContext(
                    *getCookie(),
                    "The ctrl parameter "
                    "force_value_compression is only supported if datatype "
                    "snappy is enabled on the connection");
            return cb::engine_errc::invalid_arguments;
        }
        if (valueStr == "true") {
            forceValueCompression = true;
        } else {
            forceValueCompression = false;
        }
        return cb::engine_errc::success;
        // vulcan onwards we accept two cursor_dropping control keys.
    } else if (keyStr == "supports_cursor_dropping_vulcan" ||
               keyStr == "supports_cursor_dropping") {
        if (valueStr == "true") {
            supportsCursorDropping = true;
        } else {
            supportsCursorDropping = false;
        }
        return cb::engine_errc::success;
    } else if (strncmp(param, "supports_hifi_MFU", key.size()) == 0) {
        consumerSupportsHifiMfu = (valueStr == "true");
        return cb::engine_errc::success;
    } else if (strncmp(param, "set_noop_interval", key.size()) == 0) {
        float noopInterval;
        if (safe_strtof(valueStr, noopInterval)) {
            /*
             * We need to ensure that we only set the noop interval to a value
             * that is greater or equal to the connection manager interval.
             * The reason is that if there is no DCP traffic we snooze for the
             * connection manager interval before sending the noop.
             */
            if (noopInterval >=
                engine_.getConfiguration().getConnectionManagerInterval()) {
                    noopCtx.dcpNoopTxInterval = std::chrono::duration_cast<
                            std::chrono::milliseconds>(
                            std::chrono::duration<float>(noopInterval));
                    return cb::engine_errc::success;
            }
            logger->warn(
                    "Attempt to set DCP control set_noop_interval to {}s which "
                    "is less than the connectionManagerInterval of {}s "
                    "- rejecting with {}",
                    noopInterval,
                    engine_.getConfiguration().getConnectionManagerInterval(),
                    cb::engine_errc::invalid_arguments);
            return cb::engine_errc::invalid_arguments;
        }
    } else if (strncmp(param, "set_priority", key.size()) == 0) {
        if (valueStr == "high") {
            getCookie()->getConnectionIface().setPriority(
                    ConnectionPriority::High);
            return cb::engine_errc::success;
        }
        if (valueStr == "medium") {
            getCookie()->getConnectionIface().setPriority(
                    ConnectionPriority::Medium);
            return cb::engine_errc::success;
        }
        if (valueStr == "low") {
            getCookie()->getConnectionIface().setPriority(
                    ConnectionPriority::Low);
            return cb::engine_errc::success;
        }
    } else if (keyStr == "send_stream_end_on_client_close_stream") {
        if (valueStr == "true") {
            sendStreamEndOnClientStreamClose = true;
        }
        /* Do not want to give an option to the client to disable this.
           Default is disabled, client has only a choice to enable.
           This is a one time setting and there is no point giving the client an
           option to toggle it back mid way during the connection */
        return cb::engine_errc::success;
    } else if (strncmp(param, "enable_expiry_opcode", key.size()) == 0) {
        // Expiry opcode uses the same encoding as deleteV2 (includes
        // delete time); therefore a client can only enable expiry_opcode
        // if the dcpOpen flags have includeDeleteTime set.
        enableExpiryOpcode = valueStr == "true" &&
                             includeDeleteTime == IncludeDeleteTime::Yes;

        return cb::engine_errc::success;
    } else if (keyStr == "enable_stream_id") {
        // For simplicity, user cannot turn this off, it is by default off
        // and can only be enabled one-way per Producer.
        if (valueStr == "true") {
            if (supportsSyncReplication != SyncReplication::No) {
                // MB-32318: stream-id and sync-replication denied
                return cb::engine_errc::not_supported;
            }
            multipleStreamRequests = MultipleStreamRequests::Yes;
            return cb::engine_errc::success;
        }
    } else if (key == "enable_sync_writes") {
        if (valueStr == "true") {
            if (multipleStreamRequests != MultipleStreamRequests::No) {
                // MB-32318: stream-id and sync-replication denied
                return cb::engine_errc::not_supported;
            }
            supportsSyncReplication = SyncReplication::SyncWrites;
            if (!consumerName.lock()->empty()) {
                supportsSyncReplication = SyncReplication::SyncReplication;
            }
            return cb::engine_errc::success;
        }
    } else if (key == "consumer_name" && !valueStr.empty()) {
        consumerName = valueStr;
        if (supportsSyncReplication == SyncReplication::SyncWrites) {
            supportsSyncReplication = SyncReplication::SyncReplication;
        }
        return cb::engine_errc::success;
    } else if (key == "enable_out_of_order_snapshots") {
        // For simplicity, only enabling is allowed (it is off by default)
        if (valueStr == "true") {
            outOfOrderSnapshots = OutOfOrderSnapshots::Yes;
            return cb::engine_errc::success;
        }
        if (valueStr == "true_with_seqno_advanced") {
            outOfOrderSnapshots = OutOfOrderSnapshots::YesWithSeqnoAdvanced;
            return cb::engine_errc::success;
        }
    } else if (key == "include_deleted_user_xattrs") {
        if (valueStr == "true") {
            if (includeDeletedUserXattrs == IncludeDeletedUserXattrs::Yes) {
                return cb::engine_errc::success;
            }
            // Note: Return here as there is no invalid param, we just want
            // to inform the DCP client that this Producer does not enable
            // IncludeDeletedUserXattrs, so we do not want to log as below
            return cb::engine_errc::invalid_arguments;
        }
    } else if (key == "v7_dcp_status_codes") {
        if (valueStr == "true") {
            enabledV7DcpStatus = true;
            return cb::engine_errc::success;
        }
        return cb::engine_errc::invalid_arguments;
    } else if (key == DcpControlKeys::FlatBuffersSystemEvents &&
               valueStr == "true") {
        flatBuffersSystemEventsEnabled = true;
        return cb::engine_errc::success;
    } else if (key == DcpControlKeys::ChangeStreams && valueStr == "true") {
        if (!engine_.getKVBucket()->getStorageProperties().canRetainHistory()) {
            return cb::engine_errc::not_supported;
        }

        changeStreams = true;
        return cb::engine_errc::success;
    }

    logger->warn("Invalid ctrl parameter '{}' for {}", valueStr, keyStr);

    return cb::engine_errc::invalid_arguments;
}

cb::engine_errc DcpProducer::seqno_acknowledged(uint32_t opaque,
                                                Vbid vbucket,
                                                uint64_t prepared_seqno) {
    if (!isSyncReplicationEnabled()) {
        logger->warn(
                "({}) seqno_acknowledge failed because SyncReplication is"
                " not enabled on this Producer");
        return cb::engine_errc::invalid_arguments;
    }

    if (consumerName.lock()->empty()) {
        logger->warn(
                "({}) seqno_acknowledge failed because this producer does"
                " not have an associated consumer name");
        return cb::engine_errc::invalid_arguments;
    }

    VBucketPtr vb = engine_.getVBucket(vbucket);
    if (!vb) {
        logger->warn(
                "({}) seqno_acknowledge failed because this vbucket doesn't "
                "exist",
                vbucket);
        return cb::engine_errc::not_my_vbucket;
    }

    logger->debug("({}) seqno_acknowledged: prepared_seqno:{}",
                  vbucket,
                  prepared_seqno);

    // Confirm that we only receive ack seqnos we have sent
    auto rv = streams->find(vbucket.get());
    if (rv == streams->end()) {
        throw std::logic_error(
                "Replica acked seqno:" + std::to_string(prepared_seqno) +
                " for vbucket:" + to_string(vbucket) +
                " but we don't have a StreamContainer for that vb");
    }

    std::shared_ptr<ActiveStream> stream;
    for (auto itr = rv->second->rlock(); !itr.end(); itr.next()) {
        auto s = itr.get();
        if (s->getOpaque() == opaque) {
            stream = std::dynamic_pointer_cast<ActiveStream>(s);
            break;
        }
    }

    if (!stream) {
        // No stream found, may be the case that we have just ended our
        // stream and removed the stream from our map but the consumer is
        // not yet aware and we have received a seqno ack. Just return
        // success and ignore the ack.
        return cb::engine_errc::success;
    }

    seqnoAckHook();

    return consumerName.withLock([&](const auto& lockedConsumerName) {
        return stream->seqnoAck(lockedConsumerName, prepared_seqno);
    });
}

bool DcpProducer::handleResponse(const cb::mcbp::Response& response) {
    lastReceiveTime = ep_uptime_now();
    if (doDisconnect()) {
        return false;
    }

    const auto opcode = response.getClientOpcode();
    const auto opaque = response.getOpaque();
    const auto responseStatus = response.getStatus();

    // Search for an active stream with the same opaque as the response.
    auto streamFindFn = [opaque](const StreamsMap::value_type& s) {
        auto handle = s.second->rlock();
        for (; !handle.end(); handle.next()) {
            const auto& stream = handle.get();
            if (stream && opaque == stream->getOpaque()) {
                return stream;
            }
        }
        return ContainerElement{};
    };

    const auto errorMessageHandler = [&]() -> bool {
        // Use find_if2 which will return the matching
        // shared_ptr<Stream>
        auto stream = find_if2(streamFindFn);
        std::string streamInfo("null");
        if (stream) {
            streamInfo = fmt::format(FMT_STRING("stream name:{}, {}, state:{}"),
                                     stream->getName(),
                                     stream->getVBucket(),
                                     stream->getStateName());
        }

        // For DcpCommit and DcpAbort we may see KeyEnoent or
        // Einval for the following reasons.
        // KeyEnoent:
        // In this case we receive a KeyEnoent, we need to disconnect as we
        // must have sent an a commit or abort of key that the consumer is
        // unaware of and we should never see KeyEnoent from a DcpPrepare.
        // Einval:
        // If we have seen a Einval we also need to disconnect as we must
        // have sent an invalid. Mutation or packet to the consumer e.g. we
        // sent an abort to the consumer in a non disk snapshot without it
        // having seen a prepare.
        bool allowPreV7StatusCodes =
                !enabledV7DcpStatus &&
                (responseStatus == cb::mcbp::Status::KeyEexists ||
                 (responseStatus == cb::mcbp::Status::KeyEnoent &&
                  opcode != cb::mcbp::ClientOpcode::DcpAbort &&
                  opcode != cb::mcbp::ClientOpcode::DcpCommit));

        if (allowPreV7StatusCodes ||
            responseStatus == cb::mcbp::Status::NotMyVbucket ||
            responseStatus == cb::mcbp::Status::DcpStreamNotFound ||
            responseStatus == cb::mcbp::Status::OpaqueNoMatch) {
            logger->info(
                    "DcpProducer::handleResponse received "
                    "unexpected "
                    "response:{}, Will not disconnect as {} will "
                    "affect "
                    "only one stream:{}",
                    to_string(responseStatus),
                    response.to_json(true).dump(),
                    streamInfo);
            return true;
        }
        logger->error(
                "DcpProducer::handleResponse disconnecting, received "
                "unexpected "
                "response:{} for stream:{}",
                response.to_json(true).dump(),
                streamInfo);
        return false;
    };

    switch (opcode) {
    case cb::mcbp::ClientOpcode::DcpSetVbucketState:
    case cb::mcbp::ClientOpcode::DcpSnapshotMarker: {
        // Use find_if2 which will return the matching shared_ptr<Stream>
        auto stream = find_if2(streamFindFn);
        if (stream) {
            auto* as = stream.get();
            if (opcode == cb::mcbp::ClientOpcode::DcpSetVbucketState) {
                as->setVBucketStateAckRecieved(*this);
            } else {
                as->snapshotMarkerAckReceived();
            }
        }

        return true;
    }
    case cb::mcbp::ClientOpcode::DcpStreamEnd:
        // The consumer could of closed the stream (DcpStreamEnd), enoent is
        // expected, but any other errors are not expected.
        if (responseStatus == cb::mcbp::Status::KeyEnoent ||
            responseStatus == cb::mcbp::Status::Success) {
            return true;
        }
        return errorMessageHandler();
    case cb::mcbp::ClientOpcode::DcpNoop:
        if (noopCtx.opaque == response.getOpaque()) {
            noopCtx.pendingRecv = false;
            noopCtx.recvTime = lastReceiveTime;
            return true;
        }
        return errorMessageHandler();
    case cb::mcbp::ClientOpcode::DcpOpen:
    case cb::mcbp::ClientOpcode::DcpAddStream:
    case cb::mcbp::ClientOpcode::DcpCloseStream:
    case cb::mcbp::ClientOpcode::DcpStreamReq:
    case cb::mcbp::ClientOpcode::DcpGetFailoverLog:
    case cb::mcbp::ClientOpcode::DcpMutation:
    case cb::mcbp::ClientOpcode::DcpDeletion:
    case cb::mcbp::ClientOpcode::DcpExpiration:
    case cb::mcbp::ClientOpcode::DcpBufferAcknowledgement:
    case cb::mcbp::ClientOpcode::DcpControl:
    case cb::mcbp::ClientOpcode::DcpSystemEvent:
    case cb::mcbp::ClientOpcode::GetErrorMap:
    case cb::mcbp::ClientOpcode::DcpPrepare:
    case cb::mcbp::ClientOpcode::DcpCommit:
    case cb::mcbp::ClientOpcode::DcpAbort:
        if (responseStatus == cb::mcbp::Status::Success) {
            return true;
        }
        return errorMessageHandler();
    default:
        std::string errorMsg(
                "DcpProducer::handleResponse received an unknown client "
                "opcode: ");
        errorMsg += response.to_json(true).dump();
        throw std::logic_error(errorMsg);
    }
}

std::pair<std::shared_ptr<Stream>, bool> DcpProducer::closeStreamInner(
        Vbid vbucket, cb::mcbp::DcpStreamId sid, bool eraseFromMapIfFound) {
    std::shared_ptr<Stream> stream;
    bool vbFound = false;

    auto rv = streams->find(vbucket.get());
    if (rv != streams->end()) {
        vbFound = true;
        // Vbucket is mapped, get exclusive access to the StreamContainer
        auto handle = rv->second->wlock();
        // Try and locate a matching stream
        for (; !handle.end(); handle.next()) {
            if (handle.get()->compareStreamId(sid)) {
                stream = handle.get();
                break;
            }
        }

        if (eraseFromMapIfFound && stream) {
            // Need to tidy up the map, call erase on the handle,
            // which will erase the current element from the container
            handle.erase();
        }
    }
    return {stream, vbFound};
}

cb::engine_errc DcpProducer::closeStream(uint32_t opaque,
                                         Vbid vbucket,
                                         cb::mcbp::DcpStreamId sid) {
    lastReceiveTime = ep_uptime_now();
    if (doDisconnect()) {
        return cb::engine_errc::disconnect;
    }

    if (!sid && multipleStreamRequests == MultipleStreamRequests::Yes) {
        logger->warn(
                "({}) closeStream request failed because a valid "
                "stream-ID is required.",
                vbucket);
        return cb::engine_errc::dcp_streamid_invalid;
    }
    if (sid && multipleStreamRequests == MultipleStreamRequests::No) {
        logger->warn(
                "({}) closeStream request failed because a "
                "stream-ID:{} is present "
                "but not required.",
                vbucket,
                sid);
        return cb::engine_errc::dcp_streamid_invalid;
    }

    /* We should not remove the stream from the streams map if we have to
       send the "STREAM_END" response asynchronously to the consumer, so
       use the value of sendStreamEndOnClientStreamClose to determine if the
       stream should be removed if found*/
    auto rv = closeStreamInner(vbucket, sid, !sendStreamEndOnClientStreamClose);

    cb::engine_errc ret;
    if (!rv.first) {
        logger->warn(
                "({}) Cannot close stream because no "
                "stream exists for this vbucket {}",
                vbucket,
                sid);
        return sid && rv.second ? cb::engine_errc::dcp_streamid_invalid
                                : cb::engine_errc::no_such_key;
    }
    if (!rv.first->isActive()) {
        logger->warn(
                "({}) Cannot close stream because "
                "stream is already marked as dead {}",
                vbucket,
                sid);
        ret = cb::engine_errc::no_such_key;
    } else {
        rv.first->setDead(cb::mcbp::DcpStreamEndStatus::Closed);
        ret = cb::engine_errc::success;
    }
    if (!sendStreamEndOnClientStreamClose) {
        /* Remove the conn from 'vb_conns map' only when we have removed the
           stream from the producer connections StreamsMap */
        engine_.getDcpConnMap().removeVBConnByVBId(getCookie(), vbucket);
    }

    return ret;
}

void DcpProducer::notifyBackfillManager() {
    if (backfillMgr) {
        backfillMgr->wakeUpTask();
    }
}

bool DcpProducer::recordBackfillManagerBytesRead(size_t bytes) {
    return backfillMgr->bytesCheckAndRead(bytes);
}

void DcpProducer::recordBackfillManagerBytesSent(size_t bytes) {
    if (backfillMgr) {
        backfillMgr->bytesSent(bytes);
    }
}

uint64_t DcpProducer::scheduleBackfillManager(VBucket& vb,
                                              std::shared_ptr<ActiveStream> s,
                                              uint64_t start,
                                              uint64_t end) {
    if (!(start <= end)) {
        return 0;
    }

    auto backfill = vb.createDCPBackfill(engine_, s, start, end);
    const auto backfillUID = backfill->getUID();
    switch (backfillMgr->schedule(std::move(backfill))) {
    case BackfillManager::ScheduleResult::Active:
        break;
    case BackfillManager::ScheduleResult::Pending:
        EP_LOG_INFO("Backfill for {} {} is pending", s->getName(), vb.getId());
        break;
    }
    return backfillUID;
}

uint64_t DcpProducer::scheduleBackfillManager(VBucket& vb,
                                              std::shared_ptr<ActiveStream> s) {
    auto backfill = vb.createDCPBackfill(engine_, std::move(s));
    const auto backfillUID = backfill->getUID();
    backfillMgr->schedule(std::move(backfill));
    return backfillUID;
}

bool DcpProducer::removeBackfill(uint64_t backfillUID) {
    std::lock_guard<std::mutex> lg(closeAllStreamsLock);

    if (backfillMgr) {
        return backfillMgr->removeBackfill(backfillUID);
    }
    return false;
}

void DcpProducer::addStats(const AddStatFn& add_stat, CookieIface& c) {
    ConnHandler::addStats(add_stat, c);

    addStat("items_sent", getItemsSent(), add_stat, c);
    addStat("items_remaining", getItemsRemaining(), add_stat, c);
    addStat("total_bytes_sent", getTotalBytesSent(), add_stat, c);
    if (isCompressionEnabled()) {
        addStat("total_uncompressed_data_size", getTotalUncompressedDataSize(),
                add_stat, c);
    }
    auto toFloatSecs = [](std::chrono::steady_clock::time_point t) {
        using namespace std::chrono;
        return duration_cast<duration<float>>(t.time_since_epoch()).count();
    };

    addStat("last_sent_time", toFloatSecs(lastSendTime), add_stat, c);
    addStat("last_receive_time",
            toFloatSecs(lastReceiveTime),
            add_stat,
            c);
    addStat("noop_enabled", noopCtx.enabled, add_stat, c);
    addStat("noop_tx_interval",
            cb::time2text(noopCtx.dcpNoopTxInterval),
            add_stat,
            c);
    addStat("noop_wait", noopCtx.pendingRecv, add_stat, c);
    addStat("force_value_compression", forceValueCompression, add_stat, c);
    addStat("cursor_dropping", supportsCursorDropping, add_stat, c);
    addStat("send_stream_end_on_client_close_stream",
            sendStreamEndOnClientStreamClose,
            add_stat,
            c);
    addStat("enable_expiry_opcode", enableExpiryOpcode, add_stat, c);
    addStat("enable_stream_id",
            multipleStreamRequests == MultipleStreamRequests::Yes,
            add_stat,
            c);
    addStat("synchronous_replication", isSyncReplicationEnabled(), add_stat, c);
    addStat("synchronous_writes", isSyncWritesEnabled(), add_stat, c);

    // Possible that the producer has had its streams closed and hence doesn't
    // have a backfill manager anymore.
    if (backfillMgr) {
        backfillMgr->addStats(*this, add_stat, c);
    }

    log.rlock()->addStats(add_stat, c);

    ExTask pointerCopy;
    { // Locking scope
        std::lock_guard<std::mutex> guard(checkpointCreator->mutex);
        pointerCopy = checkpointCreator->task;
    }

    if (pointerCopy) {
        static_cast<ActiveStreamCheckpointProcessorTask*>(pointerCopy.get())
                ->addStats(getName(), add_stat, c);
    }

    ready.addStats(getName() + ":dcp_ready_queue_", add_stat, c);

    size_t num_streams = 0;
    size_t num_dead_streams = 0;
    std::for_each(streams->begin(),
                  streams->end(),
                  [&num_streams,
                   &num_dead_streams](const StreamsMap::value_type& vt) {
                      for (auto handle = vt.second->rlock(); !handle.end();
                           handle.next()) {
                          num_streams++;
                          if (!handle.get()->isActive()) {
                              num_dead_streams++;
                          }
                      }
                  });

    addStat("num_streams", num_streams, add_stat, c);
    addStat("num_dead_streams", num_dead_streams, add_stat, c);
}

void DcpProducer::addStreamStats(const AddStatFn& add_stat,
                                 CookieIface& c,
                                 StreamStatsFormat format) {
    // Make a copy of all valid streams (under lock), and then call addStats
    // for each one. (Done in two stages to minmise how long we have the
    // streams map locked for).
    std::vector<std::shared_ptr<Stream>> valid_streams;

    std::for_each(streams->begin(),
                  streams->end(),
                  [&valid_streams](const StreamsMap::value_type& vt) {
                      for (auto handle = vt.second->rlock(); !handle.end();
                           handle.next()) {
                          auto as = handle.get();
                          if (as->isActive()) {
                              valid_streams.push_back(handle.get());
                          }
                      }
                  });

    if (format == StreamStatsFormat::Json) {
        doStreamStatsJson(valid_streams, add_stat, c);
    } else {
        doStreamStatsLegacy(valid_streams, add_stat, c);
    }
}

void DcpProducer::addTakeoverStats(const AddStatFn& add_stat,
                                   CookieIface& c,
                                   const VBucket& vb) {
    // Only do takeover stats on 'traditional' streams
    if (multipleStreamRequests == MultipleStreamRequests::Yes) {
        return;
    }

    auto rv = streams->find(vb.getId().get());

    if (rv != streams->end()) {
        auto handle = rv->second->rlock();
        // Only perform takeover stats on singleton streams
        if (handle.size() == 1) {
            auto stream = handle.get();
            if (stream) {
                stream->addTakeoverStats(add_stat, c, vb);
                return;
            }
            logger->warn("({}) DcpProducer::addTakeoverStats no stream found",
                         vb.getId());
        } else if (handle.size() > 1) {
            throw std::logic_error(
                    "DcpProducer::addTakeoverStats unexpected size streams:(" +
                    std::to_string(handle.size()) + ") found " +
                    vb.getId().to_string());
        } else {
            // Logically, finding a StreamContainer with no stream is similar to
            // not finding a StreamContainer at all, both should return does_not_exist
            logger->info(
                    "({}) "
                    "DcpProducer::addTakeoverStats empty streams list found",
                    vb.getId());
        }
    } else {
        logger->info(
                "({}) "
                "DcpProducer::addTakeoverStats Unable to find stream",
                vb.getId());
    }
    // Error path - return status of does_not_exist to ensure rebalance does not
    // hang.
    add_casted_stat("status", "stream_does_not_exist", add_stat, c);
    add_casted_stat("estimate", 0, add_stat, c);
    add_casted_stat("backfillRemaining", 0, add_stat, c);
}

void DcpProducer::aggregateQueueStats(ConnCounter& aggregator) const {
    ++aggregator.totalProducers;
    aggregator.conn_queueDrain += itemsSent;
    aggregator.conn_totalBytes += totalBytesSent;
    aggregator.conn_totalUncompressedDataSize += totalUncompressedDataSize;

    auto pausedCounters = getPausedCounters();
    aggregator.conn_paused = pausedCounters.first;
    aggregator.conn_unpaused = pausedCounters.second;

    auto streamAggStats = getStreamAggStats();

    aggregator.conn_activeStreams += streamAggStats.streams;
    aggregator.conn_queueRemaining += streamAggStats.itemsRemaining;
    aggregator.conn_queueMemory += streamAggStats.readyQueueMemory;
    aggregator.conn_backfillDisk += streamAggStats.backfillItemsDisk;
    aggregator.conn_backfillMemory += streamAggStats.backfillItemsMemory;
}

void DcpProducer::notifySeqnoAvailable(Vbid vbucket, queue_op op) {
    if (!isSyncWritesEnabled() &&
        ((!isSeqnoAdvancedEnabled() && isPrepareOrAbort(op)) ||
         (isSeqnoAdvancedEnabled() && op == queue_op::pending_sync_write))) {
        // Skip notifying this producer when sync-writes are not enabled and...
        //
        // 1) When the stream does not support seqno-advance, then skip both
        //    prepare and abort as nothing will be transmitted.
        // 2) When the stream supports seqno-advance only prepares skip the
        //    notify. If the operation is an abort we will wake-up the stream.
        //    See MB-56148 as to why this distinction exists.
        return;
    }

    auto rv = streams->find(vbucket.get());

    if (rv != streams->end()) {
        auto handle = rv->second->rlock();
        for (; !handle.end(); handle.next()) {
            handle.get()->notifySeqnoAvailable(*this);
        }
    }
}

void DcpProducer::closeStreamDueToVbStateChange(
        Vbid vbucket,
        vbucket_state_t state,
        folly::SharedMutex::WriteHolder* vbstateLock) {
    if (setStreamDeadStatus(vbucket,
                            cb::mcbp::DcpStreamEndStatus::StateChanged,
                            vbstateLock)) {
        logger->debug("({}) State changed to {}, closing active stream!",
                      vbucket,
                      VBucket::toString(state));
    }
}

void DcpProducer::closeStreamDueToRollback(Vbid vbucket) {
    if (setStreamDeadStatus(vbucket, cb::mcbp::DcpStreamEndStatus::Rollback)) {
        logger->debug(
                "({}) Rollback occurred,"
                "closing stream (downstream must rollback too)",
                vbucket);
    }
}

bool DcpProducer::handleSlowStream(Vbid vbid, const CheckpointCursor* cursor) {
    if (supportsCursorDropping) {
        auto rv = streams->find(vbid.get());
        if (rv != streams->end()) {
            for (auto handle = rv->second->rlock(); !handle.end();
                 handle.next()) {
                if (handle.get()->getCursor().lock().get() == cursor) {
                    auto* as = handle.get().get();
                    return as->handleSlowStream();
                }
            }
        }
    }
    return false;
}

bool DcpProducer::setStreamDeadStatus(
        Vbid vbid,
        cb::mcbp::DcpStreamEndStatus status,
        folly::SharedMutex::WriteHolder* vbstateLock) {
    auto rv = streams->find(vbid.get());
    if (rv != streams->end()) {
        std::vector<std::shared_ptr<ActiveStream>> streamPtrs;
        // MB-35073: holding StreamContainer rlock while calling setDead
        // has been seen to cause lock inversion elsewhere.
        // Collect sharedptrs then setDead once lock is released (itr out of
        // scope).
        for (auto itr = rv->second->rlock(); !itr.end(); itr.next()) {
            streamPtrs.push_back(itr.get());
        }

        // MB-36637: At KVBucket::setVBucketState we acquire an exclusive lock
        // to vbstate and pass it down here. If that is the case, then we have
        // to avoid the call to ActiveStream::setDead(status) as it may deadlock
        // by acquiring the same lock again.
        for (const auto& stream : streamPtrs) {
            if (stream) {
                if (vbstateLock) {
                    stream->setDead(status, *vbstateLock);
                } else {
                    stream->setDead(status);
                }
            }
        }

        return true;
    }

    return false;
}

void DcpProducer::closeAllStreams() {
    closeAllStreamsPreLockHook();

    std::lock_guard<std::mutex> lg(closeAllStreamsLock);

    closeAllStreamsPostLockHook();

    lastReceiveTime = ep_uptime_now();
    std::vector<Vbid> vbvector;
    {
        std::for_each(streams->begin(),
                      streams->end(),
                      [this, &vbvector](StreamsMap::value_type& vt) {
                          vbvector.push_back((Vbid)vt.first);
                          std::vector<std::shared_ptr<ActiveStream>> streamPtrs;
                          // MB-35073: holding StreamContainer lock while
                          // calling setDead leads to lock inversion - so
                          // collect sharedptrs in one pass then setDead once
                          // lock is released (itr out of scope).
                          {
                              auto handle = vt.second->wlock();
                              for (; !handle.end(); handle.next()) {
                                  streamPtrs.push_back(handle.get());
                              }
                              handle.clear();
                          }

                          for (const auto& streamPtr : streamPtrs) {
                              // Explicitly ask to remove the DCPBackfill object
                              // here. 1) whilst we know the backfillMgr is
                              // alive and 2) whilst we know the ActiveStream is
                              // alive. This ensures that if the ActiveStream
                              // destructs within this function (it could if
                              // shared_ptr::ref==0) ~ActiveStream doesn't
                              // deadlock trying to remove the backfill, as
                              // DcpProducer::removeBackfill needs the
                              // closeAllStreamsLock which is already locked.
                              if (backfillMgr) {
                                  streamPtr->removeBackfill(*backfillMgr.get());
                              }
                              streamPtr->setDead(cb::mcbp::DcpStreamEndStatus::
                                                         Disconnected);
                          }
                      });
    }
    for (const auto vbid: vbvector) {
        engine_.getDcpConnMap().removeVBConnByVBId(getCookie(), vbid);
    }

    closeAllStreamsHook();

    // Destroy the backfillManager. (BackfillManager task also
    // may hold a weak reference to it while running, but that is
    // guaranteed to decay and free the BackfillManager once it
    // completes run().
    // This will terminate any tasks and delete any backfills
    // associated with this Producer.  This is necessary as if we
    // don't, then the ref-counted ptr references which exist between
    // DcpProducer and ActiveStream result in us leaking DcpProducer
    // objects (and Couchstore vBucket files, via DCPBackfill task).
    backfillMgr.reset();
}

const char* DcpProducer::getType() const {
    return "producer";
}

std::unique_ptr<DcpResponse> DcpProducer::getNextItem() {
    do {
        Vbid vbucket = Vbid(0);
        while (ready.popFront(vbucket)) {
            // This can't happen in production as the state would have
            // changed before getting here.
            if (log.rlock()->isFull()) {
                ready.pushUnique(vbucket);
                return {};
            }

            auto response = getNextItemFromVbucket(vbucket);
            if (response) {
                ready.pushUnique(vbucket);
                unPause();
                return response;
            }
        }

        // re-check the ready queue.
        // A new vbucket could of became ready and the notifier could of seen
        // paused = false, so reloop so we don't miss an operation.
    } while (!ready.empty());

    // flag we are paused
    if (!isPaused()) {
        pause(PausedReason::ReadyListEmpty);
    }
    return {};
}

std::unique_ptr<DcpResponse> DcpProducer::getNextItemFromVbucket(Vbid vbid) {
    auto rv = streams->find(vbid.get());
    if (rv == streams->end()) {
        // The vbucket is not in the map.
        return {};
    }

    // Use the resumable handle so that we can service the streams that
    // are associated with the vbucket. If a stream returns a response
    // we will ship it (i.e. leave this scope). Then next time we visit
    // the VB, we should /resume/ from the next stream in the container.
    for (auto resumableIterator = rv->second->startResumable();
         !resumableIterator.complete();
         resumableIterator.next()) {
        const auto& stream = resumableIterator.get();
        if (!stream) {
            continue;
        }

        auto response = getAndValidateNextItemFromStream(stream);
        if (response) {
            return response;
        }
    }
    return {};
}

std::unique_ptr<DcpResponse> DcpProducer::getAndValidateNextItemFromStream(
        const std::shared_ptr<ActiveStream>& stream) {
    auto response = stream->next(*this);
    if (!response) {
        return {};
    }

    // The stream gave us something, validate it
    switch (response->getEvent()) {
    case DcpResponse::Event::SnapshotMarker: {
        if (stream->endIfRequiredPrivilegesLost(*this)) {
            return stream->makeEndStreamResponse(
                    cb::mcbp::DcpStreamEndStatus::LostPrivileges);
        }
    }
    case DcpResponse::Event::Mutation:
    case DcpResponse::Event::Deletion:
    case DcpResponse::Event::Expiration:
    case DcpResponse::Event::Prepare:
    case DcpResponse::Event::Commit:
    case DcpResponse::Event::Abort:
    case DcpResponse::Event::StreamEnd:
    case DcpResponse::Event::SetVbucket:
    case DcpResponse::Event::SystemEvent:
    case DcpResponse::Event::OSOSnapshot:
    case DcpResponse::Event::SeqnoAdvanced:
        break;
    default:
        throw std::logic_error(fmt::format(
                "DcpProducer::getAndValidateNextItemFromStream: Producer ({}) "
                "is attempting to write an unexpected event:{}",
                logHeader(),
                response->to_string()));
    }

    return response;
}

void DcpProducer::setDisconnect() {
    ConnHandler::setDisconnect();
    std::for_each(
            streams->begin(), streams->end(), [](StreamsMap::value_type& vt) {
                std::vector<std::shared_ptr<Stream>> streamPtrs;
                // MB-35049: hold StreamContainer rlock while calling setDead
                // leads to lock inversion - so collect sharedptrs in one pass
                // then setDead once it is released (itr out of scope).
                for (auto itr = vt.second->rlock(); !itr.end(); itr.next()) {
                    streamPtrs.push_back(itr.get());
                }
                for (auto stream : streamPtrs) {
                    stream->setDead(cb::mcbp::DcpStreamEndStatus::Disconnected);
                }
            });
}

void DcpProducer::notifyStreamReady(Vbid vbucket) {
    // Transitioned from empty to non-empty readyQ - unpause the Producer.
    if (ready.pushUnique(vbucket)) {
        const auto [full, ackedBytes, outstanding, max] =
                log.withRLock([](auto& entry) {
                    return std::make_tuple(entry.isFull(),
                                           entry.getAckedBytes(),
                                           entry.getBytesOutstanding(),
                                           entry.getMaxBytes());
                });

        if (full) {
            logger->info(
                    "Unable to notify paused connection because "
                    "DcpProducer::BufferLog is full; ackedBytes:{}, "
                    "bytesSent:{}, maxBytes:{}",
                    ackedBytes,
                    outstanding,
                    max);
        } else {
            scheduleNotify();
        }
    }
}

cb::engine_errc DcpProducer::maybeDisconnect() {
    const auto now = ep_uptime_now();
    auto elapsedTime = now - lastReceiveTime.load();
    auto dcpIdleTimeout = getIdleTimeout();
    if (noopCtx.enabled && elapsedTime > dcpIdleTimeout) {
        logger->warn(
                "Disconnecting because a message has not been received for "
                "the DCP idle timeout of {}s. "
                "Sent last message (e.g. mutation/noop/streamEnd) {}s ago. "
                "Received last message {}s ago. "
                "DCP noop [lastSent:{}s, lastRecv:{}s, interval:{}s, "
                "opaque:{}, pendingRecv:{}], "
                "paused:{}, pausedReason:{}",
                dcpIdleTimeout.count(),
                (now - lastSendTime.load()),
                elapsedTime,
                (now - noopCtx.sendTime),
                (now - noopCtx.recvTime),
                noopCtx.dcpNoopTxInterval.count(),
                noopCtx.opaque,
                noopCtx.pendingRecv ? "true" : "false",
                isPaused() ? "true" : "false",
                to_string(getPausedReason()));
        return cb::engine_errc::disconnect;
    }
    // Returning cb::engine_errc::failed means ignore and continue
    // without disconnecting
    return cb::engine_errc::failed;
}

cb::engine_errc DcpProducer::maybeSendNoop(
        DcpMessageProducersIface& producers) {
    if (!noopCtx.enabled) {
        // Returning cb::engine_errc::failed means ignore and continue
        // without sending a noop
        return cb::engine_errc::failed;
    }
    const auto now = ep_uptime_now();
    const auto elapsedTime(now - noopCtx.sendTime);

    // Check to see if waiting for a noop reply.
    // If not try to send a noop to the consumer if the interval has passed
    if (!noopCtx.pendingRecv && elapsedTime >= noopCtx.dcpNoopTxInterval) {
        const auto ret = producers.noop(++noopCtx.opaque);

        if (ret == cb::engine_errc::success) {
            noopCtx.pendingRecv = true;
            noopCtx.sendTime = now;
            lastSendTime = noopCtx.sendTime;
        }
        return ret;
    }

    // We have already sent a noop and are awaiting a receive or
    // the time interval has not passed.  In either case continue
    // without sending a noop.
    return cb::engine_errc::failed;
}

size_t DcpProducer::getItemsSent() {
    return itemsSent;
}

size_t DcpProducer::getItemsRemaining() const {
    size_t remainingSize = 0;
    std::for_each(
            streams->begin(),
            streams->end(),
            [&remainingSize](const StreamsMap::value_type& vt) {
                for (auto itr = vt.second->rlock(); !itr.end(); itr.next()) {
                    auto* as = itr.get().get();
                    if (as) {
                        remainingSize += as->getItemsRemaining();
                    }
                }
            });

    return remainingSize;
}

DcpProducer::StreamAggStats DcpProducer::getStreamAggStats() const {
    DcpProducer::StreamAggStats stats;

    std::for_each(
            streams->begin(),
            streams->end(),
            [&stats](const StreamsMap::value_type& vt) {
                auto itr = vt.second->rlock();
                stats.streams += itr.size();
                for (; !itr.end(); itr.next()) {
                    auto* as = itr.get().get();
                    if (as) {
                        stats.itemsRemaining += as->getItemsRemaining();
                        stats.readyQueueMemory += as->getReadyQueueMemory();
                        stats.backfillItemsDisk += as->getBackfillItemsDisk();
                        stats.backfillItemsMemory +=
                                as->getBackfillItemsMemory();
                    }
                }
            });

    return stats;
}

size_t DcpProducer::getTotalBytesSent() {
    return totalBytesSent;
}

size_t DcpProducer::getTotalUncompressedDataSize() {
    return totalUncompressedDataSize;
}

std::vector<Vbid> DcpProducer::getVBVector() {
    std::vector<Vbid> vbvector;
    std::for_each(streams->begin(),
                  streams->end(),
                  [&vbvector](StreamsMap::value_type& iter) {
                      vbvector.push_back((Vbid)iter.first);
                  });
    return vbvector;
}

bool DcpProducer::bufferLogInsert(size_t bytes) {
    return log.wlock()->insert(bytes);
}

void DcpProducer::createCheckpointProcessorTask() {
    std::lock_guard<std::mutex> guard(checkpointCreator->mutex);
    checkpointCreator->task =
            std::make_shared<ActiveStreamCheckpointProcessorTask>(
                    engine_, shared_from_this());
}

void DcpProducer::scheduleCheckpointProcessorTask() {
    std::lock_guard<std::mutex> guard(checkpointCreator->mutex);
    ExecutorPool::get()->schedule(checkpointCreator->task);
}

void DcpProducer::scheduleCheckpointProcessorTask(
        std::shared_ptr<ActiveStream> s) {
    std::lock_guard<std::mutex> guard(checkpointCreator->mutex);
    if (!checkpointCreator->task) {
        throw std::logic_error(
                "DcpProducer::scheduleCheckpointProcessorTask task is null");
    }
    static_cast<ActiveStreamCheckpointProcessorTask*>(
            checkpointCreator->task.get())
            ->schedule(s);
}

DcpProducer::StreamMapValue DcpProducer::findStreams(Vbid vbid) {
    auto it = streams->find(vbid.get());
    if (it != streams->end()) {
        return it->second;
    }
    return nullptr;
}

std::vector<DcpProducer::ContainerElement> DcpProducer::getStreams(Vbid vbid) {
    auto streams = findStreams(vbid);
    if (!streams) {
        return {}; // empty
    }
    std::vector<ContainerElement> rv;
    {
        // scope for rlock, just iterate and copy (one allocation occurs for
        // the container)
        auto handle = streams->rlock();
        rv.reserve(handle.size());
        for (; !handle.end(); handle.next()) {
            rv.emplace_back(handle.get());
        }
    }
    return rv;
}

void DcpProducer::updateStreamsMap(Vbid vbid,
                                   cb::mcbp::DcpStreamId sid,
                                   std::shared_ptr<ActiveStream>& stream) {
    using cb::tracing::Code;
    ScopeTimer1<TracerStopwatch> timer(*getCookie(), Code::StreamUpdateMap);

    updateStreamsMapHook();

    auto found = streams->find(vbid.get());

    if (found != streams->end()) {
        // vbid is already mapped found.first is a shared_ptr<StreamContainer>
        if (found->second) {
            auto handle = found->second->wlock();
            for (; !handle.end(); handle.next()) {
                auto& sp = handle.get(); // get the shared_ptr<Stream>
                if (sp->compareStreamId(sid)) {
                    // Error if found - given we just checked this
                    // in the pre-flight checks for streamRequest.
                    auto msg = fmt::format(
                            "({}) Stream ({}) request failed"
                            " because a stream unexpectedly exists in "
                            "StreamContainer for this vbucket",
                            vbid,
                            sid.to_string());
                    logger->warn(msg);
                    throw std::logic_error("DcpProducer::updateStreamsMap " +
                                           msg);
                }
            }

            /*
             * Add the Stream to the StreamContainer if we allow multiple
             * streams or if there are no other streams currently in the
             * container for this vb. We're under a writelock so we won't race
             * and accidentally create multiple streams if we don't support it.
             */
            if (multipleStreamRequests == MultipleStreamRequests::Yes ||
                handle.empty()) {
                // If we're here the vbid is mapped so we must update the
                // existing container
                handle.push_front(stream);
            } else {
                throw std::logic_error(
                        "DcpProducer::updateStreamsMap invalid state to add "
                        "multiple streams");
            }
        } else {
            throw std::logic_error("DcpProducer::updateStreamsMap " +
                                   vbid.to_string() + " is mapped to null");
        }
    } else {
        // vbid is not mapped
        streams->insert(std::make_pair(
                vbid.get(),
                std::make_shared<StreamContainer<ContainerElement>>(stream)));
    }
}

cb::mcbp::DcpStreamEndStatus DcpProducer::mapEndStreamStatus(
        CookieIface* cookie, cb::mcbp::DcpStreamEndStatus status) const {
    if (!cookie->isCollectionsSupported()) {
        switch (status) {
        case cb::mcbp::DcpStreamEndStatus::Ok:
        case cb::mcbp::DcpStreamEndStatus::Closed:
        case cb::mcbp::DcpStreamEndStatus::StateChanged:
        case cb::mcbp::DcpStreamEndStatus::Disconnected:
        case cb::mcbp::DcpStreamEndStatus::Slow:
        case cb::mcbp::DcpStreamEndStatus::BackfillFail:
        case cb::mcbp::DcpStreamEndStatus::Rollback:
            break;
        case cb::mcbp::DcpStreamEndStatus::FilterEmpty:
        case cb::mcbp::DcpStreamEndStatus::LostPrivileges:
            status = cb::mcbp::DcpStreamEndStatus::Ok;
        }
    }
    return status;
}

std::string DcpProducer::getConsumerName() const {
    return consumerName.copy();
}

bool DcpProducer::isOutOfOrderSnapshotsEnabled() const {
    return outOfOrderSnapshots != OutOfOrderSnapshots::No &&
           engine_.getKVBucket()->isByIdScanSupported();
}

bool DcpProducer::isOutOfOrderSnapshotsEnabledWithSeqnoAdvanced() const {
    return outOfOrderSnapshots == OutOfOrderSnapshots::YesWithSeqnoAdvanced &&
           engine_.getKVBucket()->isByIdScanSupported();
}

std::optional<uint64_t> DcpProducer::getHighSeqnoOfCollections(
        const Collections::VB::Filter& filter, VBucket& vbucket) {
    using cb::tracing::Code;
    ScopeTimer1<TracerStopwatch> timer(*getCookie(),
                                       Code::StreamGetCollectionHighSeq);

    if (filter.isPassThroughFilter()) {
        return std::nullopt;
    }

    uint64_t maxHighSeqno = 0;
    for (auto& coll : filter) {
        auto handle = vbucket.getManifest().lock(coll.first);
        if (!handle.valid()) {
            logger->warn(
                    "({}) DcpProducer::getHighSeqnoOfCollections(): failed "
                    "to find collectionID:{}, scopeID:{}, in the manifest",
                    vbucket.getId(),
                    coll.first,
                    coll.second.scopeId);
            // return std::nullopt as we don't want our caller to use rollback
            // optimisation for collections streams as we weren't able to find
            // the collections for the stream in the manifest.
            return std::nullopt;
        }
        auto collHighSeqno = handle.getHighSeqno();
        maxHighSeqno = std::max(maxHighSeqno, collHighSeqno);
    }

    return {maxHighSeqno};
}

void DcpProducer::setBackfillByteLimit(size_t bytes) {
    if (backfillMgr) {
        backfillMgr->setBackfillByteLimit(bytes);
    }
}

size_t DcpProducer::getBackfillByteLimit() const {
    return backfillMgr ? backfillMgr->getBackfillByteLimit() : 0;
}
