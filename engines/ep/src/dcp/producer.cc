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

#include "dcp/producer.h"

#include "backfill.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/manager.h"
#include "collections/vbucket_filter.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "common.h"
#include "connhandler_impl.h"
#include "dcp/active_stream.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/backfill-manager.h"
#include "dcp/dcpconnmap.h"
#include "dcp/response.h"
#include "executorpool.h"
#include "failover-table.h"
#include "item_eviction.h"
#include "kv_bucket.h"
#include "snappy-c.h"

#include <memcached/server_cookie_iface.h>
#include <nlohmann/json.hpp>
#include <statistics/cbstat_collector.h>

const std::chrono::seconds DcpProducer::defaultDcpNoopTxInterval(20);

DcpProducer::BufferLog::State DcpProducer::BufferLog::getState_UNLOCKED() {
    if (isEnabled_UNLOCKED()) {
        if (isFull_UNLOCKED()) {
            return Full;
        } else {
            return SpaceAvailable;
        }
    }
    return Disabled;
}

void DcpProducer::BufferLog::setBufferSize(size_t maxBytes) {
    std::unique_lock<folly::SharedMutex> wlh(logLock);
    this->maxBytes = maxBytes;
    if (maxBytes == 0) {
        bytesOutstanding = 0;
        ackedBytes.reset(0);
    }
}

bool DcpProducer::BufferLog::insert(size_t bytes) {
    std::unique_lock<folly::SharedMutex> wlh(logLock);
    bool inserted = false;
    // If the log is not enabled
    // or there is space, allow the insert
    if (!isEnabled_UNLOCKED() || !isFull_UNLOCKED()) {
        bytesOutstanding += bytes;
        inserted = true;
    }
    return inserted;
}

void DcpProducer::BufferLog::release_UNLOCKED(size_t bytes) {
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
    std::shared_lock<folly::SharedMutex> rhl(logLock);
    if (getState_UNLOCKED() == Full) {
        producer.pause(PausedReason::BufferLogFull);
        return true;
    }
    return false;
}

void DcpProducer::BufferLog::unpauseIfSpaceAvailable() {
    std::shared_lock<folly::SharedMutex> rhl(logLock);
    if (getState_UNLOCKED() == Full) {
        EP_LOG_INFO(
                "{} Unable to notify paused connection because "
                "DcpProducer::BufferLog is full; ackedBytes:{}"
                ", bytesSent:{}, maxBytes:{}",
                producer.logHeader(),
                ackedBytes,
                uint64_t(bytesOutstanding),
                uint64_t(maxBytes));
    } else {
        producer.scheduleNotify();
    }
}

void DcpProducer::BufferLog::acknowledge(size_t bytes) {
    std::unique_lock<folly::SharedMutex> wlh(logLock);
    State state = getState_UNLOCKED();
    if (state != Disabled) {
        release_UNLOCKED(bytes);
        ackedBytes += bytes;

        if (state == Full) {
            EP_LOG_INFO(
                    "{} Notifying paused connection now that "
                    "DcpProducer::BufferLog is no longer full; ackedBytes:{}"
                    ", bytesSent:{}, maxBytes:{}",
                    producer.logHeader(),
                    ackedBytes,
                    uint64_t(bytesOutstanding),
                    uint64_t(maxBytes));
            producer.scheduleNotify();
        }
    }
}

void DcpProducer::BufferLog::addStats(const AddStatFn& add_stat,
                                      const void* c) {
    std::shared_lock<folly::SharedMutex> rhl(logLock);
    if (isEnabled_UNLOCKED()) {
        producer.addStat("max_buffer_bytes", maxBytes, add_stat, c);
        producer.addStat("unacked_bytes", bytesOutstanding, add_stat, c);
        producer.addStat("total_acked_bytes", ackedBytes, add_stat, c);
        producer.addStat("flow_control", "enabled", add_stat, c);
    } else {
        producer.addStat("flow_control", "disabled", add_stat, c);
    }
}

/// Decode IncludeValue from DCP producer flags.
static IncludeValue toIncludeValue(uint32_t flags) {
    using cb::mcbp::request::DcpOpenPayload;
    if ((flags & DcpOpenPayload::NoValue) != 0) {
        return IncludeValue::No;
    }
    if ((flags & DcpOpenPayload::NoValueWithUnderlyingDatatype) != 0) {
        return IncludeValue::NoWithUnderlyingDatatype;
    }
    return IncludeValue::Yes;
}

DcpProducer::DcpProducer(EventuallyPersistentEngine& e,
                         const void* cookie,
                         const std::string& name,
                         uint32_t flags,
                         bool startTask)
    : ConnHandler(e, cookie, name),
      sendStreamEndOnClientStreamClose(false),
      consumerSupportsHifiMfu(false),
      lastSendTime(ep_current_time()),
      log(*this),
      backfillMgr(std::make_shared<BackfillManager>(
              *e.getKVBucket(), e.getDcpConnMap(), e.getConfiguration())),
      streams(makeStreamsMap(e.getConfiguration().getMaxVbuckets())),
      itemsSent(0),
      totalBytesSent(0),
      totalUncompressedDataSize(0),
      includeValue(toIncludeValue(flags)),
      includeXattrs(
              ((flags & cb::mcbp::request::DcpOpenPayload::IncludeXattrs) != 0)
                      ? IncludeXattrs::Yes
                      : IncludeXattrs::No),
      pitrEnabled((flags & cb::mcbp::request::DcpOpenPayload::PiTR) != 0
                          ? PointInTimeEnabled::Yes
                          : PointInTimeEnabled::No),
      includeDeleteTime(
              ((flags &
                cb::mcbp::request::DcpOpenPayload::IncludeDeleteTimes) != 0)
                      ? IncludeDeleteTime::Yes
                      : IncludeDeleteTime::No),
      createChkPtProcessorTsk(startTask),
      connectionSupportsSnappy(
              e.isDatatypeSupported(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
    setSupportAck(true);
    setReserved(true);
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

    engine_.setDCPPriority(getCookie(), ConnectionPriority::Medium);

    // The consumer assigns opaques starting at 0 so lets have the producer
    //start using opaques at 10M to prevent any opaque conflicts.
    noopCtx.opaque = 10000000;
    noopCtx.sendTime = ep_current_time();

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

    enableExtMetaData = false;
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
            ((flags &
              cb::mcbp::request::DcpOpenPayload::IncludeDeletedUserXattrs) != 0)
                    ? IncludeDeletedUserXattrs::Yes
                    : IncludeDeletedUserXattrs::No;
}

DcpProducer::~DcpProducer() {
    backfillMgr.reset();
}

void DcpProducer::cancelCheckpointCreatorTask() {
    LockHolder guard(checkpointCreator->mutex);
    if (checkpointCreator->task) {
        static_cast<ActiveStreamCheckpointProcessorTask*>(
                checkpointCreator->task.get())
                ->cancelTask();
        ExecutorPool::get()->cancel(checkpointCreator->task->getId());
        checkpointCreator->task.reset();
    }
}

cb::engine_errc DcpProducer::streamRequest(
        uint32_t flags,
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
    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return cb::engine_errc::disconnect;
    }

    VBucketPtr vb = engine_.getVBucket(vbucket);
    if (!vb) {
        logger->warn(
                "({}) Stream request failed because "
                "this vbucket doesn't exist",
                vbucket);
        return cb::engine_errc::not_my_vbucket;
    }

    // check for mandatory noop
    if ((includeXattrs == IncludeXattrs::Yes) || json.has_value()) {
        if (!noopCtx.enabled &&
            engine_.getConfiguration().isDcpNoopMandatoryForV5Features()) {
            logger->warn(
                    "({}) noop is mandatory for v5 features like "
                    "xattrs and collections",
                    vbucket);
            return cb::engine_errc::not_supported;
        }
    }

    if ((flags & DCP_ADD_STREAM_ACTIVE_VB_ONLY) &&
        (vb->getState() != vbucket_state_active)) {
        logger->info(
                "({}) Stream request failed because "
                "the vbucket is in state:{}, only active vbuckets were "
                "requested",
                vbucket,
                vb->toString(vb->getState()));
        return cb::engine_errc::not_my_vbucket;
    }

    if (start_seqno > end_seqno) {
        EP_LOG_WARN(
                "{} ({}) Stream request failed because the start "
                "seqno ({}) is larger than the end seqno ({}); "
                "Incorrect params passed by the DCP client",
                logHeader(),
                vbucket,
                start_seqno,
                end_seqno);
        return cb::engine_errc::out_of_range;
    }

    if (!(snap_start_seqno <= start_seqno && start_seqno <= snap_end_seqno)) {
        logger->warn(
                "({}) Stream request failed because "
                "the snap start seqno ({}) <= start seqno ({})"
                " <= snap end seqno ({}) is required",
                vbucket,
                snap_start_seqno,
                start_seqno,
                snap_end_seqno);
        return cb::engine_errc::out_of_range;
    }

    // Construct the filter before rollback checks so we ensure the client view
    // of collections is compatible with the vbucket.
    Collections::VB::Filter filter(
            json, vb->getManifest(), getCookie(), engine_);

    if (!filter.getStreamId() &&
        multipleStreamRequests == MultipleStreamRequests::Yes) {
        logger->warn(
                "Stream request for {} failed because a valid stream-ID is "
                "required.",
                vbucket);
        return cb::engine_errc::dcp_streamid_invalid;
    } else if (filter.getStreamId() &&
               multipleStreamRequests == MultipleStreamRequests::No) {
        logger->warn(
                "Stream request for {} failed because a stream-ID:{} is "
                "present "
                "but not required.",
                vbucket,
                filter.getStreamId());
        return cb::engine_errc::dcp_streamid_invalid;
    }

    // Check if this vbid can be added to this producer connection, and if
    // the vb connection map needs updating (if this is a new VB).
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
                        return cb::engine_errc::key_already_exists;
                    } else {
                        // Found a 'dead' stream which can be replaced.
                        handle.erase();

                        // Don't need to add an entry to vbucket-to-conns map
                        callAddVBConnByVBId = false;

                        break;
                    }
                }
            }
        }
    }

    std::pair<bool, std::string> need_rollback =
            vb->failovers->needsRollback(start_seqno,
                                         vb->getHighSeqno(),
                                         vbucket_uuid,
                                         snap_start_seqno,
                                         snap_end_seqno,
                                         vb->getPurgeSeqno(),
                                         flags & DCP_ADD_STREAM_STRICT_VBUUID,
                                         getHighSeqnoOfCollections(filter, *vb),
                                         rollback_seqno);

    if (need_rollback.first) {
        logger->warn(
                "({}) Stream request requires rollback to seqno:{} "
                "because {}. Client requested seqnos:{{{},{}}} "
                "snapshot:{{{},{}}} uuid:{}",
                vbucket,
                *rollback_seqno,
                need_rollback.second,
                start_seqno,
                end_seqno,
                snap_start_seqno,
                snap_end_seqno,
                vbucket_uuid);
        return cb::engine_errc::rollback;
    }

    std::vector<vbucket_failover_t> failoverEntries =
            vb->failovers->getFailoverLog();

    if (flags & DCP_ADD_STREAM_FLAG_LATEST) {
        end_seqno = vb->getHighSeqno();
    }

    if (flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
        end_seqno = engine_.getKVBucket()->getLastPersistedSeqno(vbucket);
    } else if (isPointInTimeEnabled() == PointInTimeEnabled::Yes) {
        logger->warn("DCP connections with PiTR enabled must enable DISKONLY");
        return cb::engine_errc::invalid_arguments;
    }

    if (start_seqno > end_seqno) {
        EP_LOG_WARN(
                "{} ({}) Stream request failed because "
                "the start seqno ({}) is larger than the end seqno ({}"
                "), stream request flags {}, vb_uuid {}, snapStartSeqno {}, "
                "snapEndSeqno {}; should have rolled back instead",
                logHeader(),
                vbucket,
                start_seqno,
                end_seqno,
                flags,
                vbucket_uuid,
                snap_start_seqno,
                snap_end_seqno);
        return cb::engine_errc::out_of_range;
    }

    if (start_seqno > static_cast<uint64_t>(vb->getHighSeqno())) {
        EP_LOG_WARN(
                "{} ({}) Stream request failed because "
                "the start seqno ({}) is larger than the vb highSeqno "
                "({}), stream request flags is {}, vb_uuid {}, snapStartSeqno "
                "{}, snapEndSeqno {}; should have rolled back instead",
                logHeader(),
                vbucket,
                start_seqno,
                vb->getHighSeqno(),
                flags,
                vbucket_uuid,
                snap_start_seqno,
                snap_end_seqno);
        return cb::engine_errc::out_of_range;
    }

    // Take copy of Filter's streamID, given it will be moved-from when
    // ActiveStream is constructed.
    const auto streamID = filter.getStreamId();

    std::shared_ptr<ActiveStream> s;
    try {
        s = std::make_shared<ActiveStream>(&engine_,
                                           shared_from_this(),
                                           getName(),
                                           flags,
                                           opaque,
                                           *vb,
                                           start_seqno,
                                           end_seqno,
                                           vbucket_uuid,
                                           snap_start_seqno,
                                           snap_end_seqno,
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
    /* We want to create the 'createCheckpointProcessorTask' here even if
       the stream creation fails later on in the func. The goal is to
       create the 'checkpointProcessorTask' before any valid active stream
       is created */
    if (createChkPtProcessorTsk && !checkpointCreator->task) {
        createCheckpointProcessorTask();
        scheduleCheckpointProcessorTask();
    }

    {
        folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
        if (vb->getState() == vbucket_state_dead) {
            logger->warn(
                    "({}) Stream request failed because "
                    "this vbucket is in dead state",
                    vbucket);
            return cb::engine_errc::not_my_vbucket;
        }

        if (vb->isReceivingInitialDiskSnapshot()) {
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

    // See MB-25820:  Ensure that callback is called only after all other
    // possible error cases have been tested.  This is to ensure we do not
    // generate two responses for a single streamRequest.
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(nullptr,
                                                                     true);
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
    auto freqCount = item.getFreqCounterValue();
    if (consumerSupportsHifiMfu) {
        // The consumer supports the hifi_mfu eviction
        // policy, therefore use the frequency counter.
        return freqCount;
    }
    // The consumer does not support the hifi_mfu
    // eviction policy, therefore map from the 8-bit
    // probabilistic counter (256 states) to NRU (4 states).
    return ItemEviction::convertFreqCountToNRUValue(freqCount);
}

cb::unique_item_ptr DcpProducer::toUniqueItemPtr(
        std::unique_ptr<Item>&& item) const {
    return {item.release(), cb::ItemDeleter(&engine_)};
}

cb::engine_errc DcpProducer::step(DcpMessageProducersIface& producers) {
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

    auto* mutationResponse = dynamic_cast<MutationResponse*>(resp.get());
    if (mutationResponse) {
        itmCpy = std::make_unique<Item>(*mutationResponse->getItem());
        if (isCompressionEnabled()) {
            /**
             * Retrieve the uncompressed length if the document is compressed.
             * This is to account for the total number of bytes if the data
             * was sent as uncompressed
             */
            if (mcbp::datatype::is_snappy(itmCpy->getDataType())) {
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
            if (sendStreamEndOnClientStreamClose ||
                se->getFlags() != cb::mcbp::DcpStreamEndStatus::Closed) {
                // We did not remove the ConnHandler earlier so we could wait to
                // send the streamEnd We have done that now, remove it.
                engine_.getDcpConnMap().removeVBConnByVBId(getCookie(),
                                                           se->getVbucket());
                std::shared_ptr<Stream> stream;
                bool vbFound;
                std::tie(stream, vbFound) = closeStreamInner(
                        se->getVbucket(), resp->getStreamId(), true);
                if (!stream) {
                    throw std::logic_error(
                            "DcpProducer::step(StreamEnd): no stream was "
                            "found "
                            "for " +
                            se->getVbucket().to_string() + " " +
                            resp->getStreamId().to_string());
                } else {
                    Expects(!stream->isActive());
                }
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
                    s->getEventData(),
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
    }

    if (ret == cb::engine_errc::success) {
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

    lastSendTime = ep_current_time();
    return ret;
}

cb::engine_errc DcpProducer::bufferAcknowledgement(uint32_t opaque,
                                                   Vbid vbucket,
                                                   uint32_t buffer_bytes) {
    lastReceiveTime = ep_current_time();
    log.acknowledge(buffer_bytes);
    return cb::engine_errc::success;
}

cb::engine_errc DcpProducer::deletionV1OrV2(IncludeDeleteTime includeDeleteTime,
                                            MutationResponse& mutationResponse,
                                            DcpMessageProducersIface& producers,
                                            std::unique_ptr<Item> itmCpy,
                                            cb::engine_errc ret,
                                            cb::mcbp::DcpStreamId sid) {
    if (includeDeleteTime == IncludeDeleteTime::Yes) {
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
    lastReceiveTime = ep_current_time();
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
                    getCookie(),
                    "Unsupported value '" + keyStr +
                            "' for ctrl parameter 'backfill_order'");
            return cb::engine_errc::invalid_arguments;
        }
        return cb::engine_errc::success;

    } else if (strncmp(param, "connection_buffer_size", key.size()) == 0) {
        uint32_t size;
        if (parseUint32(valueStr.c_str(), &size)) {
            /* Size 0 implies the client (DCP consumer) does not support
               flow control */
            log.setBufferSize(size);
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
    } else if (strncmp(param, "enable_ext_metadata", key.size()) == 0) {
        if (valueStr == "true") {
            enableExtMetaData = true;
        } else {
            enableExtMetaData = false;
        }
        return cb::engine_errc::success;
    } else if (strncmp(param, "force_value_compression", key.size()) == 0) {
        if (!isSnappyEnabled()) {
            engine_.setErrorContext(getCookie(), "The ctrl parameter "
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
        uint32_t noopInterval;
        if (parseUint32(valueStr.c_str(), &noopInterval)) {
            /*
             * We need to ensure that we only set the noop interval to a value
             * that is a multiple of the connection manager interval. The reason
             * is that if there is no DCP traffic we snooze for the connection
             * manager interval before sending the noop.
             */
            if (noopInterval % engine_.getConfiguration().
                    getConnectionManagerInterval() == 0) {
                noopCtx.dcpNoopTxInterval = std::chrono::seconds(noopInterval);
                return cb::engine_errc::success;
            } else {
                logger->warn(
                        "The ctrl parameter "
                        "set_noop_interval:{} is being set to seconds."
                        "This is not a multiple of the "
                        "connectionManagerInterval:{} "
                        "of seconds, and so is not supported.",
                        noopInterval,
                        engine_.getConfiguration()
                                .getConnectionManagerInterval());
                return cb::engine_errc::invalid_arguments;
            }
        }
    } else if (strncmp(param, "set_priority", key.size()) == 0) {
        if (valueStr == "high") {
            engine_.setDCPPriority(getCookie(), ConnectionPriority::High);
            return cb::engine_errc::success;
        } else if (valueStr == "medium") {
            engine_.setDCPPriority(getCookie(), ConnectionPriority::Medium);
            return cb::engine_errc::success;
        } else if (valueStr == "low") {
            engine_.setDCPPriority(getCookie(), ConnectionPriority::Low);
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
            if (!consumerName.empty()) {
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
    } else if (key == "include_deleted_user_xattrs") {
        if (valueStr == "true") {
            if (includeDeletedUserXattrs == IncludeDeletedUserXattrs::Yes) {
                return cb::engine_errc::success;
            } else {
                // Note: Return here as there is no invalid param, we just want
                // to inform the DCP client that this Producer does not enable
                // IncludeDeletedUserXattrs, so we do not want to log as below
                return cb::engine_errc::invalid_arguments;
            }
        }
    } else if (key == "v7_dcp_status_codes") {
        if (valueStr == "true") {
            enabledV7DcpStatus = true;
            return cb::engine_errc::success;
        } else {
            return cb::engine_errc::invalid_arguments;
        }
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

    if (consumerName.empty()) {
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

    if (seqnoAckHook) {
        seqnoAckHook();
    }

    return stream->seqnoAck(consumerName, prepared_seqno);
}

bool DcpProducer::handleResponse(const cb::mcbp::Response& response) {
    lastReceiveTime = ep_current_time();
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
            streamInfo = fmt::format(fmt("stream name:{}, {}, state:{}"),
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
                    response.toJSON(true).dump(),
                    streamInfo);
            return true;
        }
        logger->error(
                "DcpProducer::handleResponse disconnecting, received "
                "unexpected "
                "response:{} for stream:{}",
                response.toJSON(true).dump(),
                streamInfo);
        return false;
    };

    switch (opcode) {
    case cb::mcbp::ClientOpcode::DcpSetVbucketState:
    case cb::mcbp::ClientOpcode::DcpSnapshotMarker: {
        // Use find_if2 which will return the matching shared_ptr<Stream>
        auto stream = find_if2(streamFindFn);
        if (stream) {
            auto* as = static_cast<ActiveStream*>(stream.get());
            if (opcode == cb::mcbp::ClientOpcode::DcpSetVbucketState) {
                as->setVBucketStateAckRecieved();
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
        errorMsg += response.toJSON(true).dump();
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
    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return cb::engine_errc::disconnect;
    }

    if (!sid && multipleStreamRequests == MultipleStreamRequests::Yes) {
        logger->warn(
                "({}) closeStream request failed because a valid "
                "stream-ID is required.",
                vbucket);
        return cb::engine_errc::dcp_streamid_invalid;
    } else if (sid && multipleStreamRequests == MultipleStreamRequests::No) {
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
    } else {
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
    backfillMgr->bytesSent(bytes);
}

bool DcpProducer::scheduleBackfillManager(VBucket& vb,
                                          std::shared_ptr<ActiveStream> s,
                                          uint64_t start,
                                          uint64_t end) {
    if (start <= end) {
        switch (backfillMgr->schedule(
                vb.createDCPBackfill(engine_, s, start, end))) {
        case BackfillManager::ScheduleResult::Active:
            break;
        case BackfillManager::ScheduleResult::Pending:
            EP_LOG_INFO(
                    "Backfill for {} {} is pending", s->getName(), vb.getId());
            break;
        }
        return true;
    }
    return false;
}

bool DcpProducer::scheduleBackfillManager(VBucket& vb,
                                          std::shared_ptr<ActiveStream> s,
                                          CollectionID cid) {
    backfillMgr->schedule(vb.createDCPBackfill(engine_, s, cid));
    return true;
}

void DcpProducer::addStats(const AddStatFn& add_stat, const void* c) {
    ConnHandler::addStats(add_stat, c);

    addStat("items_sent", getItemsSent(), add_stat, c);
    addStat("items_remaining", getItemsRemaining(), add_stat, c);
    addStat("total_bytes_sent", getTotalBytesSent(), add_stat, c);
    if (isCompressionEnabled()) {
        addStat("total_uncompressed_data_size", getTotalUncompressedDataSize(),
                add_stat, c);
    }
    addStat("last_sent_time", lastSendTime, add_stat, c);
    addStat("last_receive_time", lastReceiveTime, add_stat, c);
    addStat("noop_enabled", noopCtx.enabled, add_stat, c);
    addStat("noop_wait", noopCtx.pendingRecv, add_stat, c);
    addStat("enable_ext_metadata", enableExtMetaData, add_stat, c);
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

    log.addStats(add_stat, c);

    ExTask pointerCopy;
    { // Locking scope
        LockHolder guard(checkpointCreator->mutex);
        pointerCopy = checkpointCreator->task;
    }

    if (pointerCopy) {
        static_cast<ActiveStreamCheckpointProcessorTask*>(pointerCopy.get())
                ->addStats(getName(), add_stat, c);
    }

    ready.addStats(getName() + ":dcp_ready_queue_", add_stat, c);

    // Make a copy of all valid streams (under lock), and then call addStats
    // for each one. (Done in two stages to minmise how long we have the
    // streams map locked for).
    std::vector<std::shared_ptr<Stream>> valid_streams;

    std::for_each(streams->begin(),
                  streams->end(),
                  [&valid_streams](const StreamsMap::value_type& vt) {
                      for (auto handle = vt.second->rlock(); !handle.end();
                           handle.next()) {
                          valid_streams.push_back(handle.get());
                      }
                  });

    for (const auto& stream : valid_streams) {
        stream->addStats(add_stat, c);
    }

    addStat("num_streams", valid_streams.size(), add_stat, c);
}

void DcpProducer::addTakeoverStats(const AddStatFn& add_stat,
                                   const void* c,
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
            logger->warn(
                    "({}) "
                    "DcpProducer::addTakeoverStats Stream type is {} and not "
                    "the expected Active",
                    vb.getId(),
                    stream->getStreamTypeName());
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
    add_casted_stat("status", "does_not_exist", add_stat, c);
    add_casted_stat("estimate", 0, add_stat, c);
    add_casted_stat("backfillRemaining", 0, add_stat, c);
}

void DcpProducer::aggregateQueueStats(ConnCounter& aggregator) const {
    ++aggregator.totalProducers;
    aggregator.conn_queueDrain += itemsSent;
    aggregator.conn_totalBytes += totalBytesSent;
    aggregator.conn_totalUncompressedDataSize += totalUncompressedDataSize;
    aggregator.conn_queueRemaining += getItemsRemaining();
}

void DcpProducer::notifySeqnoAvailable(Vbid vbucket,
                                       uint64_t seqno,
                                       SyncWriteOperation syncWrite) {
    if (syncWrite == SyncWriteOperation::Yes &&
        getSyncReplSupport() == SyncReplication::No) {
        // Don't bother notifying this Producer if the operation is a prepare
        // and we do not support SyncWrites or SyncReplication. It wouldn't send
        // anything anyway and we'd run a bunch of tasks on NonIO threads, front
        // end worker threads and potentially AuxIO threads.
        return;
    }

    auto rv = streams->find(vbucket.get());

    if (rv != streams->end()) {
        auto handle = rv->second->rlock();
        for (; !handle.end(); handle.next()) {
            if (handle.get()->isActive()) {
                handle.get()->notifySeqnoAvailable(seqno);
            }
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
                    auto* as =
                            static_cast<ActiveStream*>(handle.get().get());
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
    if (closeAllStreamsPreLockHook) {
        closeAllStreamsPreLockHook();
    }

    std::lock_guard<std::mutex> lg(closeAllStreamsLock);

    if (closeAllStreamsPostLockHook) {
        closeAllStreamsPostLockHook();
    }

    lastReceiveTime = ep_current_time();
    std::vector<Vbid> vbvector;
    {
        std::for_each(streams->begin(),
                      streams->end(),
                      [this, &vbvector](StreamsMap::value_type& vt) {
                          vbvector.push_back((Vbid)vt.first);
                          std::vector<std::shared_ptr<Stream>> streamPtrs;
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
                              streamPtr->setDead(cb::mcbp::DcpStreamEndStatus::
                                                         Disconnected);
                          }
                      });
    }
    for (const auto vbid: vbvector) {
        engine_.getDcpConnMap().removeVBConnByVBId(getCookie(), vbid);
    }

    if (closeAllStreamsHook) {
        closeAllStreamsHook();
    }

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
        unPause();

        Vbid vbucket = Vbid(0);
        while (ready.popFront(vbucket)) {
            if (log.pauseIfFull()) {
                ready.pushUnique(vbucket);
                return NULL;
            }

            auto rv = streams->find(vbucket.get());
            if (rv == streams->end()) {
                // The vbucket is not in the map.
                continue;
            }

            std::unique_ptr<DcpResponse> response;

            // Use the resumable handle so that we can service the streams that
            // are associated with the vbucket. If a stream returns a response
            // we will ship it (i.e. leave this scope). Then next time we visit
            // the VB, we should /resume/ from the next stream in the container.
            for (auto resumableIterator = rv->second->startResumable();
                 !resumableIterator.complete();
                 resumableIterator.next()) {
                const std::shared_ptr<Stream>& stream = resumableIterator.get();

                if (stream) {
                    response = stream->next();

                    if (response) {
                        // VB gave us something, validate it
                        switch (response->getEvent()) {
                        case DcpResponse::Event::SnapshotMarker: {
                            if (stream->endIfRequiredPrivilegesLost(
                                        getCookie())) {
                                return static_cast<ActiveStream*>(stream.get())
                                        ->makeEndStreamResponse(
                                                cb::mcbp::DcpStreamEndStatus::
                                                        LostPrivileges);
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
                            throw std::logic_error(
                                    std::string("DcpProducer::getNextItem: "
                                                "Producer (") +
                                    logHeader() +
                                    ") is attempting to "
                                    "write an unexpected event:" +
                                    response->to_string());
                        }

                        ready.pushUnique(vbucket);
                        return response;
                    } // else next stream for vb
                }
            }
        }

        // flag we are paused
        pause(PausedReason::ReadyListEmpty);

        // re-check the ready queue.
        // A new vbucket could of became ready and the notifier could of seen
        // paused = false, so reloop so we don't miss an operation.
    } while(!ready.empty());

    return nullptr;
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
    if (ready.pushUnique(vbucket)) {
        // Transitioned from empty to non-empty readyQ - unpause the Producer.
        log.unpauseIfSpaceAvailable();
    }
}

void DcpProducer::immediatelyNotify() {
    engine_.getDcpConnMap().notifyPausedConnection(shared_from_this());
}

void DcpProducer::scheduleNotify() {
    engine_.getDcpConnMap().addConnectionToPending(shared_from_this());
}

cb::engine_errc DcpProducer::maybeDisconnect() {
    const auto now = ep_current_time();
    auto elapsedTime = now - lastReceiveTime;
    auto dcpIdleTimeout = getIdleTimeout();
    if (noopCtx.enabled && std::chrono::seconds(elapsedTime) > dcpIdleTimeout) {
        logger->info(
                "Disconnecting because a message has not been received for "
                "the DCP idle timeout of {}s. "
                "Sent last message (e.g. mutation/noop/streamEnd) {}s ago. "
                "Received last message {}s ago. "
                "Last sent noop {}s ago. "
                "DCP noop interval is {}s. "
                "opaque: {}, pendingRecv: {}.",
                dcpIdleTimeout.count(),
                (now - lastSendTime),
                elapsedTime,
                (now - noopCtx.sendTime),
                noopCtx.dcpNoopTxInterval.count(),
                noopCtx.opaque,
                noopCtx.pendingRecv ? "true" : "false");
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
    std::chrono::seconds elapsedTime(ep_current_time() - noopCtx.sendTime);

    // Check to see if waiting for a noop reply.
    // If not try to send a noop to the consumer if the interval has passed
    if (!noopCtx.pendingRecv && elapsedTime >= noopCtx.dcpNoopTxInterval) {
        const auto ret = producers.noop(++noopCtx.opaque);

        if (ret == cb::engine_errc::success) {
            noopCtx.pendingRecv = true;
            noopCtx.sendTime = ep_current_time();
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
    return log.insert(bytes);
}

void DcpProducer::createCheckpointProcessorTask() {
    LockHolder guard(checkpointCreator->mutex);
    checkpointCreator->task =
            std::make_shared<ActiveStreamCheckpointProcessorTask>(
                    engine_, shared_from_this());
}

void DcpProducer::scheduleCheckpointProcessorTask() {
    LockHolder guard(checkpointCreator->mutex);
    ExecutorPool::get()->schedule(checkpointCreator->task);
}

void DcpProducer::scheduleCheckpointProcessorTask(
        std::shared_ptr<ActiveStream> s) {
    LockHolder guard(checkpointCreator->mutex);
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

void DcpProducer::updateStreamsMap(Vbid vbid,
                                   cb::mcbp::DcpStreamId sid,
                                   std::shared_ptr<ActiveStream>& stream) {
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
        const void* cookie, cb::mcbp::DcpStreamEndStatus status) const {
    if (!engine_.isCollectionsSupported(cookie)) {
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
    return consumerName;
}

bool DcpProducer::isOutOfOrderSnapshotsEnabled() const {
    return outOfOrderSnapshots == OutOfOrderSnapshots::Yes &&
           engine_.getKVBucket()->isByIdScanSupported();
}

std::optional<uint64_t> DcpProducer::getHighSeqnoOfCollections(
        const Collections::VB::Filter& filter, VBucket& vbucket) const {
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
                    coll.second);
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

DcpProducer::StreamsMap::SmartPtr DcpProducer::makeStreamsMap(
        size_t maxNumVBuckets) {
    // If we know the maximum number of vBuckets which will exist for this
    // Bucket (normally 1024), we can simply create a AtomicHashArray with
    // capacity == size (i.e. load factor of 1.0), given the key (Vbid) is
    // gauranteed to be unique and not collide with any other.
    StreamsMap::Config config;
    config.maxLoadFactor = 1.0;
    return StreamsMap::create(maxNumVBuckets, config);
}
