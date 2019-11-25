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
#include "common.h"
#include "connhandler_impl.h"
#include "dcp/active_stream.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/backfill-manager.h"
#include "dcp/dcpconnmap.h"
#include "dcp/notifier_stream.h"
#include "dcp/response.h"
#include "executorpool.h"
#include "failover-table.h"
#include "item_eviction.h"
#include "kv_bucket.h"
#include "snappy-c.h"
#include "statwriter.h"

#include <memcached/server_cookie_iface.h>

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
        EP_LOG_INFO(
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
                uint64_t(ackedBytes),
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
                    uint64_t(ackedBytes),
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
      notifyOnly((flags & cb::mcbp::request::DcpOpenPayload::Notifier) != 0),
      sendStreamEndOnClientStreamClose(false),
      consumerSupportsHifiMfu(false),
      lastSendTime(ep_current_time()),
      log(*this),
      backfillMgr(std::make_shared<BackfillManager>(engine_)),
      streams(streamsMapSize),
      itemsSent(0),
      totalBytesSent(0),
      totalUncompressedDataSize(0),
      includeValue(toIncludeValue(flags)),
      includeXattrs(
              ((flags & cb::mcbp::request::DcpOpenPayload::IncludeXattrs) != 0)
                      ? IncludeXattrs::Yes
                      : IncludeXattrs::No),
      includeDeleteTime(
              ((flags &
                cb::mcbp::request::DcpOpenPayload::IncludeDeleteTimes) != 0)
                      ? IncludeDeleteTime::Yes
                      : IncludeDeleteTime::No),
      createChkPtProcessorTsk(startTask) {
    setSupportAck(true);
    setReserved(true);
    pause(PausedReason::Initializing);
    if (notifyOnly) {
        setLogHeader("DCP (Notifier) " + getName() + " -");
    } else {
        setLogHeader("DCP (Producer) " + getName() + " -");
    }
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

    // MB-28468: Reduce the minimum log level of FTS DCP streams as they are
    // very noisy due to creating streams for non-existing vBuckets. Future
    // development of FTS should remedy this, however for now, we need to
    // reduce their verbosity as they cause the memcached log to rotate early.
    if (name.find("eq_dcpq:fts") != std::string::npos) {
        logger->set_level(spdlog::level::level_enum::critical);
        // Unregister this logger so that any changes in verbosity will not
        // be reflected in this logger. This prevents us from getting in a
        // state where we change the verbosity to a more verbose value, then
        // cannot return to the original state where this logger only prints
        // critical level messages and others print info level.
        logger->unregister();
    }

    engine_.setDCPPriority(getCookie(), CONN_PRIORITY_MED);

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
    }
}

ENGINE_ERROR_CODE DcpProducer::streamRequest(
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
        boost::optional<cb::const_char_buffer> json) {
    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    VBucketPtr vb = engine_.getVBucket(vbucket);
    if (!vb) {
        logger->warn(
                "({}) Stream request failed because "
                "this vbucket doesn't exist",
                vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    // check for mandatory noop
    if ((includeXattrs == IncludeXattrs::Yes) || json.is_initialized()) {
        if (!noopCtx.enabled &&
            engine_.getConfiguration().isDcpNoopMandatoryForV5Features()) {
            logger->warn(
                    "({}) noop is mandatory for v5 features like "
                    "xattrs and collections",
                    vbucket);
            return ENGINE_ENOTSUP;
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
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (!notifyOnly && start_seqno > end_seqno) {
        EP_LOG_WARN(
                "{} ({}) Stream request failed because the start "
                "seqno ({}) is larger than the end seqno ({}); "
                "Incorrect params passed by the DCP client",
                logHeader(),
                vbucket,
                start_seqno,
                end_seqno);
        return ENGINE_ERANGE;
    }

    if (!notifyOnly && !(snap_start_seqno <= start_seqno &&
        start_seqno <= snap_end_seqno)) {
        logger->warn(
                "({}) Stream request failed because "
                "the snap start seqno ({}) <= start seqno ({})"
                " <= snap end seqno ({}) is required",
                vbucket,
                snap_start_seqno,
                start_seqno,
                snap_end_seqno);
        return ENGINE_ERANGE;
    }

    // Construct the filter before rollback checks so we ensure the client view
    // of collections is compatible with the vbucket.
    Collections::VB::Filter filter(json, vb->getManifest());

    if (!filter.getStreamId() &&
        multipleStreamRequests == MultipleStreamRequests::Yes) {
        logger->warn(
                "Stream request for {} failed because a valid stream-ID is "
                "required.",
                vbucket);
        return ENGINE_DCP_STREAMID_INVALID;
    } else if (filter.getStreamId() &&
               multipleStreamRequests == MultipleStreamRequests::No) {
        logger->warn(
                "Stream request for {} failed because a stream-ID:{} is "
                "present "
                "but not required.",
                vbucket,
                filter.getStreamId());
        return ENGINE_DCP_STREAMID_INVALID;
    }

    // If we are a notify stream then we can't use the start_seqno supplied
    // since if it is greater than the current high seqno then it will always
    // trigger a rollback. As a result we should use the current high seqno for
    // rollback purposes.
    uint64_t notifySeqno = start_seqno;
    if (notifyOnly && start_seqno > static_cast<uint64_t>(vb->getHighSeqno())) {
        start_seqno = static_cast<uint64_t>(vb->getHighSeqno());
    }

    std::pair<bool, std::string> need_rollback =
            vb->failovers->needsRollback(start_seqno,
                                         vb->getHighSeqno(),
                                         vbucket_uuid,
                                         snap_start_seqno,
                                         snap_end_seqno,
                                         vb->getPurgeSeqno(),
                                         flags & DCP_ADD_STREAM_STRICT_VBUUID,
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
        return ENGINE_ROLLBACK;
    }

    std::vector<vbucket_failover_t> failoverEntries =
            vb->failovers->getFailoverLog();

    if (flags & DCP_ADD_STREAM_FLAG_LATEST) {
        end_seqno = vb->getHighSeqno();
    }

    if (flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
        end_seqno = engine_.getKVBucket()->getLastPersistedSeqno(vbucket);
    }

    if (!notifyOnly && start_seqno > end_seqno) {
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
        return ENGINE_ERANGE;
    }

    if (!notifyOnly && start_seqno > static_cast<uint64_t>(vb->getHighSeqno()))
    {
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
        return ENGINE_ERANGE;
    }

    std::shared_ptr<Stream> s;
    if (notifyOnly) {
        s = std::make_shared<NotifierStream>(&engine_,
                                             shared_from_this(),
                                             getName(),
                                             flags,
                                             opaque,
                                             vbucket,
                                             notifySeqno,
                                             end_seqno,
                                             vbucket_uuid,
                                             snap_start_seqno,
                                             snap_end_seqno);
    } else {
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
                                               std::move(filter));
        } catch (const cb::engine_error& e) {
            logger->warn(
                    "({}) Stream request failed because "
                    "the filter cannot be constructed, returning:{}",
                    Vbid(vbucket),
                    e.code().value());
            return ENGINE_ERROR_CODE(e.code().value());
        }
        /* We want to create the 'createCheckpointProcessorTask' here even if
           the stream creation fails later on in the func. The goal is to
           create the 'checkpointProcessorTask' before any valid active stream
           is created */
        if (createChkPtProcessorTsk && !checkpointCreator->task) {
            createCheckpointProcessorTask();
            scheduleCheckpointProcessorTask();
        }
    }

    bool add_vb_conn_map = true;
    {
        folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
        if (vb->getState() == vbucket_state_dead) {
            logger->warn(
                    "({}) Stream request failed because "
                    "this vbucket is in dead state",
                    vbucket);
            return ENGINE_NOT_MY_VBUCKET;
        }

        if (vb->isReceivingInitialDiskSnapshot()) {
            logger->info(
                    "({}) Stream request failed because this vbucket"
                    "is currently receiving its initial disk snapshot",
                    vbucket);
            return ENGINE_TMPFAIL;
        }

        if (!notifyOnly) {
            // MB-19428: Only activate the stream if we are adding it to the
            // streams map.
            static_cast<ActiveStream*>(s.get())->setActive();
        }

        add_vb_conn_map = updateStreamsMap(vbucket, filter.getStreamId(), s);
    }

    // See MB-25820:  Ensure that callback is called only after all other
    // possible error cases have been tested.  This is to ensure we do not
    // generate two responses for a single streamRequest.
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL,
                                                                     true);
    ENGINE_ERROR_CODE rv = callback(failoverEntries.data(),
                                    failoverEntries.size(),
                                    getCookie());
    ObjectRegistry::onSwitchThread(epe);
    if (rv != ENGINE_SUCCESS) {
        logger->warn(
                "({}) Couldn't add failover log to "
                "stream request due to error {}",
                vbucket,
                rv);
    }

    notifyStreamReady(vbucket);

    if (add_vb_conn_map) {
        engine_.getDcpConnMap().addVBConnByVBId(shared_from_this(), vbucket);
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

ENGINE_ERROR_CODE DcpProducer::step(struct dcp_message_producers* producers) {

    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE ret;
    if ((ret = maybeDisconnect()) != ENGINE_FAILED) {
          return ret;
    }

    if ((ret = maybeSendNoop(producers)) != ENGINE_FAILED) {
        return ret;
    }

    std::unique_ptr<DcpResponse> resp;
    if (rejectResp) {
        resp = std::move(rejectResp);
    } else {
        resp = getNextItem();
        if (!resp) {
            return ENGINE_EWOULDBLOCK;
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
            StreamEndResponse* se = static_cast<StreamEndResponse*>(resp.get());
            ret = producers->stream_end(
                    se->getOpaque(),
                    se->getVbucket(),
                    mapEndStreamStatus(getCookie(), se->getFlags()),
                    resp->getStreamId());

            if (sendStreamEndOnClientStreamClose) {
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
                }
            }
            break;
        }
        case DcpResponse::Event::Commit: {
            CommitSyncWrite* csr = static_cast<CommitSyncWrite*>(resp.get());
            ret = producers->commit(csr->getOpaque(),
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
            ret = producers->mutation(mutationResponse->getOpaque(),
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
                ret = producers->expiration(
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
            ret = producers->prepare(mutationResponse->getOpaque(),
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
            AbortSyncWrite& abort = dynamic_cast<AbortSyncWrite&>(*resp);
            ret = producers->abort(abort.getOpaque(),
                                   abort.getVbucket(),
                                   abort.getKey(),
                                   abort.getPreparedSeqno(),
                                   abort.getAbortSeqno());
            break;
        }
        case DcpResponse::Event::SnapshotMarker:
        {
            SnapshotMarker* s = static_cast<SnapshotMarker*>(resp.get());
            ret = producers->marker(s->getOpaque(),
                                    s->getVBucket(),
                                    s->getStartSeqno(),
                                    s->getEndSeqno(),
                                    s->getFlags(),
                                    s->getHighCompletedSeqno(),
                                    boost::none, /*max visible seqno */
                                    resp->getStreamId());
            break;
        }
        case DcpResponse::Event::SetVbucket:
        {
            SetVBucketState* s = static_cast<SetVBucketState*>(resp.get());
            ret = producers->set_vbucket_state(
                    s->getOpaque(), s->getVBucket(), s->getState());
            break;
        }
        case DcpResponse::Event::SystemEvent: {
            SystemEventProducerMessage* s =
                    static_cast<SystemEventProducerMessage*>(resp.get());
            ret = producers->system_event(
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
        default:
        {
            logger->warn(
                    "Unexpected dcp event ({}), "
                    "disconnecting",
                    resp->to_string());
            ret = ENGINE_DISCONNECT;
            break;
        }
    }

    if (ret == ENGINE_E2BIG) {
        rejectResp = std::move(resp);
    }

    if (ret == ENGINE_SUCCESS) {
        switch (resp->getEvent()) {
        case DcpResponse::Event::Abort:
        case DcpResponse::Event::Commit:
        case DcpResponse::Event::Deletion:
        case DcpResponse::Event::Expiration:
        case DcpResponse::Event::Mutation:
        case DcpResponse::Event::Prepare:
        case DcpResponse::Event::SystemEvent:
            itemsSent++;
            break;
        case DcpResponse::Event::AddStream:
        case DcpResponse::Event::SeqnoAcknowledgement:
        case DcpResponse::Event::SetVbucket:
        case DcpResponse::Event::SnapshotMarker:
        case DcpResponse::Event::StreamReq:
        case DcpResponse::Event::StreamEnd:
            break;
        }

        totalBytesSent.fetch_add(resp->getMessageSize());
    }

    lastSendTime = ep_current_time();
    return ret;
}

ENGINE_ERROR_CODE DcpProducer::bufferAcknowledgement(uint32_t opaque,
                                                     Vbid vbucket,
                                                     uint32_t buffer_bytes) {
    lastReceiveTime = ep_current_time();
    log.acknowledge(buffer_bytes);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpProducer::deletionV1OrV2(
        IncludeDeleteTime includeDeleteTime,
        MutationResponse& mutationResponse,
        dcp_message_producers* producers,
        std::unique_ptr<Item> itmCpy,
        ENGINE_ERROR_CODE ret,
        cb::mcbp::DcpStreamId sid) {
    if (includeDeleteTime == IncludeDeleteTime::Yes) {
        ret = producers->deletion_v2(
                mutationResponse.getOpaque(),
                toUniqueItemPtr(std::move(itmCpy)),
                mutationResponse.getVBucket(),
                *mutationResponse.getBySeqno(),
                mutationResponse.getRevSeqno(),
                mutationResponse.getItem()->getDeleteTime(),
                sid);
    } else {
        ret = producers->deletion(mutationResponse.getOpaque(),
                                  toUniqueItemPtr(std::move(itmCpy)),
                                  mutationResponse.getVBucket(),
                                  *mutationResponse.getBySeqno(),
                                  mutationResponse.getRevSeqno(),
                                  sid);
    }
    return ret;
}

ENGINE_ERROR_CODE DcpProducer::control(uint32_t opaque,
                                       cb::const_char_buffer key,
                                       cb::const_char_buffer value) {
    lastReceiveTime = ep_current_time();
    const char* param = key.data();
    std::string keyStr(key.data(), key.size());
    std::string valueStr(value.data(), value.size());

    if (strncmp(param, "connection_buffer_size", key.size()) == 0) {
        uint32_t size;
        if (parseUint32(valueStr.c_str(), &size)) {
            /* Size 0 implies the client (DCP consumer) does not support
               flow control */
            log.setBufferSize(size);
            return ENGINE_SUCCESS;
        }
    } else if (strncmp(param, "stream_buffer_size", key.size()) == 0) {
        logger->warn(
                "The ctrl parameter stream_buffer_size is"
                "not supported by this engine");
        return ENGINE_ENOTSUP;
    } else if (strncmp(param, "enable_noop", key.size()) == 0) {
        if (valueStr == "true") {
            noopCtx.enabled = true;
        } else {
            noopCtx.enabled = false;
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "enable_ext_metadata", key.size()) == 0) {
        if (valueStr == "true") {
            enableExtMetaData = true;
        } else {
            enableExtMetaData = false;
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "force_value_compression", key.size()) == 0) {
        if (!engine_.isDatatypeSupported(getCookie(),
                               PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
            engine_.setErrorContext(getCookie(), "The ctrl parameter "
                  "force_value_compression is only supported if datatype "
                  "snappy is enabled on the connection");
            return ENGINE_EINVAL;
        }
        if (valueStr == "true") {
            forceValueCompression = true;
        } else {
            forceValueCompression = false;
        }
        return ENGINE_SUCCESS;
        // vulcan onwards we accept two cursor_dropping control keys.
    } else if (keyStr == "supports_cursor_dropping_vulcan" ||
               keyStr == "supports_cursor_dropping") {
        if (valueStr == "true") {
            supportsCursorDropping = true;
        } else {
            supportsCursorDropping = false;
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "supports_hifi_MFU", key.size()) == 0) {
        consumerSupportsHifiMfu = (valueStr == "true");
        return ENGINE_SUCCESS;
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
                return ENGINE_SUCCESS;
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
                return ENGINE_EINVAL;
            }
        }
    } else if (strncmp(param, "set_priority", key.size()) == 0) {
        if (valueStr == "high") {
            engine_.setDCPPriority(getCookie(), CONN_PRIORITY_HIGH);
            return ENGINE_SUCCESS;
        } else if (valueStr == "medium") {
            engine_.setDCPPriority(getCookie(), CONN_PRIORITY_MED);
            return ENGINE_SUCCESS;
        } else if (valueStr == "low") {
            engine_.setDCPPriority(getCookie(), CONN_PRIORITY_LOW);
            return ENGINE_SUCCESS;
        }
    } else if (keyStr == "send_stream_end_on_client_close_stream") {
        if (valueStr == "true") {
            sendStreamEndOnClientStreamClose = true;
        }
        /* Do not want to give an option to the client to disable this.
           Default is disabled, client has only a choice to enable.
           This is a one time setting and there is no point giving the client an
           option to toggle it back mid way during the connection */
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "enable_expiry_opcode", key.size()) == 0) {
        if (valueStr == "true") {
            // Expiry opcode uses the same encoding as deleteV2 (includes
            // delete time); therefore a client enabling expiry_opcode also
            // implicitly enables includeDeletetime.
            includeDeleteTime = IncludeDeleteTime::Yes;
            enableExpiryOpcode = true;
        } else {
            enableExpiryOpcode = false;
        }
        return ENGINE_SUCCESS;
    } else if (keyStr == "enable_stream_id") {
        if (valueStr != "true") {
            // For simplicity, user cannot turn this off, it is by default off
            // and can only be enabled one-way per Producer.
            return ENGINE_EINVAL;
        }
        multipleStreamRequests = MultipleStreamRequests::Yes;
        return ENGINE_SUCCESS;
    } else if (key == "enable_sync_writes") {
        if (valueStr == "true") {
            supportsSyncReplication = SyncReplication::SyncWrites;
            if (!consumerName.empty()) {
                supportsSyncReplication = SyncReplication::SyncReplication;
            }
            return ENGINE_SUCCESS;
        }
    } else if (key == "consumer_name" && !valueStr.empty()) {
        consumerName = valueStr;
        if (supportsSyncReplication == SyncReplication::SyncWrites) {
            supportsSyncReplication = SyncReplication::SyncReplication;
        }
        return ENGINE_SUCCESS;
    }

    logger->warn("Invalid ctrl parameter '{}' for {}", valueStr, keyStr);

    return ENGINE_EINVAL;
}

ENGINE_ERROR_CODE DcpProducer::seqno_acknowledged(uint32_t opaque,
                                                  Vbid vbucket,
                                                  uint64_t prepared_seqno) {
    if (!isSyncReplicationEnabled()) {
        logger->warn(
                "({}) seqno_acknowledge failed because SyncReplication is"
                " not enabled on this Producer");
        return ENGINE_EINVAL;
    }

    if (consumerName.empty()) {
        logger->warn(
                "({}) seqno_acknowledge failed because this producer does"
                " not have an associated consumer name");
        return ENGINE_EINVAL;
    }

    VBucketPtr vb = engine_.getVBucket(vbucket);
    if (!vb) {
        logger->warn(
                "({}) seqno_acknowledge failed because this vbucket doesn't "
                "exist",
                vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    logger->debug("({}) seqno_acknowledged: prepared_seqno:{}",
                  vbucket,
                  prepared_seqno);

    // Confirm that we only receive ack seqnos we have sent
    auto rv = streams.find(vbucket.get());
    if (rv == streams.end()) {
        throw std::logic_error(
                "Replica acked seqno:" + std::to_string(prepared_seqno) +
                " for vbucket:" + to_string(vbucket) +
                " but we don't have a StreamContainer for that vb");
    }

    ActiveStream* stream = nullptr;
    for (auto itr = rv->second->rlock(); !itr.end(); itr.next()) {
        auto* s = itr.get().get();
        if (s->getOpaque() == opaque) {
            stream = dynamic_cast<ActiveStream*>(s);
            break;
        }
    }

    if (!stream) {
        // No stream found, may be the case that we have just ended our
        // stream and removed the stream from our map but the consumer is
        // not yet aware and we have received a seqno ack. Just return
        // success and ignore the ack.
        return ENGINE_SUCCESS;
    }

    return stream->seqnoAck(consumerName, prepared_seqno);
}

bool DcpProducer::handleResponse(const protocol_binary_response_header* resp) {
    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return false;
    }

    const auto opcode = resp->response.getClientOpcode();
    const auto responseStatus = resp->response.getStatus();
    if (opcode == cb::mcbp::ClientOpcode::DcpSetVbucketState ||
        opcode == cb::mcbp::ClientOpcode::DcpSnapshotMarker) {
        const auto opaque = resp->response.getOpaque();

        // Search for an active stream with the same opaque as the response.
        // Use find_if2 which will return the matching shared_ptr<Stream>
        auto stream = find_if2([opaque](const StreamsMap::value_type& s) {
            auto handle = s.second->rlock();
            for (; !handle.end(); handle.next()) {
                const auto& stream = handle.get();
                auto* as = dynamic_cast<ActiveStream*>(stream.get());
                if (as && opaque == stream->getOpaque()) {
                    return stream; // return matching shared_ptr<Stream>
                }
            }
            return ContainerElement{};
        });

        if (stream) {
            auto* as = static_cast<ActiveStream*>(stream.get());
            if (opcode == cb::mcbp::ClientOpcode::DcpSetVbucketState) {
                as->setVBucketStateAckRecieved();
            } else {
                as->snapshotMarkerAckReceived();
            }
        }

        return true;
    } else if (opcode == cb::mcbp::ClientOpcode::DcpMutation ||
               opcode == cb::mcbp::ClientOpcode::DcpDeletion ||
               opcode == cb::mcbp::ClientOpcode::DcpExpiration ||
               opcode == cb::mcbp::ClientOpcode::DcpStreamEnd ||
               opcode == cb::mcbp::ClientOpcode::DcpSystemEvent) {
        // The consumer could of closed the stream (DcpStreamEnd), enoent is
        // expected, but any other errors are not expected.
        if (responseStatus != cb::mcbp::Status::Success) {
            logger->warn(
                    "DcpProducer::handleResponse received unexpected "
                    "response:{}",
                    resp->response.toJSON(true).dump());
        }
        return true;
    } else if (opcode == cb::mcbp::ClientOpcode::DcpCommit ||
               opcode == cb::mcbp::ClientOpcode::DcpPrepare ||
               opcode == cb::mcbp::ClientOpcode::DcpAbort) {
        if (responseStatus == cb::mcbp::Status::KeyEnoent ||
            responseStatus == cb::mcbp::Status::Einval) {
            logger->error(
                    "Disconnecting. Received status {} for op:{} "
                    "response:{}",
                    to_string(responseStatus),
                    to_string(opcode),
                    resp->response.toJSON(true).dump());
            // KeyEnoent:
            // In this case we receive a KeyEnoent, we need to disconnect as we
            // must have sent an a commit or abort of key that the consumer is
            // unaware of and we should never see KeyEnoent from a DcpPrepare.
            // Einval:
            // If we have seen a Einval we also need to disconnect as we must
            // have sent an invalid. Mutation or packet to the consumer e.g. we
            // sent an abort to the consumer in a non disk snapshot without it
            // having seen a prepare.
            return false;
        } else {
            // Keep connection open for all other response codes
            if (responseStatus != cb::mcbp::Status::Success) {
                logger->warn(
                        "DcpProducer::handleResponse received unexpected "
                        "response:{}",
                        resp->response.toJSON(true).dump());
            }
            return true;
        }
    } else if (opcode == cb::mcbp::ClientOpcode::DcpNoop) {
        if (noopCtx.opaque == resp->response.getOpaque()) {
            noopCtx.pendingRecv = false;
            return true;
        }
    }

    logger->warn("Disconnecting. Trying to handle an unknown response:{}",
                 resp->response.toJSON(true).dump());

    return false;
}

std::pair<std::shared_ptr<Stream>, bool> DcpProducer::closeStreamInner(
        Vbid vbucket, cb::mcbp::DcpStreamId sid, bool eraseFromMapIfFound) {
    std::shared_ptr<Stream> stream;
    bool vbFound = false;

    auto rv = streams.find(vbucket.get());
    if (rv != streams.end()) {
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

ENGINE_ERROR_CODE DcpProducer::closeStream(uint32_t opaque,
                                           Vbid vbucket,
                                           cb::mcbp::DcpStreamId sid) {
    lastReceiveTime = ep_current_time();
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    if (!sid && multipleStreamRequests == MultipleStreamRequests::Yes) {
        logger->warn(
                "({}) closeStream request failed because a valid "
                "stream-ID is required.",
                vbucket);
        return ENGINE_DCP_STREAMID_INVALID;
    } else if (sid && multipleStreamRequests == MultipleStreamRequests::No) {
        logger->warn(
                "({}) closeStream request failed because a "
                "stream-ID:{} is present "
                "but not required.",
                vbucket,
                sid);
        return ENGINE_DCP_STREAMID_INVALID;
    }

    /* We should not remove the stream from the streams map if we have to
       send the "STREAM_END" response asynchronously to the consumer, so
       use the value of sendStreamEndOnClientStreamClose to determine if the
       stream should be removed if found*/
    auto rv = closeStreamInner(vbucket, sid, !sendStreamEndOnClientStreamClose);

    ENGINE_ERROR_CODE ret;
    if (!rv.first) {
        logger->warn(
                "({}) Cannot close stream because no "
                "stream exists for this vbucket {}",
                vbucket,
                sid);
        return sid && rv.second ? ENGINE_DCP_STREAMID_INVALID
                                : ENGINE_KEY_ENOENT;
    } else {
        if (!rv.first->isActive()) {
            logger->warn(
                    "({}) Cannot close stream because "
                    "stream is already marked as dead {}",
                    vbucket,
                    sid);
            ret = ENGINE_KEY_ENOENT;
        } else {
            rv.first->setDead(END_STREAM_CLOSED);
            ret = ENGINE_SUCCESS;
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
    backfillMgr->wakeUpTask();
}

bool DcpProducer::recordBackfillManagerBytesRead(size_t bytes, bool force) {
    if (force) {
        backfillMgr->bytesForceRead(bytes);
        return true;
    }
    return backfillMgr->bytesCheckAndRead(bytes);
}

void DcpProducer::recordBackfillManagerBytesSent(size_t bytes) {
    backfillMgr->bytesSent(bytes);
}

void DcpProducer::scheduleBackfillManager(VBucket& vb,
                                          std::shared_ptr<ActiveStream> s,
                                          uint64_t start,
                                          uint64_t end) {
    backfillMgr->schedule(vb, s, start, end);
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

    std::for_each(streams.begin(),
                  streams.end(),
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

    auto rv = streams.find(vb.getId().get());

    if (rv != streams.end()) {
        auto handle = rv->second->rlock();
        // Only perform takeover stats on singleton streams
        if (handle.size() == 1) {
            auto stream = handle.get();
            ActiveStream* as = nullptr;
            if ((as = dynamic_cast<ActiveStream*>(stream.get()))) {
                as->addTakeoverStats(add_stat, c, vb);
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

void DcpProducer::aggregateQueueStats(ConnCounter& aggregator) {
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

    auto rv = streams.find(vbucket.get());

    if (rv != streams.end()) {
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
        boost::optional<folly::SharedMutex::WriteHolder&> vbstateLock) {
    if (setStreamDeadStatus(vbucket, {}, END_STREAM_STATE, vbstateLock)) {
        logger->debug("({}) State changed to {}, closing active stream!",
                      vbucket,
                      VBucket::toString(state));
    }
}

void DcpProducer::closeStreamDueToRollback(Vbid vbucket) {
    if (setStreamDeadStatus(vbucket, {}, END_STREAM_ROLLBACK)) {
        logger->debug(
                "({}) Rollback occurred,"
                "closing stream (downstream must rollback too)",
                vbucket);
    }
}

bool DcpProducer::handleSlowStream(Vbid vbid, const CheckpointCursor* cursor) {
    if (supportsCursorDropping) {
        auto rv = streams.find(vbid.get());
        if (rv != streams.end()) {
            for (auto handle = rv->second->rlock(); !handle.end();
                 handle.next()) {
                if (handle.get()->getCursor().lock().get() == cursor) {
                    ActiveStream* as =
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
        cb::mcbp::DcpStreamId sid,
        end_stream_status_t status,
        boost::optional<folly::SharedMutex::WriteHolder&> vbstateLock) {
    auto rv = streams.find(vbid.get());
    if (rv != streams.end()) {
        std::shared_ptr<Stream> streamPtr;
        // MB-35073: holding StreamContainer rlock while calling setDead
        // has been seen to cause lock inversion elsewhere.
        // Collect sharedptr then setDead once lock is released (itr out of
        // scope).
        for (auto itr = rv->second->rlock(); !itr.end(); itr.next()) {
            if (itr.get()->compareStreamId(sid)) {
                streamPtr = itr.get();
                break;
            }
        }

        // MB-36637: At KVBucket::setVBucketState we acquire an exclusive lock
        // to vbstate and pass it down here. If that is the case, then we have
        // to avoid the call to ActiveStream::setDead(status) as it may deadlock
        // by acquiring the same lock again.
        auto* activeStream = dynamic_cast<ActiveStream*>(streamPtr.get());
        if (activeStream && vbstateLock) {
            activeStream->setDead(status, *vbstateLock);
        } else if (streamPtr) {
            streamPtr->setDead(status);
        }

        return true;
    }

    return false;
}

void DcpProducer::closeAllStreams() {
    lastReceiveTime = ep_current_time();
    std::vector<Vbid> vbvector;
    {
        std::for_each(streams.begin(),
                      streams.end(),
                      [&vbvector](StreamsMap::value_type& vt) {
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

                          for (auto streamPtr : streamPtrs) {
                              streamPtr->setDead(END_STREAM_DISCONNECTED);
                          }
                      });
    }
    for (const auto vbid: vbvector) {
        engine_.getDcpConnMap().removeVBConnByVBId(getCookie(), vbid);
    }

    // Destroy the backfillManager. (BackfillManager task also
    // may hold a weak reference to it while running, but that is
    // guaranteed to decay and free the BackfillManager once it
    // completes run().
    // This will terminate any tasks and delete any backfills
    // associated with this Producer.  This is necessary as if we
    // don't, then the RCPtr references which exist between
    // DcpProducer and ActiveStream result in us leaking DcpProducer
    // objects (and Couchstore vBucket files, via DCPBackfill task).
    backfillMgr.reset();
}

const char* DcpProducer::getType() const {
    if (notifyOnly) {
        return "notifier";
    } else {
        return "producer";
    }
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

            auto rv = streams.find(vbucket.get());
            if (rv == streams.end()) {
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
                        case DcpResponse::Event::SnapshotMarker:
                        case DcpResponse::Event::Mutation:
                        case DcpResponse::Event::Deletion:
                        case DcpResponse::Event::Expiration:
                        case DcpResponse::Event::Prepare:
                        case DcpResponse::Event::Commit:
                        case DcpResponse::Event::Abort:
                        case DcpResponse::Event::StreamEnd:
                        case DcpResponse::Event::SetVbucket:
                        case DcpResponse::Event::SystemEvent:
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
            streams.begin(), streams.end(), [](StreamsMap::value_type& vt) {
                std::vector<std::shared_ptr<Stream>> streamPtrs;
                // MB-35049: hold StreamContainer rlock while calling setDead
                // leads to lock inversion - so collect sharedptrs in one pass
                // then setDead once it is released (itr out of scope).
                for (auto itr = vt.second->rlock(); !itr.end(); itr.next()) {
                    streamPtrs.push_back(itr.get());
                }
                for (auto stream : streamPtrs) {
                    stream->setDead(END_STREAM_DISCONNECTED);
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

ENGINE_ERROR_CODE DcpProducer::maybeDisconnect() {
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
        return ENGINE_DISCONNECT;
    }
    // Returning ENGINE_FAILED means ignore and continue
    // without disconnecting
    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE DcpProducer::maybeSendNoop(
        struct dcp_message_producers* producers) {
    if (!noopCtx.enabled) {
        // Returning ENGINE_FAILED means ignore and continue
        // without sending a noop
        return ENGINE_FAILED;
    }
    std::chrono::seconds elapsedTime(ep_current_time() - noopCtx.sendTime);

    // Check to see if waiting for a noop reply.
    // If not try to send a noop to the consumer if the interval has passed
    if (!noopCtx.pendingRecv && elapsedTime >= noopCtx.dcpNoopTxInterval) {
        const auto ret = producers->noop(++noopCtx.opaque);

        if (ret == ENGINE_SUCCESS) {
            noopCtx.pendingRecv = true;
            noopCtx.sendTime = ep_current_time();
            lastSendTime = noopCtx.sendTime;
        }
        return ret;
    }

    // We have already sent a noop and are awaiting a receive or
    // the time interval has not passed.  In either case continue
    // without sending a noop.
    return ENGINE_FAILED;
}

void DcpProducer::clearQueues() {
    std::for_each(
            streams.begin(), streams.end(), [](StreamsMap::value_type& vt) {
                for (auto itr = vt.second->rlock(); !itr.end(); itr.next()) {
                    itr.get()->clear();
                }
            });
}

size_t DcpProducer::getItemsSent() {
    return itemsSent;
}

size_t DcpProducer::getItemsRemaining() {
    size_t remainingSize = 0;
    std::for_each(
            streams.begin(),
            streams.end(),
            [&remainingSize](const StreamsMap::value_type& vt) {
                for (auto itr = vt.second->rlock(); !itr.end(); itr.next()) {
                    auto* as = dynamic_cast<ActiveStream*>(itr.get().get());
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
    std::for_each(streams.begin(),
                  streams.end(),
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

std::shared_ptr<StreamContainer<std::shared_ptr<Stream>>>
DcpProducer::findStreams(Vbid vbid) {
    auto it = streams.find(vbid.get());
    if (it != streams.end()) {
        return it->second;
    }
    return nullptr;
}

bool DcpProducer::updateStreamsMap(Vbid vbid,
                                   cb::mcbp::DcpStreamId sid,
                                   std::shared_ptr<Stream>& stream) {
    auto found = streams.find(vbid.get());

    if (found != streams.end()) {
        // vbid is already mapped found.first is a shared_ptr<StreamContainer>
        if (found->second) {
            auto handle = found->second->wlock();
            for (; !handle.end(); handle.next()) {
                auto& sp = handle.get(); // get the shared_ptr<Stream>
                if (sp->compareStreamId(sid)) {
                    // Error if found and active
                    if (sp->isActive()) {
                        logger->warn(
                                "({}) Stream ({}) request failed"
                                " because a stream already exists for this "
                                "vbucket",
                                vbid,
                                sid.to_string());
                        throw cb::engine_error(
                                cb::engine_errc::key_already_exists,
                                "Stream already exists for " +
                                        vbid.to_string());
                    } else {
                        // Found a 'dead' stream, we can swap it
                        handle.swap(stream);
                        return false; // Do not update vb_conns_map
                    }
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
        streams.insert(std::make_pair(
                vbid.get(),
                std::make_shared<StreamContainer<ContainerElement>>(stream)));
    }
    return true; // Do update vb_conns_map
}

end_stream_status_t DcpProducer::mapEndStreamStatus(
        const void* cookie, end_stream_status_t status) const {
    if (status == END_STREAM_FILTER_EMPTY &&
        !engine_.isCollectionsSupported(cookie)) {
        return END_STREAM_OK;
    }
    return status;
}

std::string DcpProducer::getConsumerName() const {
    return consumerName;
}
