/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "passive_stream.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/consumer.h"
#include "dcp/response.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "replicationthrottle.h"

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <statistics/cbstat_collector.h>

#include <memory>

const std::string passiveStreamLoggingPrefix =
        "DCP (Consumer): **Deleted conn**";

PassiveStream::PassiveStream(EventuallyPersistentEngine* e,
                             std::shared_ptr<DcpConsumer> c,
                             const std::string& name,
                             uint32_t flags,
                             uint32_t opaque,
                             Vbid vb,
                             uint64_t st_seqno,
                             uint64_t en_seqno,
                             uint64_t vb_uuid,
                             uint64_t snap_start_seqno,
                             uint64_t snap_end_seqno,
                             uint64_t vb_high_seqno,
                             const Collections::ManifestUid vb_manifest_uid)
    : Stream(name,
             flags,
             opaque,
             vb,
             st_seqno,
             en_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno),
      engine(e),
      consumerPtr(c),
      last_seqno(vb_high_seqno),
      cur_snapshot_start(0),
      cur_snapshot_end(0),
      cur_snapshot_type(Snapshot::None),
      cur_snapshot_ack(false),
      cur_snapshot_prepare(false),
      vb_manifest_uid(vb_manifest_uid) {
    LockHolder lh(streamMutex);
    streamRequest_UNLOCKED(vb_uuid);
    itemsReady.store(true);
}

PassiveStream::~PassiveStream() {
    uint32_t unackedBytes = clearBuffer_UNLOCKED();
    if (state_ != StreamState::Dead) {
        // Destructed a "live" stream, log it.
        log(spdlog::level::level_enum::info,
            "({}) Destructing stream."
            " last_seqno is {}, unAckedBytes is {}.",
            vb_,
            last_seqno.load(),
            unackedBytes);
    }
}

void PassiveStream::streamRequest(uint64_t vb_uuid) {
    {
        std::unique_lock<std::mutex> lh(streamMutex);
        streamRequest_UNLOCKED(vb_uuid);
    }
    notifyStreamReady();
}

void PassiveStream::streamRequest_UNLOCKED(uint64_t vb_uuid) {
    auto stream_req_value = createStreamReqValue();

    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    pushToReadyQ(std::make_unique<StreamRequest>(vb_,
                                                 opaque_,
                                                 flags_,
                                                 start_seqno_,
                                                 end_seqno_,
                                                 start_seqno_ ? vb_uuid : 0,
                                                 snap_start_seqno_,
                                                 snap_end_seqno_,
                                                 stream_req_value));

    const char* type = (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER)
                               ? "takeover stream"
                               : "stream";

    log(spdlog::level::level_enum::info,
        "({}) Attempting to add {}: opaque_:{}, start_seqno_:{}, "
        "end_seqno_:{}, vb_uuid:{}, snap_start_seqno_:{}, snap_end_seqno_:{}, "
        "last_seqno:{}, stream_req_value:{}",
        vb_,
        type,
        opaque_,
        start_seqno_,
        end_seqno_,
        vb_uuid,
        snap_start_seqno_,
        snap_end_seqno_,
        last_seqno.load(),
        stream_req_value.empty() ? "none" : stream_req_value);
}

uint32_t PassiveStream::setDead(cb::mcbp::DcpStreamEndStatus status) {
    /* Hold buffer lock so that we clear out all items before we set the stream
       to dead state. We do not want to add any new message to the buffer or
       process any items in the buffer once we set the stream state to dead. */
    std::unique_lock<std::mutex> lg(buffer.bufMutex);
    uint32_t unackedBytes = clearBuffer_UNLOCKED();
    bool killed = false;

    LockHolder slh(streamMutex);
    if (transitionState(StreamState::Dead)) {
        killed = true;
    }

    if (killed) {
        auto severity = spdlog::level::level_enum::info;
        if (cb::mcbp::DcpStreamEndStatus::Disconnected == status) {
            severity = spdlog::level::level_enum::warn;
        }
        log(severity,
            "({}) Setting stream to dead state, last_seqno is {}, "
            "unAckedBytes is {}, status is {}",
            vb_,
            last_seqno.load(),
            unackedBytes,
            cb::mcbp::to_string(status));
    }
    return unackedBytes;
}

std::string PassiveStream::getStreamTypeName() const {
    return "Passive";
}

std::string PassiveStream::getStateName() const {
    return to_string(state_);
}

bool PassiveStream::isActive() const {
    return state_ != StreamState::Dead;
}

bool PassiveStream::isPending() const {
    return state_ == StreamState::Pending;
}

void PassiveStream::acceptStream(cb::mcbp::Status status, uint32_t add_opaque) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        log(spdlog::level::level_enum::warn,
            "({}) PassiveStream::acceptStream(): status:{} - Unable to find "
            "VBucket - cannot accept Stream.",
            vb_,
            status);
        return;
    }

    auto consumer = consumerPtr.lock();
    if (!consumer) {
        log(spdlog::level::level_enum::warn,
            "({}) PassiveStream::acceptStream(): status:{} - Unable to lock "
            "Consumer - cannot accept Stream.",
            vb_,
            status);
        return;
    }

    // SyncReplication: About to commence accepting data on this stream. Check
    // if the associated consumer supports SyncReplication, so we can later
    // correctly process Snapshot Markers.
    supportsSyncReplication = consumer->isSyncReplicationEnabled();

    // For SyncReplication streams lookup the highPreparedSeqno to check if
    // we need to re-ACK (after accepting the stream).
    const int64_t highPreparedSeqno =
            supportsSyncReplication ? vb->getHighPreparedSeqno() : 0;

    std::unique_lock<std::mutex> lh(streamMutex);
    if (isPending()) {
        pushToReadyQ(std::make_unique<AddStreamResponse>(
                add_opaque, opaque_, status));
        if (status == cb::mcbp::Status::Success) {
            // Before we receive/process anything else, send a seqno ack if we
            // are a stream for a pre-existing vBucket to ensure that the
            // replica can commit any in-flight SyncWrites if no further
            // SyncWrites are done and no disk snapshots processed by this
            // replica.
            if (highPreparedSeqno) {
                pushToReadyQ(std::make_unique<SeqnoAcknowledgement>(
                        opaque_, vb_, highPreparedSeqno));
            }
            transitionState(StreamState::Reading);
        } else {
            transitionState(StreamState::Dead);
        }
        lh.unlock();
        notifyStreamReady();
    }
}

void PassiveStream::reconnectStream(VBucketPtr& vb,
                                    uint32_t new_opaque,
                                    uint64_t start_seqno) {
    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    vb_uuid_ = start_seqno ? vb->failovers->getLatestEntry().vb_uuid : 0;

    snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
    if (info.range.getEnd() == info.start) {
        info.range.setStart(info.start);
    }

    auto stream_req_value = createStreamReqValue();

    {
        LockHolder lh(streamMutex);

        snap_start_seqno_ = info.range.getStart();
        start_seqno_ = info.start;
        snap_end_seqno_ = info.range.getEnd();
        last_seqno.store(start_seqno);

        log(spdlog::level::level_enum::info,
            "({}) Attempting to reconnect stream with opaque {}, start seq "
            "no {}, end seq no {}, snap start seqno {}, snap end seqno {}, and "
            "vb manifest uid {}",
            vb_,
            new_opaque,
            start_seqno,
            end_seqno_,
            snap_start_seqno_,
            snap_end_seqno_,
            stream_req_value.empty() ? "none" : stream_req_value);

        pushToReadyQ(std::make_unique<StreamRequest>(vb_,
                                                     new_opaque,
                                                     flags_,
                                                     start_seqno,
                                                     end_seqno_,
                                                     vb_uuid_,
                                                     snap_start_seqno_,
                                                     snap_end_seqno_,
                                                     stream_req_value));
    }
    notifyStreamReady();
}

ENGINE_ERROR_CODE PassiveStream::messageReceived(
        std::unique_ptr<DcpResponse> dcpResponse) {
    if (!dcpResponse) {
        return ENGINE_EINVAL;
    }

    if (!isActive()) {
        // If the Stream isn't active, *but* the object is still receiving
        // messages from the DcpConsumer that means the stream is still
        // registered in the streams map and hence we should ignore any
        // messages (until STREAM_END is received and the stream is removed form
        // the map).
        return ENGINE_SUCCESS;
    }

    auto seqno = dcpResponse->getBySeqno();
    if (seqno) {
        if (uint64_t(*seqno) <= last_seqno.load()) {
            log(spdlog::level::level_enum::warn,
                "({}) Erroneous (out of sequence) message ({}) received, "
                "with opaque: {}, its seqno ({}) is not "
                "greater than last received seqno ({}); "
                "Dropping mutation!",
                vb_,
                dcpResponse->to_string(),
                opaque_,
                *seqno,
                last_seqno.load());
            return ENGINE_ERANGE;
        }
    } else if (dcpResponse->getEvent() == DcpResponse::Event::SnapshotMarker) {
        auto s = static_cast<SnapshotMarker*>(dcpResponse.get());
        uint64_t snapStart = s->getStartSeqno();
        uint64_t snapEnd = s->getEndSeqno();
        if (snapStart < last_seqno.load() && snapEnd <= last_seqno.load()) {
            log(spdlog::level::level_enum::warn,
                "({}) Erroneous snapshot marker received, with "
                "opaque: {}, its start "
                "({}), and end ({}) are less than last "
                "received seqno ({}); Dropping marker!",
                vb_,
                opaque_,
                snapStart,
                snapEnd,
                last_seqno.load());
            return ENGINE_ERANGE;
        }
    }

    switch (engine->getReplicationThrottle().getStatus()) {
    case ReplicationThrottle::Status::Disconnect:
        log(spdlog::level::level_enum::warn,
            "{} Disconnecting the connection as there is "
            "no memory to complete replication",
            vb_);
        return ENGINE_DISCONNECT;
    case ReplicationThrottle::Status::Process:
        if (buffer.empty()) {
            /* Process the response here itself rather than buffering it */
            ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
            switch (dcpResponse->getEvent()) {
            case DcpResponse::Event::Mutation:
                ret = processMutation(static_cast<MutationConsumerMessage*>(
                        dcpResponse.get()));
                break;
            case DcpResponse::Event::Deletion:
                ret = processDeletion(static_cast<MutationConsumerMessage*>(
                        dcpResponse.get()));
                break;
            case DcpResponse::Event::Expiration:
                ret = processExpiration(static_cast<MutationConsumerMessage*>(
                        dcpResponse.get()));
                break;
            case DcpResponse::Event::Prepare:
                ret = processPrepare(static_cast<MutationConsumerMessage*>(
                        dcpResponse.get()));
                break;
            case DcpResponse::Event::Commit:
                ret = processCommit(
                        static_cast<CommitSyncWrite&>(*dcpResponse));
                break;
            case DcpResponse::Event::Abort:
                ret = processAbort(dynamic_cast<AbortSyncWrite&>(*dcpResponse));
                break;
            case DcpResponse::Event::SnapshotMarker:
                processMarker(static_cast<SnapshotMarker*>(dcpResponse.get()));
                break;
            case DcpResponse::Event::SetVbucket:
                processSetVBucketState(
                        static_cast<SetVBucketState*>(dcpResponse.get()));
                break;
            case DcpResponse::Event::StreamEnd: {
                LockHolder lh(streamMutex);
                transitionState(StreamState::Dead);
            } break;
            case DcpResponse::Event::SystemEvent: {
                ret = processSystemEvent(
                        *static_cast<SystemEventMessage*>(dcpResponse.get()));
                break;
            }
            case DcpResponse::Event::StreamReq:
            case DcpResponse::Event::AddStream:
            case DcpResponse::Event::SeqnoAcknowledgement:
            case DcpResponse::Event::OSOSnapshot:
            case DcpResponse::Event::SeqnoAdvanced:
                // These are invalid events for this path, they are handled by
                // the DcpConsumer class
                throw std::invalid_argument(
                        "PassiveStream::messageReceived invalid event type:" +
                        std::string(dcpResponse->to_string()));
            }

            if (ret == ENGINE_ENOMEM) {
                if (engine->getReplicationThrottle().doDisconnectOnNoMem()) {
                    log(spdlog::level::level_enum::warn,
                        "{} Disconnecting the connection as there is no "
                        "memory to complete replication; process dcp "
                        "event returned no memory",
                        vb_);
                    return ENGINE_DISCONNECT;
                }
            }
            if (ret == ENGINE_SUCCESS && seqno) {
                last_seqno.store(*seqno);
            }
            if (ret != ENGINE_TMPFAIL && ret != ENGINE_ENOMEM) {
                return ret;
            }
        }
        break;
    case ReplicationThrottle::Status::Pause:
        /* Do nothing specific here, we buffer item for this case and
           other cases below */
        break;
    }

    // Only buffer if the stream is not dead
    if (isActive()) {
        buffer.push(std::move(dcpResponse));
    }
    return ENGINE_TMPFAIL;
}

process_items_error_t PassiveStream::processBufferedMessages(
        uint32_t& processed_bytes, size_t batchSize) {
    std::unique_lock<std::mutex> lh(buffer.bufMutex);
    uint32_t count = 0;
    uint32_t message_bytes = 0;
    uint32_t total_bytes_processed = 0;
    bool failed = false, noMem = false;

    while (count < batchSize && !buffer.messages.empty()) {
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        /* If the stream is in dead state we should not process any remaining
           items in the buffer, we should rather clear them */
        if (!isActive()) {
            total_bytes_processed += clearBuffer_UNLOCKED();
            processed_bytes = total_bytes_processed;
            return all_processed;
        }

        // MB-31410: The front-end thread can process new incoming messages
        // only /after/ all the buffered ones have been processed.
        // So, here we get only a reference. We remove the message from the
        // buffer later, only /after/ we have processed it.
        // That is because the front-end thread checks if buffer.empty() for
        // deciding if it's time to start again processing new incoming
        // mutations. That happens in PassiveStream::messageReceived.
        std::unique_ptr<DcpResponse> response = std::move(buffer.front(lh));

        // Release bufMutex whilst we attempt to process the message
        // a lock inversion exists with connManager if we hold this.
        lh.unlock();

        // MB-31410: Only used for testing
        if (processBufferedMessages_postFront_Hook) {
            processBufferedMessages_postFront_Hook();
        }

        message_bytes = response->getMessageSize();
        auto seqno = response->getBySeqno();

        switch (response->getEvent()) {
        case DcpResponse::Event::Mutation:
            ret = processMutation(
                    static_cast<MutationConsumerMessage*>(response.get()));
            break;
        case DcpResponse::Event::Deletion:
            ret = processDeletion(
                    static_cast<MutationConsumerMessage*>(response.get()));
            break;
        case DcpResponse::Event::Expiration:
            ret = processExpiration(
                    static_cast<MutationConsumerMessage*>(response.get()));
            break;
        case DcpResponse::Event::Prepare:
            ret = processPrepare(
                    static_cast<MutationConsumerMessage*>(response.get()));
            break;
        case DcpResponse::Event::Commit:
            ret = processCommit(static_cast<CommitSyncWrite&>(*response));
            break;
        case DcpResponse::Event::Abort:
            ret = processAbort(dynamic_cast<AbortSyncWrite&>(*response));
            break;
        case DcpResponse::Event::SnapshotMarker:
            processMarker(static_cast<SnapshotMarker*>(response.get()));
            break;
        case DcpResponse::Event::SetVbucket:
            processSetVBucketState(
                    static_cast<SetVBucketState*>(response.get()));
            break;
        case DcpResponse::Event::StreamEnd: {
            LockHolder slh(streamMutex);
            transitionState(StreamState::Dead);
        } break;
        case DcpResponse::Event::SystemEvent: {
            ret = processSystemEvent(
                    *static_cast<SystemEventMessage*>(response.get()));
            break;
        }
        case DcpResponse::Event::StreamReq:
        case DcpResponse::Event::AddStream:
        case DcpResponse::Event::SeqnoAcknowledgement:
        case DcpResponse::Event::OSOSnapshot:
        case DcpResponse::Event::SeqnoAdvanced:
            // These are invalid events for this path, they are handled by the
            // DcpConsumer class
            throw std::invalid_argument(
                    "PassiveStream::processBufferedMessages invalid event "
                    "type:" +
                    std::string(response->to_string()));
        }

        if (ret == ENGINE_TMPFAIL || ret == ENGINE_ENOMEM) {
            failed = true;
            if (ret == ENGINE_ENOMEM) {
                noMem = true;
            }
        }

        // If we failed and the stream is not dead, just break the loop and
        // return. We will try again with processing the message at the next
        // run.
        // Note:
        //     1) no need to re-acquire bufMutex here
        //     2) we have not removed the unique_ptr from the buffer yet, but
        //        we must give the item back to the buffer queue
        if (failed && isActive()) {
            lh.lock();
            // isActive should be false if the queue was emptied, but check
            // anyway so we're more robust against any future code changes to
            // isActive and closeStream
            if (!buffer.messages.empty()) {
                buffer.front(lh) = std::move(response);
            }
            lh.unlock();
            break;
        }

        // At this point we have processed the message successfully,
        // then we can remove it from the buffer.
        // Note: we need to re-acquire bufMutex to update the buffer safely
        lh.lock();
        buffer.pop_front(lh, message_bytes);

        count++;
        if (ret != ENGINE_ERANGE) {
            total_bytes_processed += message_bytes;
        }
        if (ret == ENGINE_SUCCESS && seqno) {
            last_seqno.store(*seqno);
        }
    }

    processed_bytes = total_bytes_processed;

    if (failed) {
        if (noMem && engine->getReplicationThrottle().doDisconnectOnNoMem()) {
            log(spdlog::level::level_enum::warn,
                "{} Processor task indicating disconnection as "
                "there is no memory to complete replication; process dcp "
                "event returned no memory ",
                vb_);
            return stop_processing;
        }
        return cannot_process;
    }

    return all_processed;
}

ENGINE_ERROR_CODE PassiveStream::processMessage(
        MutationConsumerMessage* message, MessageType messageType) {
    std::array<std::string, 4> taskToString{
            {"mutation", "deletion", "expiration", "prepare"}};
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return ENGINE_DISCONNECT;
    }

    if (uint64_t(*message->getBySeqno()) < cur_snapshot_start.load() ||
        uint64_t(*message->getBySeqno()) > cur_snapshot_end.load()) {
        log(spdlog::level::level_enum::warn,
            "({}) Erroneous {} [sequence "
            "number does not fall in the expected snapshot range : "
            "{{snapshot_start ({}) <= seq_no ({}) <= "
            "snapshot_end ({})]; Dropping the {}!",
            vb_,
            taskToString[messageType],
            cur_snapshot_start.load(),
            *message->getBySeqno(),
            cur_snapshot_end.load(),
            taskToString[messageType]);
        return ENGINE_ERANGE;
    }

    switch (messageType) {
    case MessageType::Mutation:
        // Skip
        break;
    case MessageType::Deletion:
    case MessageType::Expiration:
        // The deleted value has a body, send it through the mutation path so we
        // set the deleted item with a value
        if (message->getItem()->getNBytes()) {
            return processMessage(message, MessageType::Mutation);
        }
        break;
    case MessageType::Prepare:
        // No extra processing.
        break;
    }

    // MB-17517: Check for the incoming item's CAS validity. We /shouldn't/
    // receive anything without a valid CAS, however given that versions without
    // this check may send us "bad" CAS values, we should regenerate them (which
    // is better than rejecting the data entirely).
    if (!Item::isValidCas(message->getItem()->getCas())) {
        log(spdlog::level::level_enum::warn,
            "Invalid CAS ({:#x}) received for {} {{{}, seqno:{}}}. "
            "Regenerating new CAS",
            message->getItem()->getCas(),
            taskToString[messageType],
            vb_,
            message->getItem()->getBySeqno());
        message->getItem()->setCas();
    }

    ENGINE_ERROR_CODE ret;
    DeleteSource deleteSource = DeleteSource::Explicit;
    bool switchComplete = false;
    switch (messageType) {
    case MessageType::Mutation:

        ret = engine->getKVBucket()->setWithMeta(*message->getItem(),
                                                 0,
                                                 nullptr,
                                                 consumer->getCookie(),
                                                 {vbucket_state_active,
                                                  vbucket_state_replica,
                                                  vbucket_state_pending},
                                                 CheckConflicts::No,
                                                 true,
                                                 GenerateBySeqno::No,
                                                 GenerateCas::No,
                                                 message->getExtMetaData());

        switchComplete = true;
        break;
    case MessageType::Prepare:
        ret = engine->getKVBucket()->prepare(*message->getItem(),
                                             consumer->getCookie());
        // If the the stream has received and successfully processed a pending
        // SyncWrite, then we have to flag that the Replica must notify the
        // DurabilityMonitor at snapshot-end received for the DM to move the
        // HighPreparedSeqno.
        if (ret == ENGINE_SUCCESS) {
            cur_snapshot_prepare.store(true);
        }

        switchComplete = true;
        break;
    case MessageType::Expiration:
        deleteSource = DeleteSource::TTL;
    // fallthrough with deleteSource updated
    case MessageType::Deletion:
        uint64_t delCas = 0;
        ItemMetaData meta = message->getItem()->getMetaData();
        ret = engine->getKVBucket()->deleteWithMeta(
                message->getItem()->getKey(),
                delCas,
                nullptr,
                message->getVBucket(),
                consumer->getCookie(),
                {vbucket_state_active,
                 vbucket_state_replica,
                 vbucket_state_pending},
                CheckConflicts::No,
                meta,
                GenerateBySeqno::No,
                GenerateCas::No,
                *message->getBySeqno(),
                message->getExtMetaData(),
                deleteSource);
        if (ret == ENGINE_KEY_ENOENT) {
            ret = ENGINE_SUCCESS;
        }
        switchComplete = true;
        break;
    }
    if (!switchComplete) {
        throw std::logic_error(
                std::string("PassiveStream::processMessage: "
                            "Message type not supported"));
    }
    if (ret != ENGINE_SUCCESS) {
        // ENOMEM logging is handled by maybeLogMemoryState
        if (ret != ENGINE_ENOMEM) {
            log(spdlog::level::level_enum::warn,
                "{} Got error '{}' while trying to process "
                "{} with seqno:{} cid:{}",
                vb_,
                cb::to_string(cb::to_engine_errc(ret)),
                taskToString[messageType],
                message->getItem()->getBySeqno(),
                message->getItem()->getKey().getCollectionID());
        }
    } else {
        handleSnapshotEnd(vb, *message->getBySeqno());
    }

    maybeLogMemoryState(cb::to_engine_errc(ret),
                        taskToString[messageType],
                        message->getItem()->getBySeqno());

    return ret;
}

ENGINE_ERROR_CODE PassiveStream::processMutation(
        MutationConsumerMessage* mutation) {
    return processMessage(mutation, MessageType::Mutation);
}

ENGINE_ERROR_CODE PassiveStream::processDeletion(
        MutationConsumerMessage* deletion) {
    return processMessage(deletion, MessageType::Deletion);
}

ENGINE_ERROR_CODE PassiveStream::processExpiration(
        MutationConsumerMessage* expiration) {
    return processMessage(expiration, MessageType::Expiration);
}

ENGINE_ERROR_CODE PassiveStream::processPrepare(
        MutationConsumerMessage* prepare) {
    auto result = processMessage(prepare, MessageType::Prepare);
    if (result == ENGINE_SUCCESS) {
        Expects(prepare->getItem()->getBySeqno() ==
                engine->getVBucket(vb_)->getHighSeqno())
    }
    return result;
}

void PassiveStream::seqnoAck(int64_t seqno) {
    // Only send a seqnoAck if we have an active stream that the producer has
    // responded with Success to the stream request
    if (!isActive() || isPending()) {
        log(spdlog::level::level_enum::warn,
            "{} Could not ack seqno {} because stream was in StreamState:{} "
            "Expected it to be in state {}",
            vb_,
            seqno,
            to_string(state_.load()),
            to_string(StreamState::Reading));
        return;
    }

    {
        LockHolder lh(streamMutex);
        if (!isActive()) {
            return;
        }

        pushToReadyQ(
                std::make_unique<SeqnoAcknowledgement>(opaque_, vb_, seqno));
    }
    notifyStreamReady();
}

std::string PassiveStream::to_string(StreamState st) {
    switch (st) {
    case StreamState::Pending:
        return "pending";
    case StreamState::Reading:
        return "reading";
    case StreamState::Dead:
        return "dead";
    }
    throw std::invalid_argument("PassiveStream::to_string(StreamState): " +
                                std::to_string(int(st)));
}

ENGINE_ERROR_CODE PassiveStream::processCommit(const CommitSyncWrite& commit) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto rv = vb->commit(commit.getKey(),
                         commit.getPreparedSeqno(),
                         *commit.getBySeqno(),
                         vb->lockCollections(commit.getKey()));
    if (rv != ENGINE_SUCCESS) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processCommit: {} Got error '{}' while trying to "
            "process commit",
            vb_,
            cb::to_string(cb::to_engine_errc(rv)));
    } else {
        handleSnapshotEnd(vb, *commit.getBySeqno());
    }

    return rv;
}

ENGINE_ERROR_CODE PassiveStream::processAbort(const AbortSyncWrite& abort) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto rv = vb->abort(abort.getKey(),
                        abort.getPreparedSeqno(),
                        abort.getAbortSeqno(),
                        vb->lockCollections(abort.getKey()));

    if (rv != ENGINE_SUCCESS) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processAbort: {} Got error '{}' while trying to "
            "process abort",
            vb_,
            cb::to_string(cb::to_engine_errc(rv)));
    } else {
        handleSnapshotEnd(vb, *abort.getBySeqno());
    }

    return rv;
}

ENGINE_ERROR_CODE PassiveStream::processSystemEvent(
        const SystemEventMessage& event) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    // Depending on the event, extras is different and key may even be empty
    // The specific handler will know how to interpret.
    switch (event.getSystemEvent()) {
    case mcbp::systemevent::id::CreateCollection: {
        rv = processCreateCollection(*vb, CreateCollectionEvent(event));
        break;
    }
    case mcbp::systemevent::id::DeleteCollection: {
        rv = processDropCollection(*vb, DropCollectionEvent(event));
        break;
    }
    case mcbp::systemevent::id::CreateScope: {
        rv = processCreateScope(*vb, CreateScopeEvent(event));
        break;
    }
    case mcbp::systemevent::id::DropScope: {
        rv = processDropScope(*vb, DropScopeEvent(event));
        break;
    }
    default: {
        rv = ENGINE_EINVAL;
        break;
    }
    }

    if (rv != ENGINE_SUCCESS) {
        log(spdlog::level::level_enum::warn,
            "{} Got error '{}' while trying to process "
            "system event",
            vb_,
            cb::to_string(cb::to_engine_errc(rv)));
    } else {
        handleSnapshotEnd(vb, *event.getBySeqno());
    }

    return rv;
}

ENGINE_ERROR_CODE PassiveStream::processCreateCollection(
        VBucket& vb, const CreateCollectionEvent& event) {
    try {
        vb.replicaCreateCollection(
                event.getManifestUid(),
                {event.getScopeID(), event.getCollectionID()},
                event.getKey(),
                event.getMaxTtl(),
                event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processCreateCollection {} exception {}",
            vb.getId(),
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE PassiveStream::processDropCollection(
        VBucket& vb, const DropCollectionEvent& event) {
    try {
        vb.replicaDropCollection(event.getManifestUid(),
                                 event.getCollectionID(),
                                 event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processDropCollection {} exception {}",
            vb.getId(),
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE PassiveStream::processCreateScope(
        VBucket& vb, const CreateScopeEvent& event) {
    try {
        vb.replicaCreateScope(event.getManifestUid(),
                              event.getScopeID(),
                              event.getKey(),
                              event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processCreateScope {} exception {}",
            vb.getId(),
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE PassiveStream::processDropScope(VBucket& vb,
                                                  const DropScopeEvent& event) {
    try {
        vb.replicaDropScope(
                event.getManifestUid(), event.getScopeID(), event.getBySeqno());
    } catch (std::exception& e) {
        log(spdlog::level::level_enum::warn,
            "PassiveStream::processDropScope {} exception {}",
            vb.getId(),
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    VBucketPtr vb = engine->getVBucket(vb_);

    cur_snapshot_start.store(marker->getStartSeqno());
    cur_snapshot_end.store(marker->getEndSeqno());
    const auto prevSnapType = cur_snapshot_type.load();
    cur_snapshot_type.store((marker->getFlags() & MARKER_FLAG_DISK)
                                    ? Snapshot::Disk
                                    : Snapshot::Memory);

    if (vb) {
        auto checkpointType = marker->getFlags() & MARKER_FLAG_DISK
                                      ? CheckpointType::Disk
                                      : CheckpointType::Memory;

        auto& ckptMgr = *vb->checkpointManager;

        std::optional<uint64_t> hcs = marker->getHighCompletedSeqno();
        if ((marker->getFlags() & MARKER_FLAG_DISK) &&
            !supportsSyncReplication) {
            // If this stream doesn't support SyncReplication (i.e. the producer
            // is a pre-MadHatter version) then we should consider the HCS to be
            // present but zero for disk snapshot (not possible for any
            // SyncWrites to have completed yet). If SyncReplication is
            // supported then use the value from the marker.
            hcs = 0;
        }

        if (marker->getFlags() & MARKER_FLAG_DISK) {
            // A replica could receive a duplicate DCP prepare during a disk
            // snapshot if it had previously received an uncompleted prepare.
            // We can receive a disk snapshot when we either:
            //     a) First connect
            //     b) Get cursor dropped by the active
            //
            // We selectively allow these prepares to overwrite the old one by
            // setting a duplicate prepare window in the vBucket. This will
            // allow any currently outstanding prepares to be overwritten, but
            // not any new ones.
            vb->setDuplicatePrepareWindow();
        }

        // We could be connected to a non sync-repl, so if the max-visible is
        // not transmitted (optional is false), set visible to snap-end
        auto visibleSeq =
                marker->getMaxVisibleSeqno().value_or(marker->getEndSeqno());

        if (marker->getFlags() & MARKER_FLAG_DISK && vb->getHighSeqno() == 0) {
            // Case: receiving the first snapshot in a Disk snapshot.
            // Note that replica may never execute here as the active may switch
            // directly to in-memory and send the first snapshot in a Memory
            // snapshot.

            vb->setReceivingInitialDiskSnapshot(true);
            ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                   cur_snapshot_end.load(),
                                   hcs,
                                   checkpointType,
                                   visibleSeq);
        } else {
            // Case: receiving any type of snapshot (Disk/Memory).

            // @todo: The check on CheckpointId=0 is legacy from when that could
            //   really happen (TAP). Currently CheckpointId is a positive
            //   monotonic sequence starting from 1, so the check can be
            //   removed. Deferring to a dedicated change.
            if (marker->getFlags() & MARKER_FLAG_CHK ||
                vb->checkpointManager->getOpenCheckpointId() == 0) {
                ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                       cur_snapshot_end.load(),
                                       hcs,
                                       checkpointType,
                                       visibleSeq);
            } else {
                // MB-42780: In general we cannot merge multiple snapshots into
                // the same checkpoint. The only exception is for when replica
                // receives multiple Memory checkpoints in a row.
                // Since 6.5.0 the Active behaves correctly with regard to that
                // (ie, the Active always sets the MARKER_FLAG_CHK in a snapshot
                // transition tha involves Disk snapshots), but older Producers
                // may still miss the MARKER_FLAG_CHK.
                if (prevSnapType == Snapshot::Memory &&
                    cur_snapshot_type == Snapshot::Memory) {
                    ckptMgr.extendOpenCheckpoint(cur_snapshot_end.load(),
                                                 visibleSeq);
                } else {
                    ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                           cur_snapshot_end.load(),
                                           hcs,
                                           checkpointType,
                                           visibleSeq);
                }
            }
        }

        if (marker->getFlags() & MARKER_FLAG_ACK) {
            cur_snapshot_ack = true;
        }
    }
}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getKVBucket()->setVBucketState(
            vb_, state->getState(), {}, TransferVB::Yes);
    {
        LockHolder lh(streamMutex);
        pushToReadyQ(std::make_unique<SetVBucketStateResponse>(
                opaque_, cb::mcbp::Status::Success));
    }
    notifyStreamReady();
}

void PassiveStream::handleSnapshotEnd(VBucketPtr& vb, uint64_t byseqno) {
    if (byseqno == cur_snapshot_end.load()) {
        if (cur_snapshot_type.load() == Snapshot::Disk) {
            vb->setReceivingInitialDiskSnapshot(false);
        }

        if (cur_snapshot_ack) {
            {
                LockHolder lh(streamMutex);
                pushToReadyQ(std::make_unique<SnapshotMarkerResponse>(
                        opaque_, cb::mcbp::Status::Success));
            }
            notifyStreamReady();
            cur_snapshot_ack = false;
        }

        // Notify the PassiveDM that the snapshot-end mutation has been
        // received on PassiveStream, if the snapshot contains at least one
        // Prepare. That is necessary for unblocking the High Prepared Seqno
        // in PassiveDM. Note that the HPS is what the PassiveDM acks back to
        // the Active. See comments in PassiveDM for details.

        // Disk snapshots are subject to deduplication, and may be missing
        // purged aborts. We must notify the PDM even if we have not seen a
        // prepare, to account for possible unseen prepares.
        if (cur_snapshot_prepare ||
            cur_snapshot_type.load() == Snapshot::Disk) {
            vb->notifyPassiveDMOfSnapEndReceived(byseqno);
            cur_snapshot_prepare.store(false);
        }
    }
}

void PassiveStream::addStats(const AddStatFn& add_stat, const void* c) {
    Stream::addStats(add_stat, c);

    try {
        std::array<char, 1024> buf;
        size_t bufferItems = 0;
        size_t bufferBytes = 0;
        {
            std::lock_guard<std::mutex> lg(buffer.bufMutex);
            bufferItems = buffer.messages.size();
            bufferBytes = buffer.bytes;
        }
        checked_snprintf(buf.data(),
                         buf.size(),
                         "%s:stream_%d_buffer_items",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buf.data(), bufferItems, add_stat, c);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "%s:stream_%d_buffer_bytes",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buf.data(), bufferBytes, add_stat, c);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "%s:stream_%d_last_received_seqno",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buf.data(), last_seqno.load(), add_stat, c);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "%s:stream_%d_ready_queue_memory",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buf.data(), getReadyQueueMemory(), add_stat, c);

        checked_snprintf(buf.data(),
                         buf.size(),
                         "%s:stream_%d_cur_snapshot_type",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(
                buf.data(), ::to_string(cur_snapshot_type.load()), add_stat, c);

        if (cur_snapshot_type.load() != Snapshot::None) {
            checked_snprintf(buf.data(),
                             buf.size(),
                             "%s:stream_%d_cur_snapshot_start",
                             name_.c_str(),
                             vb_.get());
            add_casted_stat(buf.data(), cur_snapshot_start.load(), add_stat, c);
            checked_snprintf(buf.data(),
                             buf.size(),
                             "%s:stream_%d_cur_snapshot_end",
                             name_.c_str(),
                             vb_.get());
            add_casted_stat(buf.data(), cur_snapshot_end.load(), add_stat, c);
        }

        checked_snprintf(buf.data(),
                         buf.size(),
                         "%s:stream_%d_cur_snapshot_prepare",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buf.data(), cur_snapshot_prepare.load(), add_stat, c);

        auto stream_req_value = createStreamReqValue();

        if (!stream_req_value.empty()) {
            checked_snprintf(buf.data(),
                             buf.size(),
                             "%s:stream_%d_vb_manifest_uid",
                             name_.c_str(),
                             vb_.get());
            add_casted_stat(buf.data(), stream_req_value.c_str(), add_stat, c);
        }

    } catch (std::exception& error) {
        EP_LOG_WARN("PassiveStream::addStats: Failed to build stats: {}",
                    error.what());
    }
}

std::unique_ptr<DcpResponse> PassiveStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return nullptr;
    }

    return popFromReadyQ();
}

uint32_t PassiveStream::clearBuffer_UNLOCKED() {
    uint32_t unackedBytes = buffer.bytes;
    buffer.messages.clear();
    buffer.bytes = 0;
    return unackedBytes;
}

bool PassiveStream::transitionState(StreamState newState) {
    log(spdlog::level::level_enum::debug,
        "PassiveStream::transitionState: ({}) "
        "Transitioning from {} to {}",
        vb_,
        to_string(state_.load()),
        to_string(newState));

    if (state_ == newState) {
        return false;
    }

    bool validTransition = false;
    switch (state_.load()) {
    case StreamState::Pending:
        if (newState == StreamState::Reading || newState == StreamState::Dead) {
            validTransition = true;
        }
        break;
    case StreamState::Reading:
        if (newState == StreamState::Dead) {
            validTransition = true;
        }
        break;

    case StreamState::Dead:
        // Once 'dead' shouldn't transition away from it.
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "PassiveStream::transitionState:"
                " newState (which is" +
                to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state_.load()) + ")");
    }

    state_ = newState;
    return true;
}

void PassiveStream::notifyStreamReady() {
    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return;
    }

    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        consumer->notifyStreamReady(vb_);
    }
}

const std::string PassiveStream::createStreamReqValue() const {
    nlohmann::json stream_req_json;
    std::ostringstream ostr;
    ostr << std::hex << static_cast<uint64_t>(vb_manifest_uid);
    stream_req_json["uid"] = ostr.str();
    return stream_req_json.dump();
}

template <typename... Args>
void PassiveStream::log(spdlog::level::level_enum severity,
                        const char* fmt,
                        Args... args) const {
    auto consumer = consumerPtr.lock();
    if (consumer) {
        consumer->getLogger().log(severity, fmt, args...);
    } else {
        if (globalBucketLogger->should_log(severity)) {
            globalBucketLogger->log(
                    severity,
                    std::string{passiveStreamLoggingPrefix}.append(fmt).data(),
                    args...);
        }
    }
}

void PassiveStream::closeIfRequiredPrivilegesLost(const void* cookie) {
    throw std::logic_error(
            "Unexpected call to PassiveStream::closeIfRequiredPrivilegesLost");
}

void PassiveStream::maybeLogMemoryState(cb::engine_errc status,
                                        const std::string& msgType,
                                        int64_t seqno) {
    bool previousNoMem = isNoMemory.load();
    if (status == cb::engine_errc::no_memory && !previousNoMem) {
        log(spdlog::level::level_enum::warn,
            "{} Got error '{}' while trying to process "
            "{} with seqno:{}",
            vb_,
            cb::to_string(status),
            msgType,
            seqno);
        isNoMemory.store(true);
    } else if (status == cb::engine_errc::success && previousNoMem) {
        log(spdlog::level::level_enum::info,
            "{} PassiveStream resuming after no-memory backoff",
            vb_);
        isNoMemory.store(false);
    }
}

PassiveStream::Buffer::Buffer() : bytes(0) {
}

PassiveStream::Buffer::~Buffer() = default;

bool PassiveStream::Buffer::empty() const {
    LockHolder lh(bufMutex);
    return messages.empty();
}

void PassiveStream::Buffer::push(std::unique_ptr<DcpResponse> message) {
    std::lock_guard<std::mutex> lg(bufMutex);
    bytes += message->getMessageSize();
    messages.push_back(std::move(message));
}

void PassiveStream::Buffer::pop_front(std::unique_lock<std::mutex>& lh,
                                      size_t bytesPopped) {
    if (messages.empty()) {
        return;
    }
    messages.pop_front();
    bytes -= bytesPopped;
}

std::unique_ptr<DcpResponse>& PassiveStream::Buffer::front(
        std::unique_lock<std::mutex>& lh) {
    return messages.front();
}
