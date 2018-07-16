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

#include "checkpoint_manager.h"
#include "dcp/consumer.h"
#include "dcp/response.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "replicationthrottle.h"

#include <memory>

PassiveStream::PassiveStream(EventuallyPersistentEngine* e,
                             std::shared_ptr<DcpConsumer> c,
                             const std::string& name,
                             uint32_t flags,
                             uint32_t opaque,
                             uint16_t vb,
                             uint64_t st_seqno,
                             uint64_t en_seqno,
                             uint64_t vb_uuid,
                             uint64_t snap_start_seqno,
                             uint64_t snap_end_seqno,
                             uint64_t vb_high_seqno)
    : Stream(name,
             flags,
             opaque,
             vb,
             st_seqno,
             en_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno,
             Type::Passive),
      engine(e),
      consumerPtr(c),
      last_seqno(vb_high_seqno),
      cur_snapshot_start(0),
      cur_snapshot_end(0),
      cur_snapshot_type(Snapshot::None),
      cur_snapshot_ack(false) {
    LockHolder lh(streamMutex);
    streamRequest_UNLOCKED(vb_uuid);
    itemsReady.store(true);
}

PassiveStream::~PassiveStream() {
    uint32_t unackedBytes = clearBuffer_UNLOCKED();
    if (state_ != StreamState::Dead) {
        // Destructed a "live" stream, log it.
        log(EXTENSION_LOG_NOTICE,
            "(vb %" PRId16
            ") Destructing stream."
            " last_seqno is %" PRIu64 ", unAckedBytes is %" PRIu32 ".",
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
    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    pushToReadyQ(std::make_unique<StreamRequest>(vb_,
                                                 opaque_,
                                                 flags_,
                                                 start_seqno_,
                                                 end_seqno_,
                                                 start_seqno_ ? vb_uuid : 0,
                                                 snap_start_seqno_,
                                                 snap_end_seqno_));

    const char* type = (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER)
                               ? "takeover stream"
                               : "stream";
    log(EXTENSION_LOG_NOTICE,
        "(vb %" PRId16 ") Attempting to add %s: opaque_:%" PRIu32
        ", "
        "start_seqno_:%" PRIu64 ", end_seqno_:%" PRIu64
        ", "
        "vb_uuid:%" PRIu64 ", snap_start_seqno_:%" PRIu64
        ", "
        "snap_end_seqno_:%" PRIu64 ", last_seqno:%" PRIu64,
        vb_,
        type,
        opaque_,
        start_seqno_,
        end_seqno_,
        vb_uuid,
        snap_start_seqno_,
        snap_end_seqno_,
        last_seqno.load());
}

uint32_t PassiveStream::setDead(end_stream_status_t status) {
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
        EXTENSION_LOG_LEVEL logLevel = EXTENSION_LOG_NOTICE;
        if (END_STREAM_DISCONNECTED == status) {
            logLevel = EXTENSION_LOG_WARNING;
        }
        log(logLevel,
            "(vb %" PRId16
            ") Setting stream to dead state, last_seqno is %" PRIu64
            ", unAckedBytes is %" PRIu32 ", status is %s",
            vb_,
            last_seqno.load(),
            unackedBytes,
            getEndStreamStatusStr(status).c_str());
    }
    return unackedBytes;
}

void PassiveStream::acceptStream(uint16_t status, uint32_t add_opaque) {
    std::unique_lock<std::mutex> lh(streamMutex);
    if (isPending()) {
        if (status == ENGINE_SUCCESS) {
            transitionState(StreamState::Reading);
        } else {
            transitionState(StreamState::Dead);
        }
        pushToReadyQ(std::make_unique<AddStreamResponse>(
                add_opaque, opaque_, status));
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
    if (info.range.end == info.start) {
        info.range.start = info.start;
    }

    snap_start_seqno_ = info.range.start;
    start_seqno_ = info.start;
    snap_end_seqno_ = info.range.end;

    log(EXTENSION_LOG_NOTICE,
        "(vb %d) Attempting to reconnect stream with opaque %" PRIu32
        ", start seq no %" PRIu64 ", end seq no %" PRIu64
        ", snap start seqno %" PRIu64 ", and snap end seqno %" PRIu64,
        vb_,
        new_opaque,
        start_seqno,
        end_seqno_,
        snap_start_seqno_,
        snap_end_seqno_);
    {
        LockHolder lh(streamMutex);
        last_seqno.store(start_seqno);
        pushToReadyQ(std::make_unique<StreamRequest>(vb_,
                                                     new_opaque,
                                                     flags_,
                                                     start_seqno,
                                                     end_seqno_,
                                                     vb_uuid_,
                                                     snap_start_seqno_,
                                                     snap_end_seqno_));
    }
    notifyStreamReady();
}

ENGINE_ERROR_CODE PassiveStream::messageReceived(
        std::unique_ptr<DcpResponse> dcpResponse) {
    if (!dcpResponse) {
        return ENGINE_EINVAL;
    }

    if (!isActive()) {
        return ENGINE_KEY_ENOENT;
    }

    auto seqno = dcpResponse->getBySeqno();
    if (seqno) {
        if (uint64_t(*seqno) <= last_seqno.load()) {
            log(EXTENSION_LOG_WARNING,
                "(vb %d) Erroneous (out of sequence) message (%s) received, "
                "with opaque: %" PRIu32 ", its seqno (%" PRIu64
                ") is not "
                "greater than last received seqno (%" PRIu64
                "); "
                "Dropping mutation!",
                vb_,
                dcpResponse->to_string(),
                opaque_,
                *seqno,
                last_seqno.load());
            return ENGINE_ERANGE;
        }
        last_seqno.store(*seqno);
    } else if (dcpResponse->getEvent() == DcpResponse::Event::SnapshotMarker) {
        auto s = static_cast<SnapshotMarker*>(dcpResponse.get());
        uint64_t snapStart = s->getStartSeqno();
        uint64_t snapEnd = s->getEndSeqno();
        if (snapStart < last_seqno.load() && snapEnd <= last_seqno.load()) {
            log(EXTENSION_LOG_WARNING,
                "(vb %d) Erroneous snapshot marker received, with "
                "opaque: %" PRIu32
                ", its start "
                "(%" PRIu64 "), and end (%" PRIu64
                ") are less than last "
                "received seqno (%" PRIu64 "); Dropping marker!",
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
        log(EXTENSION_LOG_WARNING,
            "vb:%" PRIu16
            " Disconnecting the connection as there is "
            "no memory to complete replication",
            vb_);
        return ENGINE_DISCONNECT;
    case ReplicationThrottle::Status::Process:
        if (buffer.empty()) {
            /* Process the response here itself rather than buffering it */
            ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
            switch (dcpResponse->getEvent()) {
            case DcpResponse::Event::Mutation:
                ret = processMutation(
                        static_cast<MutationResponse*>(dcpResponse.get()));
                break;
            case DcpResponse::Event::Deletion:
            case DcpResponse::Event::Expiration:
                ret = processDeletion(
                        static_cast<MutationResponse*>(dcpResponse.get()));
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
            default:
                log(EXTENSION_LOG_WARNING,
                    "(vb %d) Unknown event:%d, opaque:%" PRIu32,
                    vb_,
                    int(dcpResponse->getEvent()),
                    opaque_);
                return ENGINE_DISCONNECT;
            }

            if (ret == ENGINE_ENOMEM) {
                if (engine->getReplicationThrottle().doDisconnectOnNoMem()) {
                    log(EXTENSION_LOG_WARNING,
                        "vb:%" PRIu16
                        " Disconnecting the connection as there is no "
                        "memory to complete replication; process dcp "
                        "event returned no memory",
                        vb_);
                    return ENGINE_DISCONNECT;
                }
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

        std::unique_ptr<DcpResponse> response = buffer.pop_front(lh);

        // Release bufMutex whilst we attempt to process the message
        // a lock inversion exists with connManager if we hold this.
        lh.unlock();

        message_bytes = response->getMessageSize();

        switch (response->getEvent()) {
        case DcpResponse::Event::Mutation:
            ret = processMutation(
                    static_cast<MutationResponse*>(response.get()));
            break;
        case DcpResponse::Event::Deletion:
        case DcpResponse::Event::Expiration:
            ret = processDeletion(
                    static_cast<MutationResponse*>(response.get()));
            break;
        case DcpResponse::Event::SnapshotMarker:
            processMarker(static_cast<SnapshotMarker*>(response.get()));
            break;
        case DcpResponse::Event::SetVbucket:
            processSetVBucketState(
                    static_cast<SetVBucketState*>(response.get()));
            break;
        case DcpResponse::Event::StreamEnd: {
            LockHolder lh(streamMutex);
            transitionState(StreamState::Dead);
        } break;
        case DcpResponse::Event::SystemEvent: {
            ret = processSystemEvent(
                    *static_cast<SystemEventMessage*>(response.get()));
            break;
        }
        default:
            log(EXTENSION_LOG_WARNING,
                "PassiveStream::processBufferedMessages:"
                "(vb %" PRIu16
                ") PassiveStream ignoring "
                "unknown message type %s",
                vb_,
                response->to_string());
            continue;
        }

        if (ret == ENGINE_TMPFAIL || ret == ENGINE_ENOMEM) {
            failed = true;
            if (ret == ENGINE_ENOMEM) {
                noMem = true;
            }
        }

        // Re-acquire bufMutex so that
        // 1) we can update the buffer
        // 2) safely re-check the while conditional statement
        lh.lock();

        // If we failed and the stream is not dead, stash the DcpResponse at the
        // front of the queue and break the loop.
        if (failed && isActive()) {
            buffer.push_front(std::move(response), lh);
            break;
        }

        count++;
        if (ret != ENGINE_ERANGE) {
            total_bytes_processed += message_bytes;
        }
    }

    processed_bytes = total_bytes_processed;

    if (failed) {
        if (noMem && engine->getReplicationThrottle().doDisconnectOnNoMem()) {
            log(EXTENSION_LOG_WARNING,
                "vb:%" PRIu16
                " Processor task indicating disconnection as "
                "there is no memory to complete replication; process dcp "
                "event returned no memory ",
                vb_);
            return stop_processing;
        }
        return cannot_process;
    }

    return all_processed;
}

ENGINE_ERROR_CODE PassiveStream::processMutation(MutationResponse* mutation) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return ENGINE_DISCONNECT;
    }

    if (uint64_t(*mutation->getBySeqno()) < cur_snapshot_start.load() ||
        uint64_t(*mutation->getBySeqno()) > cur_snapshot_end.load()) {
        log(EXTENSION_LOG_WARNING,
            "(vb %d) Erroneous mutation [sequence "
            "number does not fall in the expected snapshot range : "
            "{snapshot_start (%" PRIu64 ") <= seq_no (%" PRIu64
            ") <= "
            "snapshot_end (%" PRIu64 ")]; Dropping the mutation!",
            vb_,
            cur_snapshot_start.load(),
            *mutation->getBySeqno(),
            cur_snapshot_end.load());
        return ENGINE_ERANGE;
    }

    // MB-17517: Check for the incoming item's CAS validity. We /shouldn't/
    // receive anything without a valid CAS, however given that versions without
    // this check may send us "bad" CAS values, we should regenerate them (which
    // is better than rejecting the data entirely).
    if (!Item::isValidCas(mutation->getItem()->getCas())) {
        log(EXTENSION_LOG_WARNING,
            "Invalid CAS (0x%" PRIx64 ") received for mutation {vb:%" PRIu16
            ", seqno:%" PRId64 "}. Regenerating new CAS",
            mutation->getItem()->getCas(),
            vb_,
            mutation->getItem()->getBySeqno());
        mutation->getItem()->setCas();
    }

    ENGINE_ERROR_CODE ret;
    if (vb->isBackfillPhase()) {
        ret = engine->getKVBucket()->addBackfillItem(
                *mutation->getItem(),
                GenerateBySeqno::No,
                mutation->getExtMetaData());
    } else {
        ret = engine->getKVBucket()->setWithMeta(*mutation->getItem(),
                                                 0,
                                                 NULL,
                                                 consumer->getCookie(),
                                                 {vbucket_state_active,
                                                  vbucket_state_replica,
                                                  vbucket_state_pending},
                                                 CheckConflicts::No,
                                                 true,
                                                 GenerateBySeqno::No,
                                                 GenerateCas::No,
                                                 mutation->getExtMetaData(),
                                                 true);
    }

    if (ret != ENGINE_SUCCESS) {
        log(EXTENSION_LOG_WARNING,
            "vb:%" PRIu16
            " Got error '%s' while trying to process "
            "mutation with seqno:%" PRId64,
            vb_,
            cb::to_string(cb::to_engine_errc(ret)).c_str(),
            mutation->getItem()->getBySeqno());
    } else {
        handleSnapshotEnd(vb, *mutation->getBySeqno());
    }

    return ret;
}

ENGINE_ERROR_CODE PassiveStream::processDeletion(MutationResponse* deletion) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return ENGINE_DISCONNECT;
    }

    if (uint64_t(*deletion->getBySeqno()) < cur_snapshot_start.load() ||
        uint64_t(*deletion->getBySeqno()) > cur_snapshot_end.load()) {
        log(EXTENSION_LOG_WARNING,
            "(vb %d) Erroneous deletion [sequence "
            "number does not fall in the expected snapshot range : "
            "{snapshot_start (%" PRIu64 ") <= seq_no (%" PRIu64
            ") <= "
            "snapshot_end (%" PRIu64 ")]; Dropping the deletion!",
            vb_,
            cur_snapshot_start.load(),
            *deletion->getBySeqno(),
            cur_snapshot_end.load());
        return ENGINE_ERANGE;
    }

    // The deleted value has a body, send it through the mutation path so we
    // set the deleted item with a value
    if (deletion->getItem()->getNBytes()) {
        return processMutation(deletion);
    }

    uint64_t delCas = 0;
    ENGINE_ERROR_CODE ret;
    ItemMetaData meta = deletion->getItem()->getMetaData();

    // MB-17517: Check for the incoming item's CAS validity.
    if (!Item::isValidCas(meta.cas)) {
        log(EXTENSION_LOG_WARNING,
            "Invalid CAS (0x%" PRIx64 ") received for deletion {vb:%" PRIu16
            ", seqno:%" PRId64 "}. Regenerating new CAS",
            meta.cas,
            vb_,
            *deletion->getBySeqno());
        meta.cas = Item::nextCas();
    }

    ret = engine->getKVBucket()->deleteWithMeta(deletion->getItem()->getKey(),
                                                delCas,
                                                nullptr,
                                                deletion->getVBucket(),
                                                consumer->getCookie(),
                                                {vbucket_state_active,
                                                 vbucket_state_replica,
                                                 vbucket_state_pending},
                                                CheckConflicts::No,
                                                meta,
                                                vb->isBackfillPhase(),
                                                GenerateBySeqno::No,
                                                GenerateCas::No,
                                                *deletion->getBySeqno(),
                                                deletion->getExtMetaData(),
                                                true);
    if (ret == ENGINE_KEY_ENOENT) {
        ret = ENGINE_SUCCESS;
    }

    if (ret != ENGINE_SUCCESS) {
        log(EXTENSION_LOG_WARNING,
            "vb:%" PRIu16
            " Got error '%s' while trying to process "
            "deletion with seqno:%" PRId64,
            vb_,
            cb::to_string(cb::to_engine_errc(ret)).c_str(),
            *deletion->getBySeqno());
    } else {
        handleSnapshotEnd(vb, *deletion->getBySeqno());
    }

    return ret;
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
        rv = processCreateCollection(*vb, {event});
        break;
    }
    case mcbp::systemevent::id::DeleteCollection: {
        rv = processBeginDeleteCollection(*vb, {event});
        break;
    }
    default: {
        rv = ENGINE_EINVAL;
        break;
    }
    }

    if (rv != ENGINE_SUCCESS) {
        log(EXTENSION_LOG_WARNING,
            "vb:%" PRIu16
            " Got error '%s' while trying to process "
            "system event",
            vb_,
            cb::to_string(cb::to_engine_errc(rv)).c_str());
    } else {
        handleSnapshotEnd(vb, *event.getBySeqno());
    }

    return rv;
}

ENGINE_ERROR_CODE PassiveStream::processCreateCollection(
        VBucket& vb, const CreateOrDeleteCollectionEvent& event) {
    try {
        vb.replicaAddCollection(event.getManifestUid(),
                                event.getCollectionID(),
                                event.getBySeqno());
    } catch (std::exception& e) {
        LOG(EXTENSION_LOG_WARNING,
            "PassiveStream::processCreateCollection exception %s",
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE PassiveStream::processBeginDeleteCollection(
        VBucket& vb, const CreateOrDeleteCollectionEvent& event) {
    try {
        vb.replicaBeginDeleteCollection(event.getManifestUid(),
                                        event.getCollectionID(),
                                        event.getBySeqno());
    } catch (std::exception& e) {
        LOG(EXTENSION_LOG_WARNING,
            "PassiveStream::processBeginDeleteCollection exception %s",
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    VBucketPtr vb = engine->getVBucket(vb_);

    cur_snapshot_start.store(marker->getStartSeqno());
    cur_snapshot_end.store(marker->getEndSeqno());
    cur_snapshot_type.store((marker->getFlags() & MARKER_FLAG_DISK)
                                    ? Snapshot::Disk
                                    : Snapshot::Memory);

    if (vb) {
        auto& ckptMgr = *vb->checkpointManager;
        if (marker->getFlags() & MARKER_FLAG_DISK && vb->getHighSeqno() == 0) {
            vb->setBackfillPhase(true);
            // calling setBackfillPhase sets the openCheckpointId to zero.
            ckptMgr.setBackfillPhase(cur_snapshot_start.load(),
                                     cur_snapshot_end.load());
        } else {
            if (marker->getFlags() & MARKER_FLAG_CHK ||
                vb->checkpointManager->getOpenCheckpointId() == 0) {
                ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                       cur_snapshot_end.load());
            } else {
                ckptMgr.updateCurrentSnapshotEnd(cur_snapshot_end.load());
            }
            vb->setBackfillPhase(false);
        }

        if (marker->getFlags() & MARKER_FLAG_ACK) {
            cur_snapshot_ack = true;
        }
    }
}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getKVBucket()->setVBucketState(vb_, state->getState(), true);
    {
        LockHolder lh(streamMutex);
        pushToReadyQ(std::make_unique<SetVBucketStateResponse>(opaque_,
                                                               ENGINE_SUCCESS));
    }
    notifyStreamReady();
}

void PassiveStream::handleSnapshotEnd(VBucketPtr& vb, uint64_t byseqno) {
    if (byseqno == cur_snapshot_end.load()) {
        auto& ckptMgr = *vb->checkpointManager;
        if (cur_snapshot_type.load() == Snapshot::Disk &&
            vb->isBackfillPhase()) {
            vb->setBackfillPhase(false);
            const auto id = ckptMgr.getOpenCheckpointId() + 1;
            ckptMgr.checkAndAddNewCheckpoint(id, *vb);
        } else {
            size_t mem_threshold = engine->getEpStats().mem_high_wat.load();
            size_t mem_used =
                    engine->getEpStats().getEstimatedTotalMemoryUsed();
            /* We want to add a new replica checkpoint if the mem usage is above
               high watermark (85%) */
            if (mem_threshold < mem_used) {
                const auto id = ckptMgr.getOpenCheckpointId() + 1;
                ckptMgr.checkAndAddNewCheckpoint(id, *vb);
            }
        }

        if (cur_snapshot_ack) {
            {
                LockHolder lh(streamMutex);
                pushToReadyQ(std::make_unique<SnapshotMarkerResponse>(
                        opaque_, ENGINE_SUCCESS));
            }
            notifyStreamReady();
            cur_snapshot_ack = false;
        }
        cur_snapshot_type.store(Snapshot::None);
    }
}

void PassiveStream::addStats(ADD_STAT add_stat, const void* c) {
    Stream::addStats(add_stat, c);

    try {
        const int bsize = 1024;
        char buf[bsize];
        size_t bufferItems = 0;
        size_t bufferBytes = 0;
        {
            std::lock_guard<std::mutex> lg(buffer.bufMutex);
            bufferItems = buffer.messages.size();
            bufferBytes = buffer.bytes;
        }
        checked_snprintf(
                buf, bsize, "%s:stream_%d_buffer_items", name_.c_str(), vb_);
        add_casted_stat(buf, bufferItems, add_stat, c);
        checked_snprintf(
                buf, bsize, "%s:stream_%d_buffer_bytes", name_.c_str(), vb_);
        add_casted_stat(buf, bufferBytes, add_stat, c);
        checked_snprintf(
                buf, bsize, "%s:stream_%d_items_ready", name_.c_str(), vb_);
        add_casted_stat(buf, itemsReady.load() ? "true" : "false", add_stat, c);
        checked_snprintf(buf,
                         bsize,
                         "%s:stream_%d_last_received_seqno",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buf, last_seqno.load(), add_stat, c);
        checked_snprintf(buf,
                         bsize,
                         "%s:stream_%d_ready_queue_memory",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buf, getReadyQueueMemory(), add_stat, c);

        checked_snprintf(buf,
                         bsize,
                         "%s:stream_%d_cur_snapshot_type",
                         name_.c_str(),
                         vb_);
        add_casted_stat(
                buf, ::to_string(cur_snapshot_type.load()), add_stat, c);

        if (cur_snapshot_type.load() != Snapshot::None) {
            checked_snprintf(buf,
                             bsize,
                             "%s:stream_%d_cur_snapshot_start",
                             name_.c_str(),
                             vb_);
            add_casted_stat(buf, cur_snapshot_start.load(), add_stat, c);
            checked_snprintf(buf,
                             bsize,
                             "%s:stream_%d_cur_snapshot_end",
                             name_.c_str(),
                             vb_);
            add_casted_stat(buf, cur_snapshot_end.load(), add_stat, c);
        }
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "PassiveStream::addStats: Failed to build stats: %s",
            error.what());
    }
}

std::unique_ptr<DcpResponse> PassiveStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return NULL;
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
    log(EXTENSION_LOG_INFO,
        "PassiveStream::transitionState: (vb %d) "
        "Transitioning from %s to %s",
        vb_,
        to_string(state_.load()).c_str(),
        to_string(newState).c_str());

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

    case StreamState::Backfilling:
    case StreamState::InMemory:
    case StreamState::TakeoverSend:
    case StreamState::TakeoverWait:
        // Not valid for passive streams
        break;

    case StreamState::Reading:
        if (newState == StreamState::Pending || newState == StreamState::Dead) {
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

std::string PassiveStream::getEndStreamStatusStr(end_stream_status_t status) {
    switch (status) {
    case END_STREAM_OK:
        return "The stream closed as part of normal operation";
    case END_STREAM_CLOSED:
        return "The stream closed due to a close stream message";
    case END_STREAM_DISCONNECTED:
        return "The stream closed early because the conn was disconnected";
    case END_STREAM_STATE:
        return "The stream closed early because the vbucket state changed";
    default:
        break;
    }
    return std::string{"Status unknown: " + std::to_string(status) +
                       "; this should not have happened!"};
}

void PassiveStream::log(EXTENSION_LOG_LEVEL severity,
                        const char* fmt,
                        ...) const {
    va_list va;
    va_start(va, fmt);
    auto consumer = consumerPtr.lock();
    if (consumer) {
        consumer->getLogger().vlog(severity, fmt, va);
    } else {
        static Logger defaultLogger =
                Logger("DCP (Consumer): **Deleted conn**");
        defaultLogger.vlog(severity, fmt, va);
    }
    va_end(va);
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
