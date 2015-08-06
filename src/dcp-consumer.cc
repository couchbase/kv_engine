/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include "ep_engine.h"
#include "failover-table.h"
#include "connmap.h"
#include "tapthrottle.h"
#include "dcp-consumer.h"
#include "dcp-response.h"
#include "dcp-stream.h"

class Processer : public GlobalTask {
public:
    Processer(EventuallyPersistentEngine* e, connection_t c,
                const Priority &p, double sleeptime = 1, bool shutdown = false)
        : GlobalTask(e, p, sleeptime, shutdown), conn(c) {}

    bool run();

    std::string getDescription();

private:
    connection_t conn;
};

bool Processer::run() {
    DcpConsumer* consumer = static_cast<DcpConsumer*>(conn.get());
    if (consumer->doDisconnect()) {
        return false;
    }

    switch (consumer->processBufferedItems()) {
        case all_processed:
            snooze(1);
            break;
        case more_to_process:
            snooze(0);
            break;
        case cannot_process:
            snooze(5);
            break;
        default:
            abort();
    }

    return true;
}

std::string Processer::getDescription() {
    std::stringstream ss;
    ss << "Processing buffered items for " << conn->getName();
    return ss.str();
}

DcpConsumer::DcpConsumer(EventuallyPersistentEngine &engine, const void *cookie,
                         const std::string &name)
    : Consumer(engine, cookie, name), opaqueCounter(0), processTaskId(0),
          itemsToProcess(false), lastNoopTime(ep_current_time()), backoffs(0) {
    Configuration& config = engine.getConfiguration();
    streams = new passive_stream_t[config.getMaxVbuckets()];
    setSupportAck(false);
    setLogHeader("DCP (Consumer) " + getName() + " -");
    setReserved(true);

    flowControl.enabled = config.isDcpEnableFlowControl();
    flowControl.bufferSize = config.getDcpConnBufferSize();
    flowControl.maxUnackedBytes = config.getDcpMaxUnackedBytes();

    noopInterval = config.getDcpNoopInterval();
    enableNoop = config.isDcpEnableNoop();
    sendNoopInterval = config.isDcpEnableNoop();

    ExTask task = new Processer(&engine, this, Priority::PendingOpsPriority, 1);
    processTaskId = ExecutorPool::get()->schedule(task, NONIO_TASK_IDX);
}

DcpConsumer::~DcpConsumer() {
    closeAllStreams();
    delete[] streams;
}

ENGINE_ERROR_CODE DcpConsumer::addStream(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    LockHolder lh(streamMutex);
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Add stream failed because this "
            "vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (vb->getState() == vbucket_state_active) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Add stream failed because this "
            "vbucket happens to be in active state", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    uint32_t new_opaque = ++opaqueCounter;
    failover_entry_t entry = vb->failovers->getLatestEntry();
    uint64_t start_seqno = vb->getHighSeqno();
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vbucket_uuid = entry.vb_uuid;
    uint64_t snap_start_seqno;
    uint64_t snap_end_seqno;

    vb->getCurrentSnapshot(snap_start_seqno, snap_end_seqno);

    passive_stream_t stream = streams[vbucket];
    if (stream && stream->isActive()) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Cannot add stream because one "
            "already exists", logHeader(), vbucket);
        return ENGINE_KEY_EEXISTS;
    }

    streams[vbucket] = new PassiveStream(&engine_, this, getName(), flags,
                                         new_opaque, vbucket, start_seqno,
                                         end_seqno, vbucket_uuid,
                                         snap_start_seqno, snap_end_seqno);
    ready.push_back(vbucket);
    opaqueMap_[new_opaque] = std::make_pair(opaque, vbucket);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpConsumer::closeStream(uint32_t opaque, uint16_t vbucket) {
    LockHolder lh(streamMutex);
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        opaqueMap_.erase(oitr);
    }

    passive_stream_t stream = streams[vbucket];
    if (!stream) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Cannot close stream because no "
            "stream exists for this vbucket", logHeader(), vbucket);
        return ENGINE_KEY_ENOENT;
    }

    uint32_t bytesCleared = stream->setDead(END_STREAM_CLOSED);
    flowControl.freedBytes.fetch_add(bytesCleared);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpConsumer::streamEnd(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    passive_stream_t stream = streams[vbucket];
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        LOG(EXTENSION_LOG_INFO, "%s (vb %d) End stream received with reason %d",
            logHeader(), vbucket, flags);
        StreamEndResponse* response = new StreamEndResponse(opaque, flags,
                                                            vbucket);
        err = stream->messageReceived(response);

        bool disable = false;
        if (err == ENGINE_SUCCESS &&
            itemsToProcess.compare_exchange_strong(disable, true)) {
            ExecutorPool::get()->wake(processTaskId);
        }
    }

    if (err != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) End stream received with opaque "
            "%d but does not exist", logHeader(), vbucket, opaque);
        flowControl.freedBytes.fetch_add(StreamEndResponse::baseMsgBytes);
    }

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::mutation(uint32_t opaque, const void* key,
                                        uint16_t nkey, const void* value,
                                        uint32_t nvalue, uint64_t cas,
                                        uint16_t vbucket, uint32_t flags,
                                        uint8_t datatype, uint32_t locktime,
                                        uint64_t bySeqno, uint64_t revSeqno,
                                        uint32_t exptime, uint8_t nru,
                                        const void* meta, uint16_t nmeta) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    passive_stream_t stream = streams[vbucket];
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        std::string key_str(static_cast<const char*>(key), nkey);
        value_t vblob(Blob::New(static_cast<const char*>(value), nvalue,
                      &(datatype), (uint8_t) EXT_META_LEN));
        Item *item = new Item(key_str, flags, exptime, vblob, cas, bySeqno,
                              vbucket, revSeqno);
        MutationResponse* response = new MutationResponse(item, opaque);
        err = stream->messageReceived(response);

        bool disable = false;
        if (err == ENGINE_SUCCESS &&
            itemsToProcess.compare_exchange_strong(disable, true)) {
            ExecutorPool::get()->wake(processTaskId);
        }
    }

    if (err != ENGINE_SUCCESS) {
        uint32_t bytes =
            MutationResponse::mutationBaseMsgBytes + nkey + nmeta + nvalue;
        flowControl.freedBytes.fetch_add(bytes);
    }

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::deletion(uint32_t opaque, const void* key,
                                        uint16_t nkey, uint64_t cas,
                                        uint16_t vbucket, uint64_t bySeqno,
                                        uint64_t revSeqno, const void* meta,
                                        uint16_t nmeta) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    passive_stream_t stream = streams[vbucket];
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        Item* item = new Item(key, nkey, 0, 0, NULL, 0, NULL, 0, cas, bySeqno,
                              vbucket, revSeqno);
        item->setDeleted();
        MutationResponse* response = new MutationResponse(item, opaque);
        err = stream->messageReceived(response);

        bool disable = false;
        if (err == ENGINE_SUCCESS &&
            itemsToProcess.compare_exchange_strong(disable, true)) {
            ExecutorPool::get()->wake(processTaskId);
        }
    }

    if (err != ENGINE_SUCCESS) {
        uint32_t bytes = MutationResponse::deletionBaseMsgBytes + nkey + nmeta;
        flowControl.freedBytes.fetch_add(bytes);
    }

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::expiration(uint32_t opaque, const void* key,
                                          uint16_t nkey, uint64_t cas,
                                          uint16_t vbucket, uint64_t bySeqno,
                                          uint64_t revSeqno, const void* meta,
                                          uint16_t nmeta) {
    return deletion(opaque, key, nkey, cas, vbucket, bySeqno, revSeqno, meta,
                    nmeta);
}

ENGINE_ERROR_CODE DcpConsumer::snapshotMarker(uint32_t opaque,
                                              uint16_t vbucket,
                                              uint64_t start_seqno,
                                              uint64_t end_seqno,
                                              uint32_t flags) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    passive_stream_t stream = streams[vbucket];
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        SnapshotMarker* response = new SnapshotMarker(opaque, vbucket,
                                                      start_seqno, end_seqno,
                                                      flags);
        err = stream->messageReceived(response);

        bool disable = false;
        if (err == ENGINE_SUCCESS &&
            itemsToProcess.compare_exchange_strong(disable, true)) {
            ExecutorPool::get()->wake(processTaskId);
        }
    }

    if (err != ENGINE_SUCCESS) {
        flowControl.freedBytes.fetch_add(SnapshotMarker::baseMsgBytes);
    }

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::noop(uint32_t opaque) {
    lastNoopTime = ep_current_time();
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpConsumer::flush(uint32_t opaque, uint16_t vbucket) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE DcpConsumer::setVBucketState(uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE err = ENGINE_KEY_ENOENT;
    passive_stream_t stream = streams[vbucket];
    if (stream && stream->getOpaque() == opaque && stream->isActive()) {
        SetVBucketState* response = new SetVBucketState(opaque, vbucket, state);
        err = stream->messageReceived(response);

        bool disable = false;
        if (err == ENGINE_SUCCESS &&
            itemsToProcess.compare_exchange_strong(disable, true)) {
            ExecutorPool::get()->wake(processTaskId);
        }
    }

    if (err != ENGINE_SUCCESS) {
        flowControl.freedBytes.fetch_add(SetVBucketState::baseMsgBytes);
    }

    return err;
}

ENGINE_ERROR_CODE DcpConsumer::step(struct dcp_message_producers* producers) {
    setLastWalkTime();

    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE ret;
    if ((ret = handleFlowCtl(producers)) != ENGINE_FAILED) {
        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
        }
        return ret;
    }

    if ((ret = handleNoop(producers)) != ENGINE_FAILED) {
        if (ret == ENGINE_SUCCESS) {
            ret = ENGINE_WANT_MORE;
        }
        return ret;
    }

    DcpResponse *resp = getNextItem();
    if (resp == NULL) {
        return ENGINE_SUCCESS;
    }

    ret = ENGINE_SUCCESS;
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    switch (resp->getEvent()) {
        case DCP_ADD_STREAM:
        {
            AddStreamResponse *as = static_cast<AddStreamResponse*>(resp);
            ret = producers->add_stream_rsp(getCookie(), as->getOpaque(),
                                            as->getStreamOpaque(),
                                            as->getStatus());
            break;
        }
        case DCP_STREAM_REQ:
        {
            StreamRequest *sr = static_cast<StreamRequest*> (resp);
            ret = producers->stream_req(getCookie(), sr->getOpaque(),
                                        sr->getVBucket(), sr->getFlags(),
                                        sr->getStartSeqno(), sr->getEndSeqno(),
                                        sr->getVBucketUUID(),
                                        sr->getSnapStartSeqno(),
                                        sr->getSnapEndSeqno());
            break;
        }
        case DCP_SET_VBUCKET:
        {
            SetVBucketStateResponse* vs;
            vs = static_cast<SetVBucketStateResponse*>(resp);
            ret = producers->set_vbucket_state_rsp(getCookie(), vs->getOpaque(),
                                                   vs->getStatus());
            break;
        }
        case DCP_SNAPSHOT_MARKER:
        {
            SnapshotMarkerResponse* mr;
            mr = static_cast<SnapshotMarkerResponse*>(resp);
            ret = producers->marker_rsp(getCookie(), mr->getOpaque(),
                                        mr->getStatus());
            break;
        }
        default:
            LOG(EXTENSION_LOG_WARNING, "%s Unknown consumer event (%d), "
                "disconnecting", logHeader(), resp->getEvent());
            ret = ENGINE_DISCONNECT;
    }
    ObjectRegistry::onSwitchThread(epe);
    delete resp;

    if (ret == ENGINE_SUCCESS) {
        return ENGINE_WANT_MORE;
    }
    return ret;
}

bool RollbackTask::run() {
    if (cons->doRollback(opaque, vbid, rollbackSeqno)) {
        return true;
    }
    ++(engine->getEpStats().rollbackCount);
    return false;
}

ENGINE_ERROR_CODE DcpConsumer::handleResponse(
                                        protocol_binary_response_header *resp) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    uint8_t opcode = resp->response.opcode;
    uint32_t opaque = resp->response.opaque;

    opaque_map::iterator oitr = opaqueMap_.find(opaque);

    bool validOpaque = false;
    if (oitr != opaqueMap_.end()) {
        validOpaque = isValidOpaque(opaque, oitr->second.second);
    }

    if (!validOpaque) {
        LOG(EXTENSION_LOG_WARNING, "%s Received response with opaque %ld and "
            "that stream no longer exists", logHeader());
        return ENGINE_KEY_ENOENT;
    }

    if (opcode == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ) {
        protocol_binary_response_dcp_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_dcp_stream_req*>(resp);

        uint16_t vbid = oitr->second.second;
        uint16_t status = ntohs(pkt->message.header.response.status);
        uint64_t bodylen = ntohl(pkt->message.header.response.bodylen);
        uint8_t* body = pkt->bytes + sizeof(protocol_binary_response_header);

        if (status == PROTOCOL_BINARY_RESPONSE_ROLLBACK) {
            cb_assert(bodylen == sizeof(uint64_t));
            uint64_t rollbackSeqno = 0;
            memcpy(&rollbackSeqno, body, sizeof(uint64_t));
            rollbackSeqno = ntohll(rollbackSeqno);

            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Received rollback request "
                "to rollback seq no. %llu", logHeader(), vbid, rollbackSeqno);

            ExTask task = new RollbackTask(&engine_, opaque, vbid,
                                           rollbackSeqno, this,
                                           Priority::TapBgFetcherPriority);
            ExecutorPool::get()->schedule(task, WRITER_TASK_IDX);
            return ENGINE_SUCCESS;
        }

        if (((bodylen % 16) != 0 || bodylen == 0) && status == ENGINE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d)Got a stream response with a "
                "bad failover log (length %llu), disconnecting", logHeader(),
                vbid, bodylen);
            return ENGINE_DISCONNECT;
        }

        streamAccepted(opaque, status, body, bodylen);
        return ENGINE_SUCCESS;
    } else if (opcode == PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT ||
               opcode == PROTOCOL_BINARY_CMD_DCP_CONTROL) {
        return ENGINE_SUCCESS;
    }

    LOG(EXTENSION_LOG_WARNING, "%s Trying to handle an unknown response %d, "
        "disconnecting", logHeader(), opcode);

    return ENGINE_DISCONNECT;
}

bool DcpConsumer::doRollback(uint32_t opaque, uint16_t vbid,
                             uint64_t rollbackSeqno) {
    ENGINE_ERROR_CODE err = engine_.getEpStore()->rollback(vbid, rollbackSeqno);

    if (err == ENGINE_NOT_MY_VBUCKET) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Rollback failed because the "
                "vbucket was not found", logHeader(), vbid);
        return false;
    } else if (err == ENGINE_TMPFAIL) {
        return true; // Reschedule the rollback.
    }

    cb_assert(err == ENGINE_SUCCESS);

    LockHolder lh(streamMutex);
    RCPtr<VBucket> vb = engine_.getVBucket(vbid);
    streams[vbid]->reconnectStream(vb, opaque, vb->getHighSeqno());

    return false;
}

void DcpConsumer::addStats(ADD_STAT add_stat, const void *c) {
    ConnHandler::addStats(add_stat, c);

    int max_vbuckets = engine_.getConfiguration().getMaxVbuckets();
    for (int vbucket = 0; vbucket < max_vbuckets; vbucket++) {
        passive_stream_t stream = streams[vbucket];
        if (stream) {
            stream->addStats(add_stat, c);
        }
    }

    addStat("total_backoffs", backoffs, add_stat, c);
    if (flowControl.enabled) {
        addStat("total_acked_bytes", flowControl.ackedBytes, add_stat, c);
    }
}

void DcpConsumer::aggregateQueueStats(ConnCounter* aggregator) {
    aggregator->conn_queueBackoff += backoffs;
}

process_items_error_t DcpConsumer::processBufferedItems() {
    itemsToProcess.store(false);
    process_items_error_t process_ret = all_processed;

    int max_vbuckets = engine_.getConfiguration().getMaxVbuckets();
    for (int vbucket = 0; vbucket < max_vbuckets; vbucket++) {

        passive_stream_t stream = streams[vbucket];
        if (!stream) {
            continue;
        }

        uint32_t bytes_processed;

        do {
            if (!engine_.getTapThrottle().shouldProcess()) {
                backoffs++;
                return cannot_process;
            }

            bytes_processed = 0;
            process_ret = stream->processBufferedMessages(bytes_processed);
            flowControl.freedBytes.fetch_add(bytes_processed);
        } while (bytes_processed > 0 && process_ret != cannot_process);
    }

    if (process_ret == all_processed && itemsToProcess.load()) {
        return more_to_process;
    }

    return process_ret;
}

DcpResponse* DcpConsumer::getNextItem() {
    LockHolder lh(streamMutex);

    setPaused(false);
    while (!ready.empty()) {
        uint16_t vbucket = ready.front();
        ready.pop_front();

        passive_stream_t stream = streams[vbucket];
        if (!stream) {
            continue;
        }

        DcpResponse* op = stream->next();
        if (!op) {
            continue;
        }
        switch (op->getEvent()) {
            case DCP_STREAM_REQ:
            case DCP_ADD_STREAM:
            case DCP_SET_VBUCKET:
            case DCP_SNAPSHOT_MARKER:
                break;
            default:
                LOG(EXTENSION_LOG_WARNING, "%s Consumer is attempting to write"
                    " an unexpected event %d", logHeader(), op->getEvent());
                abort();
        }

        ready.push_back(vbucket);
        return op;
    }
    setPaused(true);

    return NULL;
}

void DcpConsumer::notifyStreamReady(uint16_t vbucket) {
    std::list<uint16_t>::iterator iter =
        std::find(ready.begin(), ready.end(), vbucket);
    if (iter != ready.end()) {
        return;
    }

    ready.push_back(vbucket);

    engine_.getDcpConnMap().notifyPausedConnection(this, true);
}

void DcpConsumer::streamAccepted(uint32_t opaque, uint16_t status, uint8_t* body,
                                 uint32_t bodylen) {
    LockHolder lh(streamMutex);

    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        uint32_t add_opaque = oitr->second.first;
        uint16_t vbucket = oitr->second.second;

        passive_stream_t stream = streams[vbucket];
        if (stream && stream->getOpaque() == opaque &&
            stream->getState() == STREAM_PENDING) {
            if (status == ENGINE_SUCCESS) {
                RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
                vb->failovers->replaceFailoverLog(body, bodylen);
                EventuallyPersistentStore* st = engine_.getEpStore();
                st->scheduleVBSnapshot(Priority::VBucketPersistHighPriority,
                                st->getVBuckets().getShard(vbucket)->getId());
            }
            LOG(EXTENSION_LOG_INFO, "%s (vb %d) Add stream for opaque %ld"
                " %s with error code %d", logHeader(), vbucket, opaque,
                status == ENGINE_SUCCESS ? "succeeded" : "failed", status);
            stream->acceptStream(status, add_opaque);
        } else {
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Trying to add stream, but "
                "none exists (opaque: %d, add_opaque: %d)", logHeader(),
                vbucket, opaque, add_opaque);
        }
        opaqueMap_.erase(opaque);
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s No opaque found for add stream response "
            "with opaque %ld", logHeader(), opaque);
    }
}

bool DcpConsumer::isValidOpaque(uint32_t opaque, uint16_t vbucket) {
    LockHolder lh(streamMutex);
    passive_stream_t stream = streams[vbucket];
    return stream && stream->getOpaque() == opaque;
}

void DcpConsumer::closeAllStreams() {
    int max_vbuckets = engine_.getConfiguration().getMaxVbuckets();
    for (int vbucket = 0; vbucket < max_vbuckets; vbucket++) {
        passive_stream_t stream = streams[vbucket];
        if (stream) {
            stream->setDead(END_STREAM_DISCONNECTED);
        }
    }
}

ENGINE_ERROR_CODE DcpConsumer::handleNoop(struct dcp_message_producers* producers) {
    if (enableNoop) {
        ENGINE_ERROR_CODE ret;
        uint32_t opaque = ++opaqueCounter;
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        ret = producers->control(getCookie(), opaque, "enable_noop", 11,
                                 "true", 4);
        ObjectRegistry::onSwitchThread(epe);
        enableNoop = false;
        return ret;
    }

    if (sendNoopInterval) {
        ENGINE_ERROR_CODE ret;
        uint32_t opaque = ++opaqueCounter;
        char buf_size[10];
        snprintf(buf_size, 10, "%u", noopInterval);
        EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
        ret = producers->control(getCookie(), opaque, "set_noop_interval", 17,
                                 buf_size, strlen(buf_size));
        ObjectRegistry::onSwitchThread(epe);
        sendNoopInterval = false;
        return ret;
    }

    if ((ep_current_time() - lastNoopTime) > (noopInterval * 2)) {
        LOG(EXTENSION_LOG_WARNING, "%s Disconnecting because noop message has "
            "no been received for %u seconds", logHeader(), (noopInterval * 2));
        return ENGINE_DISCONNECT;
    }

    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE DcpConsumer::handleFlowCtl(struct dcp_message_producers* producers) {
    if (flowControl.enabled) {
        ENGINE_ERROR_CODE ret;
        uint32_t ackable_bytes = flowControl.freedBytes;
        if (flowControl.pendingControl) {
            uint32_t opaque = ++opaqueCounter;
            char buf_size[10];
            snprintf(buf_size, 10, "%u", flowControl.bufferSize);
            EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
            ret = producers->control(getCookie(), opaque,
                                     "connection_buffer_size", 22, buf_size,
                                     strlen(buf_size));
            ObjectRegistry::onSwitchThread(epe);
            flowControl.pendingControl = false;
            return (ret == ENGINE_SUCCESS) ? ENGINE_WANT_MORE : ret;
        } else if (ackable_bytes > (flowControl.bufferSize * .2)) {
            // Send a buffer ack when at least 20% of the buffer is drained
            uint32_t opaque = ++opaqueCounter;
            EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
            ret = producers->buffer_acknowledgement(getCookie(), opaque, 0,
                                                    ackable_bytes);
            ObjectRegistry::onSwitchThread(epe);
            flowControl.lastBufferAck = ep_current_time();
            flowControl.ackedBytes.fetch_add(ackable_bytes);
            flowControl.freedBytes.fetch_sub(ackable_bytes);
            return (ret == ENGINE_SUCCESS) ? ENGINE_WANT_MORE : ret;
        } else if (ackable_bytes > 0 &&
                   (ep_current_time() - flowControl.lastBufferAck) > 5) {
            // Ack at least every 5 seconds
            uint32_t opaque = ++opaqueCounter;
            EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
            ret = producers->buffer_acknowledgement(getCookie(), opaque, 0,
                                                    ackable_bytes);
            ObjectRegistry::onSwitchThread(epe);
            flowControl.lastBufferAck = ep_current_time();
            flowControl.ackedBytes.fetch_add(ackable_bytes);
            flowControl.freedBytes.fetch_sub(ackable_bytes);
            return (ret == ENGINE_SUCCESS) ? ENGINE_WANT_MORE : ret;
        }
    }
    return ENGINE_FAILED;
}

bool DcpConsumer::isStreamPresent(uint16_t vbucket)
{
    if (streams[vbucket] && streams[vbucket]->isActive()) {
        return true;
    }
    return false;
}
