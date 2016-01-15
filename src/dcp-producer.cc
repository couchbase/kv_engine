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

#include "backfill.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "dcp-producer.h"
#include "dcp-response.h"
#include "dcp-stream.h"

const uint32_t DcpProducer::defaultNoopInerval = 20;

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
    WriterLockHolder lh(logLock);
    this->maxBytes = maxBytes;
    if (maxBytes == 0) {
        bytesSent = 0;
        ackedBytes = 0;
    }
}

bool DcpProducer::BufferLog::insert(size_t bytes) {
    WriterLockHolder wlh(logLock);
    bool inserted = false;
    // If the log is not enabled
    // or there is space, allow the insert
    if (!isEnabled_UNLOCKED() || !isFull_UNLOCKED()) {
        bytesSent += bytes;
        inserted = true;
    }
    return inserted;
}

void DcpProducer::BufferLog::release_UNLOCKED(size_t bytes) {
    if (bytesSent >= bytes) {
        bytesSent -= bytes;
    } else {
        bytesSent = 0;
    }
}

bool DcpProducer::BufferLog::pauseIfFull() {
    ReaderLockHolder rlh(logLock);
    if (getState_UNLOCKED() == Full) {
        producer.setPaused(true);
        return true;
    }
    return false;
}

void DcpProducer::BufferLog::unpauseIfSpaceAvailable() {
    ReaderLockHolder rlh(logLock);
    if (getState_UNLOCKED() != Full) {
        producer.notifyPaused(true);
    }
}

void DcpProducer::BufferLog::acknowledge(size_t bytes) {
    WriterLockHolder wlh(logLock);
    State state = getState_UNLOCKED();
    if (state != Disabled) {
        release_UNLOCKED(bytes);
        ackedBytes += bytes;
        if (state == Full) {
            producer.notifyPaused(true);
        }
    }
}

void DcpProducer::BufferLog::addStats(ADD_STAT add_stat, const void *c) {
    ReaderLockHolder rlh(logLock);
    if (isEnabled_UNLOCKED()) {
        producer.addStat("max_buffer_bytes", maxBytes, add_stat, c);
        producer.addStat("unacked_bytes", bytesSent, add_stat, c);
        producer.addStat("total_acked_bytes", ackedBytes, add_stat, c);
        producer.addStat("flow_control", "enabled", add_stat, c);
    } else {
        producer.addStat("flow_control", "disabled", add_stat, c);
    }
}

DcpProducer::DcpProducer(EventuallyPersistentEngine &e, const void *cookie,
                         const std::string &name, bool isNotifier)
    : Producer(e, cookie, name), rejectResp(NULL),
      notifyOnly(isNotifier), lastSendTime(ep_current_time()), log(*this),
      itemsSent(0), totalBytesSent(0) {
    setSupportAck(true);
    setReserved(true);
    setPaused(true);

    if (notifyOnly) {
        setLogHeader("DCP (Notifier) " + getName() + " -");
    } else {
        setLogHeader("DCP (Producer) " + getName() + " -");
    }

    if (getName().find("replication") != std::string::npos) {
        engine_.setDCPPriority(getCookie(), CONN_PRIORITY_HIGH);
    } else if (getName().find("xdcr") != std::string::npos) {
        engine_.setDCPPriority(getCookie(), CONN_PRIORITY_MED);
    } else if (getName().find("views") != std::string::npos) {
        engine_.setDCPPriority(getCookie(), CONN_PRIORITY_MED);
    }

    // The consumer assigns opaques starting at 0 so lets have the producer
    //start using opaques at 10M to prevent any opaque conflicts.
    noopCtx.opaque = 10000000;
    noopCtx.sendTime = ep_current_time();

    // This is for backward compatibility with Couchbase 3.0. In 3.0 we set the
    // noop interval to 20 seconds by default, but in post 3.0 releases we set
    // it to be higher by default. Starting in 3.0.1 the DCP consumer sets the
    // noop interval of the producer when connecting so in an all 3.0.1+ cluster
    // this value will be overriden. In 3.0 however we do not set the noop
    // interval so setting this value will make sure we don't disconnect on
    // accident due to the producer and the consumer having a different noop
    // interval.
    noopCtx.noopInterval = defaultNoopInerval;
    noopCtx.pendingRecv = false;
    noopCtx.enabled = false;

    // 1 task per producer
    checkpointCreatorTask = new ActiveStreamCheckpointProcessorTask(e);
    ExecutorPool::get()->schedule(checkpointCreatorTask, AUXIO_TASK_IDX);
}

DcpProducer::~DcpProducer() {
    ExecutorPool::get()->cancel(checkpointCreatorTask->getId());
}

ENGINE_ERROR_CODE DcpProducer::streamRequest(uint32_t flags,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint64_t vbucket_uuid,
                                             uint64_t snap_start_seqno,
                                             uint64_t snap_end_seqno,
                                             uint64_t *rollback_seqno,
                                             dcp_add_failover_log callback) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (vb->checkpointManager.getOpenCheckpointId() == 0) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "this vbucket is in backfill state", logHeader(), vbucket);
        return ENGINE_TMPFAIL;
    }

    if (flags & DCP_ADD_STREAM_FLAG_LATEST) {
        end_seqno = vb->getHighSeqno();
    }

    if (flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
        end_seqno = engine_.getEpStore()->getLastPersistedSeqno(vbucket);
    }

    if (!notifyOnly && start_seqno > end_seqno) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "the start seqno (%llu) is larger than the end seqno (%llu)",
            logHeader(), vbucket, start_seqno, end_seqno);
        return ENGINE_ERANGE;
    }

    if (!notifyOnly && !(snap_start_seqno <= start_seqno &&
        start_seqno <= snap_end_seqno)) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed because "
            "the snap start seqno (%llu) <= start seqno (%llu) <= snap end "
            "seqno (%llu) is required", logHeader(), vbucket, snap_start_seqno,
            start_seqno, snap_end_seqno);
        return ENGINE_ERANGE;
    }

    bool add_vb_conn_map = true;
    std::map<uint16_t, stream_t>::iterator itr;
    {
        WriterLockHolder wlh(streamsMutex);
        if ((itr = streams.find(vbucket)) != streams.end()) {
            if (itr->second->getState() != STREAM_DEAD) {
                LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed"
                    " because a stream already exists for this vbucket",
                    logHeader(), vbucket);
                return ENGINE_KEY_EEXISTS;
            } else {
                streams.erase(vbucket);

                // Don't need to add an entry to vbucket-to-conns map
                add_vb_conn_map = false;
            }
        }
    }

    // If we are a notify stream then we can't use the start_seqno supplied
    // since if it is greater than the current high seqno then it will always
    // trigger a rollback. As a result we should use the current high seqno for
    // rollback purposes.
    uint64_t notifySeqno = start_seqno;
    if (notifyOnly && start_seqno > static_cast<uint64_t>(vb->getHighSeqno())) {
        start_seqno = static_cast<uint64_t>(vb->getHighSeqno());
    }

    if (vb->failovers->needsRollback(start_seqno, vb->getHighSeqno(),
                                     vbucket_uuid, snap_start_seqno,
                                     snap_end_seqno, vb->getPurgeSeqno(),
                                     rollback_seqno)) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream request failed "
            "because a rollback to seqno %llu is required (start seqno %llu, "
            "vb_uuid %llu, snapStartSeqno %llu, snapEndSeqno %llu)",
            logHeader(), vbucket, *rollback_seqno, start_seqno, vbucket_uuid,
            snap_start_seqno, snap_end_seqno);
        return ENGINE_ROLLBACK;
    }

    ENGINE_ERROR_CODE rv = vb->failovers->addFailoverLog(getCookie(), callback);
    if (rv != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Couldn't add failover log to "
            "stream request due to error %d", logHeader(), vbucket, rv);
        return rv;
    }

    stream_t s;
    if (notifyOnly) {
        s = new NotifierStream(&engine_, this, getName(), flags,
                               opaque, vbucket, notifySeqno,
                               end_seqno, vbucket_uuid,
                               snap_start_seqno, snap_end_seqno);
   } else {
        s = new ActiveStream(&engine_, this, getName(), flags,
                             opaque, vbucket, start_seqno,
                             end_seqno, vbucket_uuid,
                             snap_start_seqno, snap_end_seqno);
        static_cast<ActiveStream*>(s.get())->setActive();
    }

    {
        WriterLockHolder wlh(streamsMutex);
        streams[vbucket] = s;
    }

    ready.pushUnique(vbucket);

    if (add_vb_conn_map) {
        connection_t conn(this);
        engine_.getDcpConnMap().addVBConnByVBId(conn, vbucket);
    }

    return rv;
}

ENGINE_ERROR_CODE DcpProducer::getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                              dcp_add_failover_log callback) {
    (void) opaque;
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Get Failover Log failed "
            "because this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    return vb->failovers->addFailoverLog(getCookie(), callback);
}

ENGINE_ERROR_CODE DcpProducer::step(struct dcp_message_producers* producers) {
    setLastWalkTime();

    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    ENGINE_ERROR_CODE ret;
    if ((ret = maybeSendNoop(producers)) != ENGINE_FAILED) {
        return ret;
    }

    DcpResponse *resp;
    if (rejectResp) {
        resp = rejectResp;
        rejectResp = NULL;
    } else {
        resp = getNextItem();
        if (!resp) {
            return ENGINE_SUCCESS;
        }
    }

    ret = ENGINE_SUCCESS;

    Item* itmCpy = NULL;
    if (resp->getEvent() == DCP_MUTATION) {
        itmCpy = static_cast<MutationResponse*>(resp)->getItemCopy();
    }

    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL,
                                                                     true);
    switch (resp->getEvent()) {
        case DCP_STREAM_END:
        {
            StreamEndResponse *se = static_cast<StreamEndResponse*>(resp);
            ret = producers->stream_end(getCookie(), se->getOpaque(),
                                        se->getVbucket(), se->getFlags());
            break;
        }
        case DCP_MUTATION:
        {
            MutationResponse *m = dynamic_cast<MutationResponse*> (resp);
            ret = producers->mutation(getCookie(), m->getOpaque(), itmCpy,
                                      m->getVBucket(), m->getBySeqno(),
                                      m->getRevSeqno(), 0, NULL, 0,
                                      m->getItem()->getNRUValue());
            break;
        }
        case DCP_DELETION:
        {
            MutationResponse *m = static_cast<MutationResponse*>(resp);
            ret = producers->deletion(getCookie(), m->getOpaque(),
                                      m->getItem()->getKey().c_str(),
                                      m->getItem()->getNKey(),
                                      m->getItem()->getCas(),
                                      m->getVBucket(), m->getBySeqno(),
                                      m->getRevSeqno(), NULL, 0);
            break;
        }
        case DCP_SNAPSHOT_MARKER:
        {
            SnapshotMarker *s = static_cast<SnapshotMarker*>(resp);
            ret = producers->marker(getCookie(), s->getOpaque(),
                                    s->getVBucket(),
                                    s->getStartSeqno(),
                                    s->getEndSeqno(),
                                    s->getFlags());
            break;
        }
        case DCP_SET_VBUCKET:
        {
            SetVBucketState *s = static_cast<SetVBucketState*>(resp);
            ret = producers->set_vbucket_state(getCookie(), s->getOpaque(),
                                               s->getVBucket(), s->getState());
            break;
        }
        default:
        {
            LOG(EXTENSION_LOG_WARNING, "%s Unexpected dcp event (%d), "
                "disconnecting", logHeader(), resp->getEvent());
            ret = ENGINE_DISCONNECT;
            break;
        }
    }

    ObjectRegistry::onSwitchThread(epe);
    if (resp->getEvent() == DCP_MUTATION && ret != ENGINE_SUCCESS) {
        delete itmCpy;
    }

    if (ret == ENGINE_E2BIG) {
        rejectResp = resp;
    } else {
        delete resp;
    }

    lastSendTime = ep_current_time();
    return (ret == ENGINE_SUCCESS) ? ENGINE_WANT_MORE : ret;
}

ENGINE_ERROR_CODE DcpProducer::bufferAcknowledgement(uint32_t opaque,
                                                     uint16_t vbucket,
                                                     uint32_t buffer_bytes) {
    log.acknowledge(buffer_bytes);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DcpProducer::control(uint32_t opaque, const void* key,
                                       uint16_t nkey, const void* value,
                                       uint32_t nvalue) {
    const char* param = static_cast<const char*>(key);
    std::string keyStr(static_cast<const char*>(key), nkey);
    std::string valueStr(static_cast<const char*>(value), nvalue);

    if (strncmp(param, "connection_buffer_size", nkey) == 0) {
        uint32_t size;
        if (parseUint32(valueStr.c_str(), &size)) {
            /* Size 0 implies the client (DCP consumer) does not support
               flow control */
            log.setBufferSize(size);
            return ENGINE_SUCCESS;
        }
    } else if (strncmp(param, "stream_buffer_size", nkey) == 0) {
        LOG(EXTENSION_LOG_WARNING, "%s The ctrl parameter stream_buffer_size is"
            "not supported by this engine", logHeader());
        return ENGINE_ENOTSUP;
    } else if (strncmp(param, "enable_noop", nkey) == 0) {
        if (valueStr.compare("true") == 0) {
            noopCtx.enabled = true;
        } else {
            noopCtx.enabled = false;
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "set_noop_interval", nkey) == 0) {
        if (parseUint32(valueStr.c_str(), &noopCtx.noopInterval)) {
            return ENGINE_SUCCESS;
        }
    }

    LOG(EXTENSION_LOG_WARNING, "%s Invalid ctrl parameter '%s' for %s",
        logHeader(), valueStr.c_str(), keyStr.c_str());

    return ENGINE_EINVAL;
}

ENGINE_ERROR_CODE DcpProducer::handleResponse(
                                        protocol_binary_response_header *resp) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    uint8_t opcode = resp->response.opcode;
    if (opcode == PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE ||
        opcode == PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER) {
        protocol_binary_response_dcp_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_dcp_stream_req*>(resp);
        uint32_t opaque = pkt->message.header.response.opaque;

        stream_t active_stream;
        std::map<uint16_t, stream_t>::iterator itr;
        {
            ReaderLockHolder rlh(streamsMutex);
            for (itr = streams.begin() ; itr != streams.end(); ++itr) {
                active_stream = itr->second;
                Stream *str = active_stream.get();
                if (str && str->getType() == STREAM_ACTIVE) {
                    ActiveStream* as = static_cast<ActiveStream*>(str);
                    if (as && opaque == itr->second->getOpaque()) {
                        break;
                    }
                }
            }
        }

        if (itr != streams.end()) {
            ActiveStream *as = static_cast<ActiveStream*>(active_stream.get());
            if (opcode == PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE) {
                as->setVBucketStateAckRecieved();
            } else if (opcode == PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER) {
                as->snapshotMarkerAckReceived();
            }
        }

        return ENGINE_SUCCESS;
    } else if (opcode == PROTOCOL_BINARY_CMD_DCP_MUTATION ||
        opcode == PROTOCOL_BINARY_CMD_DCP_DELETION ||
        opcode == PROTOCOL_BINARY_CMD_DCP_EXPIRATION ||
        opcode == PROTOCOL_BINARY_CMD_DCP_STREAM_END) {
        // TODO: When nacking is implemented we need to handle these responses
        return ENGINE_SUCCESS;
    } else if (opcode == PROTOCOL_BINARY_CMD_DCP_NOOP) {
        if (noopCtx.opaque == resp->response.opaque) {
            noopCtx.pendingRecv = false;
            return ENGINE_SUCCESS;
        }
    }

    LOG(EXTENSION_LOG_WARNING, "%s Trying to handle an unknown response %d, "
        "disconnecting", logHeader(), opcode);

    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE DcpProducer::closeStream(uint32_t opaque, uint16_t vbucket) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    stream_t stream = findStreamByVbid(vbucket);
    ENGINE_ERROR_CODE ret;
    if (!stream) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Cannot close stream because no "
            "stream exists for this vbucket", logHeader(), vbucket);
        return ENGINE_KEY_ENOENT;
    } else if (!stream->isActive()) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Cannot close stream because "
            "stream is already marked as dead", logHeader(), vbucket);
        connection_t conn(this);
        engine_.getDcpConnMap().removeVBConnByVBId(conn, vbucket);
        ret = ENGINE_KEY_ENOENT;
    } else {
        stream->setDead(END_STREAM_CLOSED);
        connection_t conn(this);
        engine_.getDcpConnMap().removeVBConnByVBId(conn, vbucket);
        ret = ENGINE_SUCCESS;
    }

    {
        WriterLockHolder wlh(streamsMutex);
        streams.erase(vbucket);
    }

    return ret;
}

void DcpProducer::addStats(ADD_STAT add_stat, const void *c) {
    Producer::addStats(add_stat, c);

    addStat("items_sent", getItemsSent(), add_stat, c);
    addStat("items_remaining", getItemsRemaining(), add_stat, c);
    addStat("total_bytes_sent", getTotalBytes(), add_stat, c);
    addStat("last_sent_time", lastSendTime, add_stat, c);
    addStat("noop_enabled", noopCtx.enabled, add_stat, c);
    addStat("noop_wait", noopCtx.pendingRecv, add_stat, c);

    log.addStats(add_stat, c);

    ReaderLockHolder rlh(streamsMutex);
    std::map<uint16_t, stream_t>::iterator itr;
    for (itr = streams.begin(); itr != streams.end(); ++itr) {
        itr->second->addStats(add_stat, c);
    }
}

void DcpProducer::addTakeoverStats(ADD_STAT add_stat, const void* c,
                                   uint16_t vbid) {
    stream_t stream = findStreamByVbid(vbid);
    if (stream && stream->getType() == STREAM_ACTIVE) {
        ActiveStream* as = static_cast<ActiveStream*>(stream.get());
        if (as) {
            as->addTakeoverStats(add_stat, c);
        }
    }
}

void DcpProducer::aggregateQueueStats(ConnCounter* aggregator) {
    if (!aggregator) {
        LOG(EXTENSION_LOG_WARNING, "%s Pointer to the queue stats aggregator"
            " is NULL!!!", logHeader());
        return;
    }
    aggregator->conn_queueDrain += itemsSent;
    aggregator->conn_totalBytes += totalBytesSent;
    aggregator->conn_queueRemaining += getItemsRemaining();
    aggregator->conn_queueBackfillRemaining += totalBackfillBacklogs;
}

void DcpProducer::notifySeqnoAvailable(uint16_t vbucket, uint64_t seqno) {
    stream_t stream = findStreamByVbid(vbucket);
    if (stream && stream->isActive()) {
        stream->notifySeqnoAvailable(seqno);
    }
}

void DcpProducer::vbucketStateChanged(uint16_t vbucket, vbucket_state_t state) {
    stream_t stream = findStreamByVbid(vbucket);
    if (stream) {
        stream->setDead(END_STREAM_STATE);
    }
}

void DcpProducer::closeAllStreams() {
    std::list<uint16_t> vblist;
    {
        WriterLockHolder wlh(streamsMutex);
        while (!streams.empty()) {
            std::map<uint16_t, stream_t>::iterator itr = streams.begin();
            uint16_t vbid = itr->first;
            itr->second->setDead(END_STREAM_DISCONNECTED);
            streams.erase(vbid);
            vblist.push_back(vbid);
        }
    }

    connection_t conn(this);
    std::list<uint16_t>::iterator it = vblist.begin();
    for (; it != vblist.end(); ++it) {
        engine_.getDcpConnMap().removeVBConnByVBId(conn, *it);
    }
}

const char* DcpProducer::getType() const {
    if (notifyOnly) {
        return "notifier";
    } else {
        return "producer";
    }
}

DcpResponse* DcpProducer::getNextItem() {
    do {
        setPaused(false);

        uint16_t vbucket = 0;
        while (ready.popFront(vbucket)) {
            if (log.pauseIfFull()) {
                ready.pushUnique(vbucket);
                return NULL;
            }

            DcpResponse* op = NULL;
            stream_t stream;
            {
                ReaderLockHolder rlh(streamsMutex);
                std::map<uint16_t, stream_t>::iterator it = streams.find(vbucket);
                if (it == streams.end()) {
                    continue;
                }
                stream.reset(it->second);
            }

            op = stream->next();

            if (!op) {
                // stream is empty, try another vbucket.
                continue;
            }

            switch (op->getEvent()) {
                case DCP_SNAPSHOT_MARKER:
                case DCP_MUTATION:
                case DCP_DELETION:
                case DCP_EXPIRATION:
                case DCP_STREAM_END:
                case DCP_SET_VBUCKET:
                    break;
                default:
                    LOG(EXTENSION_LOG_WARNING, "%s Producer is attempting to write"
                        " an unexpected event %d", logHeader(), op->getEvent());
                    abort();
            }

            ready.pushUnique(vbucket);

            if (op->getEvent() == DCP_MUTATION || op->getEvent() == DCP_DELETION ||
                op->getEvent() == DCP_EXPIRATION) {
                itemsSent++;
            }

            totalBytesSent.fetch_add(op->getMessageSize());

            return op;
        }

        // flag we are paused
        setPaused(true);

        // re-check the ready queue.
        // A new vbucket could of became ready and the notifier could of seen
        // paused = false, so reloop so we don't miss an operation.
    } while(!ready.empty());

    return NULL;
}

void DcpProducer::setDisconnect(bool disconnect) {
    ConnHandler::setDisconnect(disconnect);

    if (disconnect) {
        ReaderLockHolder rlh(streamsMutex);
        std::map<uint16_t, stream_t>::iterator itr = streams.begin();
        for (; itr != streams.end(); ++itr) {
            itr->second->setDead(END_STREAM_DISCONNECTED);
        }
    }
}

void DcpProducer::notifyStreamReady(uint16_t vbucket, bool schedule) {
    if (ready.pushUnique(vbucket)) {
        log.unpauseIfSpaceAvailable();
    }
}

void DcpProducer::notifyPaused(bool schedule) {
    engine_.getDcpConnMap().notifyPausedConnection(this, schedule);
}

ENGINE_ERROR_CODE DcpProducer::maybeSendNoop(struct dcp_message_producers* producers) {
    if (noopCtx.enabled) {
        size_t sinceTime = ep_current_time() - noopCtx.sendTime;
        if (noopCtx.pendingRecv && sinceTime > noopCtx.noopInterval) {
            LOG(EXTENSION_LOG_WARNING, "%s Disconnected because the connection"
                " appears to be dead", logHeader());
            return ENGINE_DISCONNECT;
        } else if (!noopCtx.pendingRecv && sinceTime > noopCtx.noopInterval) {
            ENGINE_ERROR_CODE ret;
            EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
            ret = producers->noop(getCookie(), ++noopCtx.opaque);
            ObjectRegistry::onSwitchThread(epe);

            if (ret == ENGINE_SUCCESS) {
                ret = ENGINE_WANT_MORE;
            }
            noopCtx.pendingRecv = true;
            noopCtx.sendTime = ep_current_time();
            lastSendTime = ep_current_time();
            return ret;
        }
    }
    return ENGINE_FAILED;
}

bool DcpProducer::isTimeForNoop() {
    // Not Implemented
    return false;
}

void DcpProducer::setTimeForNoop() {
    // Not Implemented
}

void DcpProducer::clearQueues() {
    WriterLockHolder wlh(streamsMutex);
    std::map<uint16_t, stream_t>::iterator itr = streams.begin();
    for (; itr != streams.end(); ++itr) {
        itr->second->clear();
    }
}

void DcpProducer::appendQueue(std::list<queued_item> *q) {
    (void) q;
    abort(); // Not Implemented
}

size_t DcpProducer::getBackfillQueueSize() {
    return totalBackfillBacklogs;
}

size_t DcpProducer::getItemsSent() {
    return itemsSent;
}

size_t DcpProducer::getItemsRemaining() {
    size_t remainingSize = 0;
    ReaderLockHolder rlh(streamsMutex);
    std::map<uint16_t, stream_t>::iterator itr = streams.begin();
    for (; itr != streams.end(); ++itr) {
        Stream *s = (itr->second).get();

        if (s->getType() == STREAM_ACTIVE) {
            ActiveStream *as = static_cast<ActiveStream *>(s);
            remainingSize += as->getItemsRemaining();
        }
    }

    return remainingSize;
}

size_t DcpProducer::getTotalBytes() {
    return totalBytesSent;
}

stream_t DcpProducer::findStreamByVbid(uint16_t vbid) {
    ReaderLockHolder rlh(streamsMutex);
    stream_t stream;
    std::map<uint16_t, stream_t>::iterator itr = streams.find(vbid);
    if (itr != streams.end()) {
        stream = itr->second;
    }
    return stream;
}

std::list<uint16_t> DcpProducer::getVBList() {
    ReaderLockHolder rlh(streamsMutex);
    std::list<uint16_t> vblist;
    std::map<uint16_t, stream_t>::iterator itr = streams.begin();
    for (; itr != streams.end(); ++itr) {
        vblist.push_back(itr->first);
    }
    return vblist;
}

bool DcpProducer::windowIsFull() {
    abort(); // Not Implemented
}

void DcpProducer::flush() {
    abort(); // Not Implemented
}

bool DcpProducer::bufferLogInsert(size_t bytes) {
    return log.insert(bytes);
}

void DcpProducer::scheduleCheckpointProcessorTask(stream_t s) {
    static_cast<ActiveStreamCheckpointProcessorTask*>(checkpointCreatorTask.get())
        ->schedule(s);
}

void DcpProducer::clearCheckpointProcessorTaskQueues() {
    static_cast<ActiveStreamCheckpointProcessorTask*>(checkpointCreatorTask.get())
        ->clearQueues();
}
