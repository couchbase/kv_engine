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
#include "upr-producer.h"
#include "upr-response.h"
#include "upr-stream.h"

bool BufferLog::insert(UprResponse* response) {
    assert(!log_full);
    if (bytes_sent == 0) {
        bytes_sent += response->getMessageSize();
        return true;
    }

    if (max_bytes >= (response->getMessageSize() + bytes_sent)) {
        bytes_sent += response->getMessageSize();
        return true;
    }

    reject = response;
    log_full = true;
    return false;
}

void BufferLog::free(uint32_t bytes_to_free) {
    log_full = false;
    if (bytes_sent >= bytes_to_free) {
        bytes_sent -= bytes_to_free;
    } else {
        bytes_sent = 0;
    }
}

UprProducer::UprProducer(EventuallyPersistentEngine &e, const void *cookie,
                         const std::string &name, bool isNotifier)
    : Producer(e, cookie, name), notifyOnly(isNotifier), log(NULL) {
    setSupportAck(true);
    setReserved(true);

    if (notifyOnly) {
        setLogHeader("UPR (Notifier) " + getName() + " -");
    } else {
        setLogHeader("UPR (Producer) " + getName() + " -");
    }
}

UprProducer::~UprProducer() {}

ENGINE_ERROR_CODE UprProducer::streamRequest(uint32_t flags,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint64_t vbucket_uuid,
                                             uint64_t high_seqno,
                                             uint64_t *rollback_seqno,
                                             upr_add_failover_log callback) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    LockHolder lh(queueLock);
    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s Stream request for vbucket %d failed "
            "because this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (!notifyOnly && start_seqno > end_seqno) {
        LOG(EXTENSION_LOG_WARNING, "%s Stream request for vbucket %d failed "
            "because the start seqno (%llu) is larger than the end seqno "
            "(%llu)", logHeader(), vbucket, start_seqno, end_seqno);
        return ENGINE_ERANGE;
    }

    std::map<uint16_t, stream_t>::iterator itr;
    if ((itr = streams.find(vbucket)) != streams.end()) {
        if (itr->second->getState() != STREAM_DEAD) {
            LOG(EXTENSION_LOG_WARNING, "%s Stream request for vbucket %d failed"
                " because a stream already exists for this vbucket",
                logHeader(), vbucket);
            return ENGINE_KEY_EEXISTS;
        } else {
            streams.erase(vbucket);
            ready.remove(vbucket);
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
                                            vbucket_uuid, high_seqno,
                                            rollback_seqno)) {
        LOG(EXTENSION_LOG_WARNING, "%s Stream request for vbucket %d failed "
            "because a rollback to seqno %llu is required (start seqno %llu, "
            "vb_uuid %llu, high_seqno %llu)", logHeader(), vbucket,
            *rollback_seqno, start_seqno, vbucket_uuid, high_seqno);
        return ENGINE_ROLLBACK;
    }

    ENGINE_ERROR_CODE rv = vb->failovers->addFailoverLog(getCookie(), callback);
    if (rv != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s Couldn't add failover log to stream "
            "request due to error %d", logHeader(), rv);
        return rv;
    }

    if (notifyOnly) {
        streams[vbucket] = new NotifierStream(&engine_, this, getName(), flags,
                                              opaque, vbucket, notifySeqno,
                                              end_seqno, vbucket_uuid,
                                              high_seqno);
    } else {
        streams[vbucket] = new ActiveStream(&engine_, this, getName(), flags,
                                            opaque, vbucket, start_seqno,
                                            end_seqno, vbucket_uuid,
                                            high_seqno);
        static_cast<ActiveStream*>(streams[vbucket].get())->setActive();
    }

    LOG(EXTENSION_LOG_WARNING, "%s Stream created for vbucket %d", logHeader(),
        vbucket);
    ready.push_back(vbucket);
    return rv;
}

ENGINE_ERROR_CODE UprProducer::getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                              upr_add_failover_log callback) {
    (void) opaque;
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s Get Failover Log for vbucket %d failed "
            "because this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    return vb->failovers->addFailoverLog(getCookie(), callback);
}

ENGINE_ERROR_CODE UprProducer::step(struct upr_message_producers* producers) {
    setLastWalkTime();

    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    UprResponse *resp = getNextItem();
    if (!resp) {
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (resp->getEvent()) {
        case UPR_STREAM_END:
        {
            StreamEndResponse *se = static_cast<StreamEndResponse*> (resp);
            ret = producers->stream_end(getCookie(), se->getOpaque(),
                                        se->getVbucket(), se->getFlags());
            break;
        }
        case UPR_MUTATION:
        {
            MutationResponse *m = dynamic_cast<MutationResponse*> (resp);
            ret = producers->mutation(getCookie(), m->getOpaque(), m->getItem(),
                                      m->getVBucket(), m->getBySeqno(),
                                      m->getRevSeqno(), 0, NULL, 0,
                                      m->getItem()->getNRUValue());
            break;
        }
        case UPR_DELETION:
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
        case UPR_SNAPSHOT_MARKER:
        {
            SnapshotMarker *s = static_cast<SnapshotMarker*>(resp);
            ret = producers->marker(getCookie(), s->getOpaque(),
                                    s->getVBucket());
            break;
        }
        case UPR_SET_VBUCKET:
        {
            SetVBucketState *s = static_cast<SetVBucketState*>(resp);
            ret = producers->set_vbucket_state(getCookie(), s->getOpaque(),
                                               s->getVBucket(), s->getState());
            break;
        }
        default:
            LOG(EXTENSION_LOG_WARNING, "%s Unexpected upr event (%d), "
                "disconnecting", logHeader(), resp->getEvent());
            ret = ENGINE_DISCONNECT;
            break;
    }
    delete resp;

    if (ret == ENGINE_SUCCESS) {
        return ENGINE_WANT_MORE;
    }
    return ret;
}

ENGINE_ERROR_CODE UprProducer::noop(uint32_t opaque) {
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE UprProducer::bufferAcknowledgement(uint32_t opaque,
                                                     uint16_t vbucket,
                                                     uint32_t buffer_bytes) {
    LockHolder lh(queueLock);
    if (log) {
        bool wasFull = log->isFull();

        log->free(buffer_bytes);
        lh.unlock();

        if (wasFull) {
            engine_.getUprConnMap().notifyPausedConnection(this, true);
        }
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprProducer::control(uint32_t opaque, const void* key,
                                       uint16_t nkey, const void* value,
                                       uint32_t nvalue) {
    LockHolder lh(queueLock);
    const char* param = static_cast<const char*>(key);
    int size = atoi(static_cast<const char*>(value));

    if (strncmp(param, "connection_buffer_size", nkey) == 0) {
        if (!log) {
            log = new BufferLog(size);
        }
        return ENGINE_SUCCESS;
    } else if (strncmp(param, "stream_buffer_size", nkey) == 0) {
        return ENGINE_ENOTSUP;
    }
    return ENGINE_EINVAL;
}

ENGINE_ERROR_CODE UprProducer::handleResponse(
                                        protocol_binary_response_header *resp) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    uint8_t opcode = resp->response.opcode;
    if (opcode == PROTOCOL_BINARY_CMD_UPR_SET_VBUCKET_STATE) {
        protocol_binary_response_upr_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_upr_stream_req*>(resp);
        uint32_t opaque = pkt->message.header.response.opaque;

        std::map<uint16_t, stream_t>::iterator itr;
        for (itr = streams.begin() ; itr != streams.end(); ++itr) {
            Stream *str = itr->second.get();
            if (str && str->getType() == STREAM_ACTIVE) {
                ActiveStream* as = static_cast<ActiveStream*>(str);
                if (as && opaque == itr->second->getOpaque()) {
                    as->setVBucketStateAckRecieved();
                    break;
                }
            }
        }
        return ENGINE_SUCCESS;
    } else if (opcode == PROTOCOL_BINARY_CMD_UPR_MUTATION ||
        opcode == PROTOCOL_BINARY_CMD_UPR_DELETION ||
        opcode == PROTOCOL_BINARY_CMD_UPR_EXPIRATION ||
        opcode == PROTOCOL_BINARY_CMD_UPR_SNAPSHOT_MARKER ||
        opcode == PROTOCOL_BINARY_CMD_UPR_STREAM_END) {
        // TODO: When nacking is implemented we need to handle these responses
        return ENGINE_SUCCESS;
    }

    LOG(EXTENSION_LOG_WARNING, "%s Trying to handle an unknown response %d, "
        "disconnecting", logHeader(), opcode);

    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE UprProducer::closeStream(uint32_t opaque, uint16_t vbucket) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    LockHolder lh(queueLock);
    std::map<uint16_t, stream_t>::iterator itr;
    if ((itr = streams.find(vbucket)) == streams.end()) {
        LOG(EXTENSION_LOG_WARNING, "%s Cannot close stream for vbucket %d "
                "because no stream exists for this vbucket", logHeader(), vbucket);
        return ENGINE_KEY_ENOENT;
    } else {
        if (!itr->second->isActive()) {
            LOG(EXTENSION_LOG_WARNING, "%s Cannot close stream for vbucket %d "
                    "as stream is already marked as dead", logHeader(), vbucket);
            streams.erase(vbucket);
            ready.remove(vbucket);
            return ENGINE_KEY_ENOENT;
        }
    }

    stream_t stream = itr->second;
    streams.erase(vbucket);
    ready.remove(vbucket);
    lh.unlock();

    stream->setDead(END_STREAM_CLOSED);
    return ENGINE_SUCCESS;
}

void UprProducer::addStats(ADD_STAT add_stat, const void *c) {
    ConnHandler::addStats(add_stat, c);

    LockHolder lh(queueLock);

    if (log) {
        addStat("max_buffer", log->getBufferSize(), add_stat, c);
        addStat("bytes_sent", log->getBytesSent(), add_stat, c);
        addStat("flow_control", "enabled", add_stat, c);
    } else {
        addStat("flow_control", "disabled", add_stat, c);
    }

    std::map<uint16_t, stream_t>::iterator itr;
    for (itr = streams.begin(); itr != streams.end(); ++itr) {
        itr->second->addStats(add_stat, c);
    }
}

void UprProducer::addTakeoverStats(ADD_STAT add_stat, const void* c,
                                   uint16_t vbid) {
    LockHolder lh(queueLock);
    std::map<uint16_t, stream_t>::iterator itr = streams.find(vbid);
    if (itr != streams.end()) {
        Stream *s = itr->second.get();
        if (s && s->getType() == STREAM_ACTIVE) {
            ActiveStream* as = static_cast<ActiveStream*>(s);
            if (as) {
                as->addTakeoverStats(add_stat, c);
            }
        }
    }
}

void UprProducer::aggregateQueueStats(ConnCounter* aggregator) {
    LockHolder lh(queueLock);
    if (!aggregator) {
        LOG(EXTENSION_LOG_WARNING, "%s Pointer to the queue stats aggregator"
            " is NULL!!!", logHeader());
        return;
    }

    aggregator->conn_queueBackfillRemaining += totalBackfillBacklogs;
}

void UprProducer::notifySeqnoAvailable(uint16_t vbucket, uint64_t seqno) {
    LockHolder lh(queueLock);
    std::map<uint16_t, stream_t>::iterator itr = streams.find(vbucket);
    if (itr != streams.end() && itr->second->isActive()) {
        stream_t stream = itr->second;
        lh.unlock();
        stream->notifySeqnoAvailable(seqno);
    }
}

void UprProducer::vbucketStateChanged(uint16_t vbucket, vbucket_state_t state) {
    LockHolder lh(queueLock);
    std::map<uint16_t, stream_t>::iterator itr = streams.find(vbucket);
    if (itr != streams.end()) {
        stream_t stream = itr->second;
        lh.unlock();
        stream->setDead(END_STREAM_STATE);
    }
}

void UprProducer::closeAllStreams() {
    LockHolder lh(queueLock);
    std::map<uint16_t, stream_t>::iterator itr = streams.begin();
    for (; itr != streams.end(); ++itr) {
        itr->second->setDead(END_STREAM_DISCONNECTED);
    }
}

const char* UprProducer::getType() const {
    if (notifyOnly) {
        return "notifier";
    } else {
        return "producer";
    }
}

UprResponse* UprProducer::getNextItem() {
    LockHolder lh(queueLock);

    UprResponse* op;
    if (log) {
        if (log->isFull()) {
            setPaused(true);
            return NULL;
        } else if ((op = log->getRejectResponse())) {
            log->clearRejectResponse();
            return op;
        }
    }

    setPaused(false);
    while (!ready.empty()) {
        uint16_t vbucket = ready.front();
        ready.pop_front();

        if (streams.find(vbucket) == streams.end()) {
            continue;
        }
        op = streams[vbucket]->next();
        if (!op) {
            continue;
        }

        switch (op->getEvent()) {
            case UPR_SNAPSHOT_MARKER:
            case UPR_MUTATION:
            case UPR_DELETION:
            case UPR_STREAM_END:
            case UPR_SET_VBUCKET:
                break;
            default:
                LOG(EXTENSION_LOG_WARNING, "%s Producer is attempting to write"
                    " an unexpected event %d", logHeader(), op->getEvent());
                abort();
        }

        ready.push_front(vbucket);
        if (log && !log->insert(op)) {
            setPaused(true);
            return NULL;
        }

        return op;
    }

    setPaused(true);
    return NULL;
}

bool UprProducer::isValidStream(uint32_t opaque, uint16_t vbucket) {
    std::map<uint16_t, stream_t>::iterator itr = streams.find(vbucket);
    if (itr != streams.end() && opaque == itr->second->getOpaque() &&
        itr->second->isActive()) {
        return true;
    }
    return false;
}

void UprProducer::setDisconnect(bool disconnect) {
    ConnHandler::setDisconnect(disconnect);

    if (disconnect) {
        LockHolder lh(queueLock);
        std::map<uint16_t, stream_t>::iterator itr = streams.begin();
        for (; itr != streams.end(); ++itr) {
            itr->second->setDead(END_STREAM_DISCONNECTED);
        }
    }
}

void UprProducer::notifyStreamReady(uint16_t vbucket, bool schedule) {
    LockHolder lh(queueLock);

    std::list<uint16_t>::iterator iter =
        std::find(ready.begin(), ready.end(), vbucket);
    if (iter != ready.end()) {
        return;
    }

    ready.push_back(vbucket);
    lh.unlock();

    if (!log || (log && !log->isFull())) {
        engine_.getUprConnMap().notifyPausedConnection(this, schedule);
    }
}

bool UprProducer::isTimeForNoop() {
    // Not Implemented
    return false;
}

void UprProducer::setTimeForNoop() {
    // Not Implemented
}

void UprProducer::clearQueues() {
    LockHolder lh(queueLock);
    std::map<uint16_t, stream_t>::iterator itr = streams.begin();
    for (; itr != streams.end(); ++itr) {
        itr->second->clear();
    }
}

void UprProducer::appendQueue(std::list<queued_item> *q) {
    (void) q;
    abort(); // Not Implemented
}

size_t UprProducer::getBackfillQueueSize() {
    return totalBackfillBacklogs;
}

bool UprProducer::windowIsFull() {
    abort(); // Not Implemented
}

void UprProducer::flush() {
    abort(); // Not Implemented
}
