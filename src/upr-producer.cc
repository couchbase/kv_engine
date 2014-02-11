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
#include "upr-stream.h"

UprProducer::UprProducer(EventuallyPersistentEngine &e, const void *cookie,
                         const std::string &n)
    : Producer(e) {
    conn_ = new Connection(this, cookie, n);
    conn_->setSupportAck(true);
    conn_->setLogHeader("UPR (Producer) " + conn_->getName() + " -");
    setReserved(false);
}

ENGINE_ERROR_CODE UprProducer::streamRequest(uint32_t flags,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint64_t vbucket_uuid,
                                             uint64_t high_seqno,
                                             uint64_t *rollback_seqno,
                                             upr_add_failover_log callback) {
    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s Stream request for vbucket %d failed "
            "because this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (start_seqno > end_seqno) {
        LOG(EXTENSION_LOG_WARNING, "%s Stream request for vbucket %d failed "
            "because the start seqno (%llu) is larger than the end seqno "
            "(%llu)", logHeader(), vbucket, start_seqno, end_seqno);
        return ENGINE_ERANGE;
    }

    std::map<uint16_t, ActiveStream*>::iterator itr = streams.find(vbucket);
    if (itr != streams.end()) {
        if (itr->second->getState() != STREAM_DEAD) {
            LOG(EXTENSION_LOG_WARNING, "%s Stream request for vbucket %d failed"
                " because a stream already exists for this vbucket",
                logHeader(), vbucket);
            return ENGINE_KEY_EEXISTS;
        } else {
            delete itr->second;
            streams.erase(vbucket);
        }
    }

    if(vb->failovers->needsRollback(start_seqno, vb->getHighSeqno(),
                                    vbucket_uuid, high_seqno, rollback_seqno)) {
        LOG(EXTENSION_LOG_WARNING, "%s Stream request for vbucket %d failed "
            "because a rollback to seqno %llu is required (start seqno %llu, "
            "vb_uuid %llu, high_seqno %llu)", logHeader(), vbucket,
            *rollback_seqno, start_seqno, vbucket_uuid, high_seqno);
        if((*rollback_seqno) == 0) {
            // rollback point of 0 indicates that the entry was missing
            // entirely, report as key not found per transport spec.
            return ENGINE_KEY_ENOENT;
        }
        return ENGINE_ROLLBACK;
    }

    ENGINE_ERROR_CODE rv = vb->failovers->addFailoverLog(conn_->cookie, callback);
    if (rv != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s Couldn't add failover log to stream "
            "request due to error %d", logHeader(), rv);
        return rv;
    }

    streams[vbucket] = new ActiveStream(&engine_, conn_->name, flags,
                                        opaque, vbucket, start_seqno, end_seqno,
                                        vbucket_uuid, high_seqno);
    streams[vbucket]->setActive();

    return rv;
}

ENGINE_ERROR_CODE UprProducer::getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                              upr_add_failover_log callback) {
    (void) opaque;
    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s Get Failover Log for vbucket %d failed "
            "because this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    return vb->failovers->addFailoverLog(conn_->cookie, callback);
}

ENGINE_ERROR_CODE UprProducer::step(struct upr_message_producers* producers) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    UprResponse *resp = getNextItem();

    if (!resp) {
        return ENGINE_SUCCESS;
    }

    switch (resp->getEvent()) {
        case UPR_STREAM_END:
        {
            StreamEndResponse *se = static_cast<StreamEndResponse*> (resp);
            producers->stream_end(conn_->cookie, se->getOpaque(),
                                  se->getVbucket(), se->getFlags());
            break;
        }
        case UPR_MUTATION:
        {
            MutationResponse *m = dynamic_cast<MutationResponse*> (resp);
            producers->mutation(conn_->cookie, m->getOpaque(), m->getItem(),
                                m->getVBucket(), m->getBySeqno(),
                                m->getRevSeqno(), 0, NULL, 0,
                                INITIAL_NRU_VALUE);
            break;
        }
        case UPR_DELETION:
        {
            MutationResponse *m = static_cast<MutationResponse*>(resp);
            producers->deletion(conn_->cookie, m->getOpaque(),
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
            producers->marker(conn_->cookie, s->getOpaque(), s->getVBucket());
            break;
        }
        case UPR_SET_VBUCKET:
        {
            SetVBucketState *s = static_cast<SetVBucketState*>(resp);
            producers->set_vbucket_state(conn_->cookie, s->getOpaque(),
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
    return ret;
}

ENGINE_ERROR_CODE UprProducer::handleResponse(
                                        protocol_binary_response_header *resp) {
    uint8_t opcode = resp->response.opcode;
    if (opcode == PROTOCOL_BINARY_CMD_UPR_SET_VBUCKET_STATE) {
        protocol_binary_response_upr_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_upr_stream_req*>(resp);
        uint32_t opaque = pkt->message.header.response.opaque;

        std::map<uint16_t, ActiveStream*>::iterator itr;
        for (itr = streams.begin() ; itr != streams.end(); ++itr) {
            if (opaque == itr->second->getOpaque()) {
                itr->second->setVBucketStateAckRecieved();
                break;
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
    std::map<uint16_t, ActiveStream*>::iterator itr;
    for (itr = streams.begin() ; itr != streams.end(); ++itr) {
        if (vbucket == itr->second->getVBucket()) {
            // Found the stream for the vbucket.. nuke it!
            // @todo is there any pending tasks that is running for the
            //       stream that I need to wait for?
            Stream *stream = itr->second;
            streams.erase(itr);
            delete stream;
            return ENGINE_SUCCESS;
        }
    }

    LOG(EXTENSION_LOG_WARNING, "%s Failed to close stream for vbucket %d "
        "because no stream exists for that vbucket", logHeader(), vbucket);

    return ENGINE_NOT_MY_VBUCKET;
}

void UprProducer::addStats(ADD_STAT add_stat, const void *c) {
    ConnHandler::addStats(add_stat, c);

    LockHolder lh(queueLock);
    std::map<uint16_t, ActiveStream*>::iterator itr;
    for (itr = streams.begin(); itr != streams.end(); ++itr) {
        itr->second->addStats(add_stat, c);
    }
}

void UprProducer::addTakeoverStats(ADD_STAT add_stat, const void* c,
                                   uint16_t vbid) {
    LockHolder lh(queueLock);
    std::map<uint16_t, ActiveStream*>::iterator itr = streams.find(vbid);
    if (itr == streams.end()) {
        // Deal with no stream
        return;
    }

    itr->second->addTakeoverStats(add_stat, c);
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

UprResponse* UprProducer::getNextItem() {
    LockHolder lh(queueLock);
    std::map<uint16_t, ActiveStream*>::iterator itr = streams.begin();
    for (; itr != streams.end(); ++itr) {
        UprResponse* op = itr->second->next();

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
        return op;
    }
    return NULL;
}

bool UprProducer::isValidStream(uint32_t opaque, uint16_t vbucket) {
    std::map<uint16_t, ActiveStream*>::iterator itr = streams.find(vbucket);
    if (itr != streams.end() && opaque == itr->second->getOpaque() &&
        itr->second->isActive()) {
        return true;
    }
    return false;
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
    std::map<uint16_t, ActiveStream*>::iterator itr = streams.begin();
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
