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
#include "tapconnmap.h"
#include "upr-consumer.h"
#include "upr-response.h"
#include "upr-stream.h"

UprConsumer::UprConsumer(EventuallyPersistentEngine &engine, const void *cookie,
                         const std::string &name)
    : Consumer(engine, cookie, name), opaqueCounter(0) {
    setSupportAck(false);
    setLogHeader("UPR (Consumer) " + getName() + " -");
    setReserved(true);
}

UprConsumer::~UprConsumer() {
    std::map<uint16_t, PassiveStream*>::iterator itr = streams_.begin();
    for (; itr != streams_.end(); ++itr) {
        delete itr->second;
    }
    streams_.clear();
}

ENGINE_ERROR_CODE UprConsumer::addStream(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    LockHolder lh(streamMutex);
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        LOG(EXTENSION_LOG_WARNING, "%s Add stream for vbucket %d failed because"
            "this vbucket doesn't exist", logHeader(), vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }

    failover_entry_t entry = vb->failovers->getLatestEntry();
    uint64_t start_seqno = vb->getHighSeqno();
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vbucket_uuid = entry.vb_uuid;
    uint64_t high_seqno = entry.by_seqno;
    uint32_t new_opaque = ++opaqueCounter;

    std::map<uint16_t, PassiveStream*>::iterator itr = streams_.find(vbucket);
    if (itr != streams_.end() && itr->second->isActive()) {
        LOG(EXTENSION_LOG_WARNING, "%s Cannot add stream for vbucket %d because"
            "one already exists", logHeader(), vbucket);
        return ENGINE_KEY_EEXISTS;
    } else if (itr != streams_.end()) {
        delete itr->second;
        streams_.erase(itr);
        ready.remove(vbucket);
    }

    streams_[vbucket] = new PassiveStream(this, getName(), flags, new_opaque,
                                          vbucket, start_seqno, end_seqno,
                                          vbucket_uuid, high_seqno);
    ready.push_back(vbucket);
    opaqueMap_[new_opaque] = std::make_pair(opaque, vbucket);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprConsumer::closeStream(uint32_t opaque, uint16_t vbucket) {
    LockHolder lh(streamMutex);
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        opaqueMap_.erase(oitr);
    }

    std::map<uint16_t, PassiveStream*>::iterator itr;
    if ((itr = streams_.find(vbucket)) == streams_.end()) {
        LOG(EXTENSION_LOG_WARNING, "%s Cannot close stream for vbucket %d "
            "because no stream exists for this vbucket", logHeader(), vbucket);
        return ENGINE_KEY_ENOENT;
    }

    itr->second->setDead();
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprConsumer::streamEnd(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    if (closeStream(opaque, vbucket) == ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_INFO, "%s end stream received with reason %d",
            logHeader(), flags);
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s end stream received but vbucket %d with"
            " opaque %d does not exist", logHeader(), vbucket, opaque);
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprConsumer::mutation(uint32_t opaque, const void* key,
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

    if (!isValidOpaque(opaque, vbucket)) {
        LOG(EXTENSION_LOG_INFO, "%s Dropping upr mutation for vbucket %d with "
            "opaque %ld because the stream is no longer valid", logHeader(),
            vbucket, opaque);
        return ENGINE_FAILED;
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_active) {
        LOG(EXTENSION_LOG_INFO, "%s Dropping upr mutation for vbucket %d with "
            "opaque %ld because the vbucket state is no longer valid",
            logHeader(), vbucket, opaque);
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (bySeqno <= (uint64_t)vb->getHighSeqno()) {
        LOG(EXTENSION_LOG_INFO, "%s Dropping upr mutation for vbucket %d with "
            "opaque %ld because the byseqno given (%llu) must be larger than"
            "%llu", logHeader(), vbucket, opaque, bySeqno, vb->getHighSeqno());
        return ENGINE_ERANGE;
    }

    std::string key_str(static_cast<const char*>(key), nkey);
    value_t vblob(Blob::New(static_cast<const char*>(value), nvalue,
                            &(datatype), (uint8_t) EXT_META_LEN));
    Item *item = new Item(key_str, flags, exptime, vblob, cas, bySeqno,
                          vbucket, revSeqno);

    ENGINE_ERROR_CODE ret;
    if (isBackfillPhase(vbucket)) {
        ret = engine_.getEpStore()->addTAPBackfillItem(*item, nru);
    } else {
        ret = engine_.getEpStore()->setWithMeta(*item, 0, getCookie(), true,
                                                true, nru, false);
    }

    return ret;
}

ENGINE_ERROR_CODE UprConsumer::deletion(uint32_t opaque, const void* key,
                                        uint16_t nkey, uint64_t cas,
                                        uint16_t vbucket, uint64_t bySeqno,
                                        uint64_t revSeqno, const void* meta,
                                        uint16_t nmeta) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    if (!isValidOpaque(opaque, vbucket)) {
        LOG(EXTENSION_LOG_INFO, "%s Dropping upr deletion for vbucket %d with "
            "opaque %ld because the stream is no longer valid", logHeader(),
            vbucket, opaque);
        return ENGINE_FAILED;
    }

    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_active) {
        LOG(EXTENSION_LOG_INFO, "%s Dropping upr deletion for vbucket %d with "
            "opaque %ld because the vbucket state is no longer valid",
            logHeader(), vbucket, opaque);
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (bySeqno <= (uint64_t)vb->getHighSeqno()) {
        LOG(EXTENSION_LOG_INFO, "%s Dropping upr deletion for vbucket %d with "
            "opaque %ld because the byseqno given (%llu) must be larger than"
            "%llu", logHeader(), vbucket, opaque, bySeqno, vb->getHighSeqno());
        return ENGINE_ERANGE;
    }

    uint64_t delCas = 0;
    ItemMetaData itemMeta(cas, revSeqno, 0, 0);
    std::string key_str((const char*)key, nkey);

    ENGINE_ERROR_CODE ret;
    ret = engine_.getEpStore()->deleteWithMeta(key_str, &delCas, vbucket,
                                               getCookie(), true, &itemMeta,
                                               isBackfillPhase(vbucket), false,
                                               bySeqno);

    if (ret == ENGINE_KEY_ENOENT) {
        ret = ENGINE_SUCCESS;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprConsumer::expiration(uint32_t opaque, const void* key,
                                          uint16_t nkey, uint64_t cas,
                                          uint16_t vbucket, uint64_t bySeqno,
                                          uint64_t revSeqno, const void* meta,
                                          uint16_t nmeta) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE UprConsumer::snapshotMarker(uint32_t opaque,
                                              uint16_t vbucket) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprConsumer::flush(uint32_t opaque, uint16_t vbucket) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE UprConsumer::setVBucketState(uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    if (isValidOpaque(opaque, vbucket)) {
        return Consumer::setVBucketState(opaque, vbucket, state);
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s Invalid vbucket (%d) and opaque (%ld) "
            "received for set vbucket state message", logHeader(), vbucket,
            opaque);
        return ENGINE_FAILED;
    }
}

ENGINE_ERROR_CODE UprConsumer::step(struct upr_message_producers* producers) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    UprResponse *resp = getNextItem();
    if (resp == NULL) {
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    switch (resp->getEvent()) {
        case UPR_ADD_STREAM:
        {
            AddStreamResponse *as = static_cast<AddStreamResponse*>(resp);
            ret = producers->add_stream_rsp(getCookie(), as->getOpaque(),
                                            as->getStreamOpaque(),
                                            as->getStatus());
            break;
        }
        case UPR_STREAM_REQ:
        {
            StreamRequest *sr = static_cast<StreamRequest*> (resp);
            ret = producers->stream_req(getCookie(), sr->getOpaque(),
                                        sr->getVBucket(), sr->getFlags(),
                                        sr->getStartSeqno(), sr->getEndSeqno(),
                                        sr->getVBucketUUID(),
                                        sr->getHighSeqno());
            break;
        }
        default:
            LOG(EXTENSION_LOG_WARNING, "%s Unknown consumer event (%d), "
                "disconnecting", logHeader(), resp->getEvent());
            ret = ENGINE_DISCONNECT;
    }
    delete resp;

    if (ret == ENGINE_SUCCESS) {
        return ENGINE_WANT_MORE;
    }
    return ret;
}

bool RollbackTask::run() {
    cons->doRollback(engine->getEpStore(), opaque, vbid, rollbackSeqno);
    ++(engine->getEpStats().rollbackCount);
    return false;
}

ENGINE_ERROR_CODE UprConsumer::handleResponse(
                                        protocol_binary_response_header *resp) {
    if (doDisconnect()) {
        return ENGINE_DISCONNECT;
    }

    uint8_t opcode = resp->response.opcode;
    if (opcode == PROTOCOL_BINARY_CMD_UPR_STREAM_REQ) {
        protocol_binary_response_upr_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_upr_stream_req*>(resp);

        uint16_t status = ntohs(pkt->message.header.response.status);
        uint32_t opaque = pkt->message.header.response.opaque;
        uint64_t bodylen = ntohl(pkt->message.header.response.bodylen);
        uint8_t* body = pkt->bytes + sizeof(protocol_binary_response_header);

        if (status == ENGINE_ROLLBACK) {
            assert(bodylen == sizeof(uint64_t));
            uint64_t rollbackSeqno = 0;
            memcpy(&rollbackSeqno, body, sizeof(uint64_t));
            rollbackSeqno = ntohll(rollbackSeqno);

            opaque_map::iterator oitr = opaqueMap_.find(opaque);
            if (oitr != opaqueMap_.end()) {
                uint16_t vbid = oitr->second.second;
                if (isValidOpaque(opaque, vbid)) {
                    ExTask task = new RollbackTask(&engine_,
                                                   opaque, vbid,
                                                   rollbackSeqno, this,
                                                   Priority::TapBgFetcherPriority);
                    ExecutorPool::get()->schedule(task, READER_TASK_IDX);
                    return ENGINE_SUCCESS;
                } else {
                    LOG(EXTENSION_LOG_WARNING, "%s : Opaque %lu for vbid %u "
                            "not valid!", logHeader(), opaque, vbid);
                    return ENGINE_FAILED;
                }
            } else {
                LOG(EXTENSION_LOG_WARNING, "%s Opaque not found",
                        logHeader());
                return ENGINE_FAILED;
            }
        }

        if (((bodylen % 16) != 0 || bodylen == 0) && status == ENGINE_SUCCESS) {
            LOG(EXTENSION_LOG_WARNING, "%s Got a stream response with a bad "
                "failover log (length %llu), disconnecting", logHeader(),
                bodylen);
            return ENGINE_DISCONNECT;
        }

        streamAccepted(opaque, status, body, bodylen);
        return ENGINE_SUCCESS;
    }

    LOG(EXTENSION_LOG_WARNING, "%s Trying to handle an unknown response %d, "
        "disconnecting", logHeader(), opcode);

    return ENGINE_DISCONNECT;
}

void UprConsumer::doRollback(EventuallyPersistentStore *st,
                             uint32_t opaque,
                             uint16_t vbid,
                             uint64_t rollbackSeqno) {
    shared_ptr<RollbackCB> cb(new RollbackCB(engine_));
    ENGINE_ERROR_CODE errCode = ENGINE_SUCCESS;
    errCode =  engine_.getEpStore()->rollback(vbid, rollbackSeqno, cb);
    if (errCode == ENGINE_ROLLBACK) {
        if (engine_.getEpStore()->resetVBucket(vbid)) {
            errCode = ENGINE_SUCCESS;
        } else {
            LOG(EXTENSION_LOG_WARNING, "Vbucket %d not found for rollback",
                    vbid);
            errCode = ENGINE_FAILED;
        }
    }

    if (errCode == ENGINE_SUCCESS) {
        RCPtr<VBucket> vb = st->getVBucket(vbid);
        streams_[vbid]->reconnectStream(vb, opaque, rollbackSeqno);
    } else {
        //TODO: If rollback failed due to internal errors, we need to
        //send an error message back to producer, so that it can terminate
        //the connection.
        LOG(EXTENSION_LOG_WARNING, "%s Rollback failed",
                logHeader());
        opaqueMap_.erase(opaque);
    }
}

void UprConsumer::addStats(ADD_STAT add_stat, const void *c) {
    ConnHandler::addStats(add_stat, c);

    LockHolder lh(streamMutex);
    std::map<uint16_t, PassiveStream*>::iterator itr;
    for (itr = streams_.begin(); itr != streams_.end(); ++itr) {
        itr->second->addStats(add_stat, c);
    }
}

UprResponse* UprConsumer::getNextItem() {
    LockHolder lh(streamMutex);

    while (!ready.empty()) {
        uint16_t vbucket = ready.front();
        ready.pop_front();

        UprResponse* op = streams_[vbucket]->next();
        if (!op) {
            continue;
        }
        switch (op->getEvent()) {
            case UPR_STREAM_REQ:
            case UPR_ADD_STREAM:
                break;
            default:
                LOG(EXTENSION_LOG_WARNING, "%s Consumer is attempting to write"
                    " an unexpected event %d", logHeader(), op->getEvent());
                abort();
        }

        ready.push_back(vbucket);
        return op;
    }
    return NULL;
}

void UprConsumer::notifyStreamReady(uint16_t vbucket) {
    std::list<uint16_t>::iterator iter =
        std::find(ready.begin(), ready.end(), vbucket);
    if (iter != ready.end()) {
        return;
    }

    bool notify = ready.empty();
    ready.push_back(vbucket);

    if (notify) {
        engine_.getUprConnMap().notifyPausedConnection(this, true);
    }
}

void UprConsumer::streamAccepted(uint32_t opaque, uint16_t status, uint8_t* body,
                                 uint32_t bodylen) {
    LockHolder lh(streamMutex);

    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        uint32_t add_opaque = oitr->second.first;
        uint16_t vbucket = oitr->second.second;
        std::map<uint16_t, PassiveStream*>::iterator sitr = streams_.find(vbucket);
        if (sitr != streams_.end() && sitr->second->getOpaque() == opaque &&
            sitr->second->getState() == STREAM_PENDING) {
            if (status == ENGINE_SUCCESS) {
                RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
                vb->failovers->replaceFailoverLog(body, bodylen);
                EventuallyPersistentStore* st = engine_.getEpStore();
                st->scheduleVBSnapshot(Priority::VBucketPersistHighPriority,
                                st->getVBuckets().getShard(vbucket)->getId());
            }
            LOG(EXTENSION_LOG_INFO, "%s Add stream for vbucket (%d) and opaque "
                "%ld was successful with error code %d", logHeader(), vbucket,
                opaque, status);
            sitr->second->acceptStream(status, add_opaque);
        } else {
            LOG(EXTENSION_LOG_WARNING, "%s Trying to add stream, but none "
                "exists (opaque: %d, add_opaque: %d)", logHeader(), opaque,
                add_opaque);
        }
        opaqueMap_.erase(opaque);
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s No opaque for add stream response",
            logHeader());
    }
}

bool UprConsumer::isValidOpaque(uint32_t opaque, uint16_t vbucket) {
    LockHolder lh(streamMutex);
    std::map<uint16_t, PassiveStream*>::iterator itr = streams_.find(vbucket);
    return itr != streams_.end() && itr->second->getOpaque() == opaque;
}
