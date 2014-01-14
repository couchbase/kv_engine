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
#include "upr-stream.h"

ENGINE_ERROR_CODE UprConsumer::addStream(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    LockHolder lh(streamMutex);
    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    uint64_t start_seqno = vb->getHighSeqno();
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vbucket_uuid = 0;
    uint64_t high_seqno = start_seqno;
    uint32_t new_opaque = ++opaqueCounter;

    std::map<uint16_t, PassiveStream*>::iterator itr = streams_.find(vbucket);
    if (itr != streams_.end() && itr->second->isActive()) {
        return ENGINE_KEY_EEXISTS;
    } else if (itr != streams_.end()) {
        delete itr->second;
        streams_.erase(itr);
    }

    streams_[vbucket] = new PassiveStream(conn_->name, flags, new_opaque,
                                          vbucket, start_seqno, end_seqno,
                                          vbucket_uuid, high_seqno);
    opaqueMap_[new_opaque] = std::make_pair(opaque, vbucket);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprConsumer::closeStream(uint32_t opaque, uint16_t vbucket) {
    LockHolder lh(streamMutex);

    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        opaqueMap_.erase(oitr);
    }

    std::map<uint16_t, PassiveStream*>::iterator itr;
    if ((itr = streams_.find(vbucket)) == streams_.end()) {
        return ENGINE_KEY_ENOENT;
    }

    if (itr->second->getOpaque() != opaque) {
        return ENGINE_KEY_EEXISTS;
    }

    itr->second->setDead();
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprConsumer::streamEnd(uint32_t opaque, uint16_t vbucket,
                                         uint32_t flags) {
    (void) opaque;
    (void) vbucket;
    (void) flags;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE UprConsumer::mutation(uint32_t opaque, const void* key,
                                        uint16_t nkey, const void* value,
                                        uint32_t nvalue, uint64_t cas,
                                        uint16_t vbucket, uint32_t flags,
                                        uint8_t datatype, uint32_t locktime,
                                        uint64_t bySeqno, uint64_t revSeqno,
                                        uint32_t exptime, uint8_t nru,
                                        const void* meta, uint16_t nmeta) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (!isValidOpaque(opaque, vbucket)) {
        return ENGINE_FAILED;
    }

    std::string key_str(static_cast<const char*>(key), nkey);
    value_t vblob(Blob::New(static_cast<const char*>(value), nvalue,
                                                     NULL, 0));
    Item *item = new Item(key_str, flags, exptime, vblob, cas, bySeqno,
                          vbucket, revSeqno);

    if (isBackfillPhase(vbucket)) {
        ret = engine_.getEpStore()->addTAPBackfillItem(*item, meta, nru);
    } else {
        ret = engine_.getEpStore()->setWithMeta(*item, 0, conn_->cookie, true,
                                                 true, nru);
    }
    return ret;
}

ENGINE_ERROR_CODE UprConsumer::deletion(uint32_t opaque, const void* key,
                                        uint16_t nkey, uint64_t cas,
                                        uint16_t vbucket, uint64_t bySeqno,
                                        uint64_t revSeqno, const void* meta,
                                        uint16_t nmeta) {
    ENGINE_ERROR_CODE ret;

    if (!isValidOpaque(opaque, vbucket)) {
        return ENGINE_FAILED;
    }

    uint64_t delCas = 0;
    ItemMetaData itemMeta(cas, revSeqno, 0, 0);
    std::string key_str((const char*)key, nkey);
    ret = engine_.getEpStore()->deleteWithMeta(key_str, &delCas, vbucket,
                                               conn_->cookie, true, &itemMeta,
                                               isBackfillPhase(vbucket));

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
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE UprConsumer::snapshotMarker(uint32_t opaque,
                                              uint16_t vbucket) {
    (void) opaque;
    (void) vbucket;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE UprConsumer::flush(uint32_t opaque, uint16_t vbucket) {
    (void) opaque;
    (void) vbucket;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE UprConsumer::setVBucketState(uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state) {
    (void) opaque;
    (void) vbucket;
    (void) state;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE UprConsumer::step(struct upr_message_producers* producers) {
    UprResponse *resp = getNextItem();

    if (resp == NULL) {
        return ENGINE_SUCCESS; // Change to tmpfail once mcd layer is fixed
    }

    switch (resp->getEvent()) {
        case UPR_ADD_STREAM:
        {
            AddStreamResponse *as = static_cast<AddStreamResponse*>(resp);
            producers->add_stream_rsp(conn_->cookie, as->getOpaque(),
                                      as->getStreamOpaque(), as->getStatus());
            break;
        }
        case UPR_STREAM_REQ:
        {
            StreamRequest *sr = static_cast<StreamRequest*> (resp);
            producers->stream_req(conn_->cookie, sr->getOpaque(),
                                  sr->getVBucket(), sr->getFlags(),
                                  sr->getStartSeqno(), sr->getEndSeqno(),
                                  sr->getVBucketUUID(), sr->getHighSeqno());
            break;
        }
        default:
            LOG(EXTENSION_LOG_WARNING, "Unknown consumer event, "
                "disconnecting");
            return ENGINE_DISCONNECT;
    }

    delete resp;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprConsumer::handleResponse(
                                        protocol_binary_response_header *resp) {
    uint8_t opcode = resp->response.opcode;
    if (opcode == PROTOCOL_BINARY_CMD_UPR_STREAM_REQ) {
        protocol_binary_response_upr_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_upr_stream_req*>(resp);

        uint16_t status = ntohs(pkt->message.header.response.status);
        uint32_t opaque = pkt->message.header.response.opaque;
        uint64_t bodylen = ntohl(pkt->message.header.response.bodylen);
        uint64_t rollbackSeqno = 0;

        if (bodylen == sizeof(uint64_t)) {
            memcpy(&rollbackSeqno, pkt->bytes + sizeof(pkt), sizeof(uint64_t));
            rollbackSeqno = ntohll(rollbackSeqno);
        }

        if (status == ENGINE_ROLLBACK) {
            return ENGINE_ENOTSUP;
        }

        streamAccepted(opaque, status);
        return ENGINE_SUCCESS;
    }

    LOG(EXTENSION_LOG_WARNING, "%s Trying to handle an unknown response %d, "
        "disconnecting", logHeader(), opcode);

    return ENGINE_DISCONNECT;
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
    std::map<uint16_t, PassiveStream*>::iterator itr = streams_.begin();
    for (; itr != streams_.end(); ++itr) {
        UprResponse* op = itr->second->next();

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
        return op;
    }
    return NULL;
}

void UprConsumer::streamAccepted(uint32_t opaque, uint16_t status) {
    LockHolder lh(streamMutex);
    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        uint32_t add_opaque = oitr->second.first;
        uint16_t vbucket = oitr->second.second;
        std::map<uint16_t, PassiveStream*>::iterator sitr = streams_.find(vbucket);
        if (sitr != streams_.end() && sitr->second->getOpaque() == opaque &&
            sitr->second->getState() == STREAM_PENDING) {
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
