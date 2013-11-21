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

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprAddStream(const void* cookie,
                                                           uint32_t opaque,
                                                           uint16_t vbucket,
                                                           uint32_t flags)
{
    ConnHandler* handler =
        reinterpret_cast<ConnHandler*> (getEngineSpecific(cookie));
    if (!handler) {
        return ENGINE_DISCONNECT;
    }

    UprConsumer* consumer = dynamic_cast<UprConsumer*>(handler);
    if (!consumer) {
        return ENGINE_DISCONNECT;
    }

    return consumer->addPendingStream(vbucket, opaque, flags);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprCloseStream(const void* cookie,
                                                             uint16_t vbucket)
{
    (void) cookie;
    (void) vbucket;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprStreamEnd(const void* cookie,
                                                           uint32_t opaque,
                                                           uint16_t vbucket,
                                                           uint32_t flags)
{
    (void) cookie;
    (void) opaque;
    (void) vbucket;
    (void) flags;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprSnapshotMarker(const void* cookie,
                                                                uint32_t opaque,
                                                                uint16_t vbucket)
{
    (void) cookie;
    (void) opaque;
    (void) vbucket;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprMutation(const void* cookie,
                                                          uint32_t opaque,
                                                          const void *key,
                                                          uint16_t nkey,
                                                          const void *value,
                                                          uint32_t nvalue,
                                                          uint64_t cas,
                                                          uint16_t vbucket,
                                                          uint32_t flags,
                                                          uint8_t datatype,
                                                          uint64_t bySeqno,
                                                          uint64_t revSeqno,
                                                          uint32_t expiration,
                                                          uint32_t lockTime)
{
    (void) opaque;
    (void) datatype;
    (void) bySeqno;
    (void) lockTime;
    UprConsumer* consumer = getUprConsumer(cookie);
    if (!consumer) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to lookup UPR consumer connection.. Disconnecting");
        return ENGINE_DISCONNECT;
    }

    std::string k(static_cast<const char*>(key), nkey);
    ENGINE_ERROR_CODE ret = ConnHandlerMutate(consumer, k, cookie, flags, expiration, cas,
                                              revSeqno, vbucket, true, value, nvalue);
    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprDeletion(const void* cookie,
                                                          uint32_t opaque,
                                                          const void *key,
                                                          uint16_t nkey,
                                                          uint64_t cas,
                                                          uint16_t vbucket,
                                                          uint64_t bySeqno,
                                                          uint64_t revSeqno)
{
    (void) opaque;
    (void) bySeqno;
    UprConsumer* consumer = getUprConsumer(cookie);
    if (!consumer) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to lookup UPR consumer connection.. Disconnecting");
        return ENGINE_DISCONNECT;
    }

    std::string k(static_cast<const char*>(key), nkey);
    ItemMetaData itemMeta(cas, DEFAULT_REV_SEQ_NUM, 0, 0);

    if (itemMeta.cas == 0) {
        itemMeta.cas = Item::nextCas();
    }
    itemMeta.revSeqno = (revSeqno != 0) ? revSeqno : DEFAULT_REV_SEQ_NUM;

    return ConnHandlerDelete(consumer, k, cookie, vbucket, true, itemMeta);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprExpiration(const void* cookie,
                                                            uint32_t opaque,
                                                            const void *key,
                                                            uint16_t nkey,
                                                            uint64_t cas,
                                                            uint16_t vbucket,
                                                            uint64_t bySeqno,
                                                            uint64_t revSeqno)
{
    return uprDeletion(cookie, opaque, key, nkey, cas,
                       vbucket, bySeqno, revSeqno);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprFlush(const void* cookie,
                                                       uint32_t opaque,
                                                       uint16_t vbucket)
{
    (void) opaque;
    (void) vbucket;
    LOG(EXTENSION_LOG_WARNING, "%s Received flush.\n");

    return flush(cookie, 0);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprSetVbucketState(const void* cookie,
                                                                 uint32_t opaque,
                                                                 uint16_t vbucket,
                                                                 vbucket_state_t state)
{
    (void) cookie;
    (void) opaque;
    (void) vbucket;
    (void) state;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprResponseHandler(const void* cookie,
                                                                 protocol_binary_response_header *response)
{
    uint8_t opcode = response->response.opcode;
    if (opcode == PROTOCOL_BINARY_CMD_UPR_STREAM_REQ) {
        protocol_binary_response_upr_stream_req* pkt =
            reinterpret_cast<protocol_binary_response_upr_stream_req*>(response);

        uint16_t status = ntohs(pkt->message.header.response.status);
        uint32_t opaque = ntohl(pkt->message.header.response.opaque);
        uint64_t bodylen = ntohl(pkt->message.header.response.bodylen);
        uint64_t rollbackSeqno = 0;

        if (bodylen == sizeof(uint64_t)) {
            memcpy(&rollbackSeqno, pkt->bytes + sizeof(pkt), sizeof(uint64_t));
            rollbackSeqno = ntohll(rollbackSeqno);
        }

        return uprStreamReqResponse(cookie, opaque, status, rollbackSeqno);
    }

    LOG(EXTENSION_LOG_WARNING, "Trying to handle an unknown response, "
        "disconnecting");

    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprStreamReqResponse(const void* cookie,
                                                                   uint32_t opaque,
                                                                   uint16_t status,
                                                                   uint64_t rollbackSeqno) {
    (void) rollbackSeqno;

    void *specific = getEngineSpecific(cookie);
    if (specific == NULL) {
        return ENGINE_DISCONNECT;
    }

    UprConsumer* consumer = reinterpret_cast<UprConsumer*>(specific);

    if (status == ENGINE_ROLLBACK) {
        return ENGINE_ENOTSUP;
    } else {
        consumer->streamAccepted(opaque, status);
    }

    return ENGINE_SUCCESS;
}

UprConsumer* EventuallyPersistentEngine::getUprConsumer(const void* cookie) {
    ConnHandler* handler =
        reinterpret_cast<ConnHandler*>(getEngineSpecific(cookie));

    if (handler) {
        return dynamic_cast<UprConsumer *>(handler);
    }
    return NULL;
}
