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


ENGINE_ERROR_CODE EventuallyPersistentEngine::uprStep(const void* cookie,
                                                      struct upr_message_producers *producers)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    UprProducer *connection = getUprProducer(cookie);
    if (!connection) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to lookup UPR connection.. Disconnecting\n");
        return ENGINE_DISCONNECT;
    }

    connection->lastWalkTime = ep_current_time();

    uint16_t vbucket = 0;
    uint16_t event = 0;
    uint32_t opaque = 0;
    uint8_t nru = 0;
    Item* itm = connection->getNextItem(cookie, &vbucket, event, nru, opaque);
    switch (event) {
        case UPR_STREAM_END:
            producers->stream_end(cookie, opaque, vbucket, /*Flags*/0);
            break;
        case UPR_PAUSE:
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "Unexpected upr event, disconnecting");
            ret = ENGINE_DISCONNECT;
            break;
    }

    (void) itm;

    return ret;
}


ENGINE_ERROR_CODE EventuallyPersistentEngine::uprOpen(const void* cookie,
                                                       uint32_t opaque,
                                                       uint32_t seqno,
                                                       uint32_t flags,
                                                       void *stream_name,
                                                       uint16_t nname)
{
    (void) seqno;
    std::string connName(static_cast<const char*>(stream_name), nname);

    uint64_t backfillAge = 0;
    std::vector<uint16_t> vbuckets;
    vbuckets.push_back((uint16_t)opaque);
    std::map<uint16_t, uint64_t> lastCheckpointIds;

    ConnHandler *handler = NULL;
    if (flags & UPR_OPEN_PRODUCER) {
        handler = uprConnMap_->newProducer(cookie,
                                           connName,
                                           flags,
                                           backfillAge,
                                           static_cast<int>(configuration.getTapKeepalive()),
                                           vbuckets,
                                           lastCheckpointIds);
    } else {
        handler = uprConnMap_->newConsumer(cookie, connName);
    }

    assert(handler);
    storeEngineSpecific(cookie, handler);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprStreamReq(const void* cookie,
                                                           uint32_t flags,
                                                           uint32_t opaque,
                                                           uint16_t vbucket,
                                                           uint64_t start_seqno,
                                                           uint64_t end_seqno,
                                                           uint64_t vbucket_uuid,
                                                           uint64_t high_seqno,
                                                           uint64_t *rollback_seqno)
{
    void *specific = getEngineSpecific(cookie);
    UprProducer *producer = NULL;

    if (specific == NULL) {
        return ENGINE_DISCONNECT;
    }

    producer = reinterpret_cast<UprProducer*>(specific);
    return producer->addStream(flags, opaque, vbucket, start_seqno, end_seqno,
                               vbucket_uuid, high_seqno, rollback_seqno);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprGetFailoverLog(const void* cookie,
                                                                uint32_t opaque,
                                                                uint16_t vbucket,
                                                                upr_add_failover_log callback)
{
    (void) cookie;
    (void) opaque;
    (void) vbucket;
    (void) callback;
    return ENGINE_ENOTSUP;
}
