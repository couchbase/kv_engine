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
    UprProducer *connection = getUprProducer(cookie);
    if (!connection) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to lookup UPR connection.. Disconnecting\n");
        return ENGINE_DISCONNECT;
    }

    connection->paused.set(false);

    uint16_t ret;
    item* itm = NULL;
    void *es = NULL;
    uint16_t *nes = 0;
    uint8_t *ttl;
    uint32_t *flags = 0;
    uint32_t *seqno = 0;
    uint16_t *vbucket = 0;
    bool retry = false;

    connection->lastWalkTime = ep_current_time();

    do {
        ret = doWalkUprQueue(cookie, &itm, &es, nes, ttl, flags,
                             seqno, vbucket, connection, retry, producers);
    } while (retry);

    if (ret != UPR_PAUSE && ret != UPR_DISCONNECT) {
        connection->lastMsgTime = ep_current_time();
        if (ret == UPR_NOOP) {
            *seqno = 0;
        } else {
            ++stats.numTapFetched; //TODO dliao: add numUprFetched
            *seqno = connection->getSeqno();
            if (connection->requestAck(ret, *vbucket)) {
                *flags = TAP_FLAG_ACK;
                connection->seqnoAckRequested = *seqno;
            }

            if (ret == UPR_MUTATION) {
                if (connection->haveFlagByteorderSupport()) {
                    *flags |= TAP_FLAG_NETWORK_BYTE_ORDER;
                }
            }
        }
    } else {
        connection->paused.set(true);
        connection->notifySent.set(false);
    }

    return ENGINE_SUCCESS; //TODO: dliao
}

ENGINE_ERROR_CODE EventuallyPersistentEngine:: uprOpen(const void* cookie,
                                                       uint32_t opaque,
                                                       uint32_t seqno,
                                                       uint32_t flags,
                                                       void *name,
                                                       uint16_t nname,
                                                       upr_open_handler handler)
{
    return ENGINE_ENOTSUP;
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
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprGetFailoverLog(const void* cookie,
                                                                uint32_t opaque,
                                                                uint16_t vbucket,
                                                                upr_add_failover_log callback)
{
    return ENGINE_ENOTSUP;
}
