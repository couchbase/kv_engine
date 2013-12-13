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

    if (getUprConsumer(cookie)) {
        UprConsumer *consumer = getUprConsumer(cookie);
        UprResponse *resp = consumer->peekNextItem();

        if (resp == NULL) {
            return ENGINE_SUCCESS; // Change to tmpfail once mcd layer is fixed
        }

        switch (resp->getEvent()) {
            case UPR_ADD_STREAM:
            {
                AddStreamResponse *as = dynamic_cast<AddStreamResponse*>(resp);
                producers->add_stream_rsp(cookie, as->getOpaque(),
                                          as->getStreamOpaque(), as->getStatus());
                break;
            }
            case UPR_STREAM_REQ:
            {
                StreamRequest *sr = dynamic_cast<StreamRequest*> (resp);
                producers->stream_req(cookie, sr->getOpaque(), sr->getVBucket(),
                                      sr->getFlags(), sr->getStartSeqno(),
                                      sr->getEndSeqno(), sr->getVBucketUUID(),
                                      sr->getHighSeqno());
                break;
            }
            default:
                LOG(EXTENSION_LOG_WARNING, "Unknown consumer event, "
                    "disconnecting");
                return ENGINE_DISCONNECT;
        }

        consumer->popNextItem();
        return ENGINE_SUCCESS;
    } else if (getUprProducer(cookie)) {
        UprProducer* producer = getUprProducer(cookie);
        UprResponse *resp = producer->peekNextItem();

        if (!resp) {
            return ENGINE_SUCCESS;
        }

        switch (resp->getEvent()) {
            case UPR_STREAM_END:
            {
                StreamEndResponse *se = dynamic_cast<StreamEndResponse*> (resp);
                producers->stream_end(cookie, se->getOpaque(), se->getVbucket(),
                                      se->getFlags());
                break;
            }
            case UPR_MUTATION:
            {
                MutationResponse *m = dynamic_cast<MutationResponse*> (resp);
                producers->mutation(cookie, m->getOpaque(), m->getItem(),
                                    m->getVBucket(), m->getBySeqno(),
                                    m->getRevSeqno(), 0);
                break;
            }
            case UPR_DELETION:
            {
                MutationResponse *m = dynamic_cast<MutationResponse*>(resp);
                producers->deletion(cookie, m->getOpaque(),
                                    m->getItem()->getKey().c_str(),
                                    m->getItem()->getNKey(),
                                    m->getItem()->getCas(),
                                    m->getVBucket(), m->getBySeqno(),
                                    m->getRevSeqno());
                break;
            }
            default:
                LOG(EXTENSION_LOG_WARNING, "Unexpected upr event, disconnecting");
                ret = ENGINE_DISCONNECT;
                break;
        }
        producer->popNextItem();
    } else {
        LOG(EXTENSION_LOG_WARNING, "Null UPR connection... Disconnecting");
    }

    return ret;
}


ENGINE_ERROR_CODE EventuallyPersistentEngine::uprOpen(const void* cookie,
                                                       uint32_t opaque,
                                                       uint32_t seqno,
                                                       uint32_t flags,
                                                       void *stream_name,
                                                       uint16_t nname)
{
    (void) opaque;
    (void) seqno;
    std::string connName(static_cast<const char*>(stream_name), nname);

    ConnHandler *handler = NULL;
    if (flags & UPR_OPEN_PRODUCER) {
        handler = uprConnMap_->newProducer(cookie, connName);
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
                                                           uint64_t *rollback_seqno,
                                                           upr_add_failover_log callback)
{
    (void) callback;
    UprProducer *producer = getUprProducer(cookie);
    if (producer) {
        return producer->addStream(vbucket, opaque, flags, start_seqno,
                                   end_seqno, vbucket_uuid, high_seqno,
                                   rollback_seqno);
    } else {
        return ENGINE_DISCONNECT;
    }
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

UprProducer* EventuallyPersistentEngine::getUprProducer(const void *cookie) {
    ConnHandler* handler =
        reinterpret_cast<ConnHandler*>(getEngineSpecific(cookie));
    return dynamic_cast<UprProducer*>(handler);
}
