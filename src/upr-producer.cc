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


ENGINE_ERROR_CODE UprProducer::step(const void* cookie,
                                    struct upr_message_producers *producers)
{
    UprResponse *resp;
    ENGINE_ERROR_CODE ret = ENGINE_WANT_MORE;

    while ((resp = peekNextItem()) != NULL && ret == ENGINE_WANT_MORE) {
        StreamEndResponse *se;
        MutationResponse *m;

        switch (resp->getEvent()) {
        case UPR_STREAM_END:
            se = dynamic_cast<StreamEndResponse*> (resp);
            ret = producers->stream_end(cookie, se->getOpaque(),
                                        se->getVbucket(),
                                        se->getFlags());
            break;
        case UPR_MUTATION:
            m = dynamic_cast<MutationResponse*> (resp);
            ret = producers->mutation(cookie, m->getOpaque(), m->getItem(),
                                      m->getVBucket(), m->getBySeqno(),
                                      m->getRevSeqno(), 0, NULL, 0);
            break;
        case UPR_DELETION:
            m = dynamic_cast<MutationResponse*>(resp);
            ret = producers->deletion(cookie, m->getOpaque(),
                                      m->getItem()->getKey().c_str(),
                                      m->getItem()->getNKey(),
                                      m->getItem()->getCas(),
                                      m->getVBucket(), m->getBySeqno(),
                                      m->getRevSeqno(), NULL, 0);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "Unexpected upr event, disconnecting");
            ret = ENGINE_DISCONNECT;
            break;
        }

        switch (ret) {
        case ENGINE_SUCCESS:
        case ENGINE_WANT_MORE:
            popNextItem();
        default:
            LOG(EXTENSION_LOG_WARNING, "Failed to insert message: %d",
                (int)ret);
            break;
        }

        return ENGINE_SUCCESS;
    }

    if (ret == ENGINE_SUCCESS || ret == ENGINE_WANT_MORE) {
        // Should be ENGINE_WANT_MORE if we have more data to send
        LockHolder lh(queueLock);
        if (readyQ.empty()) {
            ret = ENGINE_SUCCESS;
        } else {
            ret = ENGINE_WANT_MORE;
        }
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
    UprProducer *producer = getUprProducer(cookie);
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    if (producer) {
        RCPtr<VBucket> vb = getVBucket(vbucket);
        if (!vb) {
            return ENGINE_NOT_MY_VBUCKET;
        }
        size_t logsize = vb->failovers.table.size();
        if(logsize > 0) {
            vbucket_failover_t *logentries = new vbucket_failover_t[logsize];
            vbucket_failover_t *logentry = logentries;
            for(FailoverTable::table_t::iterator it = vb->failovers.table.begin();
                it != vb->failovers.table.end();
                ++it) {
                logentry->uuid = it->first;
                logentry->seqno = it->second;
                logentry++;
            }
            LOG(EXTENSION_LOG_WARNING, "Sending outgoing failover log with %d entries\n", logsize);
            rv = callback(logentries, logsize, cookie);
            delete[] logentries;
            if(rv != ENGINE_SUCCESS) {
                return rv;
            }
        } else {
            LOG(EXTENSION_LOG_WARNING, "Failover log was empty (this shouldn't happen)\n", logsize);
        }

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
