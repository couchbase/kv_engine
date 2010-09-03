/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include "dispatcher.hh"

const uint32_t TapConnection::ackWindowSize = 10;
const uint32_t TapConnection::ackHighChunkThreshold = 1000;
const uint32_t TapConnection::ackMediumChunkThreshold = 100;
const uint32_t TapConnection::ackLowChunkThreshold = 10;
const rel_time_t TapConnection::ackGracePeriod = 5 * 60;


bool TapConnection::windowIsFull() {
    if (!ackSupported) {
        return false;
    }

    if (seqno >= seqnoReceived) {
        if ((seqno - seqnoReceived) <= ackWindowSize) {
            return false;
        }
    } else {
        uint32_t n = static_cast<uint32_t>(-1) - seqnoReceived + seqno;
        if (n <= ackWindowSize) {
            return false;
        }
    }

    return true;
}

bool TapConnection::requestAck() {
    if (!ackSupported) {
        return false;
    }

    uint32_t qsize = queue->size() + vBucketLowPriority.size() +
        vBucketHighPriority.size();
    uint32_t mod = 1;

    if (qsize >= ackHighChunkThreshold) {
        mod = ackHighChunkThreshold;
    } else if (qsize >= ackMediumChunkThreshold) {
        mod = ackMediumChunkThreshold;
    } else if (qsize >= ackLowChunkThreshold) {
        mod = ackLowChunkThreshold;
    }

    if ((recordsFetched % mod) == 0) {
        ++seqno;
        return true;
    } else {
        return false;
    }
}

void TapConnection::rollback() {
    std::list<TapLogElement>::iterator i = tapLog.begin();
    while (i != tapLog.end()) {
        switch (i->event) {
        case TAP_VBUCKET_SET:
            {
                TapVBucketEvent e(i->event, i->vbucket, i->state);
                if (i->state == pending) {
                    addVBucketHighPriority(e);
                } else {
                    addVBucketLowPriority(e);
                }
            }
            break;
        case TAP_MUTATION:
            addEvent(i->key, i->vbucket, queue_op_set);
            break;
        default:
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Internal error. Not implemented");
            abort();
        }
        tapLog.erase(i);
        i = tapLog.begin();
    }
}

ENGINE_ERROR_CODE TapConnection::processAck(uint32_t s,
                                            uint16_t status,
                                            const std::string &msg) {

    seqnoReceived = s;
    expiry_time = ep_current_time() + ackGracePeriod;

    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Received negative TAP ack from <%s> (#%u): Code: %u (%s)\n",
                         client.c_str(), seqnoReceived, status, msg.c_str());
        doDisconnect = true;
        expiry_time = 0;
        return ENGINE_DISCONNECT;
    } else {
        // @todo optimize this by using algorithm
        std::list<TapLogElement>::iterator iter = tapLog.begin();
        while (iter != tapLog.end() && (*iter).seqno == seqnoReceived) {
            tapLog.erase(iter);
            iter = tapLog.begin();
        }
    }

    return ENGINE_SUCCESS;
}

void TapConnection::encodeVBucketStateTransition(const TapVBucketEvent &ev, void **es,
                                                 uint16_t *nes, uint16_t *vbucket) const
{
    *vbucket = ev.vbucket;
    switch (ev.state) {
    case active:
        *es = const_cast<void*>(static_cast<const void*>(&VBucket::ACTIVE));
        break;
    case replica:
        *es = const_cast<void*>(static_cast<const void*>(&VBucket::REPLICA));
        break;
    case pending:
        *es = const_cast<void*>(static_cast<const void*>(&VBucket::PENDING));
        break;
    case dead:
        *es = const_cast<void*>(static_cast<const void*>(&VBucket::DEAD));
        break;
    default:
        // Illegal vbucket state
        abort();
    }
    *nes = sizeof(vbucket_state_t);
}

class TapBGFetchCallback : public DispatcherCallback {
public:
    TapBGFetchCallback(EventuallyPersistentEngine *e, const std::string &n,
                       SERVER_CORE_API *capi, const std::string &k,
                       uint64_t r, const void *c) :
        epe(e), name(n), core(capi), key(k), rowid(r), cookie(c),
        init(gethrtime()), start(0) {
        assert(epe);
        assert(core);
        assert(cookie);
    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;

        start = gethrtime();
        RememberingCallback<GetValue> gcb;

        EventuallyPersistentStore *epstore = epe->getEpStore();
        assert(epstore);

        epstore->getUnderlying()->get(key, rowid, gcb);
        gcb.waitForValue();
        assert(gcb.fired);

        if (gcb.val.getStatus() == ENGINE_SUCCESS) {
            ReceivedItemTapOperation tapop;
            epe->performTapOp(name, tapop, gcb.val.getValue());
            core->notify_io_complete(cookie, ENGINE_SUCCESS);
        }

        CompletedBGFetchTapOperation tapop;
        epe->performTapOp(name, tapop, epe);

        return false;
    }

private:
    EventuallyPersistentEngine *epe;
    const std::string           name;
    SERVER_CORE_API            *core;
    std::string                 key;
    uint64_t                    rowid;
    const void                 *cookie;

    hrtime_t init;
    hrtime_t start;
};

void TapConnection::queueBGFetch(const std::string &key, uint64_t id) {
    LockHolder lh(backfillLock);
    backfillQueue.push(TapBGFetchQueueItem(key, id));
    ++bgQueued;
    ++bgQueueSize;
    assert(!empty());
    assert(!idle());
    assert(!complete());
}

void TapConnection::runBGFetch(EventuallyPersistentEngine *e, Dispatcher *dispatcher,
                               SERVER_CORE_API *core, const void *cookie) {
    LockHolder lh(backfillLock);
    TapBGFetchQueueItem qi(backfillQueue.front());
    backfillQueue.pop();
    --bgQueueSize;
    lh.unlock();

    shared_ptr<TapBGFetchCallback> dcb(new TapBGFetchCallback(e, client, core,
                                                              qi.key, qi.id,
                                                              cookie));
    ++bgJobIssued;
    ++e->getEpStore()->bgFetchQueue;
    dispatcher->schedule(dcb, NULL, Priority::BgFetcherPriority);
}

void TapConnection::gotBGItem(Item *i) {
    LockHolder lh(backfillLock);
    backfilledItems.push(i);
    ++bgResultSize;
    assert(hasItem());
}

void TapConnection::completedBGFetchJob(EventuallyPersistentEngine *epe) {
    ++bgJobCompleted;
    --epe->getEpStore()->bgFetchQueue;
}

Item* TapConnection::nextFetchedItem() {
    assert(hasItem());
    LockHolder lh(backfillLock);
    Item *rv = backfilledItems.front();
    assert(rv);
    backfilledItems.pop();
    --bgResultSize;
    return rv;
}
