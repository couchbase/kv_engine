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


TapConnection::TapConnection(EventuallyPersistentEngine &theEngine,
                             const std::string &n,
                             uint32_t f):
    engine(theEngine),
    client(n),
    queue(NULL),
    queue_set(NULL),
    flags(f),
    recordsFetched(0),
    pendingFlush(false),
    expiry_time((rel_time_t)-1),
    reconnects(0),
    disconnects(0),
    connected(true),
    paused(false),
    backfillAge(0),
    dumpQueue(false),
    doRunBackfill(false),
    pendingBackfill(true),
    vbucketFilter(),
    vBucketHighPriority(),
    vBucketLowPriority(),
    doDisconnect(false),
    seqno(0),
    seqnoReceived(static_cast<uint32_t>(-1)),
    ackSupported(false),
    notifySent(false)
{
    evaluateFlags();
    queue = new std::list<QueuedItem>;
    queue_set = new std::set<QueuedItem>;

    if (ackSupported) {
        expiry_time = ep_current_time() + ackGracePeriod;
    }
}

void TapConnection::evaluateFlags()
{
    dumpQueue = (flags & TAP_CONNECT_FLAG_DUMP) == TAP_CONNECT_FLAG_DUMP;
    ackSupported = (flags & TAP_CONNECT_SUPPORT_ACK) == TAP_CONNECT_SUPPORT_ACK;
}

void TapConnection::setBackfillAge(uint64_t age, bool reconnect) {
    if (reconnect) {
        if (!(flags & TAP_CONNECT_FLAG_BACKFILL)) {
            age = backfillAge;
        }

        if (age == backfillAge) {
            // we didn't change the critera...
            return;
        }
    }

    if (flags & TAP_CONNECT_FLAG_BACKFILL) {
        backfillAge = age;
    }

    if (backfillAge < (uint64_t)ep_real_time()) {
        doRunBackfill = true;
        pendingBackfill = true;
    }
}

void TapConnection::setVBucketFilter(const std::vector<uint16_t> &vbuckets)
{
    VBucketFilter diff;

    // time to join the filters..
    if (flags & TAP_CONNECT_FLAG_LIST_VBUCKETS) {
        VBucketFilter filter(vbuckets);
        diff = vbucketFilter.filter_diff(filter);
        backFillVBucketFilter = filter.filter_intersection(diff);

        std::stringstream ss;
        ss << client.c_str() << ": Changing the vbucket filter from "
           << vbucketFilter << " to "
           << filter << " (diff: " << diff << ")" << std::endl
           << "Using: " << backFillVBucketFilter << " for backfill."
           << std::endl;
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         ss.str().c_str());
        vbucketFilter = filter;
        if (!diff.empty()) {
            if (backfillAge < (uint64_t)ep_real_time()) {
                doRunBackfill = true;
                pendingBackfill = true;
            }
        }
    }

    // Note that we do re-evaluete all entries when we suck them out of the
    // queue to send them..
    if (flags & TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS) {
        const std::vector<uint16_t> &vec = diff.getVector();
        for (std::vector<uint16_t>::const_iterator it = vec.begin();
             it != vec.end(); ++it) {
            if (vbucketFilter(*it)) {
                TapVBucketEvent hi(TAP_VBUCKET_SET, *it, pending);
                TapVBucketEvent lo(TAP_VBUCKET_SET, *it, active);
                addVBucketHighPriority(hi);
                addVBucketLowPriority(lo);
            }
        }
        dumpQueue = true;
    }
}

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

bool TapConnection::requestAck(tap_event_t event) {
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

    if ((recordsFetched % mod) == 0 || event == TAP_VBUCKET_SET) {
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
                                            const std::string &msg)
{
    std::list<TapLogElement>::iterator iter = tapLog.begin();
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    seqnoReceived = s;
    expiry_time = ep_current_time() + ackGracePeriod;

    switch (status) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        // @todo optimize this by using algorithm
        while (iter != tapLog.end() && (*iter).seqno == seqnoReceived) {
            tapLog.erase(iter);
            iter = tapLog.begin();
        }

        if (complete() && idle()) {
            // We've got all of the ack's need, now we can shut down the
            // stream
            doDisconnect = true;
            expiry_time = 0;
            ret = ENGINE_DISCONNECT;
        }
        break;

    case PROTOCOL_BINARY_RESPONSE_EBUSY:
    case PROTOCOL_BINARY_RESPONSE_ETMPFAIL:
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Received temporary TAP nack from <%s> (#%u): Code: %u (%s)\n",
                         client.c_str(), seqnoReceived, status, msg.c_str());
        // reschedule all of the events for that seqno...
        while (iter != tapLog.end() && (*iter).seqno == seqnoReceived) {
            switch (iter->event) {
            case TAP_VBUCKET_SET:
                {
                    TapVBucketEvent e(iter->event, iter->vbucket, iter->state);
                    if (iter->state == pending) {
                        addVBucketHighPriority(e);
                    } else {
                        addVBucketLowPriority(e);
                    }
                }
                break;
            case TAP_MUTATION:
                addEvent(iter->key, iter->vbucket, queue_op_set);
                break;
            default:
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Internal error. Not implemented");
                abort();
            }
            tapLog.erase(iter);
            iter = tapLog.begin();
        }
        break;
    default:
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Received negative TAP ack from <%s> (#%u): Code: %u (%s)\n",
                         client.c_str(), seqnoReceived, status, msg.c_str());
        doDisconnect = true;
        expiry_time = 0;
        ret = ENGINE_DISCONNECT;
    }

    return ret;
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
                       const std::string &k,
                       uint64_t r, const void *c) :
        epe(e), name(n), key(k), rowid(r), cookie(c),
        init(gethrtime()), start(0) {
        assert(epe);
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
            // if the tap connection is closed, then free an Item instance
            if (!epe->performTapOp(name, tapop, gcb.val.getValue())) {
                delete gcb.val.getValue();
            }
            epe->getServerApi()->cookie->notify_io_complete(cookie, ENGINE_SUCCESS);
        }

        CompletedBGFetchTapOperation tapop;
        epe->performTapOp(name, tapop, epe);

        hrtime_t stop = gethrtime();
        EPStats &stats = epe->getEpStats();

        if (stop > start && start > init) {
            // skip the measurement if the counter wrapped...
            ++stats.tapBgNumOperations;
            hrtime_t w = (start - init) / 1000;
            stats.tapBgWait += w;
            stats.tapBgWaitHisto.add(w);
            stats.tapBgMinWait.setIfLess(w);
            stats.tapBgMaxWait.setIfBigger(w);

            hrtime_t l = (stop - start) / 1000;
            stats.tapBgLoad += l;
            stats.tapBgLoadHisto.add(l);
            stats.tapBgMinLoad.setIfLess(l);
            stats.tapBgMaxLoad.setIfBigger(l);
        }

        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Fetching item from disk for tap:  " << key;
        return ss.str();
    }

private:
    EventuallyPersistentEngine *epe;
    const std::string           name;
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

void TapConnection::runBGFetch(Dispatcher *dispatcher, const void *cookie) {
    LockHolder lh(backfillLock);
    TapBGFetchQueueItem qi(backfillQueue.front());
    backfillQueue.pop();
    --bgQueueSize;
    lh.unlock();

    shared_ptr<TapBGFetchCallback> dcb(new TapBGFetchCallback(&engine, client,
                                                              qi.key, qi.id,
                                                              cookie));
    ++bgJobIssued;
    ++engine.getEpStore()->bgFetchQueue;
    dispatcher->schedule(dcb, NULL, Priority::TapBgFetcherPriority);
}

void TapConnection::gotBGItem(Item *i) {
    LockHolder lh(backfillLock);
    backfilledItems.push(i);
    ++bgResultSize;
    assert(hasItem());
}

void TapConnection::completedBGFetchJob() {
    ++bgJobCompleted;
    --engine.getEpStore()->bgFetchQueue;
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
