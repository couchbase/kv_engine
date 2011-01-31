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

Atomic<uint64_t> TapConnection::tapCounter(1);
size_t TapProducer::bgMaxPending = 500;
uint32_t TapProducer::ackWindowSize = 10;
uint32_t TapProducer::ackInterval = 1000;
rel_time_t TapProducer::ackGracePeriod = 5 * 60;
double TapProducer::backoffSleepTime = 1.0;
uint32_t TapProducer::initialAckSequenceNumber = 0;
double TapProducer::requeueSleepTime = 0.1;

TapProducer::TapProducer(EventuallyPersistentEngine &theEngine,
                         const void *c,
                         const std::string &n,
                         uint32_t f):
    TapConnection(theEngine, c, n),
    queue(NULL),
    queueSize(0),
    queue_set(NULL),
    flags(f),
    recordsFetched(0),
    pendingFlush(false),
    reconnects(0),
    paused(false),
    backfillAge(0),
    dumpQueue(false),
    doRunBackfill(false),
    pendingBackfill(true),
    vbucketFilter(),
    vBucketHighPriority(),
    vBucketLowPriority(),
    queueMemSize(0),
    queueFill(0),
    queueDrain(0),
    seqno(initialAckSequenceNumber),
    seqnoReceived(initialAckSequenceNumber - 1),
    notifySent(false)
{
    evaluateFlags();
    queue = new std::list<QueuedItem>;
    queue_set = new std::set<QueuedItem>;

    if (supportAck) {
        expiryTime = ep_current_time() + ackGracePeriod;
    }
}

void TapProducer::evaluateFlags()
{
    std::stringstream ss;

    if (flags & TAP_CONNECT_FLAG_DUMP) {
        dumpQueue = true;
        ss << ",dump";
    }

    if (flags & TAP_CONNECT_SUPPORT_ACK) {
        TapVBucketEvent hi(TAP_OPAQUE, 0, (vbucket_state_t)htonl(TAP_OPAQUE_ENABLE_AUTO_NACK));
        addVBucketHighPriority(hi);
        supportAck = true;
        ss << ",ack";
    }

    if (flags & TAP_CONNECT_FLAG_BACKFILL) {
        ss << ",backfill";
    }

    if (flags & TAP_CONNECT_FLAG_LIST_VBUCKETS) {
        ss << ",vblist";
    }

    if (flags & TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS) {
        ss << ",takeover";
    }

    if (ss.str().length() > 0) {
        std::stringstream m;
        m.setf(std::ios::hex);
        m << flags << " (" << ss.str().substr(1) << ")";
        flagsText.assign(m.str());
    }
}

void TapProducer::setBackfillAge(uint64_t age, bool reconnect) {
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

void TapProducer::setVBucketFilter(const std::vector<uint16_t> &vbuckets)
{
    VBucketFilter diff;

    // time to join the filters..
    if (flags & TAP_CONNECT_FLAG_LIST_VBUCKETS) {
        VBucketFilter filter(vbuckets);
        diff = vbucketFilter.filter_diff(filter);
        backFillVBucketFilter = filter.filter_intersection(diff);

        std::stringstream ss;
        ss << getName().c_str() << ": Changing the vbucket filter from "
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

        std::stringstream f;
        f << vbucketFilter;
        filterText.assign(f.str());
    }

    // Note that we do re-evaluete all entries when we suck them out of the
    // queue to send them..
    if (flags & TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS) {
        const std::vector<uint16_t> &vec = diff.getVector();
        for (std::vector<uint16_t>::const_iterator it = vec.begin();
             it != vec.end(); ++it) {
            if (vbucketFilter(*it)) {
                TapVBucketEvent hi(TAP_VBUCKET_SET, *it, vbucket_state_pending);
                TapVBucketEvent lo(TAP_VBUCKET_SET, *it, vbucket_state_active);
                addVBucketHighPriority(hi);
                addVBucketLowPriority(lo);
            }
        }
        dumpQueue = true;
    } else {
        const std::vector<uint16_t> &vec = diff.getVector();
        for (std::vector<uint16_t>::const_iterator it = vec.begin();
             it != vec.end(); ++it) {
            if (vbucketFilter(*it)) {
                TapVBucketEvent hi(TAP_OPAQUE, *it, (vbucket_state_t)htonl(TAP_OPAQUE_INITIAL_VBUCKET_STREAM));
                addVBucketHighPriority(hi);
            }
        }
    }
}

bool TapProducer::windowIsFull() {
    if (!supportAck) {
        return false;
    }

    if (seqno >= seqnoReceived) {
        if ((seqno - seqnoReceived) <= (ackWindowSize * ackInterval)) {
            return false;
        }
    } else {
        uint32_t n = static_cast<uint32_t>(-1) - seqnoReceived + seqno;
        if (n <= (ackWindowSize * ackInterval)) {
            return false;
        }
    }

    return true;
}

bool TapProducer::requestAck(tap_event_t event) {
    if (!supportAck) {
        return false;
    }

    ++seqno;
    return (event == TAP_VBUCKET_SET || // always ack vbucket state change
            event == TAP_OPAQUE || // always ack opaque messages
            ((seqno - 1) % ackInterval) == 0 || // ack at a regular interval
            empty()); // but if we're almost up to date, ack more often
}

void TapProducer::rollback() {
    LockHolder lh(queueLock);
    std::list<TapLogElement>::iterator i = tapLog.begin();
    while (i != tapLog.end()) {
        switch (i->event) {
        case TAP_VBUCKET_SET:
            {
                TapVBucketEvent e(i->event, i->vbucket, i->state);
                if (i->state == vbucket_state_pending) {
                    addVBucketHighPriority_UNLOCKED(e);
                } else {
                    addVBucketLowPriority_UNLOCKED(e);
                }
            }
            break;
        case TAP_MUTATION:
            addEvent_UNLOCKED(i->key, i->vbucket, queue_op_set);
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

/**
 * Dispatcher task to wake a tap connection.
 */
class TapResumeCallback : public DispatcherCallback {
public:
    TapResumeCallback(EventuallyPersistentEngine &e, TapProducer &c)
        : engine(e), connection(c) {

    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d;
        (void)t;
        const void *cookie = connection.getCookie();
        connection.setSuspended(false);
        if (cookie) {
            engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
        }
        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Notifying suspended tap connection: " << connection.getName();
        return ss.str();
    }

private:
    EventuallyPersistentEngine &engine;
    TapProducer              &connection;
};

const void *TapProducer::getCookie() const {
    return cookie;
}


bool TapProducer::isSuspended() const {
    return suspended.get();
}

void TapProducer::setSuspended(bool value)
{
    if (value) {
        if (backoffSleepTime > 0 && !suspended.get()) {
            Dispatcher *d = engine.getEpStore()->getNonIODispatcher();
            d->schedule(shared_ptr<DispatcherCallback>
                        (new TapResumeCallback(engine, *this)),
                        NULL, Priority::TapResumePriority, backoffSleepTime,
                        false);
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Suspend %s for %.2f secs\n", getName().c_str(),
                             backoffSleepTime);


        } else {
            // backoff disabled, or already in a suspended state
            return;
        }
    }
    suspended.set(value);
}

void TapProducer::reschedule_UNLOCKED(const std::list<TapLogElement>::iterator &iter)
{
    ++numTmpfailSurvivors;
    switch (iter->event) {
    case TAP_VBUCKET_SET:
        {
            TapVBucketEvent e(iter->event, iter->vbucket, iter->state);
            if (iter->state == vbucket_state_pending) {
                addVBucketHighPriority_UNLOCKED(e);
            } else {
                addVBucketLowPriority_UNLOCKED(e);
            }
        }
        break;
    case TAP_MUTATION:
        addEvent_UNLOCKED(iter->key, iter->vbucket, queue_op_set);
        break;
    default:
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Internal error. Not implemented");
        abort();
    }
}

ENGINE_ERROR_CODE TapProducer::processAck(uint32_t s,
                                            uint16_t status,
                                            const std::string &msg)
{
    LockHolder lh(queueLock);
    std::list<TapLogElement>::iterator iter = tapLog.begin();
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    expiryTime = ep_current_time() + ackGracePeriod;
    seqnoReceived = s;

    /* Implicit ack _every_ message up until this message */
    while (iter != tapLog.end() && iter->seqno != s) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Implicit ack <%s> (#%u)\n",
                         getName().c_str(), iter->seqno);
        ++iter;
    }

    switch (status) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        /* And explicit ack this message! */
        if (iter != tapLog.end()) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Explicit ack <%s> (#%u)\n",
                             getName().c_str(), iter->seqno);
            ++iter;
            tapLog.erase(tapLog.begin(), iter);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Explicit ack <%s> of nonexisting entry (#%u)\n",
                             getName().c_str(), s);
        }

        lh.unlock();
        if (complete() && idle()) {
            // We've got all of the ack's need, now we can shut down the
            // stream
            setDisconnect(true);
            expiryTime = 0;
            ret = ENGINE_DISCONNECT;
        }
        break;

    case PROTOCOL_BINARY_RESPONSE_EBUSY:
    case PROTOCOL_BINARY_RESPONSE_ETMPFAIL:
        setSuspended(true);
        ++numTapNack;
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Received temporary TAP nack from <%s> (#%u): Code: %u (%s)\n",
                         getName().c_str(), seqnoReceived, status, msg.c_str());

        // Reschedule _this_ sequence number..
        if (iter != tapLog.end()) {
            reschedule_UNLOCKED(iter);
            ++iter;
        }
        tapLog.erase(tapLog.begin(), iter);
        break;
    default:
        tapLog.erase(tapLog.begin(), iter);
        ++numTapNack;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Received negative TAP ack from <%s> (#%u): Code: %u (%s)\n",
                         getName().c_str(), seqnoReceived, status, msg.c_str());
        setDisconnect(true);
        expiryTime = 0;
        ret = ENGINE_DISCONNECT;
    }

    return ret;
}

void TapProducer::encodeVBucketStateTransition(const TapVBucketEvent &ev, void **es,
                                                 uint16_t *nes, uint16_t *vbucket) const
{
    *vbucket = ev.vbucket;
    switch (ev.state) {
    case vbucket_state_active:
        *es = const_cast<void*>(static_cast<const void*>(&VBucket::ACTIVE));
        break;
    case vbucket_state_replica:
        *es = const_cast<void*>(static_cast<const void*>(&VBucket::REPLICA));
        break;
    case vbucket_state_pending:
        *es = const_cast<void*>(static_cast<const void*>(&VBucket::PENDING));
        break;
    case vbucket_state_dead:
        *es = const_cast<void*>(static_cast<const void*>(&VBucket::DEAD));
        break;
    default:
        // Illegal vbucket state
        abort();
    }
    *nes = sizeof(vbucket_state_t);
}

bool TapProducer::waitForBackfill() {
    if ((bgJobIssued - bgJobCompleted) > bgMaxPending) {
        return true;
    }
    return false;
}

/**
 * A Dispatcher job that performs a background fetch on behalf of tap.
 */
class TapBGFetchCallback : public DispatcherCallback {
public:
    TapBGFetchCallback(EventuallyPersistentEngine *e, const std::string &n,
                       const std::string &k, uint16_t vbid, uint16_t vbv,
                       uint64_t r, const void *c) :
        epe(e), name(n), key(k), vbucket(vbid), vbver(vbv), rowid(r), cookie(c),
        init(gethrtime()), start(0), counter(e->getEpStore()->bgFetchQueue) {
        assert(epe);
        assert(cookie);
    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;

        start = gethrtime();
        RememberingCallback<GetValue> gcb;

        EPStats &stats = epe->getEpStats();
        EventuallyPersistentStore *epstore = epe->getEpStore();
        assert(epstore);

        epstore->getROUnderlying()->get(key, rowid, vbucket, vbver, gcb);
        gcb.waitForValue();
        assert(gcb.fired);

        if (gcb.val.getStatus() == ENGINE_SUCCESS) {
            ReceivedItemTapOperation tapop;
            // if the tap connection is closed, then free an Item instance
            if (!epe->tapConnMap.performTapOp(name, tapop, gcb.val.getValue())) {
                delete gcb.val.getValue();
            }
            epe->notifyIOComplete(cookie, ENGINE_SUCCESS);
        } else { // If a tap bg fetch job is failed, schedule it again.
            RCPtr<VBucket> vb = epstore->getVBucket(vbucket);
            if (vb) {
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
                StoredValue *v = epstore->fetchValidValue(vb, key, bucket_num);
                if (v) {
                    d.snooze(t, TapProducer::requeueSleepTime);
                    ++stats.numTapBGFetchRequeued;
                    return true;
                }
            }
        }

        CompletedBGFetchTapOperation tapop;
        epe->tapConnMap.performTapOp(name, tapop, epe);

        hrtime_t stop = gethrtime();

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
    uint16_t                    vbucket;
    uint16_t                    vbver;
    uint64_t                    rowid;
    const void                 *cookie;

    hrtime_t init;
    hrtime_t start;

    BGFetchCounter counter;
};

void TapProducer::queueBGFetch(const std::string &key, uint64_t id,
                                 uint16_t vb, uint16_t vbv) {
    LockHolder lh(backfillLock);
    backfillQueue.push(TapBGFetchQueueItem(key, id, vb, vbv));
    ++bgQueued;
    ++bgQueueSize;
    assert(!empty());
    assert(!idle());
    assert(!complete());
}

void TapProducer::runBGFetch(Dispatcher *dispatcher, const void *c) {
    LockHolder lh(backfillLock);
    TapBGFetchQueueItem qi(backfillQueue.front());
    backfillQueue.pop();
    --bgQueueSize;
    lh.unlock();

    shared_ptr<TapBGFetchCallback> dcb(new TapBGFetchCallback(&engine,
                                                              getName(), qi.key,
                                                              qi.vbucket, qi.vbversion,
                                                              qi.id, c));
    ++bgJobIssued;
    dispatcher->schedule(dcb, NULL, Priority::TapBgFetcherPriority);
}

void TapProducer::gotBGItem(Item *i, bool implicitEnqueue) {
    LockHolder lh(backfillLock);
    // implicitEnqueue is used for the optimized disk fetch wherein we
    // receive the item and want the stats to reflect an
    // enqueue/execute cycle.
    if (implicitEnqueue) {
        ++bgQueued;
        ++bgJobIssued;
        ++bgJobCompleted;
    }
    backfilledItems.push(i);
    ++bgResultSize;
    assert(hasItem());
}

void TapProducer::completedBGFetchJob() {
    ++bgJobCompleted;
}

Item* TapProducer::nextFetchedItem() {
    assert(hasItem());
    LockHolder lh(backfillLock);
    Item *rv = backfilledItems.front();
    assert(rv);
    backfilledItems.pop();
    --bgResultSize;
    return rv;
}

void TapProducer::addStats(ADD_STAT add_stat, const void *c) {
    TapConnection::addStats(add_stat, c);
    addStat("qlen", getQueueSize(), add_stat, c);
    addStat("qlen_high_pri", vBucketHighPriority.size(), add_stat, c);
    addStat("qlen_low_pri", vBucketLowPriority.size(), add_stat, c);
    addStat("vb_filters", vbucketFilter.size(), add_stat, c);
    addStat("vb_filter", filterText.c_str(), add_stat, c);
    addStat("rec_fetched", recordsFetched, add_stat, c);
    if (recordsSkipped > 0) {
        addStat("rec_skipped", recordsSkipped, add_stat, c);
    }
    addStat("idle", idle(), add_stat, c);
    addStat("empty", empty(), add_stat, c);
    addStat("complete", complete(), add_stat, c);
    addStat("has_item", hasItem(), add_stat, c);
    addStat("has_queued_item", hasQueuedItem(), add_stat, c);
    addStat("bg_wait_for_results", waitForBackfill(), add_stat, c);
    addStat("bg_queue_size", bgQueueSize, add_stat, c);
    addStat("bg_queued", bgQueued, add_stat, c);
    addStat("bg_result_size", bgResultSize, add_stat, c);
    addStat("bg_results", bgResults, add_stat, c);
    addStat("bg_jobs_issued", bgJobIssued, add_stat, c);
    addStat("bg_jobs_completed", bgJobCompleted, add_stat, c);
    addStat("bg_backlog_size", getBacklogSize(), add_stat, c);
    addStat("flags", flagsText, add_stat, c);
    addStat("suspended", isSuspended(), add_stat, c);
    addStat("paused", paused, add_stat, c);
    addStat("pending_backfill", pendingBackfill, add_stat, c);
    addStat("pending_disk_backfill", !isPendingDiskBackfill(), add_stat, c);

    addStat("queue_memory", getQueueMemory(), add_stat, c);
    addStat("queue_fill", getQueueFillTotal(), add_stat, c);
    addStat("queue_drain", getQueueDrainTotal(), add_stat, c);
    addStat("queue_backoff", getQueueBackoff(), add_stat, c);
    addStat("queue_backfillremaining", getBacklogSize(), add_stat, c);
    addStat("queue_itemondisk", getRemaingOnDisk(), add_stat, c);

    if (reconnects > 0) {
        addStat("reconnects", reconnects, add_stat, c);
    }
    if (backfillAge != 0) {
        addStat("backfill_age", (size_t)backfillAge, add_stat, c);
    }

    if (supportAck) {
        addStat("ack_seqno", seqno, add_stat, c);
        addStat("recv_ack_seqno", seqnoReceived, add_stat, c);
        addStat("ack_log_size", getTapAckLogSize(), add_stat, c);
        addStat("ack_window_full", windowIsFull(), add_stat, c);
        if (windowIsFull()) {
            addStat("expires", expiryTime - ep_current_time(), add_stat, c);
        }
        addStat("num_tap_nack", numTapNack, add_stat, c);
        addStat("num_tap_tmpfail_survivors", numTmpfailSurvivors, add_stat, c);
        addStat("ack_playback_size", getTapLogSize(), add_stat, c);
     }
}

void TapProducer::processedEvent(tap_event_t event, ENGINE_ERROR_CODE ret)
{
    (void)ret;
    assert(event == TAP_ACK);
}

/**************** TAP Consumer **********************************************/
TapConsumer::TapConsumer(EventuallyPersistentEngine &theEngine,
                         const void *c,
                         const std::string &n) :
    TapConnection(theEngine, c, n)
{ /* EMPTY */ }

void TapConsumer::addStats(ADD_STAT add_stat, const void *c) {
    TapConnection::addStats(add_stat, c);
    addStat("num_delete", numDelete, add_stat, c);
    addStat("num_delete_failed", numDeleteFailed, add_stat, c);
    addStat("num_flush", numFlush, add_stat, c);
    addStat("num_flush_failed", numFlushFailed, add_stat, c);
    addStat("num_mutation", numMutation, add_stat, c);
    addStat("num_mutation_failed", numMutationFailed, add_stat, c);
    addStat("num_opaque", numOpaque, add_stat, c);
    addStat("num_opaque_failed", numOpaqueFailed, add_stat, c);
    addStat("num_vbucket_set", numVbucketSet, add_stat, c);
    addStat("num_vbucket_set_failed", numVbucketSetFailed, add_stat, c);
    addStat("num_unknown", numUnknown, add_stat, c);
}

void TapConsumer::processedEvent(tap_event_t event, ENGINE_ERROR_CODE ret)
{
    switch (event) {
    case TAP_ACK:
        /* A tap consumer should _NEVER_ receive a tap ack */
        abort();
        break;

    case TAP_FLUSH:
        if (ret == ENGINE_SUCCESS) {
            ++numFlush;
        } else {
            ++numFlushFailed;
        }
        break;

    case TAP_DELETION:
        if (ret == ENGINE_SUCCESS) {
            ++numDelete;
        } else {
            ++numDeleteFailed;
        }
        break;

    case TAP_MUTATION:
        if (ret == ENGINE_SUCCESS) {
            ++numMutation;
        } else {
            ++numMutationFailed;
        }
        break;

    case TAP_OPAQUE:
        if (ret == ENGINE_SUCCESS) {
            ++numOpaque;
        } else {
            ++numOpaqueFailed;
        }
        break;

    case TAP_VBUCKET_SET:
        if (ret == ENGINE_SUCCESS) {
            ++numVbucketSet;
        } else {
            ++numVbucketSetFailed;
        }
        break;

    default:
        ++numUnknown;
    }
}

bool TapProducer::isTimeForNoop() {
    return noop.swap(false);
}

void TapProducer::setTimeForNoop()
{
    noop.set(true);
}
