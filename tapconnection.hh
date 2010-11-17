/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TAPCONNECTION_HH
#define TAPCONNECTION_HH 1

#include <set>

#include "common.hh"
#include "atomic.hh"
#include "mutex.hh"
#include "locks.hh"
#include "vbucket.hh"

// forward decl
class EventuallyPersistentEngine;
class TapConnMap;
class BackFillVisitor;
class StrategicSqlite3;
class TapBGFetchCallback;
class CompleteBackfillOperation;
class Dispatcher;
class Item;

struct TapStatBuilder;
struct PopulateEventsBody;

#define TAP_OPAQUE_ENABLE_AUTO_NACK 0
#define TAP_OPAQUE_INITIAL_VBUCKET_STREAM 1

/**
 * A tap event that represents a change to the state of a vbucket.
 *
 * The tap stream may include other events than data mutation events,
 * but the data structures in the TapConnection does only store a key
 * for the item to store. We don't want to add more data to those elements,
 * because that could potentially consume a lot of memory (the tap queue
 * may have a lot of elements).
 */
class TapVBucketEvent {
public:
    /**
     * Create a new instance of the TapVBucketEvent and initialize
     * its members.
     * @param ev Type of event
     * @param b The bucket this event belongs to
     * @param s The state change for this event
     */
    TapVBucketEvent(tap_event_t ev, uint16_t b, vbucket_state_t s) :
        event(ev), vbucket(b), state(s) {}
    tap_event_t event;
    uint16_t vbucket;
    vbucket_state_t state;
};

/**
 * Represents an item that has been sent over tap, but may need to be
 * rolled back if acks fail.
 */
class TapLogElement {
public:
    TapLogElement(uint32_t s, const TapVBucketEvent &e) :
        seqno(s),
        event(e.event),
        vbucket(e.vbucket),
        state(e.state),
        key() // Not used, but need to initialize
    {
        // EMPTY
    }


    TapLogElement(uint32_t s, const QueuedItem &i) :
        seqno(s),
        event(TAP_MUTATION), // just set it to TAP_MUTATION.. I'll fix it if I have to replay the log
        vbucket(i.getVBucketId()),
        state(active),  // Not used, but I need to initialize...
        key(i.getKey())
    {
        // EMPTY
    }

    uint32_t seqno;
    tap_event_t event;
    uint16_t vbucket;

    vbucket_state_t state;
    std::string key;
};

/**
 * An item queued for background fetch from tap.
 */
class TapBGFetchQueueItem {
public:
    TapBGFetchQueueItem(const std::string &k, uint64_t i) :
        key(k), id(i) {}

    const std::string key;
    const uint64_t id;
};

/**
 * Class used by the EventuallyPersistentEngine to keep track of all
 * information needed per Tap connection.
 */
class TapConnection {
public:
    void completeBackfill() {
        pendingBackfill = false;

        if (complete() && idle()) {
            // There is no data for this connection..
            // Just go ahead and disconnect it.
            doDisconnect = true;
        }
    }

    /**
     * Invoked each time a background item fetch completes.
     */
    void gotBGItem(Item *item);

    /**
     * Invoked once per batch bg fetch job.
     */
    void completedBGFetchJob();

    const std::string& getName() const {
        return client;
    }

private:
    friend class EventuallyPersistentEngine;
    friend class TapConnMap;
    friend class BackFillVisitor;
    friend class TapBGFetchCallback;
    friend struct TapStatBuilder;
    friend struct PopulateEventsBody;

    /**
     * Add a new item to the tap queue. You need to hold the queue lock
     * before calling this function
     * The item may be ignored if the TapConnection got a vbucket filter
     * associated and the item's vbucket isn't part of the filter.
     *
     * @return true if the the queue was empty
     */
    bool addEvent_UNLOCKED(const QueuedItem &it) {
        if (vbucketFilter(it.getVBucketId())) {
            bool wasEmpty = queue->empty();
            std::pair<std::set<QueuedItem>::iterator, bool> ret;
            ret = queue_set->insert(it);
            if (ret.second) {
                queue->push_back(it);
                ++queueSize;
            }
            return wasEmpty;
        } else {
            return queue->empty();
        }
    }

    /**
     * Add a new item to the tap queue.
     * The item may be ignored if the TapConnection got a vbucket filter
     * associated and the item's vbucket isn't part of the filter.
     *
     * @return true if the the queue was empty
     */
    bool addEvent(const QueuedItem &it) {
        LockHolder lh(queueLock);
        return addEvent_UNLOCKED(it);
    }

    /**
     * Add a key to the tap queue. You need the queue lock to call this
     * @return true if the the queue was empty
     */
    bool addEvent_UNLOCKED(const std::string &key, uint16_t vbid, enum queue_operation op) {
        return addEvent_UNLOCKED(QueuedItem(key, vbid, op));
    }

    bool addEvent(const std::string &key, uint16_t vbid, enum queue_operation op) {
        LockHolder lh(queueLock);
        return addEvent_UNLOCKED(key, vbid, op);
    }

    void addTapLogElement(const QueuedItem &qi) {
        if (ackSupported) {
            TapLogElement log(seqno, qi);
            tapLog.push_back(log);
        }
    }

    void addTapLogElement(const TapVBucketEvent &e) {
        if (ackSupported && e.event != TAP_NOOP) {
            // add to the log!
            TapLogElement log(seqno, e);
            tapLog.push_back(log);
        }
    }

    QueuedItem next() {
        assert(!empty());
        do {
            LockHolder lh(queueLock);
            QueuedItem qi = queue->front();
            queue->pop_front();
            queue_set->erase(qi);
            --queueSize;

            if (vbucketFilter(qi.getVBucketId())) {
                ++recordsFetched;
                addTapLogElement(qi);
                return qi;
            }
        } while (!empty());

        return QueuedItem("", 0xffff, queue_op_set);
    }

    void addVBucketHighPriority_UNLOCKED(TapVBucketEvent &ev) {
        vBucketHighPriority.push(ev);
    }


    /**
     * Add a new high priority TapVBucketEvent to this TapConnection. A high
     * priority TapVBucketEvent will bypass the the normal queue of events to
     * be sent to the client, and be sent the next time it is possible to
     * send data over the tap connection.
     */
    void addVBucketHighPriority(TapVBucketEvent &ev) {
        LockHolder lh(queueLock);
        addVBucketHighPriority_UNLOCKED(ev);
    }

    /**
     * Get the next high priority TapVBucketEvent for this TapConnection.
     */
    TapVBucketEvent nextVBucketHighPriority_UNLOCKED() {
        TapVBucketEvent ret(TAP_PAUSE, 0, active);
        if (!vBucketHighPriority.empty()) {
            ret = vBucketHighPriority.front();
            vBucketHighPriority.pop();

            // We might have objects in our queue that aren't in our filter
            // If so, just skip them..
            switch (ret.event) {
            case TAP_NOOP:
                break;
            case TAP_OPAQUE:
                opaqueCommandCode = (uint32_t)ret.state;
                if (ntohl(TAP_OPAQUE_ENABLE_AUTO_NACK)) {
                    break;
                }
                // FALLTHROUGH
            default:
                if (!vbucketFilter(ret.vbucket)) {
                    return nextVBucketHighPriority_UNLOCKED();
                }
            }

            ++recordsFetched;
            addTapLogElement(ret);
        }
        return ret;
    }

    TapVBucketEvent nextVBucketHighPriority() {
        LockHolder lh(queueLock);
        return nextVBucketHighPriority_UNLOCKED();
    }

    void addVBucketLowPriority_UNLOCKED(TapVBucketEvent &ev) {
        vBucketLowPriority.push(ev);
    }

    /**
     * Add a new low priority TapVBucketEvent to this TapConnection. A low
     * priority TapVBucketEvent will only be sent when the tap connection
     * doesn't have any other events to send.
     */
    void addVBucketLowPriority(TapVBucketEvent &ev) {
        LockHolder lh(queueLock);
        addVBucketLowPriority_UNLOCKED(ev);
    }

    /**
     * Get the next low priority TapVBucketEvent for this TapConnection.
     */
    TapVBucketEvent nextVBucketLowPriority_UNLOCKED() {
        TapVBucketEvent ret(TAP_PAUSE, 0, active);
        if (!vBucketLowPriority.empty()) {
            ret = vBucketLowPriority.front();
            vBucketLowPriority.pop();
            // We might have objects in our queue that aren't in our filter
            // If so, just skip them..
            if (ret.event != TAP_NOOP && !vbucketFilter(ret.vbucket)) {
                return nextVBucketHighPriority_UNLOCKED();
            }
            ++recordsFetched;
            addTapLogElement(ret);
        }
        return ret;
    }

    TapVBucketEvent nextVBucketLowPriority() {
        LockHolder lh(queueLock);
        return nextVBucketLowPriority_UNLOCKED();
    }

    bool idle() {
        return empty() && vBucketLowPriority.empty() && vBucketHighPriority.empty() && tapLog.empty();
    }

    bool hasItem() {
        return bgResultSize != 0;
    }

    bool hasQueuedItem() {
        LockHolder lh(queueLock);
        return !queue->empty();
    }

    bool empty() {
        return bgQueueSize == 0 && bgResultSize == 0 && !hasQueuedItem();
    }

    /**
     * Find out how much stuff this thing has to do.
     */
    size_t getBacklogSize() {
        LockHolder lh(queueLock);
        return bgResultSize + bgQueueSize
            + (bgJobIssued - bgJobCompleted) + queueSize;
    }

    size_t getQueueSize() {
        LockHolder lh(queueLock);
        return queueSize;
    }

    Item* nextFetchedItem();

    void flush() {
        LockHolder lh(queueLock);
        pendingFlush = true;
        /* No point of keeping the rep queue when someone wants to flush it */
        queue->clear();
        queueSize = 0;
        queue_set->clear();
    }

    bool shouldFlush() {
        bool ret = pendingFlush;
        pendingFlush = false;
        return ret;
    }

    // This method is called while holding the tapNotifySync lock.
    void appendQueue(std::list<QueuedItem> *q) {
        LockHolder lh(queueLock);
        queue->splice(queue->end(), *q);
        queueSize = queue->size();
    }

    /**
     * A backfill is pending if the iterator is active or there are
     * background fetch jobs running.
     */
    bool isPendingBackfill() {
        return pendingBackfill || (bgJobIssued - bgJobCompleted) != 0;
    }

    /**
     * A TapConnection is complete when it has nothing to transmit and
     * a disconnect was requested at the end.
     */
    bool complete(void) {
        return dumpQueue && empty() && !isPendingBackfill();
    }

    /**
     * Queue an item to be background fetched.
     *
     * @param key the item's key
     * @param id the disk id of the item to fetch
     */
    void queueBGFetch(const std::string &key, uint64_t id);

    /**
     * Run some background fetch jobs.
     */
    void runBGFetch(Dispatcher *dispatcher, const void *cookie);

    TapConnection(EventuallyPersistentEngine &theEngine,
                  const std::string &n,
                  uint32_t f);

    ~TapConnection() {
        delete queue;
        delete queue_set;
    }

    ENGINE_ERROR_CODE processAck(uint32_t seqno, uint16_t status, const std::string &msg);

    /**
     * Is the tap ack window full?
     * @return true if the window is full and no more items should be sent
     */
    bool windowIsFull();

    /**
     * Should we request a TAP ack for this message?
     * @param event the event type for this message
     * @return true if we should request a tap ack (and start a new sequence)
     */
    bool requestAck(tap_event_t event);

    /**
     * Get the current tap sequence number.
     */
    uint32_t getSeqno() {
        return seqno;
    }

    /**
     * Rollback the tap stream to the last ack
     */
    void rollback();


    void encodeVBucketStateTransition(const TapVBucketEvent &ev, void **es,
                                      uint16_t *nes, uint16_t *vbucket) const;


    static uint64_t nextTapId() {
        return tapCounter++;
    }

    static std::string getAnonTapName() {
        std::stringstream s;
        s << "eq_tapq:anon_";
        s << TapConnection::nextTapId();
        return s.str();
    }

    void evaluateFlags();

    bool waitForBackfill();

    /**
     * The engine that owns the connection
     */
    EventuallyPersistentEngine &engine;

    /**
     * String used to identify the client.
     * @todo design the connect packet and fill inn som info here
     */
    std::string client;

    //! Lock held during queue operations.
    Mutex queueLock;
    /**
     * The queue of keys that needs to be sent (this is the "live stream")
     */
    std::list<QueuedItem> *queue;
    /**
     * Calling size() on a list is a heavy operation (it will traverse
     * the list to determine the size).. During tap backfill we're calling
     * this for every message we want to send to determine if we should
     * require a tap ack or not. Let's cache the value to stop eating up
     * the CPU :-)
     */
    size_t queueSize;

    /**
     * Set to prevent duplicate queue entries.
     *
     * Note that stl::set is O(log n) for ops we care about, so we'll
     * want to look out for this.
     */
    std::set<QueuedItem> *queue_set;
    /**
     * Flags passed by the client
     */
    uint32_t flags;
    /**
     * Counter of the number of records fetched from this stream since the
     * beginning
     */
    size_t recordsFetched;
    /**
     * Do we have a pending flush command?
     */
    bool pendingFlush;

    /**
     * when this tap conneciton expires.
     */
    rel_time_t expiry_time;

    /**
     * Number of times this client reconnected
     */
    uint32_t reconnects;

    /**
     * Number of disconnects from this client
     */
    uint32_t disconnects;

    /**
     * Is connected?
     */
    bool connected;

    /**
     * is his paused
     */
    bool paused;

    void setBackfillAge(uint64_t age, bool reconnect);

    /**
     * Backfill age for the connection
     */
    uint64_t backfillAge;


    /**
     * Dump and disconnect?
     */
    bool dumpQueue;

    /**
     * We don't want to do the backfill in the thread used by the client,
     * because that would block all clients bound to the same thread.
     * Instead we run the backfill the first time we try to walk the
     * stream (that would be in the TAP thread). This would cause the other
     * tap streams to block, but allows all clients to use the cache.
     */
    bool doRunBackfill;

    // True until a backfill has dumped all the content.
    bool pendingBackfill;

    void setVBucketFilter(const std::vector<uint16_t> &vbuckets);
    /**
     * Filter for the buckets we want.
     */
    VBucketFilter vbucketFilter;
    VBucketFilter backFillVBucketFilter;

    /**
     * VBucket status messages immediately (before userdata)
     */
    std::queue<TapVBucketEvent> vBucketHighPriority;
    /**
     * VBucket status messages sent when there is nothing else to send
     */
    std::queue<TapVBucketEvent> vBucketLowPriority;

    static Atomic<uint64_t> tapCounter;

    Atomic<size_t> bgQueueSize;
    Atomic<size_t> bgQueued;
    Atomic<size_t> bgResultSize;
    Atomic<size_t> bgResults;
    Atomic<size_t> bgJobIssued;
    Atomic<size_t> bgJobCompleted;
    Atomic<size_t> numTapNack;
    Atomic<size_t> numTmpfailSurvivors;

    // True if this should be disconnected as soon as possible
    bool doDisconnect;

    // Current tap sequence number (for ack's)
    uint32_t seqno;

    // The last tap sequence number received
    uint32_t seqnoReceived;

    // does the client support tap acking?
    bool ackSupported;

    bool hasPendingAcks() {
        return !tapLog.empty();
    }

    std::list<TapLogElement> tapLog;

    size_t getTapAckLogSize(void) {
        LockHolder lh(queueLock);
        return tapLog.size();
    }

    Mutex backfillLock;
    std::queue<TapBGFetchQueueItem> backfillQueue;
    std::queue<Item*> backfilledItems;

    /**
     * We don't want the tap notify thread to send multiple tap notifications
     * for the same connection. We set the notifySent member right before
     * sending notify_io_complete (we're holding the tap lock), and clear
     * it in doWalkTapQueue...
     */
    bool notifySent;

    /**
     * We might send userdata with tap opaque messages, but we need
     * to provide the memory for it (that need to persist until the next
     * invokation of doWalkTapStream(). I don't want to do memory allocation
     * for the command code, so let's just keep a variable here and use it
     * whenever we may need it.
     */
    uint32_t opaqueCommandCode;


    static size_t bgMaxPending;

    // Constants used to enforce the tap ack protocol
    static const uint32_t ackWindowSize;
    static const uint32_t ackHighChunkThreshold;
    static const uint32_t ackMediumChunkThreshold;
    static const uint32_t ackLowChunkThreshold;
    static const rel_time_t ackGracePeriod;

    DISALLOW_COPY_AND_ASSIGN(TapConnection);
};

#endif
