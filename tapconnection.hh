/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TAPCONNECTION_HH
#define TAPCONNECTION_HH 1

// forward decl
class EventuallyPersistentEngine;
class BackFillVisitor;


/**
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
 * Class used by the EventuallyPersistentEngine to keep track of all
 * information needed per Tap connection.
 */
class TapConnection {
    friend class EventuallyPersistentEngine;
    friend class BackFillVisitor;
private:
    /**
     * Add a new item to the tap queue.
     * The item may be ignored if the TapConnection got a vbucket filter
     * associated and the item's vbucket isn't part of the filter.
     *
     * @return true if the the queue was empty
     */
    bool addEvent(const QueuedItem &it) {
        if (vbucketFilter(it.getVBucketId())) {
            bool wasEmpty = queue->empty();
            std::pair<std::set<QueuedItem>::iterator, bool> ret;
            ret = queue_set->insert(it);
            if (ret.second) {
                queue->push_back(it);
            }
            return wasEmpty;
        } else {
            return queue->empty();
        }
    }

    /**
     * Add a key to the tap queue.
     * @return true if the the queue was empty
     */
    bool addEvent(const std::string &key, uint16_t vbid, enum queue_operation op) {
        return addEvent(QueuedItem(key, vbid, op));
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
        QueuedItem qi = queue->front();
        queue->pop_front();
        queue_set->erase(qi);
        ++recordsFetched;
        addTapLogElement(qi);

        return qi;
    }

    /**
     * Add a new high priority TapVBucketEvent to this TapConnection. A high
     * priority TapVBucketEvent will bypass the the normal queue of events to
     * be sent to the client, and be sent the next time it is possible to
     * send data over the tap connection.
     */
    void addVBucketHighPriority(TapVBucketEvent &ev) {
        vBucketHighPriority.push(ev);
    }

    /**
     * Get the next high priority TapVBucketEvent for this TapConnection.
     */
    TapVBucketEvent nextVBucketHighPriority() {
        TapVBucketEvent ret(TAP_PAUSE, 0, active);
        if (!vBucketHighPriority.empty()) {
            ret = vBucketHighPriority.front();
            vBucketHighPriority.pop();
            ++recordsFetched;
            addTapLogElement(ret);
        }
        return ret;
    }

    /**
     * Add a new low priority TapVBucketEvent to this TapConnection. A low
     * priority TapVBucketEvent will only be sent when the tap connection
     * doesn't have any other events to send.
     */
    void addVBucketLowPriority(TapVBucketEvent &ev) {
        vBucketLowPriority.push(ev);
    }

    /**
     * Get the next low priority TapVBucketEvent for this TapConnection.
     */
    TapVBucketEvent nextVBucketLowPriority() {
        TapVBucketEvent ret(TAP_PAUSE, 0, active);
        if (!vBucketLowPriority.empty()) {
            ret = vBucketLowPriority.front();
            vBucketLowPriority.pop();
            ++recordsFetched;
            addTapLogElement(ret);
        }
        return ret;
    }

    bool idle() {
        return queue->empty() && vBucketLowPriority.empty() && vBucketHighPriority.empty();
    }

    bool empty() {
        return queue->empty();
    }

    void flush() {
        pendingFlush = true;
        /* No point of keeping the rep queue when someone wants to flush it */
        queue->clear();
        queue_set->clear();
    }

    bool shouldFlush() {
        bool ret = pendingFlush;
        pendingFlush = false;
        return ret;
    }

    // This method is called while holding the tapNotifySync lock.
    void appendQueue(std::list<QueuedItem> *q) {
        queue->splice(queue->end(), *q);
    }

    void completeBackfill() {
        pendingBackfill = false;

        if (complete() && idle()) {
            // There is no data for this connection..
            // Just go ahead and disconnect it.
            doDisconnect = true;
        }
    }

    bool complete(void) {
        return dumpQueue && empty() && !pendingBackfill;
    }

    TapConnection(const std::string &n, uint32_t f):
        client(n), queue(NULL), queue_set(NULL), flags(f),
        recordsFetched(0), pendingFlush(false), expiry_time((rel_time_t)-1),
        reconnects(0), connected(true), paused(false), backfillAge(0),
        doRunBackfill(false), pendingBackfill(true), vbucketFilter(),
        vBucketHighPriority(), vBucketLowPriority(), doDisconnect(false),
        seqno(0), seqnoReceived(static_cast<uint32_t>(-1)),
        ackSupported((f & TAP_CONNECT_SUPPORT_ACK) == TAP_CONNECT_SUPPORT_ACK)
    {
        queue = new std::list<QueuedItem>;
        queue_set = new std::set<QueuedItem>;

        if (ackSupported) {
            expiry_time = ep_current_time() + ackGracePeriod;
        }
    }

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
     * @return true if we should request a tap ack (and start a new sequence)
     */
    bool requestAck();

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


    /**
     * String used to identify the client.
     * @todo design the connect packet and fill inn som info here
     */
    std::string client;
    /**
     * The queue of keys that needs to be sent (this is the "live stream")
     */
    std::list<QueuedItem> *queue;
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
     * Is connected?
     */
    bool connected;

    /**
     * is his paused
     */
    bool paused;

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

    /**
     * Filter for the buckets we want.
     */
    VBucketFilter vbucketFilter;

    /**
     * VBucket status messages immediately (before userdata)
     */
    std::queue<TapVBucketEvent> vBucketHighPriority;
    /**
     * VBucket status messages sent when there is nothing else to send
     */
    std::queue<TapVBucketEvent> vBucketLowPriority;

    static Atomic<uint64_t> tapCounter;

    // True if this should be disconnected as soon as possible
    bool doDisconnect;

    // Current tap sequence number (for ack's)
    uint32_t seqno;

    // The last tap sequence number received
    uint32_t seqnoReceived;

    // does the client support tap acking?
    bool ackSupported;

    std::list<TapLogElement> tapLog;

    // Constants used to enforce the tap ack protocol
    static const uint32_t ackWindowSize;
    static const uint32_t ackHighChunkThreshold;
    static const uint32_t ackMediumChunkThreshold;
    static const uint32_t ackLowChunkThreshold;
    static const rel_time_t ackGracePeriod;

    DISALLOW_COPY_AND_ASSIGN(TapConnection);
};

#endif
