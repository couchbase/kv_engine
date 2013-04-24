/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_TAPCONNECTION_H_
#define SRC_TAPCONNECTION_H_ 1

#include "config.h"

#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "atomic.h"
#include "common.h"
#include "locks.h"
#include "mutex.h"
#include "vbucket.h"

// forward decl
class EventuallyPersistentEngine;
class TapConnMap;
class TapProducer;
class BackFillVisitor;
class TapBGFetchCallback;
class CompleteBackfillOperation;
class Dispatcher;
class Item;

struct TapStatBuilder;
struct TapAggStatBuilder;
struct PopulateEventsBody;

#define MAX_TAP_KEEP_ALIVE 3600
#define MAX_TAKEOVER_TAP_LOG_SIZE 10
#define MINIMUM_BACKFILL_RESIDENT_THRESHOLD 0.7
#define DEFAULT_BACKFILL_RESIDENT_THRESHOLD 0.9

/**
 * A tap event that represents a change to the state of a vbucket.
 *
 * The tap stream may include other events than data mutation events,
 * but the data structures in the TapProducer does only store a key
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
        state(e.state)
    {
        // EMPTY
    }


    TapLogElement(uint32_t s, const queued_item &qi) :
        seqno(s),
        event(TAP_MUTATION),
        vbucket(qi->getVBucketId()),
        state(vbucket_state_active),
        item(qi)
    {
        switch(item->getOperation()) {
        case queue_op_set:
            event = TAP_MUTATION;
            break;
        case queue_op_del:
            event = TAP_DELETION;
            break;
        case queue_op_flush:
            event = TAP_FLUSH;
            break;
        case queue_op_checkpoint_start:
            event = TAP_CHECKPOINT_START;
            break;
        case queue_op_checkpoint_end:
            event = TAP_CHECKPOINT_END;
            break;
        default:
            break;
        }
    }

    uint32_t seqno;
    tap_event_t event;
    uint16_t vbucket;

    vbucket_state_t state;
    queued_item item;
};

/**
 * Aggregator object to count all tap stats.
 */
struct TapCounter {
    TapCounter()
        : tap_queue(0), totalTaps(0),
          tap_queueFill(0), tap_queueDrain(0), tap_queueBackoff(0),
          tap_queueBackfillRemaining(0), tap_queueItemOnDisk(0), tap_totalBacklogSize(0)
    {}

    size_t      tap_queue;
    size_t      totalTaps;

    size_t      tap_queueFill;
    size_t      tap_queueDrain;
    size_t      tap_queueBackoff;
    size_t      tap_queueBackfillRemaining;
    size_t      tap_queueItemOnDisk;
    size_t      tap_totalBacklogSize;
};

typedef enum {
    backfill,
    checkpoint_start,
    checkpoint_end,
    checkpoint_end_synced
} tap_checkpoint_state;

/**
 * Checkpoint state of each vbucket in TAP stream.
 */
class TapCheckpointState {
public:
    TapCheckpointState() :
        currentCheckpointId(0), lastSeqNum(0), bgResultSize(0),
        bgJobIssued(0), bgJobCompleted(0), lastItem(false), state(backfill) {}

    TapCheckpointState(uint16_t vb, uint64_t checkpointId, tap_checkpoint_state s) :
        vbucket(vb), currentCheckpointId(checkpointId), lastSeqNum(0),
        bgResultSize(0), bgJobIssued(0), bgJobCompleted(0),
        lastItem(false), state(s) {}

    bool isBgFetchCompleted(void) const {
        return bgResultSize == 0 && (bgJobIssued - bgJobCompleted) == 0;
    }

    uint16_t vbucket;
    // Id of the checkpoint that is currently referenced by the given TAP client's cursor.
    uint64_t currentCheckpointId;
    // Last sequence number sent to the slave.
    uint32_t lastSeqNum;

    // Number of bg-fetched items for a given vbucket, which are ready for streaming.
    size_t bgResultSize;
    // Number of bg-fetched jobs issued for a given vbucket.
    size_t bgJobIssued;
    // Number of bg-fetched jobs completed for a given vbucket
    size_t bgJobCompleted;

    // True if the TAP cursor reaches to the last item at its current checkpoint.
    bool lastItem;
    tap_checkpoint_state state;
};

/**
 * A class containing the config parameters for TAP module.
 */
class TapConfig {
public:
    TapConfig(EventuallyPersistentEngine &e);
    uint32_t getAckWindowSize() const {
        return ackWindowSize;
    }

    uint32_t getAckInterval() const {
        return ackInterval;
    }

    rel_time_t getAckGracePeriod() const {
        return ackGracePeriod;
    }

    uint32_t getAckInitialSequenceNumber() const {
        return ackInitialSequenceNumber;
    }

    size_t getBgMaxPending() const {
        return bgMaxPending;
    }

    double getBackoffSleepTime() const {
        return backoffSleepTime;
    }

    double getRequeueSleepTime() const {
        return requeueSleepTime;
    }

    size_t getBackfillBacklogLimit() const {
        return backfillBacklogLimit;
    }

    double getBackfillResidentThreshold() const {
        return backfillResidentThreshold;
    }

protected:
    friend class TapConfigChangeListener;
    friend class EventuallyPersistentEngine;

    void setAckWindowSize(size_t value) {
        ackWindowSize = static_cast<uint32_t>(value);
    }

    void setAckInterval(size_t value) {
        ackInterval = static_cast<uint32_t>(value);
    }

    void setAckGracePeriod(size_t value) {
        ackGracePeriod = static_cast<rel_time_t>(value);
    }

    void setAckInitialSequenceNumber(size_t value) {
        ackInitialSequenceNumber = static_cast<uint32_t>(value);
    }

    void setBgMaxPending(size_t value) {
        bgMaxPending = value;
    }

    void setBackoffSleepTime(double value) {
        backoffSleepTime = value;
    }

    void setRequeueSleepTime(double value) {
        requeueSleepTime = value;
    }

    void setBackfillBacklogLimit(size_t value) {
        backfillBacklogLimit = value;
    }

    void setBackfillResidentThreshold(double value) {
        if (value < MINIMUM_BACKFILL_RESIDENT_THRESHOLD) {
            value = DEFAULT_BACKFILL_RESIDENT_THRESHOLD;
        }
        backfillResidentThreshold = value;
    }

    static void addConfigChangeListener(EventuallyPersistentEngine &engine);

private:
    // Constants used to enforce the tap ack protocol
    uint32_t ackWindowSize;
    uint32_t ackInterval;
    rel_time_t ackGracePeriod;

    /**
     * To ease testing of corner cases we need to be able to seed the
     * initial tap sequence numbers (if not we would have to wrap an uin32_t)
     */
    uint32_t ackInitialSequenceNumber;

    // Parameters to control the backoff behavior of TAP producer
    size_t bgMaxPending;
    double backoffSleepTime;
    double requeueSleepTime;

    // Parameters to control the backfill
    size_t backfillBacklogLimit;
    double backfillResidentThreshold;

    EventuallyPersistentEngine &engine;
};

/**
 * TAP stream ep-engine specific data payload
 */
class TapEngineSpecific {
public:

    // size of item revision seq number
    static const short int sizeRevSeqno;
    // size of item specific extra data
    static const short int sizeExtra;
    // size of complete specific data
    static const short int sizeTotal;

    /**
     * Read engine specific data for a given tap event type
     *
     * @param ev tap event
     * @param engine_specific input tap engine specific data
     * @param nengine size of input data (bytes)
     * @param output sequence number
     * @param extra additional item specific data
     */
    static void readSpecificData(tap_event_t ev, void *engine_specific, uint16_t nengine,
                                 uint64_t *seqnum, uint8_t *extra = NULL);

    /**
     * Pack engine specific data for a given tap event type
     *
     * @param ev tap event
     * @param tp tap producer connection
     * @param seqnum item sequence number
     * @param nru value of the item replicated
     * @return size of tap engine specific data (bytes)
     */
    static uint16_t packSpecificData(tap_event_t ev, TapProducer *tp, uint64_t seqnum,
                                     uint8_t nru = 0xff);
};

/**
 * An abstract class representing a TAP connection. There are two different
 * types of a TAP connection, a producer and a consumer. The producers needs
 * to be able of being kept across connections, but the consumers don't contain
 * anything that can't be recreated.
 */
class TapConnection {
protected:
    /**
     * We need to be able to generate unique names, so let's just use a 64 bit counter
     */
    static Atomic<uint64_t> tapCounter;

    /**
     * The engine that owns the connection
     */
    EventuallyPersistentEngine &engine;
    /**
     * The cookie representing this connection (provided by the memcached code)
     */
    const void *cookie;
    /**
     * The name for this connection
     */
    std::string name;

    /**
     * Tap connection creation time
     */
    rel_time_t created;

    /**
     * Connection token created at TAP connection instantiation
     */
    hrtime_t connToken;

    /**
     * when this tap conneciton expires.
     */
    rel_time_t expiryTime;

    /**
     * Is this tap conenction connected?
     */
    bool connected;

    /**
     * Should we disconnect as soon as possible?
     */
    bool disconnect;

    /**
     * Number of times this connection was disconnected
     */
    Atomic<size_t> numDisconnects;

    bool supportAck;

    bool supportCheckpointSync;

    Atomic<bool> reserved;

    EPStats &stats;

    TapConnection(EventuallyPersistentEngine &theEngine,
                  const void *c, const std::string &n);

    template <typename T>
    void addStat(const char *nm, T val, ADD_STAT add_stat, const void *c);

    void addStat(const char *nm, bool val, ADD_STAT add_stat, const void *c) {
        addStat(nm, val ? "true" : "false", add_stat, c);
    }

    void setLogHeader(const std::string &header) {
        logString = header;
    }

public:
    /**
     * Release the reference "upstream".
     * @param force Should we force the release upstream even if the
     *              internal state indicates that the object isn't
     *              reserved upstream.
     */
    void releaseReference(bool force = false);

    //! cookie used by this connection
    const void *getCookie() const;

    //! cookie used by this connection
    void setCookie(const void *c) {
        cookie = c;
    }

    static uint64_t nextTapId() {
        return tapCounter++;
    }

    static std::string getAnonName() {
        std::stringstream s;
        s << "eq_tapq:anon_";
        s << nextTapId();
        return s.str();
    }

    const char* logHeader();

    virtual ~TapConnection();
    virtual const std::string &getName() const { return name; }
    void setName(const std::string &n) { name.assign(n); }
    void setReserved(bool r) { reserved = r; }
    bool isReserved() const { return reserved; }

    virtual const char *getType() const = 0;

    virtual void addStats(ADD_STAT add_stat, const void *c) {
        addStat("type", getType(), add_stat, c);
        addStat("created", created, add_stat, c);
        addStat("connected", connected, add_stat, c);
        addStat("pending_disconnect", doDisconnect(), add_stat, c);
        addStat("supports_ack", supportAck, add_stat, c);
        addStat("reserved", reserved, add_stat, c);

        if (numDisconnects > 0) {
            addStat("disconnects", numDisconnects, add_stat, c);
        }
    }

    virtual void processedEvent(tap_event_t event, ENGINE_ERROR_CODE ret) {
        (void)event;
        (void)ret;
    }

    void setSupportAck(bool ack) {
        supportAck = ack;
    }

    bool supportsAck() const {
        return supportAck;
    }

    void setSupportCheckpointSync(bool checkpointSync) {
        supportCheckpointSync = checkpointSync;
    }

    bool supportsCheckpointSync() const {
        return supportCheckpointSync;
    }

    void setExpiryTime(rel_time_t t) {
        expiryTime = t;
    }

    rel_time_t getExpiryTime() {
        return expiryTime;
    }

    hrtime_t getConnectionToken() const {
        return connToken;
    }

    void setConnected(bool s) {
        if (!s) {
            ++numDisconnects;
        }
        connected = s;
    }

    bool isConnected() {
        return connected;
    }

    bool doDisconnect() {
        return disconnect;
    }

    void setDisconnect(bool val) {
        disconnect = val;
    }

    static const char* opaqueCmdToString(uint32_t opaque_code);

private:
    std::string logString;
};

/**
 * Holder class for the
 */
class TapConsumer : public TapConnection {
private:
    Atomic<size_t> numDelete;
    Atomic<size_t> numDeleteFailed;
    Atomic<size_t> numFlush;
    Atomic<size_t> numFlushFailed;
    Atomic<size_t> numMutation;
    Atomic<size_t> numMutationFailed;
    Atomic<size_t> numOpaque;
    Atomic<size_t> numOpaqueFailed;
    Atomic<size_t> numVbucketSet;
    Atomic<size_t> numVbucketSetFailed;
    Atomic<size_t> numCheckpointStart;
    Atomic<size_t> numCheckpointStartFailed;
    Atomic<size_t> numCheckpointEnd;
    Atomic<size_t> numCheckpointEndFailed;
    Atomic<size_t> numUnknown;

public:
    TapConsumer(EventuallyPersistentEngine &theEngine,
                const void *c,
                const std::string &n);
    virtual void processedEvent(tap_event_t event, ENGINE_ERROR_CODE ret);
    virtual void addStats(ADD_STAT add_stat, const void *c);
    virtual const char *getType() const { return "consumer"; };
    virtual bool processCheckpointCommand(tap_event_t event, uint16_t vbucket,
                                          uint64_t checkpointId);
    virtual void checkVBOpenCheckpoint(uint16_t);
    void setBackfillPhase(bool isBackfill, uint16_t vbucket);
    bool isBackfillPhase(uint16_t vbucket);
};



/**
 * Class used by the EventuallyPersistentEngine to keep track of all
 * information needed per Tap connection.
 */
class TapProducer : public TapConnection {
public:
    virtual void addStats(ADD_STAT add_stat, const void *c);
    virtual void processedEvent(tap_event_t event, ENGINE_ERROR_CODE ret);
    virtual const char *getType() const { return "producer"; };

    void aggregateQueueStats(TapCounter* stats_aggregator);

    bool isSuspended() const;
    void setSuspended_UNLOCKED(bool value);
    void setSuspended(bool value);

    bool isTimeForNoop();
    void setTimeForNoop();

    void completeBackfill() {
        LockHolder lh(queueLock);
        if (pendingBackfillCounter > 0) {
            --pendingBackfillCounter;
        }
        completeBackfillCommon_UNLOCKED();
    }

    void scheduleDiskBackfill() {
        LockHolder lh(queueLock);
        ++diskBackfillCounter;
    }

    void completeDiskBackfill() {
        LockHolder lh(queueLock);
        if (diskBackfillCounter > 0) {
            --diskBackfillCounter;
        }
        completeBackfillCommon_UNLOCKED();
    }

    /**
     * Invoked each time a background item fetch completes.
     */
    void completeBGFetchJob(Item *item, uint16_t vbid, bool implicitEnqueue);

    /**
     * Find out how many items are still remaining from backfill.
     */
    size_t getBackfillRemaining() {
        LockHolder lh(queueLock);
        return getBackfillRemaining_UNLOCKED();
    }

    void incrBackfillRemaining(size_t incr);

    /**
     * Return the current backfill queue size.
     * This differs from getBackfillRemaining() that returns the approximated size
     * of total backfill backlogs.
     */
    size_t getBackfillQueueSize() {
        LockHolder lh(queueLock);
        return getBackfillQueueSize_UNLOCKED();
    }

    /**
     * Return the live replication queue size.
     */
    size_t getQueueSize() {
        LockHolder lh(queueLock);
        return getQueueSize_UNLOCKED();
    }

    void setTapFlagByteorderSupport(bool enable) {
        tapFlagByteorderSupport = enable;
    }
    bool haveTapFlagByteorderSupport(void) const {
        return tapFlagByteorderSupport;
    }

    bool isReconnected() const {
        return reconnects > 0;
    }

    void clearQueues() {
        LockHolder lh(queueLock);
        clearQueues_UNLOCKED();
    }

private:
    friend class EventuallyPersistentEngine;
    friend class TapConnMap;
    friend class BackFillVisitor;
    friend class TapBGFetchCallback;
    friend struct TapStatBuilder;
    friend struct TapAggStatBuilder;
    friend struct PopulateEventsBody;
    friend class TapEngineSpecific;

    /**
     * Get the next item (e.g., checkpoint_start, checkpoint_end, tap_mutation, or
     * tap_deletion) to be transmitted.
     */
    Item *getNextItem(const void *c, uint16_t *vbucket, tap_event_t &ret,
                      uint8_t &nru);

    /**
     * Check if TAP_DUMP or TAP_TAKEOVER is completed and close the connection if
     * all messages including vbucket_state change commands are sent.
     */
    TapVBucketEvent checkDumpOrTakeOverCompletion();

    void completeBackfillCommon_UNLOCKED() {
        if (mayCompleteDumpOrTakeover_UNLOCKED() && idle_UNLOCKED()) {
            // There is no data for this connection..
            // Just go ahead and disconnect it.
            setDisconnect(true);
        }
    }

    /**
     * Add a new item to the tap queue. You need to hold the queue lock
     * before calling this function
     * The item may be ignored if the TapProducer got a vbucket filter
     * associated and the item's vbucket isn't part of the filter.
     *
     * @return true if the the queue was empty
     */
    bool addEvent_UNLOCKED(const queued_item &it);

    /**
     * Add a new item to the tap queue.
     * The item may be ignored if the TapProducer got a vbucket filter
     * associated and the item's vbucket isn't part of the filter.
     *
     * @return true if the the queue was empty
     */
    bool addEvent(const queued_item &it) {
        LockHolder lh(queueLock);
        return addEvent_UNLOCKED(it);
    }

    /**
     * Add a key to the tap queue. You need the queue lock to call this
     * @return true if the the queue was empty
     */
    bool addEvent_UNLOCKED(const std::string &key, uint16_t vbid, enum queue_operation op) {
        queued_item qi(new QueuedItem(key, vbid, op));
        return addEvent_UNLOCKED(qi);
    }

    bool addEvent(const std::string &key, uint16_t vbid, enum queue_operation op) {
        LockHolder lh(queueLock);
        return addEvent_UNLOCKED(key, vbid, op);
    }

    void addTapLogElement_UNLOCKED(const queued_item &qi) {
        if (supportAck) {
            TapLogElement log(seqno, qi);
            tapLog.push_back(log);
            stats.memOverhead.incr(sizeof(TapLogElement));
            assert(stats.memOverhead.get() < GIGANTOR);
        }
    }
    void addTapLogElement(const queued_item &qi) {
        LockHolder lh(queueLock);
        addTapLogElement_UNLOCKED(qi);
    }

    void addTapLogElement_UNLOCKED(const TapVBucketEvent &e) {
        if (supportAck) {
            // add to the log!
            TapLogElement log(seqno, e);
            tapLog.push_back(log);
            stats.memOverhead.incr(sizeof(TapLogElement));
            assert(stats.memOverhead.get() < GIGANTOR);
        }
    }

    /**
     * Get the next item from the queue that has items fetched from memory.
     */
    queued_item nextFgFetched_UNLOCKED(bool &shouldPause);

    void addVBucketHighPriority_UNLOCKED(TapVBucketEvent &ev) {
        vBucketHighPriority.push(ev);
    }


    /**
     * Add a new high priority TapVBucketEvent to this TapProducer. A high
     * priority TapVBucketEvent will bypass the the normal queue of events to
     * be sent to the client, and be sent the next time it is possible to
     * send data over the tap connection.
     */
    void addVBucketHighPriority(TapVBucketEvent &ev) {
        LockHolder lh(queueLock);
        addVBucketHighPriority_UNLOCKED(ev);
    }

    /**
     * Get the next high priority TapVBucketEvent for this TapProducer
     */
    TapVBucketEvent nextVBucketHighPriority_UNLOCKED();

    TapVBucketEvent nextVBucketHighPriority() {
        LockHolder lh(queueLock);
        return nextVBucketHighPriority_UNLOCKED();
    }

    void addVBucketLowPriority_UNLOCKED(TapVBucketEvent &ev) {
        vBucketLowPriority.push(ev);
    }

    /**
     * Add a new low priority TapVBucketEvent to this TapProducer. A low
     * priority TapVBucketEvent will only be sent when the tap connection
     * doesn't have any other events to send.
     */
    void addVBucketLowPriority(TapVBucketEvent &ev) {
        LockHolder lh(queueLock);
        addVBucketLowPriority_UNLOCKED(ev);
    }

    /**
     * Get the next low priority TapVBucketEvent for this TapProducer.
     */
    TapVBucketEvent nextVBucketLowPriority_UNLOCKED();

    TapVBucketEvent nextVBucketLowPriority() {
        LockHolder lh(queueLock);
        return nextVBucketLowPriority_UNLOCKED();
    }

    void addCheckpointMessage_UNLOCKED(const queued_item &qi) {
        checkpointMsgs.push(qi);
    }

    /**
     * Add a checkpoint start / end message to the checkpoint message queue. These messages
     * are used for synchronizing checkpoints between tap producer and consumer.
     */
    void addCheckpointMessage(const queued_item &qi) {
        LockHolder lh(queueLock);
        addCheckpointMessage_UNLOCKED(qi);
    }

    queued_item nextCheckpointMessage_UNLOCKED();

    queued_item nextCheckpointMessage() {
        LockHolder lh(queueLock);
        return nextCheckpointMessage_UNLOCKED();
    }

    bool hasItemFromVBHashtable_UNLOCKED() {
        return !queue->empty() || hasNextFromCheckpoints_UNLOCKED();
    }

    bool hasItemFromDisk_UNLOCKED() {
        return !backfilledItems.empty();
    }

    bool emptyQueue_UNLOCKED() {
        return !hasItemFromDisk_UNLOCKED() && (bgJobIssued - bgJobCompleted) == 0 &&
               !hasItemFromVBHashtable_UNLOCKED();
    }

    bool idle_UNLOCKED() {
        return emptyQueue_UNLOCKED() && vBucketLowPriority.empty() &&
               vBucketHighPriority.empty() && checkpointMsgs.empty() && tapLog.empty();
    }

    bool idle() {
        LockHolder lh(queueLock);
        return idle_UNLOCKED();
    }

    bool hasItemFromDisk() {
        LockHolder lh(queueLock);
        return hasItemFromDisk_UNLOCKED();
    }

    bool hasItemFromVBHashtable() {
        LockHolder lh(queueLock);
        return hasItemFromVBHashtable_UNLOCKED();
    }

    bool emptyQueue() {
        LockHolder lh(queueLock);
        return emptyQueue_UNLOCKED();
    }

    size_t getBackfillRemaining_UNLOCKED();

    size_t getBackfillQueueSize_UNLOCKED();

    size_t getQueueSize_UNLOCKED();

    size_t getQueueMemory() {
        return queueMemSize;
    }

    size_t getRemaingOnDisk() {
         LockHolder lh(queueLock);
         return bgJobIssued - bgJobCompleted;
    }

    size_t getQueueFillTotal() {
         return queueFill;
    }

    size_t getQueueDrainTotal() {
         return queueDrain;
    }

    size_t getQueueBackoff() {
         return numTapNack;
    }

    /**
     * Get the total number of remaining items from all checkpoints.
     */
    size_t getRemainingOnCheckpoints_UNLOCKED();
    size_t getRemainingOnCheckpoints() {
        LockHolder lh(queueLock);
        return getRemainingOnCheckpoints_UNLOCKED();
    }

    bool hasNextFromCheckpoints_UNLOCKED();
    bool hasNextFromCheckpoints() {
        LockHolder lh(queueLock);
        return hasNextFromCheckpoints_UNLOCKED();
    }

    /**
     * Get the next item from the queue that has items fetched from disk.
     */
    Item* nextBgFetchedItem_UNLOCKED();

    void flush();

    bool shouldFlush() {
        bool ret = pendingFlush;
        pendingFlush = false;
        return ret;
    }

    // This method is called while holding the tapNotifySync lock.
    void appendQueue(std::list<queued_item> *q);

    bool isPendingDiskBackfill() {
        LockHolder lh(queueLock);
        return diskBackfillCounter > 0;
    }

    /**
     * A backfill is pending if the backfill thread is still running
     */
    bool isPendingBackfill_UNLOCKED() {
        return doRunBackfill || pendingBackfillCounter > 0 || diskBackfillCounter > 0;
    }

    bool isPendingBackfill() {
        LockHolder lh(queueLock);
        return isPendingBackfill_UNLOCKED();
    }

    /**
     * Items from backfill are all successfully transmitted to the destination?
     */
    bool isBackfillCompleted_UNLOCKED() {
        return backfillCompleted;
    }

    bool isBackfillCompleted() {
        LockHolder lh(queueLock);
        return isBackfillCompleted_UNLOCKED();
    }

    void scheduleBackfill_UNLOCKED(const std::vector<uint16_t> &vblist);

    void scheduleBackfill(const std::vector<uint16_t> &vblist) {
        LockHolder lh(queueLock);
        scheduleBackfill_UNLOCKED(vblist);
    }

    bool runBackfill(VBucketFilter &vbFilter);

    /**
     * True if the TAP producer doesn't have any queued items and is ready for
     * for completing TAP_DUMP or TAP_VBUCKET_TAKEOVER.
     */
    bool mayCompleteDumpOrTakeover_UNLOCKED(void) {
        return (dumpQueue || doTakeOver) && isBackfillCompleted_UNLOCKED() &&
               emptyQueue_UNLOCKED();
    }

    bool mayCompleteDumpOrTakeover(void) {
        LockHolder lh(queueLock);
        return mayCompleteDumpOrTakeover_UNLOCKED();
    }

    /**
     * Queue an item to be background fetched.
     *
     * @param key the item's key
     * @param id the disk id of the item to fetch
     * @param vb the vbucket ID
     */
    void queueBGFetch_UNLOCKED(const std::string &key, uint64_t id,
                               uint16_t vb);

    TapProducer(EventuallyPersistentEngine &theEngine,
                const void *cookie,
                const std::string &n,
                uint32_t f);

    ~TapProducer() {
        delete queue;
        delete []specificData;
        delete []transmitted;
        assert(!isReserved());
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
     * @param vbucket the vbucket Id for this message
     * @return true if we should request a tap ack (and start a new sequence)
     */
    bool requestAck(tap_event_t event, uint16_t vbucket);

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

    void evaluateFlags();

    bool waitForCheckpointMsgAck();

    bool waitForOpaqueMsgAck();

    void setRegisteredClient(bool isRegisteredClient);

    void setClosedCheckpointOnlyFlag(bool isClosedCheckpointOnly);

    bool SetCursorToOpenCheckpoint(uint16_t vbucket);

    void setTakeOverCompletionPhase(bool completionPhase) {
        takeOverCompletionPhase = completionPhase;
    }

    bool checkBackfillCompletion_UNLOCKED();
    bool checkBackfillCompletion() {
        LockHolder lh(queueLock);
        return checkBackfillCompletion_UNLOCKED();
    }

    void setBackfillAge(uint64_t age, bool reconnect);

    void setVBucketFilter(const std::vector<uint16_t> &vbuckets,
                          bool notifyCompletion = false);

    const VBucketFilter &getVBucketFilter() {
        LockHolder lh(queueLock);
        return vbucketFilter;
    }

    bool checkVBucketFilter(uint16_t vbucket) {
        LockHolder lh(queueLock);
        return vbucketFilter(vbucket);
    }

    /**
     * Register the unified queue cursor for this TAP producer.
     */
    void registerTAPCursor(const std::map<uint16_t, uint64_t> &lastCheckpointIds);

    size_t getTapAckLogSize(void) {
        LockHolder lh(queueLock);
        return tapLog.size();
    }

    void reschedule_UNLOCKED(const std::list<TapLogElement>::iterator &iter);

    void clearQueues_UNLOCKED();


    //! Lock held during queue operations.
    Mutex queueLock;
    //! Queue of live stream items that needs to be sent
    std::list<queued_item> *queue;
    //! Live stream queue size
    size_t queueSize;
    //! Queue of items backfilled from disk
    std::queue<Item*> backfilledItems;
    //! List of items that are waiting for acks from the client
    std::list<TapLogElement> tapLog;

    //! Keeps track of items transmitted per VBucket
    Atomic<size_t> *transmitted;

    //! VBucket status messages immediately (before userdata)
    std::queue<TapVBucketEvent> vBucketHighPriority;
    //! VBucket status messages sent when there is nothing else to send
    std::queue<TapVBucketEvent> vBucketLowPriority;

    //! Checkpoint start and end messages
    std::queue<queued_item> checkpointMsgs;
    //! Checkpoint state per vbucket
    std::map<uint16_t, TapCheckpointState> tapCheckpointState;

    //! Flags passed by the client
    uint32_t flags;
    //! Number of records fetched from this stream since the
    size_t recordsFetched;
    //! Number of records skipped due to changing the filter on the connection
    Atomic<size_t> recordsSkipped;
    //! Do we have a pending flush command?
    bool pendingFlush;
    //! Number of times this client reconnected
    uint32_t reconnects;
    //! Connection is temporarily paused?
    Atomic<bool> paused;
    //! Backfill age for the connection
    uint64_t backfillAge;

    //! Dump and disconnect?
    bool dumpQueue;
    //! Take over and disconnect?
    bool doTakeOver;
    //! Take over completion phase?
    bool takeOverCompletionPhase;

    //! Should a new backfill task be scheduled now?
    bool doRunBackfill;
    //! True if items from backfill are all successfully transmitted to the destination.
    bool backfillCompleted;
    //! Number of pending backfill tasks
    size_t pendingBackfillCounter;
    //! Number of vbuckets that are currently scheduled for disk backfill.
    size_t diskBackfillCounter;
    //! Total backfill backlogs
    size_t totalBackfillBacklogs;

    //! Filter for the vbuckets we want.
    VBucketFilter vbucketFilter;
    //! Filter for the vbuckets that require backfill by the next backfill task
    VBucketFilter backFillVBucketFilter;
    //! vbuckets that are being backfilled by the current backfill session
    std::set<uint16_t> backfillVBuckets;

    Atomic<size_t> bgResultSize;
    Atomic<size_t> bgJobIssued;
    Atomic<size_t> bgJobCompleted;
    Atomic<size_t> numTapNack;
    Atomic<size_t> queueMemSize;
    Atomic<size_t> queueFill;
    Atomic<size_t> queueDrain;
    Atomic<size_t> checkpointMsgCounter;
    Atomic<size_t> opaqueMsgCounter;

    //! Current tap sequence number (for ack's)
    uint32_t seqno;
    //! The last tap sequence number received
    uint32_t seqnoReceived;
    //! The last tap sequence number for which an ack is requested
    uint32_t seqnoAckRequested;
    //! Flag indicating if the pending memcached connection is notified
    Atomic<bool> notifySent;

    //! tap opaque command code.
    uint32_t opaqueCommandCode;

    //! Is this tap connection in a suspended state
    bool suspended;
    //! Textual representation of the vbucket filter.
    std::string filterText;
    //! Textual representation of the flags..
    std::string flagsText;

    //! Is this TAP producer for the registered TAP client?
    Atomic<bool> registeredTAPClient;
    //! Is this TAP producer for replicating items from the closed checkpoints only?
    Atomic<bool> closedCheckpointOnly;

    Atomic<rel_time_t> lastWalkTime;
    Atomic<rel_time_t> lastMsgTime;

    bool isLastAckSucceed;
    bool isSeqNumRotated;

    //! Should we send a NOOP message now?
    Atomic<bool> noop;
    size_t numNoops;

    //! Does the Tap Consumer know about the byteorder bug for the flags
    bool tapFlagByteorderSupport;

    //! EP-engine specific item info
    uint8_t *specificData;
    //! Timestamp of backfill start
    time_t backfillTimestamp;

    DISALLOW_COPY_AND_ASSIGN(TapProducer);
};

#endif  // SRC_TAPCONNECTION_H_
