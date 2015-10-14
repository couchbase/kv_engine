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
#include "statwriter.h"

// forward decl
class ConnHandler;
class EventuallyPersistentEngine;
class TapConnMap;
class TapProducer;
class BGFetchCallback;
class CompleteBackfillOperation;
class Dispatcher;
class Item;
class VBucketFilter;

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
class VBucketEvent {
public:
    /**
     * Create a new instance of the VBucketEvent and initialize
     * its members.
     * @param ev Type of event
     * @param b The bucket this event belongs to
     * @param s The state change for this event
     */
    VBucketEvent(uint16_t ev, uint16_t b, vbucket_state_t s) :
        event(ev), vbucket(b), state(s) {}
    uint16_t event;
    uint16_t vbucket;
    vbucket_state_t state;
};

/**
 * Aggregator object to count all tap stats.
 */
struct ConnCounter {
    ConnCounter()
        : conn_queue(0), totalConns(0), totalProducers(0),
          conn_queueFill(0), conn_queueDrain(0), conn_totalBytes(0), conn_queueRemaining(0),
          conn_queueBackoff(0), conn_queueBackfillRemaining(0), conn_queueItemOnDisk(0),
          conn_totalBacklogSize(0)
    {}

    ConnCounter& operator+=(const ConnCounter& other) {
        conn_queue += other.conn_queue;
        totalConns += other.totalConns;
        totalProducers += other.totalProducers;
        conn_queueFill += other.conn_queueFill;
        conn_queueDrain += other.conn_queueDrain;
        conn_totalBytes += other.conn_totalBytes;
        conn_queueRemaining += other.conn_queueRemaining;
        conn_queueBackoff += other.conn_queueBackoff;
        conn_queueBackfillRemaining += other.conn_queueBackfillRemaining;
        conn_queueItemOnDisk += other.conn_queueItemOnDisk;
        conn_totalBacklogSize += other.conn_totalBacklogSize;

        return *this;
    }

    size_t      conn_queue;
    size_t      totalConns;
    size_t      totalProducers;

    size_t      conn_queueFill;
    size_t      conn_queueDrain;
    size_t      conn_totalBytes;
    size_t      conn_queueRemaining;
    size_t      conn_queueBackoff;
    size_t      conn_queueBackfillRemaining;
    size_t      conn_queueItemOnDisk;
    size_t      conn_totalBacklogSize;
};

/**
 * Represents an item that has been sent over tap, but may need to be
 * rolled back if acks fail.
 */
class TapLogElement {

public:

    TapLogElement(uint32_t seqno, const VBucketEvent &e)
        : seqno_(seqno), event_(e.event), vbucket_(e.vbucket),
          state_(e.state) { }

    TapLogElement(uint32_t seqno, const queued_item &qi)
    {
        seqno_ = seqno;
        event_ = TAP_MUTATION;
        vbucket_ = qi->getVBucketId();
        state_ = vbucket_state_active;
        item_ = qi;

        switch(item_->getOperation()) {
        case queue_op_set:
            event_ = TAP_MUTATION;
            break;
        case queue_op_del:
            event_ = TAP_DELETION;
            break;
        case queue_op_flush:
            event_ = TAP_FLUSH;
            break;
        case queue_op_checkpoint_start:
            event_ = TAP_CHECKPOINT_START;
            break;
        case queue_op_checkpoint_end:
            event_ = TAP_CHECKPOINT_END;
            break;
        default:
            break;
        }
    }

    uint32_t seqno_;
    uint16_t event_;
    uint16_t vbucket_;

    vbucket_state_t state_;
    queued_item item_;
};

typedef enum {
    TAP_CONN, //!< TAP connnection
    DCP_CONN  //!< DCP connection
} conn_type_t;

class ConnHandler : public RCValue {
public:
    ConnHandler(EventuallyPersistentEngine& engine, const void* c,
                const std::string& name);

    virtual ~ConnHandler() {}

    virtual ENGINE_ERROR_CODE addStream(uint32_t opaque, uint16_t vbucket,
                                        uint32_t flags);

    virtual ENGINE_ERROR_CODE closeStream(uint32_t opaque, uint16_t vbucket);

    virtual ENGINE_ERROR_CODE streamEnd(uint32_t opaque, uint16_t vbucket,
                                        uint32_t flags);

    virtual ENGINE_ERROR_CODE mutation(uint32_t opaque, const void* key,
                                       uint16_t nkey, const void* value,
                                       uint32_t nvalue, uint64_t cas,
                                       uint16_t vbucket, uint32_t flags,
                                       uint8_t datatype, uint32_t locktime,
                                       uint64_t bySeqno, uint64_t revSeqno,
                                       uint32_t exptime, uint8_t nru,
                                       const void* meta, uint16_t nmeta);

    virtual ENGINE_ERROR_CODE deletion(uint32_t opaque, const void* key,
                                       uint16_t nkey, uint64_t cas,
                                       uint16_t vbucket, uint64_t bySeqno,
                                       uint64_t revSeqno, const void* meta,
                                       uint16_t nmeta);

    virtual ENGINE_ERROR_CODE expiration(uint32_t opaque, const void* key,
                                         uint16_t nkey, uint64_t cas,
                                         uint16_t vbucket, uint64_t bySeqno,
                                         uint64_t revSeqno, const void* meta,
                                         uint16_t nmeta);

    virtual ENGINE_ERROR_CODE snapshotMarker(uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint32_t flags);

    virtual ENGINE_ERROR_CODE flushall(uint32_t opaque, uint16_t vbucket);

    virtual ENGINE_ERROR_CODE setVBucketState(uint32_t opaque, uint16_t vbucket,
                                              vbucket_state_t state);

    virtual ENGINE_ERROR_CODE getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                             dcp_add_failover_log callback);

    virtual ENGINE_ERROR_CODE streamRequest(uint32_t flags,
                                            uint32_t opaque,
                                            uint16_t vbucket,
                                            uint64_t start_seqno,
                                            uint64_t end_seqno,
                                            uint64_t vbucket_uuid,
                                            uint64_t snapStartSeqno,
                                            uint64_t snapEndSeqno,
                                            uint64_t *rollback_seqno,
                                            dcp_add_failover_log callback);

    virtual ENGINE_ERROR_CODE noop(uint32_t opaque);

    virtual ENGINE_ERROR_CODE bufferAcknowledgement(uint32_t opaque,
                                                    uint16_t vbucket,
                                                    uint32_t buffer_bytes);

    virtual ENGINE_ERROR_CODE control(uint32_t opaque, const void* key,
                                      uint16_t nkey, const void* value,
                                      uint32_t nvalue);

    virtual ENGINE_ERROR_CODE step(struct dcp_message_producers* producers);

    virtual ENGINE_ERROR_CODE handleResponse(
                                        protocol_binary_response_header *resp);

    EventuallyPersistentEngine& engine() {
        return engine_;
    }

    const char* logHeader() {
        return logString.c_str();
    }

    void setLogHeader(const std::string &header) {
        logString = header;
    }

    void releaseReference(bool force = false);

    void setSupportAck(bool ack) {
        supportAck = ack;
    }

    bool supportsAck() const {
        return supportAck;
    }

    void setSupportCheckpointSync(bool checkpointSync) {
        supportCheckpointSync_ = checkpointSync;
    }

    bool supportsCheckpointSync() const {
        return supportCheckpointSync_;
    }

    virtual const char *getType() const = 0;

    template <typename T>
    void addStat(const char *nm, const T &val, ADD_STAT add_stat, const void *c) {
        std::stringstream tap;
        tap << name << ":" << nm;
        std::stringstream value;
        value << val;
        std::string n = tap.str();
        add_casted_stat(n.data(), value.str().data(), add_stat, c);
    }

    void addStat(const char *nm, bool val, ADD_STAT add_stat, const void *c) {
        addStat(nm, val ? "true" : "false", add_stat, c);
    }

    virtual void addStats(ADD_STAT add_stat, const void *c) {
        addStat("type", getType(), add_stat, c);
        addStat("created", created, add_stat, c);
        addStat("connected", connected, add_stat, c);
        addStat("pending_disconnect", disconnect, add_stat, c);
        addStat("supports_ack", supportAck, add_stat, c);
        addStat("reserved", reserved.load(), add_stat, c);

        if (numDisconnects > 0) {
            addStat("disconnects", numDisconnects.load(), add_stat, c);
        }
    }

    virtual void aggregateQueueStats(ConnCounter* stats_aggregator) {
        // Empty
    }

    virtual void processedEvent(uint16_t event, ENGINE_ERROR_CODE ret) {
        (void) event;
        (void) ret;
    }

    const std::string &getName() const {
        return name;
    }

    void setName(const std::string &n) {
        name.assign(n);
    }

    bool setReserved(bool r) {
        bool inverse = !r;
        return reserved.compare_exchange_strong(inverse, r);
    }

    bool isReserved() const {
        return reserved;
    }

    const void *getCookie() const {
        return cookie;
    }

    void setCookie(const void *c) {
        cookie = c;
    }

    void setExpiryTime(rel_time_t t) {
        expiryTime = t;
    }

    rel_time_t getExpiryTime() {
        return expiryTime;
    }

    void setLastWalkTime() {
        lastWalkTime.store(ep_current_time());
    }

    rel_time_t getLastWalkTime() {
        return lastWalkTime.load();
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

    virtual void setDisconnect(bool val) {
        disconnect = val;
    }

    static std::string getAnonName() {
        uint64_t nextConnId = counter_++;
        std::stringstream s;
        s << "eq_tapq:anon_";
        s << nextConnId;
        return s.str();
    }

    hrtime_t getConnectionToken() const {
        return connToken;
    }

protected:
    EventuallyPersistentEngine &engine_;
    EPStats &stats;
    bool supportCheckpointSync_;

private:

     //! The name for this connection
    std::string name;

    //! The string used to prefix all log messages for this connection
    std::string logString;

    //! The cookie representing this connection (provided by the memcached code)
    const void* cookie;

    //! Whether or not the connection is reserved in the memcached layer
    AtomicValue<bool> reserved;

    //! Connection token created at connection instantiation time
    hrtime_t connToken;

    //! Connection creation time
    rel_time_t created;

    //! The last time this connection's step function was called
    AtomicValue<rel_time_t> lastWalkTime;

    //! Should we disconnect as soon as possible?
    bool disconnect;

    //! Is this tap conenction connected?
    bool connected;

    //! Number of times this connection was disconnected
    AtomicValue<size_t> numDisconnects;

    //! when this tap conneciton expires.
    rel_time_t expiryTime;

    //! Whether or not this connection supports acking
    bool supportAck;

    //! A counter used to generate unique names
    static AtomicValue<uint64_t> counter_;
};

typedef enum {
    backfill,
    checkpoint_start,
    checkpoint_end,
    checkpoint_end_synced
} proto_checkpoint_state;


/**
 * Checkpoint state of each vbucket in TAP or DCP stream.
 */
class CheckpointState {
public:
    CheckpointState() :
        currentCheckpointId(0), lastSeqNum(0), bgResultSize(0),
        bgJobIssued(0), bgJobCompleted(0), lastItem(false), state(backfill) {}

    CheckpointState(uint16_t vb, uint64_t checkpointId, proto_checkpoint_state s) :
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
    proto_checkpoint_state state;
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
    static void readSpecificData(uint16_t ev, void *engine_specific, uint16_t nengine,
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
    static uint16_t packSpecificData(uint16_t ev, TapProducer *tp, uint64_t seqnum,
                                     uint8_t nru = 0xff);
};


/**
 */
class Consumer : public ConnHandler {
private:
    AtomicValue<size_t> numDelete;
    AtomicValue<size_t> numDeleteFailed;
    AtomicValue<size_t> numFlush;
    AtomicValue<size_t> numFlushFailed;
    AtomicValue<size_t> numMutation;
    AtomicValue<size_t> numMutationFailed;
    AtomicValue<size_t> numOpaque;
    AtomicValue<size_t> numOpaqueFailed;
    AtomicValue<size_t> numVbucketSet;
    AtomicValue<size_t> numVbucketSetFailed;
    AtomicValue<size_t> numCheckpointStart;
    AtomicValue<size_t> numCheckpointStartFailed;
    AtomicValue<size_t> numCheckpointEnd;
    AtomicValue<size_t> numCheckpointEndFailed;
    AtomicValue<size_t> numUnknown;

public:
    Consumer(EventuallyPersistentEngine &theEngine, const void* cookie,
             const std::string& name);
    virtual void processedEvent(uint16_t event, ENGINE_ERROR_CODE ret);
    virtual void addStats(ADD_STAT add_stat, const void *c);
    virtual const char *getType() const { return "consumer"; };
    virtual void checkVBOpenCheckpoint(uint16_t);
    void setBackfillPhase(bool isBackfill, uint16_t vbucket);
    bool isBackfillPhase(uint16_t vbucket);
    ENGINE_ERROR_CODE setVBucketState(uint32_t opaque, uint16_t vbucket,
                                      vbucket_state_t state);
};


/*
 * auxIODispatcher/GIO task that performs a background fetch on behalf
 * of TAP/DCP.
 */
class BGFetchCallback : public GlobalTask {
public:
    BGFetchCallback(EventuallyPersistentEngine *e, const std::string &n,
                    const std::string &k, uint16_t vbid, hrtime_t token,
                    const Priority &p, double sleeptime = 0) :
        GlobalTask(e, p, sleeptime, false), name(n), key(k), epe(e),
        init(gethrtime()), connToken(token), vbucket(vbid)
    {
        cb_assert(epe);
    }

    bool run();

    std::string getDescription() {
        std::stringstream ss;
        ss << "Fetching item from disk for tap: " << key;
        return ss.str();
    }

private:
    const std::string name;
    const std::string key;
    EventuallyPersistentEngine *epe;
    hrtime_t init;
    hrtime_t connToken;
    uint16_t vbucket;
};


class TapConsumer : public Consumer {
public:
    TapConsumer(EventuallyPersistentEngine &e, const void *c,
                const std::string &n);

    ~TapConsumer() {}

    ENGINE_ERROR_CODE mutation(uint32_t opaque, const void* key, uint16_t nkey,
                               const void* value, uint32_t nvalue, uint64_t cas,
                               uint16_t vbucket, uint32_t flags,
                               uint8_t datatype, uint32_t locktime,
                               uint64_t bySeqno, uint64_t revSeqno,
                               uint32_t exptime, uint8_t nru, const void* meta,
                               uint16_t nmeta);

    ENGINE_ERROR_CODE deletion(uint32_t opaque, const void* key, uint16_t nkey,
                               uint64_t cas, uint16_t vbucket, uint64_t bySeqno,
                               uint64_t revSeqno, const void* meta,
                               uint16_t nmeta);

    bool processCheckpointCommand(uint8_t event, uint16_t vbucket,
                                  uint64_t checkpointId);
};

class Notifiable {
public:
    Notifiable()
      : suspended(false), paused(false),
        notificationScheduled(false), notifySent(false) {}

    virtual ~Notifiable() {}

    bool isPaused() {
        return paused;
    }

    void setPaused(bool p) {
        paused.store(p);
    }

    bool isNotificationScheduled() {
        return notificationScheduled;
    }

    bool setNotificationScheduled(bool val) {
        bool inverse = !val;
        return notificationScheduled.compare_exchange_strong(inverse, val);
    }

    bool setNotifySent(bool val) {
        bool inverse = !val;
        return notifySent.compare_exchange_strong(inverse, val);
    }

    bool sentNotify() {
        return notifySent;
    }

    bool setSuspended(bool val) {
        bool inverse = !val;
        return suspended.compare_exchange_strong(inverse, val);
    }

    bool isSuspended() {
        return suspended;
    }

private:
    //! Is this tap connection in a suspended state
    AtomicValue<bool> suspended;
    //! Connection is temporarily paused?
    AtomicValue<bool> paused;
    //! Flag indicating if the notification event is scheduled
    AtomicValue<bool> notificationScheduled;
        //! Flag indicating if the pending memcached connection is notified
    AtomicValue<bool> notifySent;
};

class Producer : public ConnHandler, public Notifiable {
public:
    Producer(EventuallyPersistentEngine &engine, const void* cookie,
             const std::string& name) :
        ConnHandler(engine, cookie, name),
        Notifiable(),
        vbucketFilter(),
        totalBackfillBacklogs(0),
        reconnects(0) {}

    void addStats(ADD_STAT add_stat, const void *c);

    bool isReconnected() const {
        return reconnects > 0;
    }

    void reconnected() {
        ++reconnects;
    }

    virtual bool isTimeForNoop() = 0;

    virtual void setTimeForNoop() = 0;

    const char *getType() const { return "producer"; }

    virtual void clearQueues() = 0;

    virtual void appendQueue(std::list<queued_item> *q) = 0;

    virtual size_t getBackfillQueueSize() = 0;

    void incrBackfillRemaining(size_t incr) {
        LockHolder lh(queueLock);
        totalBackfillBacklogs += incr;
    }

    virtual void flush() = 0;

    virtual bool windowIsFull() = 0;

    const VBucketFilter &getVBucketFilter() {
        LockHolder lh(queueLock);
        return vbucketFilter;
    }

    virtual ~Producer() {}

protected:
    friend class ConnMap;

    //! Lock held during queue operations.
    Mutex queueLock;
    //! Filter for the vbuckets we want.
    VBucketFilter vbucketFilter;
    //! Total backfill backlogs
    size_t totalBackfillBacklogs;

private:
    //! Number of times this client reconnected
    uint32_t reconnects;
};

/**
 * Class used by the EventuallyPersistentEngine to keep track of all
 * information needed per Tap or DCP connection.
 */
class TapProducer : public Producer {
public:
    TapProducer(EventuallyPersistentEngine &engine,
             const void *cookie,
             const std::string &n,
             uint32_t f);

    virtual ~TapProducer() {
        delete queue;
        delete []specificData;
        delete []transmitted;
        cb_assert(!isReserved());
    }

    virtual void addStats(ADD_STAT add_stat, const void *c);
    virtual void processedEvent(uint16_t event, ENGINE_ERROR_CODE ret);

    void aggregateQueueStats(ConnCounter* stats_aggregator);

    void suspendedConnection_UNLOCKED(bool value);
    void suspendedConnection(bool value);

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
     * Get the next item (e.g., checkpoint_start, checkpoint_end, tap_mutation, or
     * tap_deletion) to be transmitted.
     */
    Item *getNextItem(const void *c, uint16_t *vbucket, uint16_t &ret,
                      uint8_t &nru);

    /**
     * Find out how many items are still remaining from backfill.
     */
    size_t getBackfillRemaining() {
        LockHolder lh(queueLock);
        return getBackfillRemaining_UNLOCKED();
    }

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

    void setFlagByteorderSupport(bool enable) {
        flagByteorderSupport = enable;
    }
    bool haveFlagByteorderSupport(void) const {
        return flagByteorderSupport;
    }

    void clearQueues() {
        LockHolder lh(queueLock);
        clearQueues_UNLOCKED();
    }

    static const char* opaqueCmdToString(uint32_t opaque_code);

protected:
    friend class EventuallyPersistentEngine;
    friend class ConnMap;
    friend class TapConnMap;
    friend class BGFetchCallback;
    friend struct TapStatBuilder;
    friend struct TapAggStatBuilder;
    friend struct PopulateEventsBody;
    friend class TapEngineSpecific;

    /**
     * Check if TAP_DUMP or TAP_TAKEOVER is completed and close the connection if
     * all messages including vbucket_state change commands are sent.
     */
    VBucketEvent checkDumpOrTakeOverCompletion();

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

    void addLogElement_UNLOCKED(const queued_item &qi) {
        if (supportsAck()) {
            TapLogElement log(seqno, qi);
            ackLog_.push_back(log);
            stats.memOverhead.fetch_add(sizeof(TapLogElement));
            cb_assert(stats.memOverhead.load() < GIGANTOR);
        }
    }

    void addLogElement(const queued_item &qi) {
        LockHolder lh(queueLock);
        addLogElement_UNLOCKED(qi);
    }

    void addLogElement_UNLOCKED(const VBucketEvent &e) {
        if (supportsAck()) {
            // add to the log!
            TapLogElement log(seqno, e);
            ackLog_.push_back(log);
            stats.memOverhead.fetch_add(sizeof(TapLogElement));
            cb_assert(stats.memOverhead.load() < GIGANTOR);
        }
    }

    /**
     * Get the next item from the queue that has items fetched from memory.
     */
    queued_item nextFgFetched_UNLOCKED(bool &shouldPause);

    void addVBucketHighPriority_UNLOCKED(VBucketEvent &ev) {
        vBucketHighPriority.push(ev);
    }


    /**
     * Add a new high priority VBucketEvent to this TapProducer. A high
     * priority VBucketEvent will bypass the the normal queue of events to
     * be sent to the client, and be sent the next time it is possible to
     * send data over the tap connection.
     */
    void addVBucketHighPriority(VBucketEvent &ev) {
        LockHolder lh(queueLock);
        addVBucketHighPriority_UNLOCKED(ev);
    }

    /**
     * Get the next high priority VBucketEvent for this TapProducer
     */
    VBucketEvent nextVBucketHighPriority_UNLOCKED();

    VBucketEvent nextVBucketHighPriority() {
        LockHolder lh(queueLock);
        return nextVBucketHighPriority_UNLOCKED();
    }

    void addVBucketLowPriority_UNLOCKED(VBucketEvent &ev) {
        vBucketLowPriority.push(ev);
    }

    /**
     * Add a new low priority VBucketEvent to this TapProducer. A low
     * priority VBucketEvent will only be sent when the tap connection
     * doesn't have any other events to send.
     */
    void addVBucketLowPriority(VBucketEvent &ev) {
        LockHolder lh(queueLock);
        addVBucketLowPriority_UNLOCKED(ev);
    }

    /**
     * Get the next low priority VBucketEvent for this TapProducer.
     */
    VBucketEvent nextVBucketLowPriority_UNLOCKED();

    VBucketEvent nextVBucketLowPriority() {
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
            vBucketHighPriority.empty() && checkpointMsgs.empty() && ackLog_.empty();
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

    ENGINE_ERROR_CODE processAck(uint32_t seqno, uint16_t status, const std::string &msg);

    /**
     * Is the tap ack window full?
     * @return true if the window is full and no more items should be sent
     */
    bool windowIsFull();

    /**
     * Should we request an ack for this message?
     * @param event the event type for this message
     * @param vbucket the vbucket Id for this message
     * @return true if we should request an ack (and start a new sequence)
     */
    virtual bool requestAck(uint16_t event, uint16_t vbucket);

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


    void encodeVBucketStateTransition(const VBucketEvent &ev, void **es,
                                      uint16_t *nes, uint16_t *vbucket) const;

    void evaluateFlags();

    bool waitForCheckpointMsgAck();

    bool waitForOpaqueMsgAck();

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

    bool checkVBucketFilter(uint16_t vbucket) {
        LockHolder lh(queueLock);
        return vbucketFilter(vbucket);
    }

    /**
     * Register the unified queue cursor for this producer.
     */
    void registerCursor(const std::map<uint16_t, uint64_t> &lastCheckpointIds);

    size_t getTapAckLogSize(void) {
        LockHolder lh(queueLock);
        return ackLog_.size();
    }

    void reschedule_UNLOCKED(const std::list<TapLogElement>::iterator &iter);

    void clearQueues_UNLOCKED();

    //! Queue of live stream items that needs to be sent
    std::list<queued_item> *queue;
    //! Live stream queue size
    size_t queueSize;
    //! Queue of items backfilled from disk
    std::queue<Item*> backfilledItems;
    //! List of items that are waiting for acks from the client
    std::list<TapLogElement> ackLog_;

    //! Keeps track of items transmitted per VBucket
    AtomicValue<size_t> *transmitted;

    //! VBucket status messages immediately (before userdata)
    std::queue<VBucketEvent> vBucketHighPriority;
    //! VBucket status messages sent when there is nothing else to send
    std::queue<VBucketEvent> vBucketLowPriority;

    //! Checkpoint start and end messages
    std::queue<queued_item> checkpointMsgs;
    //! Checkpoint state per vbucket
    std::map<uint16_t, CheckpointState> checkpointState_;

    //! Flags passed by the client
    uint32_t flags;
    //! Dump and disconnect?
    bool dumpQueue;
    //! Number of records fetched from this stream since the
    size_t recordsFetched;
    //! Number of records skipped due to changing the filter on the connection
    AtomicValue<size_t> recordsSkipped;
    //! Do we have a pending flush command?
    bool pendingFlush;
    //! Backfill age for the connection
    uint64_t backfillAge;

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

    //! Filter for the vbuckets that require backfill by the next backfill task
    VBucketFilter backFillVBucketFilter;
    //! vbuckets that are being backfilled by the current backfill session
    std::set<uint16_t> backfillVBuckets;

    AtomicValue<size_t> bgResultSize;
    AtomicValue<size_t> bgJobIssued;
    AtomicValue<size_t> bgJobCompleted;
    AtomicValue<size_t> numTapNack;
    AtomicValue<size_t> queueMemSize;
    AtomicValue<size_t> queueFill;
    AtomicValue<size_t> queueDrain;
    AtomicValue<size_t> checkpointMsgCounter;
    AtomicValue<size_t> opaqueMsgCounter;

    //! Current tap sequence number (for ack's)
    uint32_t seqno;
    //! The last tap sequence number received
    uint32_t seqnoReceived;
    //! The last tap sequence number for which an ack is requested
    uint32_t seqnoAckRequested;

    //! tap opaque command code.
    uint32_t opaqueCommandCode;

    //! Textual representation of the vbucket filter.
    std::string filterText;
    //! Textual representation of the flags..
    std::string flagsText;

    AtomicValue<rel_time_t> lastMsgTime;

    bool isLastAckSucceed;
    bool isSeqNumRotated;

    //! Should we send a NOOP message now?
    AtomicValue<bool> noop;
    size_t numNoops;

    //! Does the Tap Consumer know about the byteorder bug for the flags
    bool flagByteorderSupport;

    //! EP-engine specific item info
    uint8_t *specificData;
    //! Timestamp of backfill start
    time_t backfillTimestamp;

    DISALLOW_COPY_AND_ASSIGN(TapProducer);
};

#endif  // SRC_TAPCONNECTION_H_
