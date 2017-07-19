/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "logger.h"
#include "statwriter.h"
#include "utility.h"
#include "vb_filter.h"
#include "vbucket.h"

// forward decl
class ConnHandler;
class EventuallyPersistentEngine;
class CompleteBackfillOperation;
class Dispatcher;
class Item;
class VBucketFilter;

struct TapStatBuilder;
struct TapAggStatBuilder;
struct PopulateEventsBody;



#define MAX_TAKEOVER_TAP_LOG_SIZE 10
#define MINIMUM_BACKFILL_RESIDENT_THRESHOLD 0.7
#define DEFAULT_BACKFILL_RESIDENT_THRESHOLD 0.9


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

    virtual ENGINE_ERROR_CODE mutation(uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       uint16_t vbucket,
                                       uint32_t flags,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t expiration,
                                       uint32_t lock_time,
                                       cb::const_byte_buffer meta,
                                       uint8_t nru);

    virtual ENGINE_ERROR_CODE deletion(uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       uint16_t vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       cb::const_byte_buffer meta);

    virtual ENGINE_ERROR_CODE expiration(uint32_t opaque,
                                         const DocKey& key,
                                         cb::const_byte_buffer value,
                                         size_t priv_bytes,
                                         uint8_t datatype,
                                         uint64_t cas,
                                         uint16_t vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         cb::const_byte_buffer meta);

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

    /**
     * Sub-classes must implement a method that processes a response
     * to a request initiated by itself.
     *
     * @param resp A mcbp response message to process.
     * @returns true/false which will be converted to SUCCESS/DISCONNECT by the
     *          engine.
     */
    virtual bool handleResponse(protocol_binary_response_header* resp);

    virtual ENGINE_ERROR_CODE systemEvent(uint32_t opaque,
                                          uint16_t vbucket,
                                          mcbp::systemevent::id event,
                                          uint64_t bySeqno,
                                          cb::const_byte_buffer key,
                                          cb::const_byte_buffer eventData);

    EventuallyPersistentEngine& engine() {
        return engine_;
    }

    const char* logHeader() {
        return logger.prefix.c_str();
    }

    void setLogHeader(const std::string &header) {
        logger.prefix = header;
    }

    const Logger& getLogger() const;

    void releaseReference(bool force = false);

    void setSupportAck(bool ack) {
        supportAck.store(ack);
    }

    bool supportsAck() const {
        return supportAck.load();
    }

    void setSupportCheckpointSync(bool checkpointSync) {
        supportCheckpointSync_ = checkpointSync;
    }

    bool supportsCheckpointSync() const {
        return supportCheckpointSync_;
    }

    virtual const char *getType() const = 0;

    template <typename T>
    void addStat(const char *nm, const T &val, ADD_STAT add_stat, const void *c) const {
        std::stringstream tap;
        tap << name << ":" << nm;
        std::stringstream value;
        value << val;
        std::string n = tap.str();
        add_casted_stat(n.data(), value.str().data(), add_stat, c);
    }

    void addStat(const char *nm, bool val, ADD_STAT add_stat, const void *c) const {
        addStat(nm, val ? "true" : "false", add_stat, c);
    }

    virtual void addStats(ADD_STAT add_stat, const void *c) {
        addStat("type", getType(), add_stat, c);
        addStat("created", created.load(), add_stat, c);
        addStat("connected", connected.load(), add_stat, c);
        addStat("pending_disconnect", disconnect.load(), add_stat, c);
        addStat("supports_ack", supportAck.load(), add_stat, c);
        addStat("reserved", reserved.load(), add_stat, c);

        if (numDisconnects > 0) {
            addStat("disconnects", numDisconnects.load(), add_stat, c);
        }
    }

    virtual void aggregateQueueStats(ConnCounter& stats_aggregator) {
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
        // MB-23454: Explicitly copying the string to avoid buggy string COW
        // leading to a data race being identified by ThreadSanitizer
        name = std::string(n.begin(), n.end());
    }

    bool setReserved(bool r) {
        bool inverse = !r;
        return reserved.compare_exchange_strong(inverse, r);
    }

    bool isReserved() const {
        return reserved;
    }

    const void *getCookie() const {
        return cookie.load();
    }

    void setCookie(const void *c) {
        cookie.store(const_cast<void*>(c));
    }

    void setExpiryTime(rel_time_t t) {
        expiryTime = t;
    }

    rel_time_t getExpiryTime() {
        return expiryTime;
    }

    void setLastWalkTime();

    rel_time_t getLastWalkTime() {
        return lastWalkTime.load();
    }

    void setConnected(bool s) {
        if (!s) {
            ++numDisconnects;
        }
        connected.store(s);
    }

    bool isConnected() {
        return connected.load();
    }

    bool doDisconnect() {
        return disconnect.load();
    }

    virtual void setDisconnect(bool val) {
        disconnect.store(val);
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

    //! The logger for this connection
    Logger logger;

private:

     //! The name for this connection
    std::string name;

    //! The cookie representing this connection (provided by the memcached code)
    std::atomic<void*> cookie;

    //! Whether or not the connection is reserved in the memcached layer
    std::atomic<bool> reserved;

    //! Connection token created at connection instantiation time
    hrtime_t connToken;

    //! Connection creation time
    std::atomic<rel_time_t> created;

    //! The last time this connection's step function was called
    std::atomic<rel_time_t> lastWalkTime;

    //! Should we disconnect as soon as possible?
    std::atomic<bool> disconnect;

    //! Is this tap conenction connected?
    std::atomic<bool> connected;

    //! Number of times this connection was disconnected
    std::atomic<size_t> numDisconnects;

    //! when this tap conneciton expires.
    rel_time_t expiryTime;

    //! Whether or not this connection supports acking
    std::atomic<bool> supportAck;

    //! A counter used to generate unique names
    static std::atomic<uint64_t> counter_;
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
 */
class Consumer : public ConnHandler {
private:
    std::atomic<size_t> numDelete;
    std::atomic<size_t> numDeleteFailed;
    std::atomic<size_t> numFlush;
    std::atomic<size_t> numFlushFailed;
    std::atomic<size_t> numMutation;
    std::atomic<size_t> numMutationFailed;
    std::atomic<size_t> numOpaque;
    std::atomic<size_t> numOpaqueFailed;
    std::atomic<size_t> numVbucketSet;
    std::atomic<size_t> numVbucketSetFailed;
    std::atomic<size_t> numCheckpointStart;
    std::atomic<size_t> numCheckpointStartFailed;
    std::atomic<size_t> numCheckpointEnd;
    std::atomic<size_t> numCheckpointEndFailed;
    std::atomic<size_t> numUnknown;

public:
    Consumer(EventuallyPersistentEngine &theEngine, const void* cookie,
             const std::string& name);
    virtual ~Consumer() {
    }
    virtual void processedEvent(uint16_t event, ENGINE_ERROR_CODE ret);
    virtual void addStats(ADD_STAT add_stat, const void *c);
    virtual const char *getType() const { return "consumer"; };
    virtual void checkVBOpenCheckpoint(uint16_t);
    bool isBackfillPhase(uint16_t vbucket);
    ENGINE_ERROR_CODE setVBucketState(uint32_t opaque, uint16_t vbucket,
                                      vbucket_state_t state);
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

    /** Pause the connection.
     *
     * @param reason Why the connection was paused - for debugging / diagnostic
     */
    void pause(std::string reason = "unknown") {
        paused.store(true);
        {
            std::lock_guard<std::mutex> guard(pausedReason.mutex);
            pausedReason.string = reason;
        }
    }

    void unPause() {
        paused.store(false);
    }

    std::string getPausedReason() const {
        std::lock_guard<std::mutex> guard(pausedReason.mutex);
        return pausedReason.string;
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
    //! Description of why the connection is paused.
    struct pausedReason {
        mutable std::mutex mutex;
        std::string string;
    } pausedReason;

    //! Is this tap connection in a suspended state
    std::atomic<bool> suspended;
    //! Connection is temporarily paused?
    std::atomic<bool> paused;
    //! Flag indicating if the notification event is scheduled
    std::atomic<bool> notificationScheduled;
        //! Flag indicating if the pending memcached connection is notified
    std::atomic<bool> notifySent;
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

    virtual ~Producer() {
    }

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

    virtual size_t getBackfillQueueSize() = 0;

    void incrBackfillRemaining(size_t incr) {
        LockHolder lh(queueLock);
        totalBackfillBacklogs += incr;
    }

    const VBucketFilter &getVBucketFilter() {
        LockHolder lh(queueLock);
        return vbucketFilter;
    }

protected:
    friend class ConnMap;

    //! Lock held during queue operations.
    std::mutex queueLock;
    //! Filter for the vbuckets we want.
    VBucketFilter vbucketFilter;
    //! Total backfill backlogs
    size_t totalBackfillBacklogs;

private:
    //! Number of times this client reconnected
    uint32_t reconnects;
};

#endif  // SRC_TAPCONNECTION_H_
