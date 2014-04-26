/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TAPCONNMAP_HH
#define TAPCONNMAP_HH 1

#include <map>
#include <list>
#include <iterator>

#include "common.hh"
#include "queueditem.hh"
#include "locks.hh"
#include "syncobject.hh"

// Forward declaration
class TapConnection;
class TapConsumer;
class TapProducer;
class TapConnection;
class Item;
class EventuallyPersistentEngine;

typedef SingleThreadedRCPtr<TapConnection> connection_t;

/**
 * Base class for operations performed on tap connections.
 *
 * @see TapConnMap::performTapOp
 */
template <typename V>
class TapOperation {
public:
    virtual ~TapOperation() {}
    virtual void perform(TapProducer *tc, V arg) = 0;
};

/**
 * Indicate the tap operation is complete.
 */
class CompleteBackfillTapOperation : public TapOperation<void*> {
public:
    void perform(TapProducer *tc, void* arg);
};

/**
 * Indicate that we are going to schedule a tap disk backfill for a given vbucket.
 */
class ScheduleDiskBackfillTapOperation : public TapOperation<void*> {
public:
    void perform(TapProducer *tc, void* arg);
};

/**
 * Indicate the tap backfill disk stream thing is complete for a given vbucket.
 */
class CompleteDiskBackfillTapOperation : public TapOperation<void*> {
public:
    void perform(TapProducer *tc, void* arg);
};

/**
 * Complete a bg fetch job and give the item to the given tap connection.
 */
class CompletedBGFetchTapOperation : public TapOperation<Item*> {
public:
    CompletedBGFetchTapOperation(hrtime_t token, uint16_t vb, bool ie=false) :
        connToken(token), vbid(vb), implicitEnqueue(ie) {}

    void perform(TapProducer *tc, Item* arg);
private:
    hrtime_t connToken;
    uint16_t vbid;
    bool implicitEnqueue;
};

class TAPSessionStats {
public:
    TAPSessionStats() : normalShutdown(true) {}

    bool wasReplicationCompleted(const std::string &name) const;

    void clearStats(const std::string &name);

    bool normalShutdown;
    std::map<std::string, std::string> stats;
};

/**
 * Tap connection notifier that wakes up paused connections.
 */
class TapConnNotifier {
public:
    TapConnNotifier(EventuallyPersistentEngine &e, Dispatcher *d)
        : engine(e), dispatcher(d), minSleepTime(DEFAULT_MIN_STIME)  { }

    void start();

    void stop();

    void notifyMutationEvent();

    bool notifyConnections();

private:
    static const double DEFAULT_MIN_STIME;

    EventuallyPersistentEngine &engine;
    Dispatcher *dispatcher;
    TaskId task;
    double minSleepTime;
    Atomic<bool> pendingNotification;
};

/**
 * A collection of tap connections.
 */
class TapConnMap {
public:
    TapConnMap(EventuallyPersistentEngine &theEngine);
    ~TapConnMap();

    void initialize();

    /**
     * Disconnect a tap connection by its cookie.
     */
    void disconnect(const void *cookie, int tapKeepAlive);

    /**
     * Perform a TapOperation for a named tap connection while holding
     * appropriate locks.
     *
     * @param name the name of the tap connection to run the op
     * @param tapop the operation to perform
     * @param arg argument for the tap operation
     *
     * @return true if the tap connection was valid and the operation
     *         was performed
     */
    template <typename V>
    bool performTapOp(const std::string &name, TapOperation<V> &tapop, V arg) {
        bool ret(true);
        LockHolder lh(notifySync);

        connection_t tc = findByName_UNLOCKED(name);
        if (tc.get()) {
            TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
            assert(tp != NULL);
            tapop.perform(tp, arg);
            lh.unlock();
            notifyPausedConnection(tp);
        } else {
            ret = false;
        }

        return ret;
    }

    /**
     * Return true if the TAP connection with the given name is still alive
     */
    bool checkConnectivity(const std::string &name);

    /**
     * Set some backfilled events for a named conn.
     */
    bool setEvents(const std::string &name,
                   std::list<queued_item> *q);

    /**
     * Get the size of the named backfill queue.
     *
     * @return the size, or -1 if we can't find the queue
     */
    ssize_t backfillQueueDepth(const std::string &name);

    /**
     * Add an event to all tap connections telling them to flush their
     * items.
     */
    void addFlushEvent();

    /**
     * Notify the paused connections that are responsible for replicating
     * a given vbucket.
     * @param vbid vbucket id
     */
    void notifyVBConnections(uint16_t vbid);

    void notify_UNLOCKED() {
        ++notifyCounter;
        notifySync.notify();
    }

    /**
     * Notify anyone who's waiting for tap stuff.
     */
    void notify() {
        LockHolder lh(notifySync);
        notify_UNLOCKED();
    }

    uint32_t wait(double howlong, uint32_t previousCounter) {
        // Prevent the notify thread from busy-looping while
        // holding locks when there's work to do.
        LockHolder lh(notifySync);
        if (previousCounter == notifyCounter) {
            notifySync.wait(howlong);
        }
        return notifyCounter;
    }

    uint32_t prepareWait() {
        LockHolder lh(notifySync);
        return notifyCounter;
    }

    /**
     * Find or build a tap connection for the given cookie and with
     * the given name.
     */
    TapProducer *newProducer(const void* cookie,
                             const std::string &name,
                             uint32_t flags,
                             uint64_t backfillAge,
                             int tapKeepAlive,
                             bool isRegistered,
                             bool closedCheckpointOnly,
                             const std::vector<uint16_t> &vbuckets,
                             const std::map<uint16_t, uint64_t> &lastCheckpointIds);

    /**
     * Create a new consumer and add it in the list of TapConnections
     * @param e the engine
     * @param c the cookie representing the client
     * @return Pointer to the nw tap connection
     */
    TapConsumer *newConsumer(const void* c);

    /**
     * Call a function on each tap connection.
     */
    template <typename Fun>
    void each(Fun f) {
        LockHolder lh(notifySync);
        each_UNLOCKED(f);
    }

    /**
     * Call a function on each tap connection *without* a lock.
     */
    template <typename Fun>
    void each_UNLOCKED(Fun f) {
        std::for_each(all.begin(), all.end(), f);
    }

    /**
     * Return the number of connections for which this predicate is true.
     */
    template <typename Fun>
    size_t count_if(Fun f) {
        LockHolder lh(notifySync);
        return count_if_UNLOCKED(f);
    }

    /**
     * Return the number of connections for which this predicate is
     * true *without* a lock.
     */
    template <typename Fun>
    size_t count_if_UNLOCKED(Fun f) {
        return static_cast<size_t>(std::count_if(all.begin(), all.end(), f));
    }

    /**
     * Notify the tap connections.
     *
     * @return true if we need need to rush another run in quickly
     */
    void notifyIOThreadMain();

    bool SetCursorToOpenCheckpoint(const std::string &name, uint16_t vbucket);

    void incrBackfillRemaining(const std::string &name, size_t num_backfill_items);

    bool closeTapConnectionByName(const std::string &name);

    connection_t findByName(const std::string &name);

    void shutdownAllTapConnections();

    void scheduleBackfill(const std::set<uint16_t> &backfillVBuckets);

    bool isBackfillCompleted(std::string &name);

    void resetReplicaChain();

    void updateVBTapConnections(connection_t &conn,
                                const std::vector<uint16_t> &vbuckets);

    void removeVBTapConnections(connection_t &conn);

    /**
     * Change the vbucket filter for a given TAP producer
     * @param name TAP producer name
     * @param vbuckets a new vbucket filter
     * @param checkpoints last closed checkpoint ids for a new vbucket filter
     * @return true if the TAP producer's vbucket filter is changed successfully
     */
    bool changeVBucketFilter(const std::string &name,
                             const std::vector<uint16_t> &vbuckets,
                             const std::map<uint16_t, uint64_t> &checkpoints);

    size_t getTapNoopInterval() const {
        return tapNoopInterval;
    }

    /**
     * Load TAP-related stats from the previous engine sessions
     *
     * @param session_stats all the stats from the previous engine sessions
     */
    void loadPrevSessionStats(const std::map<std::string, std::string> &session_stats);

    /**
     * Check if the given TAP producer completed the replication before
     * shutdown or crash.
     *
     * @param name TAP producer's name
     * @return true if the replication from the given TAP producer was
     * completed before shutdown or crash.
     */
    bool prevSessionReplicaCompleted(const std::string &name) {
        return prevSessionStats.wasReplicationCompleted(name);
    }

    void notifyPausedConnection(TapProducer *tc);

    void notifyAllPausedConnections();
    bool notificationQueueEmpty();

protected:
    friend class TapConnMapValueChangeListener;

    void setTapNoopInterval(size_t value) {
        tapNoopInterval = value;
        nextTapNoop = 0;
        notify();
    }

private:

    connection_t findByName_UNLOCKED(const std::string &name);
    void getExpiredConnections_UNLOCKED(std::list<connection_t> &deadClients);

    void removeTapCursors_UNLOCKED(TapProducer *tp);

    bool mapped(connection_t &tc);

    /**
     * Clear all the session stats for a given TAP producer
     *
     * @param name TAP producer's name
     */
    void clearPrevSessionStats(const std::string &name) {
        prevSessionStats.clearStats(name);
    }

    Mutex                                    releaseLock;
    SyncObject                               notifySync;
    uint32_t                                 notifyCounter;
    std::map<const void*, connection_t>      map;
    std::list<connection_t>                  all;

    SpinLock *vbConnLocks;
    std::vector<std::list<connection_t> > vbConns;

    /* Handle to the engine who owns us */
    EventuallyPersistentEngine &engine;
    size_t tapNoopInterval;
    size_t nextTapNoop;

    AtomicQueue<connection_t> pendingTapNotifications;
    TapConnNotifier *tapConnNotifier;

    TAPSessionStats prevSessionStats;

    static size_t vbConnLockNum;
};

#endif /* TAPCONNMAP_HH */
