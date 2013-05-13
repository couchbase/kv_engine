/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef FLUSHER_H
#define FLUSHER_H 1

#include "common.hh"
#include "ep.hh"
#include "dispatcher.hh"
#include "mutation_log.hh"

#define NO_VBUCKETS_INSTANTIATED 0xFFFF

enum flusher_state {
    initializing,
    running,
    pausing,
    paused,
    stopping,
    stopped
};

class Flusher;

const double DEFAULT_MIN_SLEEP_TIME = 0.1;

class KVShard;
/**
 * Manage persistence of data for an EventuallyPersistentStore.
 */
class Flusher {
public:

    Flusher(EventuallyPersistentStore *st, KVShard *k) :
        store(st), _state(initializing), taskId(0), minSleepTime(0.1),
        forceShutdownReceived(false), doHighPriority(false), shard(k) { }

    ~Flusher() {
        if (_state != stopped) {
            LOG(EXTENSION_LOG_WARNING, "Flusher being destroyed in state %s",
                stateName(_state));

        }
    }

    bool stop(bool isForceShutdown = false);
    void wait();
    bool pause();
    bool resume();
    void initialize(size_t tid);
    void start();
    void wake(void);
    bool step(size_t tid);

    enum flusher_state state() const;
    const char * stateName() const;

    void notifyFlushEvent(void) {
        // By setting pendingMutation to true we are guaranteeing that the given
        // flusher will iterate the entire vbuckets under its shard from the
        // begining and flush for all mutations
        if (pendingMutation.cas(false, true)) {
            wake();
        }
    }
    void setTaskId(size_t newId) { taskId = newId; }

private:
    bool transition_state(enum flusher_state to);
    void doFlush();
    void completeFlush();
    void schedule_UNLOCKED();
    double computeMinSleepTime();

    const char * stateName(enum flusher_state st) const;

    uint16_t getNextVb();
    bool canSnooze(void) {
        return lpVbs.empty() && hpVbs.empty() && !pendingMutation.get();
    }

    EventuallyPersistentStore   *store;
    volatile enum flusher_state  _state;
    Mutex                        taskMutex;
    size_t                       taskId;

    double                   minSleepTime;
    rel_time_t               flushStart;
    Atomic<bool> forceShutdownReceived;
    std::queue<uint16_t> hpVbs;
    std::queue<uint16_t> lpVbs;
    bool doHighPriority;
    Atomic<bool> pendingMutation;

    KVShard *shard;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};

#endif /* FLUSHER_H */
