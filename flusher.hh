/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef FLUSHER_H
#define FLUSHER_H 1

#include "common.hh"
#include "ep.hh"
#include "dispatcher.hh"

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

/**
 * A DispatcherCallback adaptor over Flusher.
 */
class FlusherStepper : public DispatcherCallback {
public:
    FlusherStepper(Flusher* f) : flusher(f) { }
    bool callback(Dispatcher &d, TaskId t);

    std::string description() {
        return std::string("Running a flusher loop.");
    }

    hrtime_t maxExpectedDuration() {
        // Flusher can take a while, but let's report if it runs for
        // more than ten minutes.
        return 10 * 60 * 1000 * 1000;
    }

private:
    Flusher *flusher;
};

/**
 * Manage persistence of data for an EventuallyPersistentStore.
 */
class Flusher {
public:

    Flusher(EventuallyPersistentStore *st, Dispatcher *d) :
        store(st), _state(initializing), dispatcher(d),
        flushRv(0), prevFlushRv(0), minSleepTime(0.1),
        flushQueue(NULL), rejectQueue(NULL), vbStateLoaded(false), forceShutdownReceived(false) {
    }

    ~Flusher() {
        if (_state != stopped) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Flusher being destroyed in state %s\n",
                             stateName(_state));

        }
        if (rejectQueue != NULL) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Flusher being destroyed with %ld tasks in the reject queue\n",
                             rejectQueue->size());
            delete rejectQueue;
        }
    }

    bool stop(bool isForceShutdown = false);
    void wait();
    bool pause();
    bool resume();

    void initialize(TaskId);

    void start(void);
    void wake(void);
    bool step(Dispatcher&, TaskId);

    bool isVBStateLoaded() const {
        return vbStateLoaded.get();
    }

    enum flusher_state state() const;
    const char * stateName() const;
private:
    bool transition_state(enum flusher_state to);
    int doFlush();
    void completeFlush();
    void schedule_UNLOCKED();
    double computeMinSleepTime();

    EventuallyPersistentStore *store;
    volatile enum flusher_state _state;
    Mutex taskMutex;
    TaskId task;
    Dispatcher *dispatcher;
    const char * stateName(enum flusher_state st) const;

    // Current flush cycle state.
    int                      flushRv;
    int                      prevFlushRv;
    double                   minSleepTime;
    std::queue<queued_item> *flushQueue;
    std::queue<queued_item> *rejectQueue;
    rel_time_t               flushStart;
    Atomic<bool>             vbStateLoaded;
    Atomic<bool>             forceShutdownReceived;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};

#endif /* FLUSHER_H */
