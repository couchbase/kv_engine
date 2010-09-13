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

/**
 * A DispatcherCallback adaptor over Flusher.
 */
class FlusherStepper : public DispatcherCallback {
public:
    FlusherStepper(Flusher* f) : flusher(f) { }
    bool callback(Dispatcher &d, TaskId t);
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
        flushQueue(NULL) {
    }

    ~Flusher() {
        if (_state != stopped) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Flusher being destroyed in state %s\n",
                             stateName(_state));

        }
    }

    bool stop();
    void wait();
    bool pause();
    bool resume();

    void initialize(TaskId);

    void start(void);
    void wake(void);
    bool step(Dispatcher&, TaskId);

    enum flusher_state state() const;
    const char * stateName() const;
private:
    bool transition_state(enum flusher_state to);
    int doFlush();
    void completeFlush();
    void schedule_UNLOCKED();

    EventuallyPersistentStore *store;
    volatile enum flusher_state _state;
    Mutex taskMutex;
    TaskId task;
    Dispatcher *dispatcher;
    const char * stateName(enum flusher_state st) const;

    // Current flush cycle state.
    int                     flushRv;
    std::queue<QueuedItem> *flushQueue;
    std::queue<QueuedItem> *rejectQueue;
    rel_time_t              flushStart;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};

#endif /* FLUSHER_H */
