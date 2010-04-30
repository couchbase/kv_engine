/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef FLUSHER_H
#define FLUSHER_H 1

#include "common.hh"
#include "ep.hh"

enum flusher_state {
    initializing,
    running,
    pausing,
    paused,
    stopping,
    stopped
};

class Flusher {
public:
    Flusher(EventuallyPersistentStore *st):
        store(st), _state(initializing), hasInitialized(false) {
    }
    ~Flusher() {
        stop();
    }

    bool stop();
    bool pause();
    bool resume();

    void initialize();

    void run(void);

    bool transition_state(enum flusher_state to);

    enum flusher_state state() const;
    const char * const stateName() const;
private:
    EventuallyPersistentStore *store;
    volatile enum flusher_state _state;
    bool hasInitialized;
    const char * const stateName(enum flusher_state st) const;
    void maybePause(void);

    int doFlush(bool shouldWait);

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};

#endif /* FLUSHER_H */
