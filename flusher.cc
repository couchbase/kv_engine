/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "flusher.hh"

bool Flusher::stop(void) {
    return transition_state(stopping);
}

bool Flusher::pause(void) {
    return transition_state(pausing);
}

bool Flusher::resume(void) {
    return transition_state(running);
}

static bool validTransition(enum flusher_state from,
                            enum flusher_state to)
{
    bool rv(true);
    if (from == running && to == pausing) {
    } else if (from == running && to == stopping) {
    } else if (from == pausing && to == paused) {
    } else if (from == stopping && to == stopped) {
    } else if (from == paused && to == running) {
    } else if (from == paused && to == stopping) {
    } else if (from == pausing && to == stopping) {
    } else {
        rv = false;
    }
    return rv;
}

const char * const Flusher::stateName(enum flusher_state st) const {
    static const char * const stateNames[] = {
        "running", "pausing", "paused", "stopping", "stopped"
    };
    assert(st >= running && st <= stopped);
    return stateNames[st];
}

bool Flusher::transition_state(enum flusher_state to) {

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Attempting transition from %s to %s\n",
                     stateName(_state), stateName(to));

    if (!validTransition(_state, to)) {
        return false;
    }

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Transitioning from %s to %s\n",
                     stateName(_state), stateName(to));

    _state = to;
    return true;
}

const char * const Flusher::stateName() const {
    return stateName(_state);
}

enum flusher_state Flusher::state() const {
    return _state;
}

void Flusher::initialize(void) {
    rel_time_t start = ep_current_time();
    store->warmup();
    store->stats.warmupTime = ep_current_time() - start;
    store->stats.warmupComplete = true;
    hasInitialized = true;
}

void Flusher::maybePause(void) {
    if (_state == pausing) {
        transition_state(paused);
        while (_state == paused) {
            sleep(1);
        }
    }
}

void Flusher::run(void) {
    if (!hasInitialized) {
        initialize();
    }
    try {
        while (_state != stopping) {
            maybePause();

            rel_time_t start = ep_current_time();
            int n = doFlush(true);
            if (n > 0) {
                rel_time_t sleep_end = start + n;
                while (_state == running && ep_current_time() < sleep_end) {
                    sleep(1);
                }
            }

        }
        std::stringstream ss;
        ss << "Shutting down flusher (Write of all dirty items)"
           << std::endl;
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "%s",
                         ss.str().c_str());
        store->stats.min_data_age = 0;
        doFlush(false);
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Flusher stopped\n");
    } catch(std::runtime_error &e) {
        std::stringstream ss;
        ss << "Exception if flusher loop: " << e.what() << std::endl;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s",
                         ss.str().c_str());
        assert(false);
    }
    transition_state(stopped);
}

int Flusher::doFlush(bool shouldWait) {
    int rv(0);
    std::queue<std::string> *q = store->beginFlush(shouldWait);
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Looking for something to flush.\n");
    if (q) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Flushing a write queue.\n");
        std::queue<std::string> *rejectQueue = new std::queue<std::string>();
        rel_time_t flush_start = ep_current_time();
        rv = store->stats.min_data_age;

        while (!q->empty()) {
            int n = store->flushSome(q, rejectQueue);
            maybePause();
            if (n < rv) {
                rv = n;
            }
        }

        store->completeFlush(rejectQueue, flush_start);

        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Completed a flush, age of oldest item was %ds\n",
                         rv);

        delete rejectQueue;
    }
    return rv;
}
