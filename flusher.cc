/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "flusher.hh"

bool FlusherStepper::callback(Dispatcher &d, TaskId t) {
    return flusher->step(d, t);
}

bool Flusher::stop(void) {
    return transition_state(stopping);
}

void Flusher::wait(void) {
    while (_state != stopped) {
        sleep(1);
    }
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
    if (from == initializing && to == running) {
    } else if (from == running && to == pausing) {
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

const char * Flusher::stateName(enum flusher_state st) const {
    static const char * const stateNames[] = {
        "initializing", "running", "pausing", "paused", "stopping", "stopped"
    };
    assert(st >= initializing && st <= stopped);
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
    //Reschedule the task
    dispatcher->cancel(task);
    start();
    return true;
}

const char * Flusher::stateName() const {
    return stateName(_state);
}

enum flusher_state Flusher::state() const {
    return _state;
}

void Flusher::initialize(void) {
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Initializing flusher; warming up\n");

    time_t startTime = time(NULL);
    store->warmup();
    store->stats.warmupTime.set(time(NULL) - startTime);
    store->stats.warmupComplete.set(true);
    store->stats.curr_items.incr(store->stats.warmedUp.get());

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Warmup completed in %ds\n", store->stats.warmupTime.get());
    transition_state(running);
}

void Flusher::start(void) {
    task = dispatcher->schedule(shared_ptr<FlusherStepper>(new FlusherStepper(this)));
}

void Flusher::wake(void) {
    dispatcher->wake(task);
}

bool Flusher::step(Dispatcher &d, TaskId tid) {
    try {
        switch (_state) {
        case initializing:
            initialize();
            return true;
        case paused:
            return false;
        case pausing:
            transition_state(paused);
            return false;
        case running:
            {
                int n = doFlush();
                if (n > 0) {
                    if (_state == running) {
                        d.snooze(tid, n);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return true;
                }
            }
        case stopping:
            {
                std::stringstream ss;
                ss << "Shutting down flusher (Write of all dirty items)"
                   << std::endl;
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "%s",
                                 ss.str().c_str());
            }
            store->stats.min_data_age = 0;
            doFlush();
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Flusher stopped\n");
            transition_state(stopped);
            return false;
        case stopped:
            return false;
        default:
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Unexpected state in flusher: %s", stateName());
            assert(false);
        }
    } catch(std::runtime_error &e) {
        std::stringstream ss;
        ss << "Exception in flusher loop: " << e.what() << std::endl;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s",
                         ss.str().c_str());
        assert(false);
    }
}

int Flusher::doFlush() {
    int rv(store->stats.min_data_age);
    std::queue<std::string> *q = store->beginFlush();
    if (q) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Flushing a write queue.\n");
        std::queue<std::string> *rejectQueue = new std::queue<std::string>();
        rel_time_t flush_start = ep_current_time();

        while (!q->empty()) {
            int n = store->flushSome(q, rejectQueue);
            if (_state == pausing) {
                transition_state(paused);
            }
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
