/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef FLUSHER_H
#define FLUSHER_H 1

#include "ep.hh"

class Flusher {
public:
    Flusher(EventuallyPersistentStore *st):
        store(st), running(true), hasInitialized(false) {
    }
    ~Flusher() {
        stop();
    }
    void stop() {
        running = false;
    }
    void initialize() {
        rel_time_t start = ep_current_time();
        store->warmup();
        store->stats.warmupTime = ep_current_time() - start;
        store->stats.warmupComplete = true;
        hasInitialized = true;
    }

    void run() {
        running = true;
        if (!hasInitialized) {
            initialize();
        }
        try {
            while (running) {
                rel_time_t start = ep_current_time();

                int n = store->flush(true);
                if (n > 0) {
                    rel_time_t sleep_end = start + n;
                    while (running && ep_current_time() < sleep_end) {
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
            store->flush(false);
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Flusher stopped\n");
        } catch(std::runtime_error &e) {
            std::stringstream ss;
            ss << "Exception if flusher loop: " << e.what() << std::endl;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s",
                             ss.str().c_str());
            assert(false);
        }
        // Signal our completion.
        store->flusherStopped();
    }
private:
    EventuallyPersistentStore *store;
    volatile bool running;
    bool hasInitialized;
};

#endif /* FLUSHER_H */
