/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "flusher.hh"

void Flusher::stop(void) {
    running = false;
}

void Flusher::initialize(void) {
    rel_time_t start = ep_current_time();
    store->warmup();
    store->stats.warmupTime = ep_current_time() - start;
    store->stats.warmupComplete = true;
    hasInitialized = true;
}

void Flusher::run(void) {
    running = true;
    if (!hasInitialized) {
        initialize();
    }
    try {
        while (running) {
            rel_time_t start = ep_current_time();

            int n = doFlush(true);
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
        doFlush(false);
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

int Flusher::doFlush(bool shouldWait) {
    int rv(0);
    std::queue<std::string> *q = store->beginFlush(shouldWait);
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Looking for something to flush.\n");
    if (q) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Flushing a write queue.\n");
        std::queue<std::string> *rejectQueue = new std::queue<std::string>();
        RememberingCallback<bool> cb;
        rel_time_t flush_start = ep_current_time();
        rv = store->stats.min_data_age;

        while (!q->empty()) {
            int n = store->flushSome(q, cb, rejectQueue);
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
