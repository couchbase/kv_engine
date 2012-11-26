/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_MUTATION_LOG_COMPACTOR_H_
#define SRC_MUTATION_LOG_COMPACTOR_H_ 1

#include "config.h"

#include <string>

#include "common.h"
#include "dispatcher.h"
#include "mutation_log.h"
#include "stats.h"

const size_t MAX_LOG_SIZE((size_t)(unsigned int)-1);
const size_t MAX_ENTRY_RATIO(10);
const size_t LOG_COMPACTOR_QUEUE_CAP(500000);
const int MUTATION_LOG_COMPACTOR_FREQ(3600);

/**
 * Mutation log compactor config that is used to control the scheduling of
 * the log compactor
 */
class MutationLogCompactorConfig {
public:
    MutationLogCompactorConfig() :
        maxLogSize(MAX_LOG_SIZE), maxEntryRatio(MAX_ENTRY_RATIO),
        queueCap(LOG_COMPACTOR_QUEUE_CAP), sleepTime(MUTATION_LOG_COMPACTOR_FREQ)
    { /* EMPTY */ } 

    MutationLogCompactorConfig(size_t max_log_size,
                               size_t max_entry_ratio,
                               size_t queue_cap,
                               size_t stime) :
        maxLogSize(max_log_size), maxEntryRatio(max_entry_ratio),
        queueCap(queue_cap), sleepTime(stime)
    { /* EMPTY */ }

    void setMaxLogSize(size_t max_log_size) {
        maxLogSize = max_log_size;
    }

    size_t getMaxLogSize() const {
        return maxLogSize;
    }

    void setMaxEntryRatio(size_t max_entry_ratio) {
        maxEntryRatio = max_entry_ratio;
    }

    size_t getMaxEntryRatio() const {
        return maxEntryRatio;
    }

    void setQueueCap(size_t queue_cap) {
        queueCap = queue_cap;
    }

    size_t getQueueCap() const {
        return queueCap;
    }

    void setSleepTime(size_t stime) {
        sleepTime = stime;
    }

    size_t getSleepTime() const {
        return sleepTime;
    }

private:
    size_t maxLogSize;
    size_t maxEntryRatio;
    size_t queueCap;
    size_t sleepTime;
};

// Forward declaration.
class EventuallyPersistentStore;

/**
 * Dispatcher task that compacts a mutation log file if the compaction condition
 * is satisfied.
 */
class MutationLogCompactor : public DispatcherCallback {
public:
    MutationLogCompactor(EventuallyPersistentStore *ep_store,
                         MutationLog &log,
                         MutationLogCompactorConfig &config,
                         EPStats &st) :
        epStore(ep_store), mutationLog(log), compactorConfig(config), stats(st)
    { /* EMPTY */ }

    bool callback(Dispatcher &d, TaskId &t);

    /**
     * Description of task.
     */
    std::string description() {
        std::string rv("MutationLogCompactor: Writing hash table items to a new log file");
        return rv;
    }

private:
    EventuallyPersistentStore *epStore;
    MutationLog &mutationLog;
    MutationLogCompactorConfig &compactorConfig;
    EPStats &stats;
};

#endif  // SRC_MUTATION_LOG_COMPACTOR_H_
