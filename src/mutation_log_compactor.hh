/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MUTATION_LOG_COMPACTOR_HH
#define MUTATION_LOG_COMPACTOR_HH 1

#include "common.hh"
#include "dispatcher.hh"
#include "mutation_log.hh"

// Forward declaration.
class EventuallyPersistentStore;

/**
 * Dispatcher task that compacts a mutation log file if the compaction condition
 * is satisfied.
 */
class MutationLogCompactor : public DispatcherCallback {
public:
    MutationLogCompactor(EventuallyPersistentStore *ep_store) :
        epStore(ep_store) { }

    bool callback(Dispatcher &d, TaskId &t);

    /**
     * Description of task.
     */
    std::string description() {
        return "MutationLogCompactor: Writing hash table items to new log file";
    }

private:
    EventuallyPersistentStore *epStore;
};

#endif /* MUTATION_LOG_COMPACTOR_HH */
