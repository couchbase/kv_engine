/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef INVALID_VBTABLE_REMOVER_HH
#define INVALID_VBTABLE_REMOVER_HH 1

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"

const int INVALID_VBTABLE_DEL_FREQ(600);

// Forward declaration.
class EventuallyPersistentEngine;

/**
 * Periodically remove invalid vbucket tables from the underlying database
 */
class InvalidVBTableRemover : public DispatcherCallback {
public:
    InvalidVBTableRemover(EventuallyPersistentEngine *e) : engine(e) { }

    bool callback(Dispatcher &d, TaskId &t);

    /**
     * Description of task.
     */
    std::string description() {
        std::string rv("Removing an invalid vbucket table from DB");
        return rv;
    }

private:
    EventuallyPersistentEngine         *engine;
};

#endif /* INVALID_VBTABLE_REMOVER_HH */
