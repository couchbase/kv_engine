/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_CHECKPOINT_REMOVER_H_
#define SRC_CHECKPOINT_REMOVER_H_ 1

#include "config.h"

#include <assert.h>
#include <set>
#include <string>

#include "common.h"
#include "dispatcher.h"
#include "stats.h"

class EventuallyPersistentStore;

/**
 * Dispatcher job responsible for removing closed unreferenced checkpoints from memory.
 */
class ClosedUnrefCheckpointRemover : public DispatcherCallback {
public:

    /**
     * Construct ClosedUnrefCheckpointRemover.
     * @param s the store
     * @param st the stats
     */
    ClosedUnrefCheckpointRemover(EventuallyPersistentStore *s, EPStats &st,
                                 size_t interval) :
        store(s), stats(st), sleepTime(interval), available(true) {}

    bool callback(Dispatcher &d, TaskId &t);

    std::string description() {
        return std::string("Removing closed unreferenced checkpoints from memory");
    }

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    size_t                     sleepTime;
    bool                       available;
};

#endif  // SRC_CHECKPOINT_REMOVER_H_
