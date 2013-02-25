/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef STATSNAP_HH
#define STATSNAP_HH 1

#include "common.hh"

#include <string>
#include <map>

#include "dispatcher.hh"

const int STATSNAP_FREQ(60);

class EventuallyPersistentEngine;

/**
 * Periodically take a snapshot of the stats and record it in the main DB.
 */
class StatSnap : public DispatcherCallback {
public:
    StatSnap(EventuallyPersistentEngine *e, bool runOneTimeOnly = false) :
        engine(e), runOnce(runOneTimeOnly) {}

    /**
     * Called when the task is run to perform the stats snapshot.
     */
    bool callback(Dispatcher &d, TaskId &t);

    /**
     * Description of task.
     */
    std::string description() {
        std::string rv("Updating stat snapshot on disk");
        return rv;
    }

private:
    EventuallyPersistentEngine         *engine;
    bool                                runOnce;
};

#endif /* STATSNAP_HH */
