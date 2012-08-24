/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "invalid_vbtable_remover.hh"
#include "ep_engine.h"

const size_t PERSISTENCE_QUEUE_SIZE_THRESHOLD(1000000);

bool InvalidVBTableRemover::callback(Dispatcher &d, TaskId &t) {
    size_t queueSize = engine->getEpStats().queue_size.get() +
                       engine->getEpStats().flusher_todo.get();
    if (queueSize < PERSISTENCE_QUEUE_SIZE_THRESHOLD) {
        // TODO: We need to determine the persistence queue size threshould dynamically
        // by considering various stats. More elegant solution would be to implement the
        // dynamic configuration manager in ns_server, which adjusts this threshold dynamically
        // based on various monitoring stats from each node.
        hrtime_t start_time(gethrtime());
        engine->getEpStore()->getRWUnderlying()->destroyInvalidVBuckets(true);
        hrtime_t wall_time = (gethrtime() - start_time) / 1000;
        engine->getEpStats().diskInvalidVBTableDelHisto.add(wall_time);
    }
    d.snooze(t, INVALID_VBTABLE_DEL_FREQ);
    return true;
}
