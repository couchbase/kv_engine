/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "mutation_log_compactor.hh"
#include "ep.hh"

bool MutationLogCompactor::callback(Dispatcher &d, TaskId &t) {
    size_t sleeptime = 0;
    bool rv = epStore->compactMutationLog(sleeptime);
    d.snooze(t, sleeptime);
    return rv;
}
