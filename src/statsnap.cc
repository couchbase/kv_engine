/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include <iostream>
#include <cstdlib>
#include <string>
#include <map>

#include "common.hh"
#include "dispatcher.hh"
#include "ep_engine.h"
#include "statsnap.hh"

bool StatSnap::callback(Dispatcher &d, TaskId &t) {
    engine->getEpStore()->snapshotStats();
    if (runOnce) {
        return false;
    }
    d.snooze(t, STATSNAP_FREQ);
    return true;
}
