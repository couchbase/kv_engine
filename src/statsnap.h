/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_STATSNAP_H_
#define SRC_STATSNAP_H_ 1

#include "config.h"

#include <map>
#include <string>

#include "common.h"
#include "dispatcher.h"
#include "stats.h"

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

#endif  // SRC_STATSNAP_H_
