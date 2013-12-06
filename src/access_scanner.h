/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#ifndef SRC_ACCESS_SCANNER_H_
#define SRC_ACCESS_SCANNER_H_ 1

#include "config.h"

#include <string>

#include "common.h"
#include "dispatcher.h"
#include "ep_engine.h"
#include "tasks.h"

// Forward declaration.
class EventuallyPersistentStore;
class AccessScannerValueChangeListener;

class AccessScanner : public GlobalTask {
    friend class AccessScannerValueChangeListener;
public:
    AccessScanner(EventuallyPersistentStore &_store, EPStats &st,
                  const Priority &p, double sleeptime = 0)
        : GlobalTask(&_store.getEPEngine(), p, sleeptime),
          completedCount(0), store(_store), stats(st), sleepTime(sleeptime),
          available(true) { }

    bool run();
    std::string getDescription();
    size_t startTime();
    Atomic<size_t> completedCount;

private:
    EventuallyPersistentStore &store;
    EPStats &stats;
    double sleepTime;
    bool available;
};

#endif  // SRC_ACCESS_SCANNER_H_
