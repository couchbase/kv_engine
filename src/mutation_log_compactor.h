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

#ifndef SRC_MUTATION_LOG_COMPACTOR_H_
#define SRC_MUTATION_LOG_COMPACTOR_H_ 1

#include "config.h"

#include <string>

#include "common.h"
#include "dispatcher.h"
#include "mutation_log.h"

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

#endif  // SRC_MUTATION_LOG_COMPACTOR_H_
