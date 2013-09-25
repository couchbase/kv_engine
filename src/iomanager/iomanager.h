/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#ifndef SRC_IOMANAGER_IOMANAGER_H_
#define SRC_IOMANAGER_IOMANAGER_H_ 1

#include "config.h"

#include <string>

#include "scheduler.h"
#include "tasks.h"

class IOManager : public ExecutorPool {
public:
    static IOManager *get();

    size_t scheduleTask(ExTask task, task_type_t task_IDX);

    IOManager(size_t maxThreads)
        : ExecutorPool(maxThreads, 3) {} // 0 - writers 1 - readers 2 - auxIO

private:
    static Mutex initGuard;
    static IOManager *instance;
};

#endif  // SRC_IOMANAGER_IOMANAGER_H_

