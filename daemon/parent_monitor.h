/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#pragma once

#include "config.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <platform/platform.h>

/* Class which monitors the parent process and will terminate this
 * process if the parent ever dies.
 */
class ParentMonitor {
public:
    /* Creates a ParentMonitor which will use a background thread to
     * periodically verify the existence of the specified parent_ID
     * (pid on *ix, processID on Windows).
     */
    ParentMonitor(int parent_id);

    /* Shuts down the background thread */
    ~ParentMonitor();

protected:
    /* Main function for the background thread. */
    static void thread_main(void* arg);

    cb_thread_t thread;
    std::mutex mutex;
    std::condition_variable shutdown_cv;
    std::atomic<bool> active;
    const cb_pid_t parent_pid;

#ifdef WIN32
    HANDLE handle;
#endif
};

