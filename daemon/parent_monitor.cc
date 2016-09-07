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
#include "parent_monitor.h"

#include "memcached.h"
#include <iostream>
#include <platform/strerror.h>

ParentMonitor::ParentMonitor(int parent_id) : parent_pid(parent_id) {
    active = true;
#ifdef WIN32
    handle = OpenProcess(SYNCHRONIZE, FALSE, parent_id);
    if (handle == INVALID_HANDLE_VALUE) {
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to open parent process: %s",
                    cb_strerror(GetLastError()).c_str());
    }
#endif
    if (cb_create_named_thread(&thread,
                               ParentMonitor::thread_main,
                               this, 0, "mc:parent_mon") != 0) {
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to open parent process: %s",
                    cb_strerror(GetLastError()).c_str());
    }
}

ParentMonitor::~ParentMonitor() {
    active = false;
    cb_join_thread(thread);
}

void ParentMonitor::thread_main(void* arg) {
    auto* monitor = reinterpret_cast<ParentMonitor*>(arg);

    while (true) {
        // Wait for either the shutdown condvar to be notified, or for
        // 1s. If we hit the timeout then time to check the parent.
        std::unique_lock<std::mutex> lock(monitor->mutex);
        if (monitor->shutdown_cv.wait_for(
                lock,
                std::chrono::seconds(1),
                [monitor]{return !monitor->active;})) {
            // No longer monitoring - exit thread.
            return;
        } else {
            // Check our parent.
#ifdef WIN32
            if (WaitForSingleObject(monitor->handle, 0) != WAIT_TIMEOUT) {
                std::cerr << "Parent process " << monitor->parent_pid
                          << " died. Exiting" << std::endl;
                std::cerr.flush();
                ExitProcess(EXIT_FAILURE);
            }
#else
            if (kill(monitor->parent_pid, 0) == -1 && errno == ESRCH) {
                std::cerr << "Parent process " << monitor->parent_pid
                          << " died. Exiting" << std::endl;
                std::cerr.flush();
                _exit(1);
            }
#endif
        }
    }
}
