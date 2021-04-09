/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "parent_monitor.h"

#include "log_macros.h"

#include <platform/platform_thread.h>
#include <platform/strerror.h>

#include <iostream>
#ifndef WIN32
#include <csignal>
#endif

ParentMonitor::ParentMonitor(int parent_id) : parent_pid(parent_id) {
#ifdef WIN32
    handle = OpenProcess(SYNCHRONIZE, FALSE, parent_id);
    if (handle == INVALID_HANDLE_VALUE) {
        FATAL_ERROR(EXIT_FAILURE,
                    "Failed to open parent process: {}",
                    cb_strerror());
    }
#endif
    thread = std::thread{[this]() {
        cb_set_thread_name("mc:parent_mon");
        while (true) {
            // Wait for either the shutdown condvar to be notified, or for
            // 1s. If we hit the timeout then time to check the parent.
            std::unique_lock<std::mutex> lock(mutex);
            if (shutdown_cv.wait_for(lock, std::chrono::seconds(1), [this] {
                    return !active;
                })) {
                // No longer monitoring - exit thread.
                return;
            }

            // Check our parent.
            bool die = false;

#ifdef WIN32
            if (WaitForSingleObject(handle, 0) != WAIT_TIMEOUT) {
                die = true;
            }
#else
            if (kill(parent_pid, 0) == -1 && errno == ESRCH) {
                die = true;
            }
#endif

            if (die) {
                std::cerr << "Parent process " << parent_pid << " died. Exiting"
                          << std::endl;
                std::cerr.flush();
                std::_Exit(EXIT_FAILURE);
            }
        }
    }};
}

ParentMonitor::~ParentMonitor() {
    active = false;
    shutdown_cv.notify_all();
    thread.join();
}
