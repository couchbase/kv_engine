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

#pragma once

#ifdef WIN32
#include <folly/portability/Windows.h>
#endif

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

/* Class which monitors the parent process and will terminate this
 * process if the parent ever dies.
 */
class ParentMonitor {
public:
    /* Creates a ParentMonitor which will use a background thread to
     * periodically verify the existence of the specified parent_ID
     * (pid on *ix, processID on Windows).
     */
    explicit ParentMonitor(int parent_id);

    /* Shuts down the background thread */
    ~ParentMonitor();

protected:
    std::thread thread;
    std::mutex mutex;
    std::condition_variable shutdown_cv;
    std::atomic<bool> active{true};
    const int parent_pid;

#ifdef WIN32
    HANDLE handle;
#endif
};

