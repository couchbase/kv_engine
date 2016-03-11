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

#include "buckets.h"
#include "connection.h"
#include "memcached/server_api.h"
#include "task.h"

#include <string>


class McbpDestroyBucketTask : public Task {
public:
    McbpDestroyBucketTask(const std::string& name_,
                          bool force_,
                          Connection* connection_)
    : thread(name_, force_, connection_, this) {
    }

    // start the bucket deletion
    // May throw std::bad_alloc if we're failing to start the thread
    void start() {
        thread.start();
    }

    virtual bool execute() override {
        return true;
    }

    // notifyExecutionComplete is called from the executor while holding
    // the mutex lock, but we're calling notify_io_complete which in turn
    // tries to lock the threads lock. We held that lock when we scheduled
    // the task, so thread sanitizer will complain about potential
    // deadlock since we're now locking in the opposite order.
    // Given that we know that the executor is _blocked_ waiting for
    // this call to complete (and no one else should touch this object
    // while waiting for the call) lets just unlock and lock again..
    virtual void notifyExecutionComplete() override {
        if (thread.getConnection() != nullptr) {
            getMutex().unlock();
            // @todo i need to fix this for greenstack
            Cookie cookie(thread.getConnection());
            notify_io_complete(&cookie, thread.getResult());
            getMutex().lock();
        }
    }

    DestroyBucketThread thread;
};
