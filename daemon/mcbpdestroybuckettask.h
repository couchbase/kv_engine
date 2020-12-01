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

#include "bucket_threads.h"
#include "buckets.h"
#include "connection.h"
#include "memcached.h"
#include "task.h"

#include <string>


class McbpDestroyBucketTask : public Task {
public:
    McbpDestroyBucketTask(const std::string& name_,
                          bool force_,
                          Cookie* cookie_)
        : thread(name_, force_, cookie_, this) {
    }

    // start the bucket deletion
    // May throw std::bad_alloc if we're failing to start the thread
    void start() {
        thread.start();
    }

    Status execute() override {
        return Status::Finished;
    }

    void notifyExecutionComplete() override {
        auto* cookie = thread.getCookie();
        if (cookie) {
            ::notifyIoComplete(*cookie, thread.getResult());
        }
    }

protected:
    DestroyBucketThread thread;
};
