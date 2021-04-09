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
