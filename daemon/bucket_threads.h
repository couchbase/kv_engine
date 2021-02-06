/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include <memcached/engine_error.h>
#include <platform/thread.h>

class Connection;
class Task;

/**
 * The CreateBucketThread is as the name implies a thread who creates a new
 * bucket.
 */
class CreateBucketThread : public Couchbase::Thread {
public:
    /**
     * Initialize this bucket creation thread.
     *
     * @param name_ the name of the bucket to create
     * @param config_ the buckets configuration
     * @param type_ the type of bucket to create
     * @param connection_ the connection that requested the operation (and
     *                    should be signalled when the creation is complete)
     *
     * @throws std::illegal_arguments if bucket name contains illegal
     *                                characters
     */
    CreateBucketThread(std::string name_,
                       std::string config_,
                       const BucketType type_,
                       Connection& connection_,
                       Task* task_)
        : Couchbase::Thread("mc:bucket_add"),
          name(std::move(name_)),
          config(std::move(config_)),
          type(type_),
          connection(connection_),
          task(task_),
          result(cb::engine_errc::disconnect) {
        // Empty
    }

    ~CreateBucketThread() override {
        waitForState(Couchbase::ThreadState::Zombie);
    }

    Connection& getConnection() const {
        return connection;
    }

    cb::engine_errc getResult() const {
        return result;
    }

    const std::string& getErrorMessage() const {
        return error;
    }

protected:
    void run() override;

private:
    /**
     * The actual implementation of the bucket creation.
     */
    void create();

    const std::string name;
    const std::string config;
    BucketType type;
    Connection& connection;
    Task* task;
    cb::engine_errc result;
    std::string error;
};

/**
 * The DestroyBucketThread is as the name implies a thread is responsible for
 * deleting a bucket.
 */
class DestroyBucketThread : public Couchbase::Thread {
public:
    /**
     * Initialize this bucket creation task.
     *
     * @param name_ the name of the bucket to delete
     * @param force_ should the bucket be forcibly shut down or should it
     *               try to perform a clean shutdown
     * @param cookie_ the cookie that requested the operation (may be
     *                nullptr if the system itself requested the deletion)
     * @param task_ the task to notify when deletion is complete
     */
    DestroyBucketThread(std::string name_,
                        bool force_,
                        Cookie* cookie_,
                        Task* task_)
        : Couchbase::Thread("mc:bucket_del"),
          name(std::move(name_)),
          force(force_),
          cookie(cookie_),
          task(task_),
          result(cb::engine_errc::disconnect) {
    }

    ~DestroyBucketThread() override {
        waitForState(Couchbase::ThreadState::Zombie);
    }

    cb::engine_errc getResult() const {
        return result;
    }

    Cookie* getCookie() {
        return cookie;
    }

protected:
    void run() override;

private:
    /**
     * The actual implementation of the bucket deletion.
     */
    void destroy();

    const std::string name;
    const bool force;
    Cookie* cookie;
    Task* task;
    cb::engine_errc result;
};
