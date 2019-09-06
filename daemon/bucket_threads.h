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
    CreateBucketThread(const std::string& name_,
                       const std::string& config_,
                       const BucketType type_,
                       Connection& connection_,
                       Task* task_)
        : Couchbase::Thread("mc:bucket_add"),
          name(name_),
          config(config_),
          type(type_),
          connection(connection_),
          task(task_),
          result(ENGINE_DISCONNECT) {
        // Empty
    }

    ~CreateBucketThread() {
        waitForState(Couchbase::ThreadState::Zombie);
    }

    Connection& getConnection() const {
        return connection;
    }

    ENGINE_ERROR_CODE getResult() const {
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

    std::string name;
    std::string config;
    BucketType type;
    Connection& connection;
    Task* task;
    ENGINE_ERROR_CODE result;
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
          result(ENGINE_DISCONNECT) {
    }

    ~DestroyBucketThread() override {
        waitForState(Couchbase::ThreadState::Zombie);
    }

    ENGINE_ERROR_CODE getResult() const {
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

    std::string name;
    bool force;
    Cookie* cookie;
    Task* task;
    ENGINE_ERROR_CODE result;
};
