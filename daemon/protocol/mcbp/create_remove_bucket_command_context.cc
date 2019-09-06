/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "create_remove_bucket_command_context.h"

#include <daemon/enginemap.h>
#include <daemon/executorpool.h>
#include <daemon/mcbp.h>
#include <daemon/mcbpdestroybuckettask.h>

/**
 * Override the CreateBucketTask so that we can have our own notification
 * mechanism to kickstart the clients thread
 */
class McbpCreateBucketTask : public Task {
public:
    McbpCreateBucketTask(const std::string& name_,
                         const std::string& config_,
                         const BucketType& type_,
                         Cookie& cookie_)
        : thread(name_, config_, type_, cookie_.getConnection(), this),
          cookie(cookie_) {
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
        notify_io_complete(static_cast<const void*>(&cookie),
                           thread.getResult());
    }

    CreateBucketThread thread;
    Cookie& cookie;
};

ENGINE_ERROR_CODE CreateRemoveBucketCommandContext::initial() {
    if (request.getClientOpcode() == cb::mcbp::ClientOpcode::CreateBucket) {
        state = State::Create;
    } else {
        state = State::Remove;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE CreateRemoveBucketCommandContext::create() {
    auto k = request.getKey();
    auto v = request.getValue();

    std::string name(reinterpret_cast<const char*>(k.data()), k.size());
    std::string value(reinterpret_cast<const char*>(v.data()), v.size());
    std::string config;

    // Check if (optional) config was included after the value.
    auto marker = value.find('\0');
    if (marker != std::string::npos) {
        config.assign(&value[marker + 1]);
    }

    std::string errors;
    auto type = module_to_bucket_type(value.c_str());
    task = std::make_shared<McbpCreateBucketTask>(name, config, type, cookie);
    std::lock_guard<std::mutex> guard(task->getMutex());
    reinterpret_cast<McbpCreateBucketTask*>(task.get())->start();
    executorPool->schedule(task, false);

    state = State::Done;
    return ENGINE_EWOULDBLOCK;
}

ENGINE_ERROR_CODE CreateRemoveBucketCommandContext::remove() {
    auto k = request.getKey();
    auto v = request.getValue();

    std::string name(reinterpret_cast<const char*>(k.data()), k.size());
    std::string config(reinterpret_cast<const char*>(v.data()), v.size());
    bool force = false;

    std::vector<struct config_item> items(2);
    items[0].key = "force";
    items[0].datatype = DT_BOOL;
    items[0].value.dt_bool = &force;
    items[1].key = NULL;

    if (parse_config(config.c_str(), items.data(), stderr) != 0) {
        return ENGINE_EINVAL;
    }

    task = std::make_shared<McbpDestroyBucketTask>(name, force, &cookie);
    std::lock_guard<std::mutex> guard(task->getMutex());
    reinterpret_cast<McbpDestroyBucketTask*>(task.get())->start();
    executorPool->schedule(task, false);

    state = State::Done;
    return ENGINE_EWOULDBLOCK;
}

ENGINE_ERROR_CODE CreateRemoveBucketCommandContext::step() {
    try {
        auto ret = ENGINE_SUCCESS;
        do {
            switch (state) {
            case State::Initial:
                ret = initial();
                break;
            case State::Create:
                ret = create();
                break;
            case State::Remove:
                ret = remove();
                break;
            case State::Done:
                cookie.sendResponse(cb::mcbp::Status::Success);
                return ENGINE_SUCCESS;
            }
        } while (ret == ENGINE_SUCCESS);

        return ret;
    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }
}
