/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "bucket_management_command_context.h"

#include <daemon/bucket_destroyer.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/enginemap.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <daemon/settings.h>
#include <daemon/yielding_limited_concurrency_task.h>
#include <executor/executorpool.h>
#include <logger/logger.h>
#include <memcached/config_parser.h>
#include <serverless/config.h>
#include <utilities/json_utilities.h>

cb::engine_errc BucketManagementCommandContext::initial() {
    auto opcode = request.getClientOpcode();
    if (opcode == cb::mcbp::ClientOpcode::CreateBucket) {
        state = State::Create;
    } else if (opcode == cb::mcbp::ClientOpcode::DeleteBucket) {
        state = State::Remove;
    } else if (opcode == cb::mcbp::ClientOpcode::PauseBucket) {
        state = State::Pause;
    } else if (opcode == cb::mcbp::ClientOpcode::ResumeBucket) {
        state = State::Resume;
    }

    return cb::engine_errc::success;
}

cb::engine_errc BucketManagementCommandContext::create() {
    std::string name(request.getKeyString());
    std::string value(request.getValueString());
    std::string config;

    // Check if (optional) config was included after the value.
    auto marker = value.find('\0');
    if (marker != std::string::npos) {
        config = value.substr(marker + 1);
        value.resize(marker);
    }

    auto type = module_to_bucket_type(value);
    std::string taskname{"Create bucket [" + name + "]"};
    ExecutorPool::get()->schedule(std::make_shared<
                                  OneShotLimitedConcurrencyTask>(
            TaskId::Core_CreateBucketTask,
            taskname,
            [client = &cookie,
             nm = std::move(name),
             cfg = std::move(config),
             t = type]() {
                auto& connection = client->getConnection();
                cb::engine_errc status;
                try {
                    status = BucketManager::instance().create(
                            *client, nm, cfg, t);
                } catch (const std::runtime_error& error) {
                    LOG_WARNING_CTX("An error occurred while creating bucket",
                                    {"conn_id", connection.getId()},
                                    {"bucket", nm},
                                    {"error", error.what()});
                    status = cb::engine_errc::failed;
                }
                client->notifyIoComplete(status);
            },
            ConcurrencySemaphores::instance().bucket_management,
            std::chrono::seconds(10)));

    state = State::Done;
    return cb::engine_errc::would_block;
}

cb::engine_errc BucketManagementCommandContext::remove() {
    std::string name(request.getKeyString());
    const auto config = request.getValueString();
    bool force = false;
    std::optional<BucketType> bucket_type;

    if (!config.empty()) {
        try {
            bool invalid_arguments = false;

            try {
                auto json = nlohmann::json::parse(config);
                auto v = cb::getOptionalJsonObject(
                        json, "force", nlohmann::json::value_t::boolean);
                if (v) {
                    force = v.value().get<bool>();
                }
                v = cb::getOptionalJsonObject(
                        json, "type", nlohmann::json::value_t::string);
                if (v) {
                    bucket_type =
                            parse_bucket_type(v.value().get<std::string>());
                    if (bucket_type.value() == BucketType::Unknown) {
                        invalid_arguments = true;
                    }
                }
            } catch (const std::exception&) {
                invalid_arguments = true;
            }
            if (invalid_arguments) {
                LOG_WARNING_CTX("Invalid payload provided with delete bucket",
                                {"conn_id", connection.getId()},
                                {"config", config});
                return cb::engine_errc::invalid_arguments;
            }
        } catch (const std::exception& e) {
            LOG_WARNING_CTX(
                    "Exception occurred while parsing delete bucket payload",
                    {"conn_id", connection.getId()},
                    {"config", config},
                    {"error", e.what()});
            return cb::engine_errc::failed;
        }
    }

    // If we're connected to the given bucket we should switch to another
    // bucket first
    if (name == connection.getBucket().name) {
        associate_bucket(cookie, "");
    }

    auto [status, optDestroyer] = BucketManager::instance().startDestroy(
            std::to_string(cookie.getConnectionId()), name, force, bucket_type);

    if (status != cb::engine_errc::would_block) {
        return status;
    }
    Expects(optDestroyer);

    std::string taskname{"Delete bucket [" + name + "]"};
    using namespace std::chrono_literals;
    auto task = [destroyer = std::move(*optDestroyer),
                 client = &cookie,
                 nm = std::move(name)]() mutable
            -> std::optional<std::chrono::duration<double>> {
        auto& connection = client->getConnection();
        cb::engine_errc status;
        try {
            status = destroyer.drive();
        } catch (const std::runtime_error& error) {
            LOG_WARNING_CTX("An error occurred while deleting bucket",
                            {"conn_id", connection.getId()},
                            {"bucket", nm},
                            {"error", error.what()});
            status = cb::engine_errc::failed;
        }
        if (status == cb::engine_errc::would_block) {
            // destroyer hasn't completed yet (waiting for connections
            // or operations), try again in 10ms
            return {10ms};
        }
        client->notifyIoComplete(status);
        return {};
    };
    ExecutorPool::get()->schedule(
            std::make_shared<YieldingLimitedConcurrencyTask>(
                    TaskId::Core_DeleteBucketTask,
                    std::move(taskname),
                    std::move(task),
                    ConcurrencySemaphores::instance().bucket_management,
                    100ms));

    state = State::Done;
    return cb::engine_errc::would_block;
}

cb::engine_errc BucketManagementCommandContext::pause() {
    std::string name(request.getKeyString());
    if (name == cookie.getConnection().getBucket().name) {
        LOG_WARNING_CTX("Can't pause the connections' selected bucket",
                        {"conn_id", cookie.getConnectionId()});
        return cb::engine_errc::invalid_arguments;
    }
    // Run a background task to perform the actual pause() of the bucket, as
    // this can be long-running (need to wait for outstanding IO operations)
    ExecutorPool::get()->schedule(
            std::make_shared<OneShotLimitedConcurrencyTask>(
                    TaskId::Core_PauseBucketTask,
                    "Pause bucket",
                    [client = &cookie, nm = std::move(name)]() {
                        cb::engine_errc status;
                        try {
                            status = BucketManager::instance().pause(*client,
                                                                     nm);
                        } catch (const std::runtime_error& error) {
                            LOG_WARNING_CTX(
                                    "An error occurred while pausing "
                                    "bucket",
                                    {"conn_id", client->getConnectionId()},
                                    {"bucket", nm},
                                    {"error", error.what()});
                            status = cb::engine_errc::failed;
                        }
                        client->notifyIoComplete(status);
                    },
                    ConcurrencySemaphores::instance().bucket_management,
                    std::chrono::seconds(10)));
    state = State::Done;
    return cb::engine_errc::would_block;
}

cb::engine_errc BucketManagementCommandContext::resume() {
    cb::engine_errc status = cb::engine_errc::failed;
    try {
        status = BucketManager::instance().resume(cookie,
                                                  request.getKeyString());
    } catch (const std::runtime_error& error) {
        LOG_WARNING_CTX("An error occurred while resuming bucket",
                        {"conn_id", cookie.getConnectionId()},
                        {"bucket", request.getKeyString()},
                        {"error", error.what()});
    }
    state = State::Done;
    return status;
}

cb::engine_errc BucketManagementCommandContext::step() {
    try {
        auto ret = cb::engine_errc::success;
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
            case State::Pause:
                ret = pause();
                break;
            case State::Resume:
                ret = resume();
                break;
            case State::Done:
                cookie.sendResponse(cb::mcbp::Status::Success);
                return cb::engine_errc::success;
            }
        } while (ret == cb::engine_errc::success);

        return ret;
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
}
