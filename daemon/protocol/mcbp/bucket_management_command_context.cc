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

#include <daemon/connection.h>
#include <daemon/enginemap.h>
#include <daemon/one_shot_task.h>
#include <daemon/session_cas.h>
#include <daemon/settings.h>
#include <executor/executorpool.h>
#include <logger/logger.h>
#include <memcached/config_parser.h>
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
    auto k = request.getKey();
    auto v = request.getValue();

    std::string name(reinterpret_cast<const char*>(k.data()), k.size());
    std::string value(reinterpret_cast<const char*>(v.data()), v.size());
    std::string config;

    // Check if (optional) config was included after the value.
    auto marker = value.find('\0');
    if (marker != std::string::npos) {
        config = value.substr(marker + 1);
        value.resize(marker);
    }

    auto type = module_to_bucket_type(value);
    if (isServerlessDeployment() && type == BucketType::Memcached) {
        cookie.setErrorContext(
                "memcached buckets can't be used in serverless configuration");
        return cb::engine_errc::not_supported;
    }

    std::string taskname{"Create bucket [" + name + "]"};
    ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
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
                    LOG_WARNING(
                            "{}: An error occurred while creating bucket [{}]: "
                            "{}",
                            connection.getId(),
                            nm,
                            error.what());
                    status = cb::engine_errc::failed;
                }
                client->notifyIoComplete(status);
            },
            std::chrono::seconds(10)));

    state = State::Done;
    return cb::engine_errc::would_block;
}

cb::engine_errc BucketManagementCommandContext::remove() {
    auto k = request.getKey();
    std::string name(reinterpret_cast<const char*>(k.data()), k.size());
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
                LOG_WARNING(
                        "{} Invalid payload provided with delete bucket: {}",
                        connection.getId(),
                        config);
                return cb::engine_errc::invalid_arguments;
            }
        } catch (const std::exception& e) {
            LOG_WARNING(
                    "{} Exception occurred while parsing delete bucket "
                    "payload: "
                    "\"{}\". {}",
                    connection.getId(),
                    config,
                    e.what());
            return cb::engine_errc::failed;
        }
    }

    // If we're connected to the given bucket we should switch to another
    // bucket first
    if (name == connection.getBucket().name) {
        associate_bucket(cookie, "");
    }

    std::string taskname{"Delete bucket [" + name + "]"};
    ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
            TaskId::Core_DeleteBucketTask,
            taskname,
            [client = &cookie,
             nm = std::move(name),
             f = force,
             type = bucket_type]() {
                auto& connection = client->getConnection();
                cb::engine_errc status;
                try {
                    status = BucketManager::instance().destroy(
                            *client, nm, f, type);
                } catch (const std::runtime_error& error) {
                    LOG_WARNING(
                            "{}: An error occurred while deleting bucket [{}]: "
                            "{}",
                            connection.getId(),
                            nm,
                            error.what());
                    status = cb::engine_errc::failed;
                }
                client->notifyIoComplete(status);
            },
            std::chrono::seconds(30)));

    state = State::Done;
    return cb::engine_errc::would_block;
}

cb::engine_errc BucketManagementCommandContext::pause() {
    auto k = request.getKey();
    std::string name(reinterpret_cast<const char*>(k.data()), k.size());
    if (name == cookie.getConnection().getBucket().name) {
        LOG_WARNING("{} Can't pause the connections' selected bucket",
                    cookie.getConnectionId());
        return cb::engine_errc::invalid_arguments;
    }
    // Run a background task to perform the actual pause() of the bucket, as
    // this can be long-running (need to wait for outstanding IO operations)
    // and ns_server requires that the PauseBucket() command returns
    // immediately.
    auto pauseFunc = [client = &cookie, name]() {
        ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
                TaskId::Core_PauseBucketTask,
                "Pause bucket",
                [client, nm = std::move(name)]() {
                    try {
                        BucketManager::instance().pause(*client, nm);
                    } catch (const std::runtime_error& error) {
                        LOG_WARNING(
                                "{}: An error occurred while pausing "
                                "bucket [{}]: {}",
                                client->getConnectionId(),
                                nm,
                                error.what());
                    }
                },
                std::chrono::seconds(10)));
    };
    if (!session_cas.execute(request.getCas(), pauseFunc)) {
        return cb::engine_errc::key_already_exists;
    }
    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc BucketManagementCommandContext::resume() {
    cb::engine_errc status = cb::engine_errc::failed;
    auto resumeFunc =
            [&status, client = &cookie, name = request.getKeyString()]() {
                try {
                    status = BucketManager::instance().resume(*client, name);
                } catch (const std::runtime_error& error) {
                    LOG_WARNING(
                            "{}: An error occurred while resuming "
                            "bucket [{}]: {}",
                            client->getConnectionId(),
                            name,
                            error.what());
                }
            };
    if (!session_cas.execute(request.getCas(), resumeFunc)) {
        status = cb::engine_errc::key_already_exists;
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
