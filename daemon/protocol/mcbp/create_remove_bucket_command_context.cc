/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "create_remove_bucket_command_context.h"

#include <daemon/connection.h>
#include <daemon/enginemap.h>
#include <executor/executor.h>
#include <logger/logger.h>
#include <memcached/config_parser.h>

cb::engine_errc CreateRemoveBucketCommandContext::initial() {
    if (request.getClientOpcode() == cb::mcbp::ClientOpcode::CreateBucket) {
        state = State::Create;
    } else {
        state = State::Remove;
    }

    return cb::engine_errc::success;
}

cb::engine_errc CreateRemoveBucketCommandContext::create() {
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

    cb::executor::get().add([client = &cookie,
                             nm = std::move(name),
                             cfg = std::move(config),
                             t = type]() {
        auto& connection = client->getConnection();
        cb::engine_errc status;
        try {
            status = BucketManager::instance().create(*client, nm, cfg, t);
        } catch (const std::runtime_error& error) {
            LOG_WARNING("{}: An error occurred while creating bucket [{}]: {}",
                        connection.getId(),
                        nm,
                        error.what());
            status = cb::engine_errc::failed;
        }
        ::notifyIoComplete(*client, status);
    });

    state = State::Done;
    return cb::engine_errc::would_block;
}

cb::engine_errc CreateRemoveBucketCommandContext::remove() {
    auto k = request.getKey();
    auto v = request.getValue();

    std::string name(reinterpret_cast<const char*>(k.data()), k.size());
    std::string config(reinterpret_cast<const char*>(v.data()), v.size());
    bool force = false;

    std::vector<struct config_item> items(2);
    items[0].key = "force";
    items[0].datatype = DT_BOOL;
    items[0].value.dt_bool = &force;
    items[1].key = nullptr;

    if (parse_config(config.c_str(), items.data(), stderr) != 0) {
        return cb::engine_errc::invalid_arguments;
    }

    cb::executor::get().add([client = &cookie,
                             nm = std::move(name),
                             f = force]() {
        auto& connection = client->getConnection();
        cb::engine_errc status;
        try {
            status = BucketManager::instance().destroy(client, nm, f);
        } catch (const std::runtime_error& error) {
            LOG_WARNING("{}: An error occurred while deleting bucket [{}]: {}",
                        connection.getId(),
                        nm,
                        error.what());
            status = cb::engine_errc::failed;
        }
        ::notifyIoComplete(*client, status);
    });

    state = State::Done;
    return cb::engine_errc::would_block;
}

cb::engine_errc CreateRemoveBucketCommandContext::step() {
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
