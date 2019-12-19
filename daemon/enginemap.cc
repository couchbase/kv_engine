/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
#include "enginemap.h"
#include "engines/crash_engine/crash_engine_public.h"
#include "engines/default_engine/default_engine_public.h"
#include "engines/ep/src/ep_engine_public.h"
#include "engines/ewouldblock_engine/ewouldblock_engine_public.h"
#include "engines/nobucket/nobucket_public.h"
#include "logger/logger.h"

#include <platform/dirutils.h>
#include <string>

EngineIface* new_engine_instance(BucketType type,
                                 GET_SERVER_API get_server_api) {
    EngineIface* ret = nullptr;
    ENGINE_ERROR_CODE status = ENGINE_KEY_ENOENT;
    switch (type) {
    case BucketType::NoBucket:
        status = create_no_bucket_instance(get_server_api, &ret);
        break;
    case BucketType::Memcached:
        status = create_memcache_instance(get_server_api, &ret);
        break;
    case BucketType::Couchstore:
        status = create_ep_engine_instance(get_server_api, &ret);
        break;
    case BucketType::EWouldBlock:
        status = create_ewouldblock_instance(get_server_api, &ret);
        break;
    case BucketType::Unknown:
        // fall through with status == ENGINE_KEY_ENOENT
        break;
    }

    if (status == ENGINE_SUCCESS) {
        if (ret == nullptr) {
            throw cb::engine_error(
                    cb::engine_errc::failed,
                    "new_engine_instance: create function returned success, "
                    "but no engine handle returned");
        }
        return ret;
    }

    throw cb::engine_error(
            cb::engine_errc(status),
            "new_engine_instance(): Failed to create bucket of type: " +
                    to_string(type));
}

void create_crash_instance() {
    EngineIface* h;
    if (create_crash_engine_instance(nullptr, &h) != ENGINE_SUCCESS) {
        throw std::runtime_error(
                "create_crash_instance(): Failed to create instance of crash "
                "engine");
    }
    h->initialize(nullptr);
}

BucketType module_to_bucket_type(const std::string& module) {
    std::string nm = cb::io::basename(module);
    if (nm == "nobucket.so") {
        return BucketType::NoBucket;
    } else if (nm == "default_engine.so") {
        return BucketType::Memcached;
    } else if (nm == "ep.so") {
        return BucketType::Couchstore;
    } else if (nm == "ewouldblock_engine.so") {
        return BucketType::EWouldBlock;
    }
    return BucketType::Unknown;
}

void shutdown_all_engines() {
    // switch statement deliberately falls through all cases as all engine types
    // need shutting down. The use of a case statement ensures new bucket types
    // are also considered for shutdown (requires the non-const type input)
    auto type = BucketType::NoBucket;
    switch (type) {
    case BucketType::NoBucket:
        destroy_no_bucket_engine();
    case BucketType::Memcached:
        destroy_memcache_engine();
    case BucketType::Couchstore:
        destroy_ep_engine();
    case BucketType::EWouldBlock:
        destroy_ewouldblock_engine();
    case BucketType::Unknown:
        break;
    }
}
