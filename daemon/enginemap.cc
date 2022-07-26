/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "enginemap.h"
#include "engines/crash_engine/crash_engine_public.h"
#include "engines/default_engine/default_engine_public.h"
#include "engines/ep/src/ep_engine_public.h"
#include "engines/ewouldblock_engine/ewouldblock_engine_public.h"
#include "engines/nobucket/nobucket_public.h"

#include <platform/dirutils.h>
#include <string>

unique_engine_ptr new_engine_instance(BucketType type,
                                      GET_SERVER_API get_server_api) {
    EngineIface* ret = nullptr;
    cb::engine_errc status = cb::engine_errc::no_such_key;
    try {
        switch (type) {
        case BucketType::NoBucket:
            return create_no_bucket_instance();
        case BucketType::Memcached:
            status = create_memcache_instance(get_server_api, &ret);
            break;
        case BucketType::Couchbase:
            status = create_ep_engine_instance(get_server_api, &ret);
            break;
        case BucketType::EWouldBlock:
            return create_ewouldblock_instance(get_server_api);
        case BucketType::ClusterConfigOnly:
        case BucketType::Unknown:
            // fall through with status == cb::engine_errc::no_such_key
            break;
        }
    } catch (const std::bad_alloc&) {
        status = cb::engine_errc::no_memory;
    } catch (const std::exception&) {
        status = cb::engine_errc::failed;
    }

    if (status == cb::engine_errc::success) {
        if (ret == nullptr) {
            throw cb::engine_error(
                    cb::engine_errc::failed,
                    "new_engine_instance: create function returned success, "
                    "but no engine handle returned");
        }
        return unique_engine_ptr{ret};
    }

    throw cb::engine_error(
            cb::engine_errc(status),
            "new_engine_instance(): Failed to create bucket of type: " +
                    to_string(type));
}

void create_crash_instance() {
    auto engine = create_crash_engine_instance();
    engine->initialize({});
}

BucketType module_to_bucket_type(const std::string& module) {
    std::string nm = cb::io::basename(module);
    if (nm == "nobucket.so") {
        return BucketType::NoBucket;
    } else if (nm == "default_engine.so") {
        return BucketType::Memcached;
    } else if (nm == "ep.so") {
        return BucketType::Couchbase;
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
    case BucketType::ClusterConfigOnly:
    case BucketType::NoBucket:
        // no cleanup needed;
    case BucketType::Memcached:
        destroy_memcache_engine();
    case BucketType::Couchbase:
        destroy_ep_engine();
    case BucketType::EWouldBlock:
        // no cleanup needed;
    case BucketType::Unknown:
        break;
    }
}
