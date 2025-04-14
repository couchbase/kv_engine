/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "bucket_logger.h"

#include <logger/logger.h>
#include <utility>

#include "ep_engine.h"
#include "objectregistry.h"

// Construct the base logger with a nullptr for the sinks as they will never be
// used. Requires a unique name for registry
BucketLogger::BucketLogger(const std::string& name)
    : Logger(name, cb::logger::get()) {
}

void BucketLogger::logWithContext(spdlog::level::level_enum lvl,
                                  std::string_view msg,
                                  cb::logger::Json ctx) {
    // get engine before disabling memory tracking!
    EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();

    // Disable memory tracking for the formatting and logging of the
    // message. This is necessary because the message will be written to
    // disk (and subsequently freed) by the shared background thread (as
    // part of spdlog::async_logger) and hence we do not know which engine
    // to associate the deallocation to. Instead account any log message
    // memory to "NonBucket" (it is only transient and typically small - of
    // the order of the log message length).
    // scope of memory-tracking disablement must also cover the exception
    // handler - see MB-61032.
    NonBucketAllocationGuard guard;
    try {
        if (!ctx.is_object()) {
#if CB_DEVELOPMENT_ASSERTS
            throw std::invalid_argument(fmt::format(
                    "JSON context must be an object, not `{}`", ctx.dump()));
#else
            // In production, handle this case gracefully.
            ctx = Json{{"context", std::move(ctx)}};
#endif
        }

        auto& object = ctx.get_ref<cb::logger::Json::object_t&>();
        object.reserve(object.size() + prefixContext.size() + bool(engine) +
                       bool(connectionId));

        if (!prefixContext.empty()) {
            const auto& prefixObject =
                    prefixContext.get_ref<const nlohmann::json::object_t&>();
            for (auto it = prefixObject.crbegin(); it != prefixObject.crend();
                 ++it) {
                object.insert(object.begin(),
                              cb::logger::Json{it->first, it->second});
            }
        }

        // Write the bucket
        if (engine) {
            object.insert(object.begin(), {"bucket", engine->getName()});
        }
        // Write the ID.
        if (connectionId != 0) {
            object.insert(object.begin(), {"conn_id", connectionId});
        }

        Logger::logWithContext(lvl, msg, std::move(ctx));
    } catch (const std::exception& e) {
        // Log a fixed message about this failing - we can't really be sure
        // what arguments failed above.
        spdlog::logger::log(
                spdlog::level::err,
                "BucketLogger::logWithContext: Failed to log '{}' {}, what(): ",
                msg,
                ctx,
                e.what());
    }
}

std::shared_ptr<BucketLogger> BucketLogger::createBucketLogger(
        const std::string& name) {
    // Create a unique name using the engine name if available
    auto engine = ObjectRegistry::getCurrentEngine();
    std::string uname;

    if (engine) {
        uname.append(engine->getName());
        uname.append(".");
    }
    uname.append(name);

    auto bucketLogger = std::shared_ptr<BucketLogger>(new BucketLogger(uname));

    // Register the logger in the logger library registry
    bucketLogger->tryRegister();
    return bucketLogger;
}

std::shared_ptr<BucketLogger>& getGlobalBucketLogger() {
    // This is a process-wide singleton used by all engines, as such its memory
    // should not be allocated to any specific bucket.
    static auto logger = []() {
        NonBucketAllocationGuard guard;
        return BucketLogger::createBucketLogger(globalBucketLoggerName);
    }();
    return logger;
}
