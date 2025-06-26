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
    : PrefixLogger(name, cb::logger::get()) {
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
        using namespace cb::logger;
        // Get the prefix early so we can reserve space in the final context.
        Json basePrefix = getContextPrefix();
        // Create a new context object into which we will merge all keys.
        // Final context is the conn_id + engine + static prefix + context.
        Json finalContext = Json::object();
        finalContext.get_ref<Json::object_t&>().reserve(
                ctx.size() + basePrefix.size() + bool(engine) +
                bool(connectionId));

        // Write the ID.
        if (connectionId != 0) {
            finalContext["conn_id"] = connectionId;
        }

        // Write the bucket
        if (engine) {
            finalContext["bucket"] = engine->getName();
        }

        mergeContext(finalContext, std::move(basePrefix));
        mergeContext(finalContext, std::move(ctx));
        // Call the Logger, not the PrefixLogger, since we don't want to add
        // the prefix again.
        Logger::logWithContext(lvl, msg, std::move(finalContext));
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
