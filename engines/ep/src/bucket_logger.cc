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
BucketLogger::BucketLogger(const std::string& name, std::string p)
    : spdlog::logger(name, nullptr), prefix(std::move(p)) {
    spdLogger = cb::logger::get();

    // Take the logging level of the memcached logger so we don't format
    // anything unnecessarily
    set_level(spdLogger->level());
}

void BucketLogger::sink_it_(const spdlog::details::log_msg& msg) {
    // Use the underlying ServerAPI spdlogger to log.
    // Ideally we'd directly call spdLogger->sink_it() but it's protected so
    // instead call log() which will call sink_it_() itself.
    std::string msgString(msg.payload.begin(), msg.payload.end());
    spdLogger->log(msg.level, msgString);
}

void BucketLogger::flush_() {
    spdLogger->flush();
}

void BucketLogger::logInner(spdlog::level::level_enum lvl,
                            fmt::string_view fmt,
                            fmt::format_args args) {
    try {
        EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();
        // Disable memory tracking for the formatting and logging of the
        // message. This is necessary because the message will be written to
        // disk (and subsequently freed) by the shared background thread (as
        // part of spdlog::async_logger) and hence we do not know which engine
        // to associate the deallocation to. Instead account any log message
        // memory to "NonBucket" (it is only transient and typically small - of
        // the order of the log message length).
        NonBucketAllocationGuard guard;

        // We want to prefix the specified message with the bucket name &
        // optional prefix, but we cannot be sure that bucket name / prefix
        // doesn't contain any fmtlib formatting characters. Therefore we build
        // up the log string here then pass the already-formatted string down to
        // spdlog directly, not using spdlog's formatting functions.
        fmt::memory_buffer msg;

        // Append the id (if set)
        if (connectionId != 0) {
            fmt::format_to(std::back_inserter(msg), "{}: ", connectionId);
        }

        // Append the engine name (if applicable)
        fmt::format_to(std::back_inserter(msg),
                       "({}) ",
                       engine ? engine->getName() : "No Engine");

        // Append the given prefix (if set)
        if (!prefix.empty()) {
            fmt::format_to(std::back_inserter(msg), "{} ", prefix);
        }

        // Finally format the actual user-specified format string & args.
        fmt::vformat_to(std::back_inserter(msg), fmt, args);
        spdlog::logger::log(lvl, {msg.data(), msg.size()});
    } catch (const std::exception& e) {
        // Log a fixed message about this failing - we can't really be sure
        // what arguments failed above.
        spdlog::logger::log(spdlog::level::err,
                            "BucketLogger::logInner: Failed to log message "
                            "with format string '{}'",
                            fmt);
    }
}

void BucketLogger::logWithContext(spdlog::level::level_enum lvl,
                                  std::string_view msg,
                                  cb::logger::Json ctx) {
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

        EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();
        // Disable memory tracking for the formatting and logging of the
        // message. This is necessary because the message will be written to
        // disk (and subsequently freed) by the shared background thread (as
        // part of spdlog::async_logger) and hence we do not know which engine
        // to associate the deallocation to. Instead account any log message
        // memory to "NonBucket" (it is only transient and typically small - of
        // the order of the log message length).
        NonBucketAllocationGuard guard;

        auto& object = ctx.get_ref<cb::logger::Json::object_t&>();
        // Write the bucket
        if (engine) {
            object.insert(object.begin(), {"bucket", engine->getName()});
        }
        // Write the ID.
        if (connectionId != 0) {
            object.insert(object.begin(), {"conn_id", connectionId});
        }

        cb::logger::logWithContext(*this, lvl, msg, std::move(ctx));
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

void BucketLogger::traceWithContext(std::string_view msg,
                                    cb::logger::Json ctx) {
    logWithContext(spdlog::level::trace, msg, std::move(ctx));
}

void BucketLogger::debugWithContext(std::string_view msg,
                                    cb::logger::Json ctx) {
    logWithContext(spdlog::level::debug, msg, std::move(ctx));
}

void BucketLogger::infoWithContext(std::string_view msg, cb::logger::Json ctx) {
    logWithContext(spdlog::level::info, msg, std::move(ctx));
}

void BucketLogger::warnWithContext(std::string_view msg, cb::logger::Json ctx) {
    logWithContext(spdlog::level::warn, msg, std::move(ctx));
}

void BucketLogger::errorWithContext(std::string_view msg,
                                    cb::logger::Json ctx) {
    logWithContext(spdlog::level::err, msg, std::move(ctx));
}

void BucketLogger::criticalWithContext(std::string_view msg,
                                       cb::logger::Json ctx) {
    logWithContext(spdlog::level::critical, msg, std::move(ctx));
}

std::shared_ptr<BucketLogger> BucketLogger::createBucketLogger(
        const std::string& name, const std::string& p) {
    // Create a unique name using the engine name if available
    auto engine = ObjectRegistry::getCurrentEngine();
    std::string uname;

    if (engine) {
        uname.append(engine->getName());
        uname.append(".");
    }
    uname.append(name);

    auto bucketLogger =
            std::shared_ptr<BucketLogger>(new BucketLogger(uname, p));

    // Register the logger in the logger library registry
    cb::logger::registerSpdLogger(bucketLogger);
    return bucketLogger;
}

void BucketLogger::unregister() {
    // Unregister the logger in the logger library registry
    cb::logger::unregisterSpdLogger(name());
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
