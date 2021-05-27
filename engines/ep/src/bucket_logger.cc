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

#include <utility>

#include "ep_engine.h"
#include "objectregistry.h"

std::mutex BucketLogger::initGuard;

// Construct the base logger with a nullptr for the sinks as they will never be
// used. Requires a unique name for registry
BucketLogger::BucketLogger(const std::string& name, std::string p)
    : spdlog::logger(name, nullptr), prefix(std::move(p)) {
    spdLogger = BucketLogger::loggerAPI.load(std::memory_order_relaxed)
                        ->get_spdlogger();

    // Take the logging level of the memcached logger so we don't format
    // anything unnecessarily
    set_level(spdLogger->level());
}

BucketLogger::~BucketLogger() {
    unregister();
}

void BucketLogger::sink_it_(spdlog::details::log_msg& msg) {
    // Use the underlying ServerAPI spdlogger to log.
    // Ideally we'd directly call spdLogger->sink_it() but it's protected so
    // instead call log() which will call sink_it_() itself.
    std::string msgString(msg.raw.begin(), msg.raw.end());
    spdLogger->log(msg.level, msgString);
}

void BucketLogger::flush_() {
    spdLogger->flush();
}

void BucketLogger::setLoggerAPI(ServerLogIface* api) {
    BucketLogger::loggerAPI.store(api, std::memory_order_relaxed);

    std::unique_lock<std::mutex> lh(initGuard);
    if (getGlobalBucketLogger() == nullptr) {
        // Create the global BucketLogger
        getGlobalBucketLogger() = createBucketLogger(globalBucketLoggerName);
    }
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
    getServerLogIface()->register_spdlogger(bucketLogger);
    return bucketLogger;
}

void BucketLogger::unregister() {
    // Unregister the logger in the logger library registry
    getServerLogIface()->unregister_spdlogger(name());
}

std::string BucketLogger::prefixStringWithBucketName(
        const EventuallyPersistentEngine* engine, const char* fmt) {
    std::string fmtString;

    // Append the id (if set)
    if (connectionId != 0) {
        fmtString.append(std::to_string(connectionId) + ": ");
    }

    // Append the engine name (if applicable)
    if (engine) {
        fmtString.append('(' + std::string(engine->getName().c_str()) + ") ");
    } else {
        fmtString.append("(No Engine) ");
    }

    // Append the given prefix (if set)
    if (!prefix.empty()) {
        fmtString.append(prefix + " ");
    }

    // Append the original format string
    fmtString.append(fmt);
    return fmtString;
}

ServerLogIface* BucketLogger::getServerLogIface() {
    return loggerAPI.load(std::memory_order_relaxed);
}

std::atomic<ServerLogIface*> BucketLogger::loggerAPI;

std::shared_ptr<BucketLogger>& getGlobalBucketLogger() {
    static std::shared_ptr<BucketLogger> logger = {};
    return logger;
}
