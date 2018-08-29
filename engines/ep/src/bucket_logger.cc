/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "bucket_logger.h"
#include "ep_engine.h"
#include "objectregistry.h"

// Construct the base logger with a nullptr for the sinks as they will never be
// used. Requires a unique name for registry
BucketLogger::BucketLogger(const std::string& name, const std::string& p)
    : spdlog::logger(name, nullptr), prefix(p) {
    spdLogger = BucketLogger::loggerAPI.load(std::memory_order_relaxed)
                        ->get_spdlogger();

    // Take the logging level of the memcached logger so we don't format
    // anything unnecessarily
    set_level(spdLogger->level());
}

BucketLogger::BucketLogger(const BucketLogger& other)
    : spdlog::logger(other.name(), nullptr) {
    spdLogger = other.spdLogger;
    set_level(spdLogger->level());
}

BucketLogger::~BucketLogger() {
    unregister();
}

void BucketLogger::sink_it_(spdlog::details::log_msg& msg) {
    // Get the engine pointer for logging the bucket name.
    // Normally we would wish to stop tracking memory at this point to avoid
    // tracking any allocations or de-allocations done by the logging library
    // such as buffer allocations, or by ourselves in formatting the string.
    // However, as this method is overriden from an spdlog instance,
    // allocations have already been made and tracked as part of formatting
    // this message (from the BucketLogger->log() call to this point). There
    // is little point spending the overhead to switch thread to avoid
    // tracking the allocations of our custom formatting as this is the
    // case. Memory is not allocated in actually logging the message, this is
    // done at creation of the logger where we allocate a fixed size buffer.
    // As such, we don't have to worry about the tracking implications of
    // allocation on the calling thread and de-allocation on the processing
    // worker thread when using the async mode.
    EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();

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
    if (prefix.size() > 0) {
        fmtString.append(prefix + " ");
    }

    // Append the rest of the message and log
    fmtString.append(msg.raw.data(), msg.raw.size());
    spdLogger->log(msg.level, fmtString);
}

void BucketLogger::setLoggerAPI(ServerLogIface* api) {
    BucketLogger::loggerAPI.store(api, std::memory_order_relaxed);

    if (globalBucketLogger == nullptr) {
        // Create the global BucketLogger
        globalBucketLogger = createBucketLogger(globalBucketLoggerName);
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
    loggerAPI.load(std::memory_order_relaxed)->register_spdlogger(bucketLogger);
    return bucketLogger;
}

void BucketLogger::unregister() {
    // Unregister the logger in the logger library registry
    loggerAPI.load(std::memory_order_relaxed)->unregister_spdlogger(name());
}

std::atomic<ServerLogIface*> BucketLogger::loggerAPI;

std::shared_ptr<BucketLogger> globalBucketLogger;
