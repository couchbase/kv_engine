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
// used
BucketLogger::BucketLogger(spdlog::logger* logger)
    : spdlog::logger(logger->name(), nullptr) {
    spdLogger = logger;
    set_level(spdLogger->level());
}

void BucketLogger::_sink_it(spdlog::details::log_msg& msg) {
    EventuallyPersistentEngine* engine =
            ObjectRegistry::onSwitchThread(NULL, true);
    std::string fmtString;

    // Prepend the engine name
    if (engine) {
        fmtString = '(' + std::string(engine->getName().c_str()) + ") " +
                    msg.raw.c_str();
        spdLogger->log(msg.level, fmtString);
    } else {
        fmtString.append("(No Engine) ");
        fmtString.append(msg.raw.c_str());

        spdLogger->log(msg.level, fmtString);
    }

    ObjectRegistry::onSwitchThread(engine);
}

std::unique_ptr<BucketLogger> globalBucketLogger;
