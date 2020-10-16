/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#pragma once

#include <string>

namespace cb::sasl {
// Forward decl
class Context;
namespace logging {

enum class Level {
    /**
     * Log an error situation. Entries logged at this level contains a
     * UUID if it is bound to a connection.
     */
    Error,
    /**
     * The log message is for an authentication failure
     */
    Fail,
    /**
     * This is a non-fatal warning
     */
    Warning,
    /**
     * In informational message produced by the library
     */
    Notice,
    /**
     * Debug message
     */
    Debug,
    /**
     * Trace of internal protocol
     */
    Trace
};

/**
 * The log callback method the user of the library may configure. It
 * is called every time with the message to add to the log if the level
 * is enabled.
 */
using LogCallback = void (*)(Level level, const std::string& message);

/**
 * Specify the callback function to use for logging
 */
void set_log_callback(LogCallback callback);

/**
 * Perform logging within the CBSASL library for components which isn't bound
 * to a given client.
 *
 * @param level
 * @param message
 */
void log(Level level, const std::string& message);

/**
 * Perform logging related to a given client.
 *
 * @param level
 * @param message
 */
void log(Context* server, Level level, const std::string& message);

} // namespace logging
} // namespace cb::sasl
