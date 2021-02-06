/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "utilities/string_utilities.h"

#include <memcached/engine_error.h>
#include <chrono>

class Cookie;
struct DumpContext;
namespace cb::mcbp {
enum class Datatype : uint8_t;
} // namespace cb::mcbp
/**
 * When measuring mutex wait / lock durations, what is considered a default
 * "slow" wait/lock time and trace events should be logged?
 */
constexpr auto SlowMutexThreshold = std::chrono::milliseconds(10);

/**
 * Initialises Tracing
 */
void initializeTracing(const std::string& traceConfig,
                       std::chrono::seconds interval,
                       std::chrono::seconds max_age);

/**
 * Deinitialises Tracing
 */
void deinitializeTracing();

/**
 * IOCTL Get callback to get the tracing status
 * @param[out] value Either "enabled" or "disabled" depending on status
 */
cb::engine_errc ioctlGetTracingStatus(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      std::string& value,
                                      cb::mcbp::Datatype& datatype);

/**
 * IOCTL Get callback to get the last used tracing config
 * @param[out] value The last Phoshor config used (re-encoded as a string)
 */
cb::engine_errc ioctlGetTracingConfig(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      std::string& value,
                                      cb::mcbp::Datatype& datatype);

/// IOCT Get callback to get a list of all of the registered dumps
cb::engine_errc ioctlGetTracingList(Cookie& cookie,
                                    const StrToStrMap& arguments,
                                    std::string& value,
                                    cb::mcbp::Datatype& datatype);

/**
 * IOCTL Get callback to create a new dump from the last trace
 * @param[out] value The uuid of the newly created dump
 */
cb::engine_errc ioctlGetTracingBeginDump(Cookie& cookie,
                                         const StrToStrMap&,
                                         std::string& value,
                                         cb::mcbp::Datatype& datatype);

/**
 * IOCTL Get callback to fetch an entire trace dump
 * @param[out] value The uuid of the newly created dump
 */
cb::engine_errc ioctlGetTraceDump(Cookie& cookie,
                                  const StrToStrMap&,
                                  std::string& value,
                                  cb::mcbp::Datatype& datatype);

/**
 * IOCTL Set callback to clear a tracing dump
 * @param value The uuid of the dump to clear
 */
cb::engine_errc ioctlSetTracingClearDump(Cookie& cookie,
                                         const StrToStrMap& arguments,
                                         const std::string& value);

/**
 * IOCTL Set callback to set the tracing config to use when it starts
 * @param value The Phosphor trace config string to start tracing with
 */
cb::engine_errc ioctlSetTracingConfig(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      const std::string& value);

/**
 * IOCTL Set callback to start tracing
 */
cb::engine_errc ioctlSetTracingStart(Cookie& cookie,
                                     const StrToStrMap& arguments,
                                     const std::string& value);

/**
 * IOCTL Set callback to stop tracing
 */
cb::engine_errc ioctlSetTracingStop(Cookie& cookie,
                                    const StrToStrMap& arguments,
                                    const std::string& value);
