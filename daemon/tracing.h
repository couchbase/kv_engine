/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
