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

#include "config.h"

#include <stddef.h>
#include <memcached/types.h>

#include "memcached.h"
#include "utilities/string_utilities.h"

/**
 * IOCTL Get callback to get the tracing status
 * @param[out] value Either "enabled" or "disabled" depending on status
 */
ENGINE_ERROR_CODE ioctlGetTracingStatus(Connection* c,
                                        const StrToStrMap& arguments,
                                        std::string& value);

/**
 * IOCTL Get callback to get the last used tracing config
 * @param[out] value The last Phoshor config used (re-encoded as a string)
 */
ENGINE_ERROR_CODE ioctlGetTracingConfig(Connection* c,
                                        const StrToStrMap& arguments,
                                        std::string& value);

/**
 * IOCTL Get callback to dump the last trace
 * @param[out] value The dumped trace
 */
ENGINE_ERROR_CODE ioctlGetTracingDump(Connection*,
                                      const StrToStrMap&,
                                      std::string& value);

/**
 * IOCTL Set callback to set the tracing config to use when it starts
 * @param value The Phosphor trace config string to start tracing with
 */
ENGINE_ERROR_CODE ioctlSetTracingConfig(Connection* c,
                                        const StrToStrMap& arguments,
                                        const std::string& value);

/**
 * IOCTL Set callback to start tracing
 */
ENGINE_ERROR_CODE ioctlSetTracingStart(Connection* c,
                                       const StrToStrMap& arguments,
                                       const std::string& value);

/**
 * IOCTL Set callback to stop tracing
 */
ENGINE_ERROR_CODE ioctlSetTracingStop(Connection* c,
                                      const StrToStrMap& arguments,
                                      const std::string& value);
