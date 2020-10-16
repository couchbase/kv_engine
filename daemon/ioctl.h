/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include <memcached/engine_error.h>
#include <string>

// forward decl
class Cookie;
namespace cb::mcbp {
enum class Datatype : uint8_t;
} // namespace cb::mcbp

/**
 * Attempts to read the given property.
 * If the property could be read, return ENGINE_SUCCESS and writes
 * the value into `value`
 * Otherwise returns a status code indicating why the read failed.
 */
ENGINE_ERROR_CODE ioctl_get_property(Cookie& cookie,
                                     const std::string& key,
                                     std::string& value,
                                     cb::mcbp::Datatype& datatype);

/**
 * Attempts to set property `key` to the value `value`.
 * If the property could be written, return ENGINE_SUCCESS.
 * Otherwise returns a status code indicating why the write failed.
 */
ENGINE_ERROR_CODE ioctl_set_property(Cookie& cookie,
                                     const std::string& key,
                                     const std::string& value);
