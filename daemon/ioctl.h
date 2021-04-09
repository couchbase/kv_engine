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
 * If the property could be read, return cb::engine_errc::success and writes
 * the value into `value`
 * Otherwise returns a status code indicating why the read failed.
 */
cb::engine_errc ioctl_get_property(Cookie& cookie,
                                   const std::string& key,
                                   std::string& value,
                                   cb::mcbp::Datatype& datatype);

/**
 * Attempts to set property `key` to the value `value`.
 * If the property could be written, return cb::engine_errc::success.
 * Otherwise returns a status code indicating why the write failed.
 */
cb::engine_errc ioctl_set_property(Cookie& cookie,
                                   const std::string& key,
                                   const std::string& value);
