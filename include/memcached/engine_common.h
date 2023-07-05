/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <string_view>

struct EngineIface;
class CookieIface;

namespace cb::mcbp {
enum class Status : uint16_t;
}

/**
 * Callback for any function producing stats.
 *
 * @param key the stat's key
 * @param value the stat's value in an ascii form (e.g. text form of a number)
 * @param cookie the cookie provided by the frontend
 */
using AddStatFn = std::function<void(
        std::string_view key, std::string_view value, CookieIface& cookie)>;

/**
 * The response handler only accepts JSON or Raw datatype (not xattr or
 * snappy compressed).
 */
enum class ValueIsJson : uint8_t { Yes, No };

/**
 * Callback for adding a response packet
 * @param key The key to put in the response
 * @param extras The data to put in the extended field in the response
 * @param body The data body
 * @param json Is the value JSON or not
 * @param status The status code of the return packet (see in protocol_binary
 *               for the legal values)
 * @param cas The cas to put in the return packet
 * @param cookie The cookie provided by the frontend
 */
using AddResponseFn = std::function<void(std::string_view key,
                                         std::string_view extras,
                                         std::string_view body,
                                         ValueIsJson json,
                                         cb::mcbp::Status status,
                                         uint64_t cas,
                                         CookieIface& cookie)>;
