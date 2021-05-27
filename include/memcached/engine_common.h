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

#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/status.h>
#include <cstdint>
#include <functional>

struct EngineIface;

/**
 * Callback for any function producing stats.
 *
 * @param key the stat's key
 * @param value the stat's value in an ascii form (e.g. text form of a number)
 * @param cookie magic callback cookie
 */
using AddStatFn = std::function<void(std::string_view key,
                                     std::string_view value,
                                     gsl::not_null<const void*> cookie)>;

/**
 * Callback for adding a response backet
 * @param key The key to put in the response
 * @param extras The data to put in the extended field in the response
 * @param body The data body
 * @param datatype This is currently not used and should be set to 0
 * @param status The status code of the return packet (see in protocol_binary
 *               for the legal values)
 * @param cas The cas to put in the return packet
 * @param cookie The cookie provided by the frontend
 * @return true if return message was successfully created, false if an
 *              error occured that prevented the message from being sent
 */
using AddResponseFn = std::function<bool(std::string_view key,
                                         std::string_view extras,
                                         std::string_view body,
                                         uint8_t datatype,
                                         cb::mcbp::Status status,
                                         uint64_t cas,
                                         const void* cookie)>;
