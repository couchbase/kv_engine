/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <mcbp/protocol/status.h>
#include <cstdint>
#include <functional>
#include <gsl/gsl>

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
