/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <mcbp/protocol/status.h>
#include <stdint.h>
#include <functional>
#include <gsl/gsl>

struct EngineIface;

/**
 * Callback for any function producing stats.
 *
 * @param key the stat's key
 * @param klen length of the key
 * @param val the stat's value in an ascii form (e.g. text form of a number)
 * @param vlen length of the value
 * @param cookie magic callback cookie
 */
using AddStatFn = std::function<void(const char* key,
                                     const uint16_t klen,
                                     const char* val,
                                     const uint32_t vlen,
                                     gsl::not_null<const void*> cookie)>;

/**
 * Callback for adding a response backet
 * @param key The key to put in the response
 * @param keylen The length of the key
 * @param ext The data to put in the extended field in the response
 * @param extlen The number of bytes in the ext field
 * @param body The data body
 * @param bodylen The number of bytes in the body
 * @param datatype This is currently not used and should be set to 0
 * @param status The status code of the return packet (see in protocol_binary
 *               for the legal values)
 * @param cas The cas to put in the return packet
 * @param cookie The cookie provided by the frontend
 * @return true if return message was successfully created, false if an
 *              error occured that prevented the message from being sent
 */
using AddResponseFn = std::function<bool(const void* key,
                                         uint16_t keylen,
                                         const void* ext,
                                         uint8_t extlen,
                                         const void* body,
                                         uint32_t bodylen,
                                         uint8_t datatype,
                                         cb::mcbp::Status status,
                                         uint64_t cas,
                                         const void* cookie)>;
