/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cstdint>
#include <string>

namespace cb::mcbp {
/**
 * Definition of the legal "magic" values used in a packet.
 * See section 3.1 Magic byte
 */
enum class Magic : uint8_t {
    /// Request packet from client to server
    ClientRequest = 0x80,
    /// The alternative request packet containing frame extras
    AltClientRequest = 0x08,
    /// Response packet from server to client
    ClientResponse = 0x81,
    /// The alternative response packet containing frame extras
    AltClientResponse = 0x18,
    /// Request packet from server to client
    ServerRequest = 0x82,
    /// Response packet from client to server
    ServerResponse = 0x83
};

/**
 * Test to see if the provided magic value is a legal value or not
 * (in the case it is generated from a uint8_t received over the network).
 */
bool is_legal(Magic magic);

/**
 * Test to check if the magic represents one of the available request
 * magic's
 *
 * @throws std::invalid_argument for invalid magic
 */
bool is_request(Magic magic);

/**
 * Test to check if the magic represents one of the available response
 * magic's
 *
 * @throws std::invalid_argument for invalid magic
 */
inline bool is_response(Magic magic) {
    return !is_request(magic);
}

/**
 * Return true if the magic represents one of the client magics.
 *
 * @throws std::invalid_argument for invalid magic
 */
bool is_client_magic(Magic magic);

/**
 * Return true if the magic represents one of the server magics.
 *
 * @throws std::invalid_argument for invalid magic
 */
inline bool is_server_magic(Magic magic) {
    return !is_client_magic(magic);
}

/**
 * Is the packet using the alternative packet encoding form
 *
 * @throws std::invalid_argument for invalid magic
 */
bool is_alternative_encoding(Magic magic);

} // namespace cb::mcbp

std::string to_string(cb::mcbp::Magic magic);
