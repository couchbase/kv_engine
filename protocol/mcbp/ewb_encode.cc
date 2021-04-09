/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Helper functions to encode EWouldBlockEngine specific packets.
 */

#include "ewb_encode.h"

#include <platform/socket.h>
#include <array>

std::string ewb::encodeSequence(const std::vector<cb::engine_errc>& sequence) {
    // Encode vector to network-endian.
    std::string encoded;
    union Swap {
        uint32_t raw;
        std::array<uint8_t, 4> array;
    };
    for (const auto& err : sequence) {
        Swap s;
        s.raw = static_cast<uint32_t>(err);
        s.raw = htonl(s.raw);
        for (const auto& byte : s.array) {
            encoded.push_back(byte);
        }
    }
    return encoded;
}
