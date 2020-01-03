/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
