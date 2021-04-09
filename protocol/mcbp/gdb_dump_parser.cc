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

#include <mcbp/mcbp.h>

#include <platform/string_hex.h>
#include <algorithm>
#include <vector>

namespace cb::mcbp::gdb {

std::vector<uint8_t> parseDump(cb::const_byte_buffer blob) {
    std::vector<uint8_t> ret;

    const uint8_t* end = blob.data() + blob.size();
    const uint8_t* curr = blob.begin();
    const uint8_t* nl;
    const std::string prefix{"0x"};

    // lets do this simple, and parse line by line:
    while ((nl = std::find(curr, end, '\n')) != end) {
        // if there is an address, strip it...
        auto pos = std::find(curr, nl, ':');
        if (pos != nl) {
            curr = pos + 1;
        }

        while ((pos = std::search(curr, nl, prefix.begin(), prefix.end())) !=
               nl) {
            if (pos + 4 > nl) {
                throw std::invalid_argument(
                        "gdb::parseDump: invalid format on line");
            }
            ret.push_back(uint8_t(
                    cb::from_hex({reinterpret_cast<const char*>(pos) + 2, 2})));
            curr = pos + 1;
        }

        curr = nl + 1;
    }

    return ret;
}

} // namespace cb::mcbp::gdb
