/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <mcbp/mcbp.h>

#include <platform/string_hex.h>
#include <algorithm>
#include <vector>

namespace cb {
namespace mcbp {
namespace lldb {

std::vector<uint8_t> parseDump(cb::const_byte_buffer blob) {
    std::vector<uint8_t> ret;

    const uint8_t* end = blob.data() + blob.size();
    const uint8_t* curr = blob.begin();
    const uint8_t* nl;

    // lets do this simple, and parse line by line:
    while ((nl = std::find(curr, end, '\n')) != end) {
        // if there is an address, strip it...
        auto pos = std::find(curr, nl, ':');
        if (pos != nl) {
            curr = pos + 1;
        }

        // each number is represended by " NN "
        while (curr + 4 < nl) {
            if (isspace(*curr) && isspace(curr[3])) {
                try {
                    ret.push_back(uint8_t(cb::from_hex(
                            {reinterpret_cast<const char*>(curr) + 1, 2})));
                } catch (const std::logic_error&) {
                    // At the end we might find: '     ....'
                }
            } else {
                break;
            }
            curr += 3;
        }

        curr = nl + 1;
    }

    return ret;
}

} // namespace lldb
} // namespace mcbp
} // namespace cb
