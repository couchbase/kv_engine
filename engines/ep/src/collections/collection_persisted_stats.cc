/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "collection_persisted_stats.h"

#include <mcbp/protocol/unsigned_leb128.h>

namespace Collections::VB {
PersistedStats::PersistedStats(const char* buf, size_t size) {
    std::pair<uint64_t, cb::const_byte_buffer> decoded = {
            0, {reinterpret_cast<uint8_t*>(const_cast<char*>(buf)), size}};

    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    itemCount = decoded.first;
    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    highSeqno = decoded.first;
    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    diskSize = decoded.first;

    if (!decoded.second.empty()) {
        throw std::runtime_error(
                "PersistedStats:: cid:{} "
                "decoded stats not empty after processing");
    }
}

std::string PersistedStats::getLebEncodedStats() const {
    auto leb = cb::mcbp::unsigned_leb128<uint64_t>(itemCount);
    std::string data(leb.begin(), leb.end());

    leb = cb::mcbp::unsigned_leb128<uint64_t>(highSeqno);
    data.append(leb.begin(), leb.end());

    leb = cb::mcbp::unsigned_leb128<uint64_t>(diskSize);
    data.append(leb.begin(), leb.end());

    return data;
}
} // namespace Collections::VB
