/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collection_persisted_stats.h"

#include <mcbp/protocol/unsigned_leb128.h>
#include <ostream>

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
                "PersistedStats::ctor2 decoded buffer not empty after "
                "processing");
    }
}

PersistedStats::PersistedStats(const char* buf,
                               size_t size,
                               size_t dbSpaceUsed) {
    std::pair<uint64_t, cb::const_byte_buffer> decoded = {
            0, {reinterpret_cast<uint8_t*>(const_cast<char*>(buf)), size}};

    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    itemCount = decoded.first;
    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    highSeqno = decoded.first;

    if (decoded.second.size()) {
        decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
        diskSize = decoded.first;
    } else {
        diskSize = dbSpaceUsed;
    }

    if (!decoded.second.empty()) {
        throw std::runtime_error(
                "PersistedStats::ctor1 decoded buffer not empty after "
                "processing");
    }
}

std::string PersistedStats::getLebEncodedStatsMadHatter() const {
    auto leb = cb::mcbp::unsigned_leb128<uint64_t>(itemCount);
    std::string data(leb.begin(), leb.end());

    leb = cb::mcbp::unsigned_leb128<uint64_t>(highSeqno);
    data.append(leb.begin(), leb.end());

    return data;
}

std::string PersistedStats::getLebEncodedStats() const {
    auto data = getLebEncodedStatsMadHatter();
    auto leb = cb::mcbp::unsigned_leb128<uint64_t>(diskSize);
    data.append(leb.begin(), leb.end());
    return data;
}

std::ostream& operator<<(std::ostream& os, const PersistedStats& ps) {
    os << "{itemCount:" << ps.itemCount << " highSeqno:" << ps.highSeqno
       << " diskSize:" << ps.diskSize << "}";
    return os;
}

} // namespace Collections::VB
