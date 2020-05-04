/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#pragma once

#include <mcbp/protocol/framebuilder.h>

#include <memcached/protocol_binary.h>
#include <nlohmann/json_fwd.hpp>
#include <optional>

namespace cb::mcbp {

class Request;

class DcpSnapshotMarker {
public:
    uint64_t getStartSeqno() const {
        return startSeqno;
    }
    uint64_t getEndSeqno() const {
        return endSeqno;
    }
    uint32_t getFlags() const {
        return flags;
    }
    std::optional<uint64_t> getHighCompletedSeqno() const {
        return highCompletedSeqno;
    }
    std::optional<uint64_t> getMaxVisibleSeqno() const {
        return maxVisibleSeqno;
    }
    std::optional<uint64_t> getTimestamp() const {
        return timestamp;
    }

    void setStartSeqno(uint64_t value) {
        startSeqno = value;
    }
    void setEndSeqno(uint64_t value) {
        endSeqno = value;
    }
    void setFlags(uint32_t value) {
        flags = value;
    }
    void setHighCompletedSeqno(uint64_t value) {
        highCompletedSeqno = value;
    }
    void setMaxVisibleSeqno(uint64_t value) {
        maxVisibleSeqno = value;
    }
    void setTimestamp(uint64_t value) {
        timestamp = value;
    }

    nlohmann::json to_json() const;

protected:
    uint64_t startSeqno;
    uint64_t endSeqno;
    uint32_t flags;
    std::optional<uint64_t> highCompletedSeqno;
    std::optional<uint64_t> maxVisibleSeqno;
    std::optional<uint64_t> timestamp;
};

/**
 * DCP SnapshotMarker may use different encodings on the wire depending
 * what it carries. The first version carries everything in the extras
 * section, whereas the newer versions use a 1 byte field in extras which
 * represents the encoding used for the section in the value field.
 */
DcpSnapshotMarker decodeDcpSnapshotMarker(cb::const_byte_buffer extras,
                                          cb::const_byte_buffer value);

/**
 * Encode into the given frame the DcpSnapshotMarker (either v1 or v2.x)
 */
void encodeDcpSnapshotMarker(cb::mcbp::FrameBuilder<cb::mcbp::Request>& frame,
                             uint64_t start,
                             uint64_t end,
                             uint32_t flags,
                             std::optional<uint64_t> highCompletedSeqno,
                             std::optional<uint64_t> maxVisibleSeqno,
                             std::optional<uint64_t> timestamp);

} // end namespace cb::mcbp
