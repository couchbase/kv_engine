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

#include <boost/optional/optional_fwd.hpp>
#include <memcached/protocol_binary.h>
namespace cb {
namespace mcbp {

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
    boost::optional<uint64_t> getHighCompletedSeqno() const {
        return highCompletedSeqno;
    }
    boost::optional<uint64_t> getMaxVisibleSeqno() const {
        return maxVisibleSeqno;
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

protected:
    uint64_t startSeqno;
    uint64_t endSeqno;
    uint32_t flags;
    boost::optional<uint64_t> highCompletedSeqno;
    boost::optional<uint64_t> maxVisibleSeqno;
};

/**
 * Decode byte_buffer representing the extras for a V1 snapshot marker
 */
DcpSnapshotMarker decodeDcpSnapshotMarkerV1Extra(cb::const_byte_buffer extras);

/**
 * Decode byte_buffer representing the value for a V2 snapshot marker
 */
DcpSnapshotMarker decodeDcpSnapshotMarkerV2xValue(cb::const_byte_buffer value);

/**
 * Encode into the given frame the DcpSnapshotMarker (either v1 or v2.0)
 */
void encodeDcpSnapshotMarker(cb::mcbp::FrameBuilder<cb::mcbp::Request>& frame,
                             uint64_t start,
                             uint64_t end,
                             uint32_t flags,
                             boost::optional<uint64_t> highCompletedSeqno,
                             boost::optional<uint64_t> maxVisibleSeqno);

} // end namespace mcbp
} // end namespace cb
