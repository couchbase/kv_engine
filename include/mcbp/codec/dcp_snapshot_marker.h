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

/**
 * The DcpSnapshotMarker is a class intended to be used "in memory" and does
 * not represent the "on the wire format" like all the payload classes in
 * cb::mcbp::request::payload so it cannot be created by casting from the
 * extras/value in a request, but must be created using the decode method.
 *
 * The motivation for the class is to hide the various "on the wire" encodings
 * of the DcpSnapshotMarker (as it have fields which may not be present)
 */
class DcpSnapshotMarker {
public:
    DcpSnapshotMarker() = default;
    DcpSnapshotMarker(uint64_t start_seqno, uint64_t end_seqno, uint32_t flags)
        : startSeqno(start_seqno), endSeqno(end_seqno), flags(flags) {
    }
    DcpSnapshotMarker(uint64_t start_seqno,
                      uint64_t end_seqno,
                      uint32_t flags,
                      std::optional<uint64_t> hcs,
                      std::optional<uint64_t> mvs,
                      std::optional<uint64_t> timestamp)
        : startSeqno(start_seqno),
          endSeqno(end_seqno),
          flags(flags),
          highCompletedSeqno(std::move(hcs)),
          maxVisibleSeqno(std::move(mvs)),
          timestamp(std::move(timestamp)) {
    }

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

    /**
     * Create an instance by decoding the Request
     *
     * @param request the request containing the DcpSnapshotMarker
     * @return A new DcpSnapshotMarker instance
     * @throws std::runtime_error if the request contains an invalid /
     *         unsupported encoding
     */
    static DcpSnapshotMarker decode(const Request& request);

    /**
     * Encode the DcpSnapshotMarker into a frame to send on the wire
     *
     * @param frame the builder to send the data
     */
    void encode(cb::mcbp::FrameBuilder<cb::mcbp::Request>& frame) const;

protected:
    uint64_t startSeqno = 0;
    uint64_t endSeqno = 0;
    uint32_t flags = 0;
    std::optional<uint64_t> highCompletedSeqno;
    std::optional<uint64_t> maxVisibleSeqno;
    std::optional<uint64_t> timestamp;
};

} // end namespace cb::mcbp
