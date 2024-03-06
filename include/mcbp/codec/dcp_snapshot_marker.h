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
    DcpSnapshotMarker(uint64_t start_seqno,
                      uint64_t end_seqno,
                      request::DcpSnapshotMarkerFlag flags)
        : startSeqno(start_seqno), endSeqno(end_seqno), flags(flags) {
    }
    DcpSnapshotMarker(uint64_t start_seqno,
                      uint64_t end_seqno,
                      request::DcpSnapshotMarkerFlag flags,
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
    [[nodiscard]] auto getFlags() const {
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
    void setFlags(request::DcpSnapshotMarkerFlag value) {
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
    request::DcpSnapshotMarkerFlag flags = request::DcpSnapshotMarkerFlag::None;
    std::optional<uint64_t> highCompletedSeqno;
    std::optional<uint64_t> maxVisibleSeqno;
    std::optional<uint64_t> timestamp;
};

/// Get a JSON dump of the DcpSnapshotMarker
void to_json(nlohmann::json& json, const DcpSnapshotMarker& marker);

} // end namespace cb::mcbp
