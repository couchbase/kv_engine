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

#include "dcp_snapshot_marker_codec.h"
namespace cb::mcbp {

DcpSnapshotMarker decodeDcpSnapshotMarkerV1Extra(cb::const_byte_buffer extras) {
    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    const auto* payload =
            reinterpret_cast<const DcpSnapshotMarkerV1Payload*>(extras.data());
    DcpSnapshotMarker marker;
    marker.setStartSeqno(payload->getStartSeqno());
    marker.setEndSeqno(payload->getEndSeqno());
    marker.setFlags(payload->getFlags());
    return marker;
}

DcpSnapshotMarker decodeDcpSnapshotMarkerV2xValue(cb::const_byte_buffer value) {
    using cb::mcbp::request::DcpSnapshotMarkerFlag;
    using cb::mcbp::request::DcpSnapshotMarkerV2_0Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2xVersion;

    DcpSnapshotMarker marker;

    const auto* payload2_0 =
            reinterpret_cast<const DcpSnapshotMarkerV2_0Value*>(value.data());
    marker.setStartSeqno(payload2_0->getStartSeqno());
    marker.setEndSeqno(payload2_0->getEndSeqno());
    marker.setFlags(payload2_0->getFlags());

    // MaxVisible is sent in all V2.0 snapshot markers
    marker.setMaxVisibleSeqno(payload2_0->getMaxVisibleSeqno());

    // HighCompletedSeqno is always present in V2.0 but should only be accessed
    // when the flags have disk set.
    if (marker.getFlags() & uint32_t(DcpSnapshotMarkerFlag::Disk)) {
        marker.setHighCompletedSeqno(payload2_0->getHighCompletedSeqno());
    }
    return marker;
}

void encodeDcpSnapshotMarker(cb::mcbp::FrameBuilder<cb::mcbp::Request>& frame,
                             uint64_t start,
                             uint64_t end,
                             uint32_t flags,
                             std::optional<uint64_t> highCompletedSeqno,
                             std::optional<uint64_t> maxVisibleSeqno) {
    using cb::mcbp::request::DcpSnapshotMarkerFlag;
    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    using cb::mcbp::request::DcpSnapshotMarkerV2_0Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2xPayload;
    using cb::mcbp::request::DcpSnapshotMarkerV2xVersion;

    if (highCompletedSeqno) {
        Expects(flags & uint32_t(DcpSnapshotMarkerFlag::Disk));
    }
    if (maxVisibleSeqno || highCompletedSeqno) {
        // V2.0: sending the maxVisibleSeqno and maybe the highCompletedSeqno.
        // The highCompletedSeqno is expected to only be defined when flags has
        // the disk bit set.

        // @todo MB-36948: Expect(maxVisibleSeqno) if we're in this scope,
        // maxVisibleSeqno is expected to always be defined once the full DCP
        // producer path is passing it down.
        DcpSnapshotMarkerV2xPayload extras(DcpSnapshotMarkerV2xVersion::Zero);
        DcpSnapshotMarkerV2_0Value value;
        value.setStartSeqno(start);
        value.setEndSeqno(end);
        value.setFlags(flags);
        // @todo: MB-36948: This should change to use get()
        value.setMaxVisibleSeqno(maxVisibleSeqno.value_or(0));
        value.setHighCompletedSeqno(highCompletedSeqno.value_or(0));
        frame.setExtras(extras.getBuffer());
        frame.setValue(value.getBuffer());
    } else {
        DcpSnapshotMarkerV1Payload payload;
        payload.setStartSeqno(start);
        payload.setEndSeqno(end);
        payload.setFlags(flags);
        frame.setExtras(payload.getBuffer());
    }
}

} // namespace cb::mcbp
