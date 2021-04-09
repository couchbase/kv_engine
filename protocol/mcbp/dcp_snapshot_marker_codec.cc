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

#include <mcbp/codec/dcp_snapshot_marker.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>

namespace cb::mcbp {

nlohmann::json DcpSnapshotMarker::to_json() const {
    nlohmann::json ret;
    ret["start"] = startSeqno;
    ret["end"] = endSeqno;
    ret["flags"] = cb::to_hex(flags);
    if (highCompletedSeqno) {
        ret["high_completed_seqno"] = *highCompletedSeqno;
    }
    if (maxVisibleSeqno) {
        ret["max_visible_seqno"] = *maxVisibleSeqno;
    }
    if (timestamp) {
        ret["timestamp"] = *timestamp;
    }
    return ret;
}

DcpSnapshotMarker decodeDcpSnapshotMarkerV1Extra(cb::const_byte_buffer extras) {
    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    const auto* payload =
            reinterpret_cast<const DcpSnapshotMarkerV1Payload*>(extras.data());
    return DcpSnapshotMarker(payload->getStartSeqno(),
                             payload->getEndSeqno(),
                             payload->getFlags());
}

static DcpSnapshotMarker decodeDcpSnapshotMarkerV20Value(
        cb::const_byte_buffer value) {
    using cb::mcbp::request::DcpSnapshotMarkerFlag;
    using cb::mcbp::request::DcpSnapshotMarkerV2_0Value;

    if (value.size() != sizeof(DcpSnapshotMarkerV2_0Value)) {
        throw std::runtime_error(
                "decodeDcpSnapshotMarkerV21Value: Invalid size");
    }

    const auto* payload2_0 =
            reinterpret_cast<const DcpSnapshotMarkerV2_0Value*>(value.data());
    DcpSnapshotMarker marker(payload2_0->getStartSeqno(),
                             payload2_0->getEndSeqno(),
                             payload2_0->getFlags());

    // MaxVisible is sent in all V2.0 snapshot markers
    marker.setMaxVisibleSeqno(payload2_0->getMaxVisibleSeqno());

    // HighCompletedSeqno is always present in V2.0 but should only be accessed
    // when the flags have disk set.
    if (marker.getFlags() & uint32_t(DcpSnapshotMarkerFlag::Disk)) {
        marker.setHighCompletedSeqno(payload2_0->getHighCompletedSeqno());
    }
    return marker;
}

static DcpSnapshotMarker decodeDcpSnapshotMarkerV21Value(
        cb::const_byte_buffer value) {
    using cb::mcbp::request::DcpSnapshotMarkerV2_1Value;

    if (value.size() != sizeof(DcpSnapshotMarkerV2_1Value)) {
        throw std::runtime_error(
                "decodeDcpSnapshotMarkerV21Value: Invalid size");
    }

    // V2.1 is an extension to 2.0 by adding a timestamp.. use the 2.0 decode
    // method
    auto base = cb::const_byte_buffer{value.data(),
                                      value.size() - sizeof(uint64_t)};
    DcpSnapshotMarker marker = decodeDcpSnapshotMarkerV20Value(base);
    const auto* payload =
            reinterpret_cast<const DcpSnapshotMarkerV2_1Value*>(value.data());
    marker.setTimestamp(payload->getTimestamp());
    return marker;
}

DcpSnapshotMarker DcpSnapshotMarker::decode(const Request& request) {
    if (request.getClientOpcode() != ClientOpcode::DcpSnapshotMarker) {
        throw std::runtime_error(
                "DcpSnapshotMarker::decode: request is not a "
                "DcpSnapshotMarker");
    }
    auto extras = request.getExtdata();
    using cb::mcbp::request::DcpSnapshotMarkerV2xVersion;
    if (extras.size() == sizeof(DcpSnapshotMarkerV2xVersion)) {
        switch (extras[0]) {
        case 0:
            return decodeDcpSnapshotMarkerV20Value(request.getValue());
        case 1:
            return decodeDcpSnapshotMarkerV21Value(request.getValue());
        }
        throw std::runtime_error(
                "DcpSnapshotMarker::decode: Unknown snapshot marker version");
    }
    if (extras.size() ==
        sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload)) {
        return decodeDcpSnapshotMarkerV1Extra(extras);
    }

    throw std::runtime_error(
            "DcpSnapshotMarker::decode: Invalid extras encoding");
}

void DcpSnapshotMarker::encode(
        cb::mcbp::FrameBuilder<cb::mcbp::Request>& frame) const {
    using cb::mcbp::request::DcpSnapshotMarkerFlag;
    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    using cb::mcbp::request::DcpSnapshotMarkerV2_0Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2_1Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2xPayload;
    using cb::mcbp::request::DcpSnapshotMarkerV2xVersion;

    if (highCompletedSeqno || timestamp) {
        Expects(flags & uint32_t(DcpSnapshotMarkerFlag::Disk));
    }

    if (timestamp) {
        DcpSnapshotMarkerV2xPayload extras(DcpSnapshotMarkerV2xVersion::One);
        frame.setExtras(extras.getBuffer());

        DcpSnapshotMarkerV2_1Value value;
        value.setStartSeqno(startSeqno);
        value.setEndSeqno(endSeqno);
        value.setFlags(flags);
        value.setMaxVisibleSeqno(maxVisibleSeqno.value_or(0));
        value.setHighCompletedSeqno(highCompletedSeqno.value_or(0));
        value.setTimestamp(*timestamp);
        frame.setValue(value.getBuffer());
    } else if (maxVisibleSeqno || highCompletedSeqno) {
        // V2.0: sending the maxVisibleSeqno and maybe the highCompletedSeqno.
        // The highCompletedSeqno is expected to only be defined when flags has
        // the disk bit set.

        // @todo MB-36948: Expect(maxVisibleSeqno) if we're in this scope,
        // maxVisibleSeqno is expected to always be defined once the full DCP
        // producer path is passing it down.
        DcpSnapshotMarkerV2xPayload extras(DcpSnapshotMarkerV2xVersion::Zero);
        DcpSnapshotMarkerV2_0Value value;
        value.setStartSeqno(startSeqno);
        value.setEndSeqno(endSeqno);
        value.setFlags(flags);
        // @todo: MB-36948: This should change to use get()
        value.setMaxVisibleSeqno(maxVisibleSeqno.value_or(0));
        value.setHighCompletedSeqno(highCompletedSeqno.value_or(0));
        frame.setExtras(extras.getBuffer());
        frame.setValue(value.getBuffer());
    } else {
        DcpSnapshotMarkerV1Payload payload;
        payload.setStartSeqno(startSeqno);
        payload.setEndSeqno(endSeqno);
        payload.setFlags(flags);
        frame.setExtras(payload.getBuffer());
    }
}

} // namespace cb::mcbp
