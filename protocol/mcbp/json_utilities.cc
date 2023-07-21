/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/protocol/json_utilities.h>
#include <platform/uuid.h>

namespace cb::mcbp::request {

void to_json(nlohmann::json& json, const MutationPayload& payload) {
    json = {{"flags", cb::to_hex(payload.getFlagsInNetworkByteOrder())},
            {"expiration", payload.getExpiration()}};
}

void to_json(nlohmann::json& json, const ArithmeticPayload& payload) {
    json = {{"delta", std::to_string(payload.getDelta())},
            {"initial", std::to_string(payload.getInitial())},
            {"expiration", payload.getExpiration()}};
}

void to_json(nlohmann::json& json, const SetClusterConfigPayload& payload) {
    json = {{"epoch", std::to_string(payload.getEpoch())},
            {"revision", std::to_string(payload.getRevision())}};
}
void to_json(nlohmann::json& json, const VerbosityPayload& payload) {
    json = {"level", payload.getLevel()};
}

void to_json(nlohmann::json& json, const TouchPayload& payload) {
    json = {{"expiration", payload.getExpiration()}};
}

void to_json(nlohmann::json& json, const SetCtrlTokenPayload& payload) {
    json = {{"cas", cb::to_hex(payload.getCas())}};
}

void to_json(nlohmann::json& json,
             const SetBucketDataLimitExceededPayload& payload) {
    json = {{"status", ::to_string(payload.getStatus())}};
}

void to_json(nlohmann::json& json, const DcpOpenPayload& payload) {
    auto flags = payload.getFlags();
    auto array = nlohmann::json::array();
    array.push_back("raw: " + cb::to_hex(flags));
    if ((flags & DcpOpenPayload::Producer) == DcpOpenPayload::Producer) {
        array.push_back("Producer");
    }

    if ((flags & DcpOpenPayload::IncludeXattrs) ==
        DcpOpenPayload::IncludeXattrs) {
        array.push_back("IncludeXattrs");
    }

    if ((flags & DcpOpenPayload::NoValue) == DcpOpenPayload::NoValue) {
        array.push_back("NoValue");
    }
    if ((flags & DcpOpenPayload::IncludeDeleteTimes) ==
        DcpOpenPayload::IncludeDeleteTimes) {
        array.push_back("IncludeDeleteTimes");
    }
    if ((flags & DcpOpenPayload::NoValueWithUnderlyingDatatype) ==
        DcpOpenPayload::NoValueWithUnderlyingDatatype) {
        array.push_back("NoValueWithUnderlyingDatatype");
    }
    if ((flags & DcpOpenPayload::PiTR) == DcpOpenPayload::PiTR) {
        array.push_back("PiTR");
    }
    if ((flags & DcpOpenPayload::IncludeDeletedUserXattrs) ==
        DcpOpenPayload::IncludeDeletedUserXattrs) {
        array.push_back("IncludeDeletedUserXattrs");
    }

    json = {{"seqno", payload.getSeqno()}, {"flags", std::move(array)}};
}

static nlohmann::json decodeAddStreamFlags(uint32_t flags) {
    auto array = nlohmann::json::array();
    array.push_back("raw: " + cb::to_hex(flags));
    if ((flags & DCP_ADD_STREAM_FLAG_TAKEOVER) ==
        DCP_ADD_STREAM_FLAG_TAKEOVER) {
        array.push_back("Takeover");
    }

    if ((flags & DCP_ADD_STREAM_FLAG_DISKONLY) ==
        DCP_ADD_STREAM_FLAG_DISKONLY) {
        array.push_back("Diskonly");
    }
    if ((flags & DCP_ADD_STREAM_FLAG_TO_LATEST) ==
        DCP_ADD_STREAM_FLAG_TO_LATEST) {
        array.push_back("ToLatest");
    }
    if ((flags & DCP_ADD_STREAM_FLAG_NO_VALUE) ==
        DCP_ADD_STREAM_FLAG_NO_VALUE) {
        array.push_back("NoValue");
    }
    if ((flags & DCP_ADD_STREAM_ACTIVE_VB_ONLY) ==
        DCP_ADD_STREAM_ACTIVE_VB_ONLY) {
        array.push_back("ActiveVbOnly");
    }

    if ((flags & DCP_ADD_STREAM_ACTIVE_VB_ONLY) ==
        DCP_ADD_STREAM_ACTIVE_VB_ONLY) {
        array.push_back("ActiveVbOnly");
    }
    if ((flags & DCP_ADD_STREAM_STRICT_VBUUID) ==
        DCP_ADD_STREAM_STRICT_VBUUID) {
        array.push_back("StrictVbUuid");
    }
    if ((flags & DCP_ADD_STREAM_FLAG_FROM_LATEST) ==
        DCP_ADD_STREAM_FLAG_FROM_LATEST) {
        array.push_back("FromLatest");
    }

    if ((flags & DCP_ADD_STREAM_FLAG_IGNORE_PURGED_TOMBSTONES) ==
        DCP_ADD_STREAM_FLAG_IGNORE_PURGED_TOMBSTONES) {
        array.push_back("IgnorePurgedTombstones");
    }
    return array;
}

void to_json(nlohmann::json& json, const DcpAddStreamPayload& payload) {
    json = {{"flags", decodeAddStreamFlags(payload.getFlags())}};
}

void to_json(nlohmann::json& json, const DcpStreamReqPayload& payload) {
    json = {
            {"flags", decodeAddStreamFlags(payload.getFlags())},
            {"start_seqno", std::to_string(payload.getStartSeqno())},
            {"end_seqno", std::to_string(payload.getEndSeqno())},
            {"vbucket_uuid", std::to_string(payload.getVbucketUuid())},
            {"snap_start_seqno", std::to_string(payload.getSnapStartSeqno())},
            {"snap_end_seqno", std::to_string(payload.getSnapEndSeqno())},
    };
}

void to_json(nlohmann::json& json, const DcpStreamEndPayload& payload) {
    json = {{"status", to_string(payload.getStatus())}};
}

nlohmann::json decodeSnapshotFlags(uint32_t flags) {
    auto array = nlohmann::json::array();
    array.push_back("raw: " + cb::to_hex(flags));
    if ((flags & uint32_t(DcpSnapshotMarkerFlag::Memory)) ==
        uint32_t(DcpSnapshotMarkerFlag::Memory)) {
        array.push_back("Memory");
    }
    if ((flags & uint32_t(DcpSnapshotMarkerFlag::Disk)) ==
        uint32_t(DcpSnapshotMarkerFlag::Disk)) {
        array.push_back("Disk");
    }
    if ((flags & uint32_t(DcpSnapshotMarkerFlag::Checkpoint)) ==
        uint32_t(DcpSnapshotMarkerFlag::Checkpoint)) {
        array.push_back("Checkpoint");
    }
    if ((flags & uint32_t(DcpSnapshotMarkerFlag::Acknowledge)) ==
        uint32_t(DcpSnapshotMarkerFlag::Acknowledge)) {
        array.push_back("Acknowledge");
    }
    if ((flags & uint32_t(DcpSnapshotMarkerFlag::History)) ==
        uint32_t(DcpSnapshotMarkerFlag::History)) {
        array.push_back("History");
    }
    if ((flags & uint32_t(DcpSnapshotMarkerFlag::MayContainDuplicates)) ==
        uint32_t(DcpSnapshotMarkerFlag::MayContainDuplicates)) {
        array.push_back("MayContainDuplicates");
    }
    return array;
}

void to_json(nlohmann::json& json, const DcpSnapshotMarkerV1Payload& payload) {
    json = {{"start_seqno", std::to_string(payload.getStartSeqno())},
            {"end_seqno", std::to_string(payload.getEndSeqno())},
            {"flags", decodeSnapshotFlags(payload.getFlags())}

    };
}

void to_json(nlohmann::json& json, const DcpSnapshotMarkerV2xPayload& payload) {
    json = {{"version", int(payload.getVersion())}};
}
void to_json(nlohmann::json& json, const DcpSnapshotMarkerV2_0Value& payload) {
    to_json(json, dynamic_cast<const DcpSnapshotMarkerV1Payload&>(payload));
    json["max_visible_seqno"] = std::to_string(payload.getMaxVisibleSeqno());
    json["high_completed_seqno"] =
            std::to_string(payload.getHighCompletedSeqno());
}

void to_json(nlohmann::json& json, const DcpSnapshotMarkerV2_1Value& payload) {
    to_json(json, dynamic_cast<const DcpSnapshotMarkerV2_0Value&>(payload));
    json["timestamp"] = std::to_string(payload.getTimestamp());
}

void to_json(nlohmann::json& json, const DcpMutationPayload& payload) {
    json = {
            {"by_seqno", std::to_string(payload.getBySeqno())},
            {"rev_seqno", std::to_string(payload.getRevSeqno())},
            {"flags", cb::to_hex(payload.getFlags())},
            {"expiration", payload.getExpiration()},
            {"lock_time", payload.getLockTime()},
    };
}

void to_json(nlohmann::json& json, const DcpDeletionV1Payload& payload) {
    json = {
            {"by_seqno", std::to_string(payload.getBySeqno())},
            {"rev_seqno", std::to_string(payload.getRevSeqno())},
            {"nmeta", payload.getNmeta()},
    };
}

void to_json(nlohmann::json& json, const DcpDeletionV2Payload& payload) {
    json = {
            {"by_seqno", std::to_string(payload.getBySeqno())},
            {"rev_seqno", std::to_string(payload.getRevSeqno())},
            {"delete_time", payload.getDeleteTime()},
    };
}

void to_json(nlohmann::json& json, const DcpExpirationPayload& payload) {
    json = {
            {"by_seqno", std::to_string(payload.getBySeqno())},
            {"rev_seqno", std::to_string(payload.getRevSeqno())},
            {"delete_time", payload.getDeleteTime()},
    };
}

void to_json(nlohmann::json& json, const DcpSetVBucketState& payload) {
    json = {{"state", int(payload.getState())}};
}

void to_json(nlohmann::json& json, const DcpBufferAckPayload& payload) {
    json = {{"buffer_bytes", payload.getBufferBytes()}};
}

void to_json(nlohmann::json& json, const DcpOsoSnapshotPayload& payload) {
    auto array = nlohmann::json::array();
    array.push_back("raw: " + cb::to_hex(payload.getFlags()));
    if ((payload.getFlags() & uint32_t(DcpOsoSnapshotFlags::Start)) ==
        uint32_t(DcpOsoSnapshotFlags::Start)) {
        array.push_back("Start");
    }
    if ((payload.getFlags() & uint32_t(DcpOsoSnapshotFlags::End)) ==
        uint32_t(DcpOsoSnapshotFlags::End)) {
        array.push_back("End");
    }
    json = {"flags", std::move(array)};
}

void to_json(nlohmann::json& json, const DcpSeqnoAdvancedPayload& payload) {
    json = {{"seqno", std::to_string(payload.getSeqno())}};
}

void to_json(nlohmann::json& json, const DcpSystemEventPayload& payload) {
    // @todo decode the event
    json = {{"by_seqno", std::to_string(payload.getBySeqno())},
            {"event", payload.getEvent()},
            {"version", int(payload.getVersion())}};
}

void to_json(nlohmann::json& json, const DcpPreparePayload& payload) {
    json = {
            {"by_seqno", std::to_string(payload.getBySeqno())},
            {"rev_seqno", std::to_string(payload.getRevSeqno())},
            {"flags", cb::to_hex(payload.getFlags())},
            {"expiration", payload.getExpiration()},
            {"lock_time", payload.getLockTime()},
            {"nru", int(payload.getNru())},
            {"deleted", bool(payload.getDeleted())},
            {"durability_level", int(payload.getDurabilityLevel())},
    };
}

void to_json(nlohmann::json& json, const DcpSeqnoAcknowledgedPayload& payload) {
    json = {{"prepared_seqno", std::to_string(payload.getPreparedSeqno())}};
}
void to_json(nlohmann::json& json, const DcpCommitPayload& payload) {
    json = {{"prepared_seqno", std::to_string(payload.getPreparedSeqno())},
            {"commit_seqno", std::to_string(payload.getCommitSeqno())}};
}
void to_json(nlohmann::json& json, const DcpAbortPayload& payload) {
    json = {{"prepared_seqno", std::to_string(payload.getPreparedSeqno())},
            {"abort_seqno", std::to_string(payload.getAbortSeqno())}};
}
void to_json(nlohmann::json& json, const SetParamPayload& payload) {
    // @todo decode
    json = {{"param_type", static_cast<uint32_t>(payload.getParamType())}};
}

void to_json(nlohmann::json& json, const SetWithMetaPayload& payload) {
    json = {{"flags", cb::to_hex(payload.getFlagsInNetworkByteOrder())},
            {"expiration", payload.getExpiration()},
            {"seqno", std::to_string(payload.getSeqno())},
            {"cas", std::to_string(payload.getCas())}};
}

void to_json(nlohmann::json& json, const DelWithMetaPayload& payload) {
    json = {{"flags", cb::to_hex(payload.getFlagsInNetworkByteOrder())},
            {"delete_time", payload.getDeleteTime()},
            {"seqno", std::to_string(payload.getSeqno())},
            {"cas", std::to_string(payload.getCas())}};
}

void to_json(nlohmann::json& json, const ReturnMetaPayload& payload) {
    // @todo decode mutation type
    json = {{"mutation_type", static_cast<uint32_t>(payload.getMutationType())},
            {"flags", cb::to_hex(payload.getFlags())},
            {"expiration", payload.getExpiration()}};
}

void to_json(nlohmann::json& json, const CompactDbPayload& payload) {
    json = {
            {"purge_before_ts", std::to_string(payload.getPurgeBeforeTs())},
            {"purge_before_seq", std::to_string(payload.getPurgeBeforeSeq())},
            {"drop_deletes", bool(payload.getDropDeletes())},
            {"db_file_id", payload.getDbFileId().to_string()},
    };
}

void to_json(nlohmann::json& json, const GetErrmapPayload& payload) {
    json = {{"version", payload.getVersion()}};
}

void to_json(nlohmann::json& json, const GetCollectionIDPayload& payload) {
    json = {{"manifest_id", std::to_string(payload.getManifestId())},
            {"collection_id", payload.getCollectionId().to_string()}};
}

void to_json(nlohmann::json& json, const GetScopeIDPayload& payload) {
    json = {{"manifest_id", std::to_string(payload.getManifestId())},
            {"scope_id", payload.getScopeId().to_string()}};
}

void to_json(nlohmann::json& json, const GetRandomKeyPayload& payload) {
    json = {{"collection_id", payload.getCollectionId().to_string()}};
}

void to_json(nlohmann::json& json, const RangeScanContinuePayload& payload) {
    json = {{"id", ::to_string(payload.getId())},
            {"item_limit", payload.getItemLimit()},
            {"time_limit", payload.getTimeLimit()},
            {"byte_limit", payload.getByteLimit()}};
}

} // namespace cb::mcbp::request

namespace cb::mcbp::response {
void to_json(nlohmann::json& json,
             const RangeScanContinueMetaResponse& payload) {
    json = {{"flags", cb::to_hex(payload.getFlags())},
            {"expiry", payload.getExpiry()},
            {"seqno", std::to_string(payload.getSeqno())},
            {"cas", std::to_string(payload.getCas())},
            {"datatype", std::to_string(int(payload.getDatatype()))}};
}

void to_json(nlohmann::json& json, const DcpAddStreamPayload& payload) {
    json = {{"opaque", payload.getOpaque()}};
}

} // namespace cb::mcbp::response
