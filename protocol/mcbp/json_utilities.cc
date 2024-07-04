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

namespace cb::mcbp {
void to_json(nlohmann::json& json, const DcpOpenFlag& flags) {
    if (flags == DcpOpenFlag::None) {
        json = "None";
        return;
    }
    auto array = nlohmann::json::array();
    for (int ii = 0; ii < 32; ++ii) {
        auto bit = static_cast<DcpOpenFlag>(uint8_t(1) << ii);
        if ((flags & bit) == bit) {
            switch (bit) {
            case DcpOpenFlag::None:
                break;
            case DcpOpenFlag::Producer:
                array.emplace_back("Producer");
                break;
            case DcpOpenFlag::IncludeXattrs:
                array.emplace_back("IncludeXattrs");
                break;
            case DcpOpenFlag::NoValue:
                array.emplace_back("NoValue");
                break;
            case DcpOpenFlag::IncludeDeleteTimes:
                array.emplace_back("IncludeDeleteTimes");
                break;
            case DcpOpenFlag::NoValueWithUnderlyingDatatype:
                array.emplace_back("NoValueWithUnderlyingDatatype");
                break;
            case DcpOpenFlag::PiTR_Unsupported:
                array.emplace_back("PiTR");
                break;
            case DcpOpenFlag::IncludeDeletedUserXattrs:
                array.emplace_back("IncludeDeletedUserXattrs");
                break;

            case DcpOpenFlag::Unused:
            case DcpOpenFlag::Invalid:
            default:
                array.emplace_back(fmt::format("unknown:{:#x}",
                                               static_cast<unsigned int>(bit)));
            }
        }
    }

    if (array.size() == 1) {
        json = array.front();
    } else {
        json = array;
    }
}

void to_json(nlohmann::json& json, const DcpAddStreamFlag& flags) {
    if (flags == DcpAddStreamFlag::None) {
        json = "None";
        return;
    }
    auto array = nlohmann::json::array();
    for (int ii = 0; ii < 32; ++ii) {
        auto bit = static_cast<DcpAddStreamFlag>(uint8_t(1) << ii);
        if ((flags & bit) == bit) {
            switch (bit) {
            case DcpAddStreamFlag::None:
                break;
            case DcpAddStreamFlag::TakeOver:
                array.push_back("Takeover");
                break;
            case DcpAddStreamFlag::DiskOnly:
                array.push_back("DiskOnly");
                break;
            case DcpAddStreamFlag::ToLatest:
                array.push_back("ToLatest");
                break;
            case DcpAddStreamFlag::NoValue:
                array.push_back("NoValue");
                break;
            case DcpAddStreamFlag::ActiveVbOnly:
                array.push_back("ActiveVbOnly");
                break;
            case DcpAddStreamFlag::StrictVbUuid:
                array.push_back("StrictVbUuid");
                break;
            case DcpAddStreamFlag::FromLatest:
                array.push_back("FromLatest");
                break;
            case DcpAddStreamFlag::IgnorePurgedTombstones:
                array.push_back("IgnorePurgedTombstones");
                break;
            default:
                array.emplace_back(fmt::format("unknown:{:#x}",
                                               static_cast<unsigned int>(bit)));
            }
        }
    }

    if (array.size() == 1) {
        json = array.front();
    } else {
        json = array;
    }
}

} // namespace cb::mcbp

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
    json = {{"status", payload.getStatus()}};
}

void to_json(nlohmann::json& json, const DcpOpenPayload& payload) {
    json = {{"seqno", payload.getSeqno()}, {"flags", payload.getFlags()}};
}

void to_json(nlohmann::json& json, const DcpAddStreamPayload& payload) {
    json = {{"flags", static_cast<DcpAddStreamFlag>(payload.getFlags())}};
}

void to_json(nlohmann::json& json, const DcpStreamReqPayload& payload) {
    json = {
            {"flags", static_cast<DcpAddStreamFlag>(payload.getFlags())},
            {"start_seqno", std::to_string(payload.getStartSeqno())},
            {"end_seqno", std::to_string(payload.getEndSeqno())},
            {"vbucket_uuid", std::to_string(payload.getVbucketUuid())},
            {"snap_start_seqno", std::to_string(payload.getSnapStartSeqno())},
            {"snap_end_seqno", std::to_string(payload.getSnapEndSeqno())},
    };
}

void to_json(nlohmann::json& json, const DcpStreamEndPayload& payload) {
    json = {{"status", payload.getStatus()}};
}

void to_json(nlohmann::json& json, const DcpSnapshotMarkerFlag& flags) {
    if (flags == DcpSnapshotMarkerFlag::None) {
        json = "None";
        return;
    }
    auto array = nlohmann::json::array();
    for (int ii = 0; ii < 32; ++ii) {
        auto bit = static_cast<DcpSnapshotMarkerFlag>(uint8_t(1) << ii);
        if ((flags & bit) == bit) {
            switch (bit) {
            case DcpSnapshotMarkerFlag::None:
                break;
            case DcpSnapshotMarkerFlag::Memory:
                array.emplace_back("Memory");
                break;
            case DcpSnapshotMarkerFlag::Disk:
                array.emplace_back("Disk");
                break;
            case DcpSnapshotMarkerFlag::Checkpoint:
                array.emplace_back("Checkpoint");
                break;
            case DcpSnapshotMarkerFlag::Acknowledge:
                array.emplace_back("Acknowledge");
                break;
            case DcpSnapshotMarkerFlag::History:
                array.emplace_back("History");
                break;
            case DcpSnapshotMarkerFlag::MayContainDuplicates:
                array.emplace_back("MayContainDuplicates");
                break;
            default:
                array.emplace_back(fmt::format("unknown:{:#x}",
                                               static_cast<unsigned int>(bit)));
            }
        }
    }

    if (array.size() == 1) {
        json = array.front();
    } else {
        json = array;
    }
}
void to_json(nlohmann::json& json, const DcpSnapshotMarkerV1Payload& payload) {
    json = {{"start_seqno", std::to_string(payload.getStartSeqno())},
            {"end_seqno", std::to_string(payload.getEndSeqno())},
            {"flags", payload.getFlags()}

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

namespace cb::mcbp::subdoc {
void to_json(nlohmann::json& json, const PathFlag& flag) {
    if (flag == PathFlag::None) {
        json = "None";
        return;
    }
    auto array = nlohmann::json::array();
    for (int ii = 0; ii < 8; ++ii) {
        auto bit = static_cast<PathFlag>(uint8_t(1) << ii);
        if ((flag & bit) == bit) {
            switch (bit) {
            case PathFlag::None:
                break;
            case PathFlag::Mkdir_p:
                array.emplace_back("Mkdir_p");
                break;
            case PathFlag::ExpandMacros:
                array.emplace_back("ExpandMacros");
                break;
            case PathFlag::XattrPath:
                array.emplace_back("XattrPath");
                break;
            case PathFlag::Unused_0x02:
            case PathFlag::Unused_0x08:
            default:
                array.emplace_back(fmt::format("unknown:{:#x}",
                                               static_cast<unsigned int>(bit)));
            }
        }
    }

    if (array.size() == 1) {
        json = array.front();
    } else {
        json = array;
    }
}

void to_json(nlohmann::json& json, const DocFlag& flag) {
    if (flag == DocFlag::None) {
        json = "None";
        return;
    }
    auto array = nlohmann::json::array();
    for (int ii = 0; ii < 8; ++ii) {
        auto bit = static_cast<DocFlag>(uint8_t(1) << ii);
        if ((flag & bit) == bit) {
            switch (bit) {
            case DocFlag::None:
                break;
            case DocFlag::Mkdoc:
                array.emplace_back("Mkdoc");
                break;
            case DocFlag::Add:
                array.emplace_back("Add");
                break;
            case DocFlag::AccessDeleted:
                array.emplace_back("AccessDeleted");
                break;
            case DocFlag::CreateAsDeleted:
                array.emplace_back("CreateAsDeleted");
                break;
            case DocFlag::ReviveDocument:
                array.emplace_back("ReviveDocument");
                break;
            case DocFlag::ReplicaRead:
                array.emplace_back("ReplicaRead");
                break;
            default:
                array.emplace_back(fmt::format("unknown:{:#x}",
                                               static_cast<unsigned int>(bit)));
            }
        }
    }

    if (array.size() == 1) {
        json = array.front();
    } else {
        json = array;
    }
}

} // namespace cb::mcbp::subdoc