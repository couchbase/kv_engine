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
#include <mcbp/protocol/json_utilities.h>
#include <mcbp/protocol/request.h>
#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <platform/uuid.h>
#include <utilities/logtags.h>
#include <cctype>

namespace cb::mcbp {

void Request::setKeylen(uint16_t value) {
    if (is_alternative_encoding(getMagic())) {
        reinterpret_cast<uint8_t*>(this)[3] = gsl::narrow<uint8_t>(value);
    } else {
        keylen = htons(value);
    }
}

uint16_t Request::getKeylen() const {
    return reinterpret_cast<const Header*>(this)->getKeylen();
}

uint8_t Request::getFramingExtraslen() const {
    return reinterpret_cast<const Header*>(this)->getFramingExtraslen();
}

void Request::setFramingExtraslen(uint8_t len) {
    setMagic(cb::mcbp::Magic::AltClientRequest);
    // @todo Split the member once we know all the tests pass with the
    //       current layout (aka: noone tries to set it htons()
    reinterpret_cast<uint8_t*>(this)[2] = len;
}

uint8_t Request::getExtlen() const {
    return reinterpret_cast<const Header*>(this)->getExtlen();
}

uint32_t Request::getBodylen() const {
    return reinterpret_cast<const Header*>(this)->getBodylen();
}

uint32_t Request::getOpaque() const {
    return reinterpret_cast<const Header*>(this)->getOpaque();
}

uint64_t Request::getCas() const {
    return reinterpret_cast<const Header*>(this)->getCas();
}

std::string Request::getPrintableKey() const {
    std::string buffer{getKeyString()};
    for (auto& ii : buffer) {
        if (!std::isprint(ii)) {
            ii = '.';
        }
    }

    return buffer;
}

cb::const_byte_buffer Request::getFramingExtras() const {
    return reinterpret_cast<const Header*>(this)->getFramingExtras();
}

cb::byte_buffer Request::getFramingExtras() {
    return reinterpret_cast<Header*>(this)->getFramingExtras();
}

cb::const_byte_buffer Request::getExtdata() const {
    return reinterpret_cast<const Header*>(this)->getExtdata();
}
cb::byte_buffer Request::getExtdata() {
    return reinterpret_cast<Header*>(this)->getExtdata();
}

cb::const_byte_buffer Request::getKey() const {
    return reinterpret_cast<const Header*>(this)->getKey();
}
cb::byte_buffer Request::getKey() {
    return reinterpret_cast<Header*>(this)->getKey();
}
std::string_view Request::getKeyString() const {
    return reinterpret_cast<const Header*>(this)->getKeyString();
}
cb::const_byte_buffer Request::getValue() const {
    return reinterpret_cast<const Header*>(this)->getValue();
}
cb::byte_buffer Request::getValue() {
    return reinterpret_cast<Header*>(this)->getValue();
}
std::string_view Request::getValueString() const {
    return reinterpret_cast<const Header*>(this)->getValueString();
}
cb::const_byte_buffer Request::getFrame() const {
    return reinterpret_cast<const Header*>(this)->getFrame();
}

void Request::parseFrameExtras(const FrameInfoCallback& callback) const {
    auto fe = getFramingExtras();
    if (fe.empty()) {
        return;
    }
    size_t offset = 0;
    while (offset < fe.size()) {
        using cb::mcbp::request::FrameInfoId;

        auto idbits = size_t(fe[offset] >> 4);
        auto size = size_t(fe[offset] & 0x0f);
        ++offset;

        if (idbits == 0x0f) {
            // This is the escape byte
            if ((offset + 1) > fe.size()) {
                throw std::overflow_error(
                        "parseFrameExtras: outside frame extras");
            }
            idbits += fe[offset++];
        }

        if (size == 0x0f) {
            // This is the escape value
            if ((offset + 1) > fe.size()) {
                throw std::overflow_error(
                        "parseFrameExtras: outside frame extras");
            }
            size += fe[offset++];
        }

        const auto id = FrameInfoId(idbits);
        if ((offset + size) > fe.size()) {
            throw std::overflow_error("parseFrameExtras: outside frame extras");
        }

        cb::const_byte_buffer content{fe.data() + offset, size};
        offset += size;

        if (!callback(id, content)) {
            return;
        }
    }
}

bool Request::isQuiet() const {
    if (is_client_magic(getMagic())) {
        return is_quiet(getClientOpcode());
    }

    switch (getServerOpcode()) {
    case ServerOpcode::ClustermapChangeNotification:
    case ServerOpcode::Authenticate:
    case ServerOpcode::ActiveExternalUsers:
    case ServerOpcode::GetAuthorization:
        return false;
    }

    throw std::invalid_argument("Request::isQuiet: Uknown opcode");
}

std::optional<cb::durability::Requirements> Request::getDurabilityRequirements()
        const {
    using cb::durability::Level;
    using cb::durability::Requirements;
    Requirements ret;
    bool found = false;

    parseFrameExtras([&ret, &found](cb::mcbp::request::FrameInfoId id,
                                    cb::const_byte_buffer data) -> bool {
        if (id == cb::mcbp::request::FrameInfoId::DurabilityRequirement) {
            ret = Requirements{data};
            found = true;
            // stop parsing
            return false;
        }
        // Continue parsing
        return true;
    });
    if (found) {
        return {ret};
    }
    return {};
}

static std::string printableString(const std::string_view buffer) {
    std::string ret;
    ret.reserve(buffer.size() + 9);
    ret.append("<ud>");
    std::ranges::copy(buffer, std::back_inserter(ret));
    ret.append("</ud>");

    for (auto& ii : ret) {
        if (!std::isgraph(ii)) {
            ii = '.';
        }
    }

    return ret;
}

nlohmann::json Request::to_json(bool validated) const {
    if (!validated && !isValid()) {
        throw std::logic_error("Request::to_json(): Invalid packet");
    }

    nlohmann::json ret;
    auto m = Magic(magic);
    ret["magic"] = m;

    if (is_client_magic(m)) {
        ret["opcode"] = getClientOpcode();

        if (validated && m == Magic::AltClientRequest) {
            nlohmann::json frameid;
            parseFrameExtras([&frameid](cb::mcbp::request::FrameInfoId id,
                                        cb::const_byte_buffer buffer) -> bool {
                switch (id) {
                case request::FrameInfoId::Barrier:
                    frameid["barrier"] = true;
                    break;
                case request::FrameInfoId::DurabilityRequirement:
                    frameid["durability"] =
                            cb::durability::Requirements(buffer);
                    break;
                case request::FrameInfoId::DcpStreamId:
                    frameid["dcp stream id"] = ntohs(
                            *reinterpret_cast<const uint16_t*>(buffer.data()));
                    break;
                case request::FrameInfoId::Impersonate:
                    if (buffer[0] == '^') {
                        frameid["euid"]["user"] = cb::tagUserData(std::string{
                                reinterpret_cast<const char*>(buffer.data() +
                                                              1),
                                buffer.size() - 1});
                        frameid["euid"]["domain"] = "external";
                    } else {
                        frameid["euid"]["user"] = cb::tagUserData(std::string{
                                reinterpret_cast<const char*>(buffer.data()),
                                buffer.size()});
                        frameid["euid"]["domain"] = "local";
                    }
                    break;
                case request::FrameInfoId::PreserveTtl:
                    frameid["Preserve TTL"] = true;
                    break;
                case request::FrameInfoId::ImpersonateExtraPrivilege:
                    frameid["privilege"].push_back(std::string{
                            reinterpret_cast<const char*>(buffer.data()),
                            buffer.size()});
                    break;
                }

                return true;
            });
            if (!frameid.empty()) {
                ret["frameid"] = frameid;
            }
        }
    } else {
        ret["opcode"] = getServerOpcode();
    }

    if (validated && !getKeyString().empty()) {
        ret["key"] = printableString(getKeyString());
    }

    ret["keylen"] = getKeylen();
    ret["extlen"] = getExtlen();
    ret["datatype"] = ::to_json(getDatatype());
    ret["vbucket"] = getVBucket().get();
    ret["bodylen"] = getBodylen();
    ret["opaque"] = getOpaque();
    ret["cas"] = getCas();

    // Add opcode-specific fields for "interesting" opcodes. Ideally this
    // would be all opcodes, but adding on an as-needed basis...
    if (validated && is_client_magic(m)) {
        using namespace request;
        nlohmann::json extras;
        nlohmann::json value;
        switch (getClientOpcode()) {
        case ClientOpcode::GetEx:
        case ClientOpcode::GetExReplica:
        case ClientOpcode::Get:
        case ClientOpcode::Getq:
        case ClientOpcode::Getk:
        case ClientOpcode::Getkq:
        case ClientOpcode::Delete:
        case ClientOpcode::Deleteq:
        case ClientOpcode::Noop:
        case ClientOpcode::Quit:
        case ClientOpcode::Flush_Unsupported:
        case ClientOpcode::Version:
        case ClientOpcode::Append:
        case ClientOpcode::Prepend:
        case ClientOpcode::Stat:
        case ClientOpcode::Quitq:
        case ClientOpcode::Flushq_Unsupported:
        case ClientOpcode::Appendq:
        case ClientOpcode::Prependq:
        case ClientOpcode::Hello:
        case ClientOpcode::SaslListMechs:
        case ClientOpcode::SaslAuth:
        case ClientOpcode::SaslStep:
        case ClientOpcode::IoctlGet:
        case ClientOpcode::IoctlSet:
        case ClientOpcode::ConfigValidate:
        case ClientOpcode::ConfigReload:
        case ClientOpcode::AuditPut:
        case ClientOpcode::AuditConfigReload:
        case ClientOpcode::Shutdown:
        case ClientOpcode::SetBucketThrottleProperties:
        case ClientOpcode::Rget_Unsupported:
        case ClientOpcode::Rset_Unsupported:
        case ClientOpcode::Rsetq_Unsupported:
        case ClientOpcode::Rappend_Unsupported:
        case ClientOpcode::Rappendq_Unsupported:
        case ClientOpcode::Rprepend_Unsupported:
        case ClientOpcode::Rprependq_Unsupported:
        case ClientOpcode::Rdelete_Unsupported:
        case ClientOpcode::Rdeleteq_Unsupported:
        case ClientOpcode::Rincr_Unsupported:
        case ClientOpcode::Rincrq_Unsupported:
        case ClientOpcode::Rdecr_Unsupported:
        case ClientOpcode::Rdecrq_Unsupported:
        case ClientOpcode::GetVbucket:
        case ClientOpcode::DelVbucket:
        case ClientOpcode::TapConnect_Unsupported:
        case ClientOpcode::TapMutation_Unsupported:
        case ClientOpcode::TapDelete_Unsupported:
        case ClientOpcode::TapFlush_Unsupported:
        case ClientOpcode::TapOpaque_Unsupported:
        case ClientOpcode::TapVbucketSet_Unsupported:
        case ClientOpcode::TapCheckpointStart_Unsupported:
        case ClientOpcode::TapCheckpointEnd_Unsupported:
        case ClientOpcode::LastClosedCheckpoint_Unsupported:
        case ClientOpcode::DeregisterTapClient_Unsupported:
        case ClientOpcode::ResetReplicationChain_Unsupported:
        case ClientOpcode::SetNodeThrottleProperties_Unsupported:
        case ClientOpcode::SetActiveEncryptionKeys:
        case ClientOpcode::PruneEncryptionKeys:
        case ClientOpcode::DcpCloseStream:
        case ClientOpcode::DcpGetFailoverLog:
        case ClientOpcode::DcpFlush_Unsupported:
        case ClientOpcode::DcpNoop:
        case ClientOpcode::DcpControl:
        case ClientOpcode::DcpOsoSnapshot:
        case ClientOpcode::GetReplica:
        case ClientOpcode::StopPersistence:
        case ClientOpcode::StartPersistence:
        case ClientOpcode::CreateBucket:
        case ClientOpcode::DeleteBucket:
        case ClientOpcode::ListBuckets:
        case ClientOpcode::SelectBucket:
        case ClientOpcode::PauseBucket:
        case ClientOpcode::ResumeBucket:
        case ClientOpcode::SnapshotVbStates_Unsupported:
        case ClientOpcode::VbucketBatchCount_Unsupported:
        case ClientOpcode::CreateCheckpoint_Unsupported:
        case ClientOpcode::NotifyVbucketUpdate_Unsupported:
        case ClientOpcode::EnableTraffic:
        case ClientOpcode::DisableTraffic:
        case ClientOpcode::Ifconfig:
        case ClientOpcode::ChangeVbFilter_Unsupported:
        case ClientOpcode::CheckpointPersistence_Unsupported:
        case ClientOpcode::ObserveSeqno:
        case ClientOpcode::Observe:
        case ClientOpcode::EvictKey:
        case ClientOpcode::UnlockKey:
        case ClientOpcode::GetFailoverLog:
        case ClientOpcode::GetClusterConfig:
        case ClientOpcode::CollectionsSetManifest:
        case ClientOpcode::CollectionsGetManifest:
        case ClientOpcode::SetDriftCounterState_Unsupported:
        case ClientOpcode::GetAdjustedTime_Unsupported:
        case ClientOpcode::CollectionsGetID:
        case ClientOpcode::CollectionsGetScopeID:
        case ClientOpcode::ValidateBucketConfig:
        case ClientOpcode::Scrub_Unsupported:
        case ClientOpcode::IsaslRefresh:
        case ClientOpcode::SslCertsRefresh_Unsupported:
        case ClientOpcode::GetCtrlToken:
        case ClientOpcode::UpdateExternalUserPermissions:
        case ClientOpcode::RbacRefresh:
        case ClientOpcode::AuthProvider:
        case ClientOpcode::DropPrivilege:
        case ClientOpcode::AdjustTimeofday:
        case ClientOpcode::EwouldblockCtl:
        case ClientOpcode::Invalid:
        case ClientOpcode::RangeScanCreate:
        case ClientOpcode::GetFileFragment:
        case ClientOpcode::PrepareSnapshot:
        case ClientOpcode::ReleaseSnapshot:
        case ClientOpcode::DownloadSnapshot:
        case ClientOpcode::GetFusionStorageSnapshot:
        case ClientOpcode::ReleaseFusionStorageSnapshot:
        case ClientOpcode::MountFusionVbucket:
        case ClientOpcode::UnmountFusionVbucket:
        case ClientOpcode::SyncFusionLogstore:
        case ClientOpcode::StartFusionUploader:
        case ClientOpcode::StopFusionUploader:
        case ClientOpcode::DeleteFusionNamespace:
        case ClientOpcode::GetFusionNamespaces:
        case ClientOpcode::SetChronicleAuthToken:
        case ClientOpcode::DcpCachedValue:
        case ClientOpcode::DcpCachedKeyMeta:
        case ClientOpcode::DcpCacheTransferEnd:
            // The command don't take (or we don't support decoding) extras
            break;

        case ClientOpcode::Set:
        case ClientOpcode::Add:
        case ClientOpcode::Replace:
        case ClientOpcode::Setq:
        case ClientOpcode::Addq:
        case ClientOpcode::Replaceq:
            extras = getCommandSpecifics<MutationPayload>();
            break;
        case ClientOpcode::Increment:
        case ClientOpcode::Decrement:
        case ClientOpcode::Incrementq:
        case ClientOpcode::Decrementq:
            extras = getCommandSpecifics<ArithmeticPayload>();
            break;
        case ClientOpcode::Verbosity:
            extras = getCommandSpecifics<VerbosityPayload>();
            break;
        case ClientOpcode::Touch:
        case ClientOpcode::Gat:
        case ClientOpcode::Gatq:
            extras = getCommandSpecifics<TouchPayload>();
            break;
        case ClientOpcode::SetBucketDataLimitExceeded:
            extras = getCommandSpecifics<SetBucketDataLimitExceededPayload>();
            break;
        case ClientOpcode::SetVbucket:
            // @todo add implementation
            break;
        case ClientOpcode::GetAllVbSeqnos:
            if (!getExtdata().empty()) {
                auto extdata = getExtdata();
                // extlen is optional, and if non-zero it contains the vbucket
                // state to report and potentially also a collection ID
                RequestedVBState state;

                // vbucket state will be the first part of extras
                auto extrasState = extdata.substr(0, sizeof(RequestedVBState));
                std::ranges::copy(extrasState,
                                  reinterpret_cast<uint8_t*>(&state));
                switch (static_cast<RequestedVBState>(
                        ntohl(static_cast<int>(state)))) {
                case RequestedVBState::Alive:
                    extras["state"] = "Alive";
                    break;
                case RequestedVBState::Active:
                    extras["state"] = "Active";
                    break;
                case RequestedVBState::Replica:
                    extras["state"] = "Replica";
                    break;
                case RequestedVBState::Pending:
                    extras["state"] = "Pending";
                    break;
                case RequestedVBState::Dead:
                    extras["state"] = "Dead";
                    break;
                }

                if (extdata.size() ==
                    (sizeof(RequestedVBState) + sizeof(CollectionIDType))) {
                    CollectionIDType cid;
                    extrasState = extdata.substr(sizeof(RequestedVBState),
                                                 sizeof(CollectionIDType));
                    std::ranges::copy(extrasState,
                                      reinterpret_cast<uint8_t*>(&cid));
                    cid = ntohl(static_cast<int>(state));
                    extras["collection"] = CollectionID(cid).to_string();
                }
            }
            break;
        case ClientOpcode::DcpOpen:
            extras = getCommandSpecifics<DcpOpenPayload>();
            break;
        case ClientOpcode::DcpAddStream:
            extras = getCommandSpecifics<DcpAddStreamPayload>();
            break;
        case ClientOpcode::DcpStreamReq:
            extras = getCommandSpecifics<DcpStreamReqPayload>();
            break;
        case ClientOpcode::DcpStreamEnd:
            extras = getCommandSpecifics<DcpStreamEndPayload>();
            break;
        case ClientOpcode::DcpSnapshotMarker:
            if (getExtdata().size() == sizeof(DcpSnapshotMarkerV1Payload)) {
                extras = getCommandSpecifics<DcpSnapshotMarkerV1Payload>();
            } else if (getExtdata().size() ==
                       sizeof(DcpSnapshotMarkerV2xPayload)) {
                const auto& payload =
                        getCommandSpecifics<DcpSnapshotMarkerV2xPayload>();
                extras = payload;
                switch (payload.getVersion()) {
                case DcpSnapshotMarkerV2xVersion::Zero:
                    value = *reinterpret_cast<
                            const DcpSnapshotMarkerV2_0Value*>(
                            getValue().data());
                    break;
                case DcpSnapshotMarkerV2xVersion::Two:
                    value = *reinterpret_cast<
                            const DcpSnapshotMarkerV2_2Value*>(
                            getValue().data());
                    break;
                default:
                    throw std::invalid_argument(fmt::format(
                            "DcpSnapshotMarkerV2xVersion::invalid version:{}",
                            std::underlying_type_t<DcpSnapshotMarkerV2xVersion>(
                                    payload.getVersion())));
                }
            }
            break;
        case ClientOpcode::DcpMutation:
            extras = getCommandSpecifics<DcpMutationPayload>();
            break;
        case ClientOpcode::DcpSystemEvent:
            extras = getCommandSpecifics<DcpSystemEventPayload>();
            break;
        case ClientOpcode::DcpSeqnoAdvanced:
            extras = getCommandSpecifics<DcpSeqnoAdvancedPayload>();
            break;
        case ClientOpcode::DcpDeletion:
            if (getExtlen() == sizeof(DcpDeletionV2Payload)) {
                extras = getCommandSpecifics<DcpDeletionV2Payload>();
            } else if (getExtlen() == sizeof(DcpDeletionV1Payload)) {
                extras = getCommandSpecifics<DcpDeletionV1Payload>();
            }
            break;
        case ClientOpcode::DcpExpiration:
            extras = getCommandSpecifics<DcpExpirationPayload>();
            break;
        case ClientOpcode::DcpSetVbucketState:
            extras = getCommandSpecifics<DcpSetVBucketState>();
            break;
        case ClientOpcode::DcpBufferAcknowledgement:
            extras = getCommandSpecifics<DcpBufferAckPayload>();
            break;
        case ClientOpcode::DcpPrepare:
            extras = getCommandSpecifics<DcpPreparePayload>();
            break;
        case ClientOpcode::DcpSeqnoAcknowledged:
            extras = getCommandSpecifics<DcpSeqnoAcknowledgedPayload>();
            break;
        case ClientOpcode::DcpCommit:
            extras = getCommandSpecifics<DcpCommitPayload>();
            break;
        case ClientOpcode::DcpAbort:
            extras = getCommandSpecifics<DcpAbortPayload>();
            break;
        case ClientOpcode::SetParam:
            extras = getCommandSpecifics<SetParamPayload>();
            break;
        case ClientOpcode::GetLocked:
            if (getExtlen() == sizeof(GetLockedPayload)) {
                extras = getCommandSpecifics<GetLockedPayload>();
            }
            break;
        case ClientOpcode::GetMeta:
        case ClientOpcode::GetqMeta:
            if (getExtlen() == 1) {
                const auto& val = getExtdata().front();
                if (val == 1) {
                    extras["mode"] = "return conflict resolution mode";
                } else if (val == 2) {
                    extras["mode"] = "return datatype";
                } else {
                    extras["mode"] = std::to_string(int(val));
                }
            }
            break;
        case ClientOpcode::SetWithMeta:
        case ClientOpcode::SetqWithMeta:
        case ClientOpcode::AddWithMeta:
        case ClientOpcode::AddqWithMeta:
        case ClientOpcode::DelWithMeta:
        case ClientOpcode::DelqWithMeta:
            // @todo implement me
            break;
        case ClientOpcode::ReturnMeta:
            extras = getCommandSpecifics<ReturnMetaPayload>();
            break;
        case ClientOpcode::CompactDb:
            extras = getCommandSpecifics<CompactDbPayload>();
            break;
        case ClientOpcode::SetClusterConfig:
            extras = getCommandSpecifics<SetClusterConfigPayload>();
            break;
        case ClientOpcode::GetRandomKey:
            if (getExtlen() == sizeof(GetRandomKeyPayload)) {
                extras = getCommandSpecifics<GetRandomKeyPayload>();
            }
            break;
        case ClientOpcode::SeqnoPersistence:
            if (getExtlen() == sizeof(uint64_t)) {
                extras["seqno"] = std::to_string(
                        ntohll(*reinterpret_cast<const uint64_t*>(
                                getExtdata().data())));
            }
            break;
        case ClientOpcode::GetKeys:
            if (getExtlen() == sizeof(uint32_t)) {
                extras["number"] = ntohl(*reinterpret_cast<const uint32_t*>(
                        getExtdata().data()));
            }
            break;
        case ClientOpcode::SubdocGet:
        case ClientOpcode::SubdocExists:
        case ClientOpcode::SubdocDictAdd:
        case ClientOpcode::SubdocDictUpsert:
        case ClientOpcode::SubdocDelete:
        case ClientOpcode::SubdocReplace:
        case ClientOpcode::SubdocArrayPushLast:
        case ClientOpcode::SubdocArrayPushFirst:
        case ClientOpcode::SubdocArrayInsert:
        case ClientOpcode::SubdocArrayAddUnique:
        case ClientOpcode::SubdocCounter:
        case ClientOpcode::SubdocMultiLookup:
        case ClientOpcode::SubdocMultiMutation:
        case ClientOpcode::SubdocGetCount:
        case ClientOpcode::SubdocReplaceBodyWithXattr:
            // @todo implement me
            break;

        case ClientOpcode::RangeScanContinue:
            extras = getCommandSpecifics<RangeScanContinuePayload>();
            break;
        case ClientOpcode::RangeScanCancel:
            extras["uuid"] =
                    ::to_string(getCommandSpecifics<cb::rangescan::Id>());
            break;
        case ClientOpcode::GetCmdTimer:
            try {
                extras["opcode"] = ClientOpcode(getExtdata().front());
            } catch (const std::exception&) {
                extras["opcode"] = "unknown";
            }
            break;
        case ClientOpcode::SetCtrlToken:
            extras = getCommandSpecifics<SetCtrlTokenPayload>();
            break;
        case ClientOpcode::GetErrorMap:
            extras = getCommandSpecifics<GetErrmapPayload>();
            break;
        }
        if (!extras.is_null()) {
            ret["extras"] = extras;
        }
        if (!value.is_null()) {
            ret["value"] = std::move(value);
        }
    }
    return ret;
}

bool Request::isValid() const {
    auto m = Magic(magic);
    if (!is_legal(m) || !is_request(m)) {
        return false;
    }

    return (size_t(getFramingExtraslen()) + size_t(extlen) +
                    size_t(getKeylen()) <=
            size_t(getBodylen()));
}

} // namespace cb::mcbp

std::string to_string(cb::mcbp::request::FrameInfoId id) {
    using cb::mcbp::request::FrameInfoId;

    switch (id) {
    case FrameInfoId::Barrier:
        return "Barrier";
    case FrameInfoId::DurabilityRequirement:
        return "DurabilityRequirement";
    case FrameInfoId::DcpStreamId:
        return "DcpStreamId";
    case FrameInfoId::Impersonate:
        return "Impersonate";
    case FrameInfoId::PreserveTtl:
        return "PreserveTtl";
    case FrameInfoId::ImpersonateExtraPrivilege:
        return "ImpersonateExtraPrivilege";
    }

    throw std::invalid_argument("to_string(): Invalid frame id: " +
                                std::to_string(int(id)));
}

cb::durability::Level cb::mcbp::request::DcpPreparePayload::getDurabilityLevel()
        const {
    return cb::durability::Level(durability_level);
}

void cb::mcbp::request::DcpPreparePayload::setDurabilityLevel(
        cb::durability::Level level) {
    DcpPreparePayload::durability_level = uint8_t(level);
}
