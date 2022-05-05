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

#include <mcbp/protocol/request.h>
#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
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
    const auto key = getKey();

    std::string buffer{reinterpret_cast<const char*>(key.data()), key.size()};
    for (auto& ii : buffer) {
        if (!std::isgraph(ii)) {
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

void Request::parseFrameExtras(FrameInfoCallback callback) const {
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
    if ((getMagic() == Magic::ClientRequest) ||
        (getMagic() == Magic::AltClientRequest)) {
        switch (getClientOpcode()) {
        case ClientOpcode::Get:
        case ClientOpcode::Set:
        case ClientOpcode::Add:
        case ClientOpcode::Replace:
        case ClientOpcode::Delete:
        case ClientOpcode::Increment:
        case ClientOpcode::Decrement:
        case ClientOpcode::Quit:
        case ClientOpcode::Flush:
        case ClientOpcode::Noop:
        case ClientOpcode::Version:
        case ClientOpcode::Getk:
        case ClientOpcode::Append:
        case ClientOpcode::Prepend:
        case ClientOpcode::Stat:
        case ClientOpcode::Verbosity:
        case ClientOpcode::Touch:
        case ClientOpcode::Gat:
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
        case ClientOpcode::SetBucketComputeUnitThrottleLimits:
        case ClientOpcode::Rget_Unsupported:
        case ClientOpcode::Rset_Unsupported:
        case ClientOpcode::Rappend_Unsupported:
        case ClientOpcode::Rprepend_Unsupported:
        case ClientOpcode::Rdelete_Unsupported:
        case ClientOpcode::Rincr_Unsupported:
        case ClientOpcode::Rdecr_Unsupported:
        case ClientOpcode::SetVbucket:
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
        case ClientOpcode::GetAllVbSeqnos:
        case ClientOpcode::DcpOpen:
        case ClientOpcode::DcpAddStream:
        case ClientOpcode::DcpCloseStream:
        case ClientOpcode::DcpStreamReq:
        case ClientOpcode::DcpGetFailoverLog:
        case ClientOpcode::DcpStreamEnd:
        case ClientOpcode::DcpSnapshotMarker:
        case ClientOpcode::DcpMutation:
        case ClientOpcode::DcpDeletion:
        case ClientOpcode::DcpFlush_Unsupported:
        case ClientOpcode::DcpExpiration:
        case ClientOpcode::DcpSetVbucketState:
        case ClientOpcode::DcpNoop:
        case ClientOpcode::DcpBufferAcknowledgement:
        case ClientOpcode::DcpControl:
        case ClientOpcode::DcpSystemEvent:
        case ClientOpcode::DcpPrepare:
        case ClientOpcode::DcpSeqnoAcknowledged:
        case ClientOpcode::DcpCommit:
        case ClientOpcode::DcpAbort:
        case ClientOpcode::DcpSeqnoAdvanced:
        case ClientOpcode::DcpOsoSnapshot:
        case ClientOpcode::StopPersistence:
        case ClientOpcode::StartPersistence:
        case ClientOpcode::SetParam:
        case ClientOpcode::GetReplica:
        case ClientOpcode::CreateBucket:
        case ClientOpcode::DeleteBucket:
        case ClientOpcode::ListBuckets:
        case ClientOpcode::SelectBucket:
        case ClientOpcode::ObserveSeqno:
        case ClientOpcode::Observe:
        case ClientOpcode::EvictKey:
        case ClientOpcode::GetLocked:
        case ClientOpcode::UnlockKey:
        case ClientOpcode::GetFailoverLog:
        case ClientOpcode::LastClosedCheckpoint:
        case ClientOpcode::ResetReplicationChain_Unsupported:
        case ClientOpcode::DeregisterTapClient_Unsupported:
        case ClientOpcode::GetMeta:
        case ClientOpcode::SetWithMeta:
        case ClientOpcode::AddWithMeta:
        case ClientOpcode::SnapshotVbStates_Unsupported:
        case ClientOpcode::VbucketBatchCount_Unsupported:
        case ClientOpcode::DelWithMeta:
        case ClientOpcode::CreateCheckpoint:
        case ClientOpcode::NotifyVbucketUpdate_Unsupported:
        case ClientOpcode::EnableTraffic:
        case ClientOpcode::DisableTraffic:
        case ClientOpcode::Ifconfig:
        case ClientOpcode::ChangeVbFilter_Unsupported:
        case ClientOpcode::CheckpointPersistence_Unsupported:
        case ClientOpcode::ReturnMeta:
        case ClientOpcode::CompactDb:
        case ClientOpcode::SetClusterConfig:
        case ClientOpcode::GetClusterConfig:
        case ClientOpcode::GetRandomKey:
        case ClientOpcode::SeqnoPersistence:
        case ClientOpcode::GetKeys:
        case ClientOpcode::CollectionsSetManifest:
        case ClientOpcode::CollectionsGetManifest:
        case ClientOpcode::CollectionsGetID:
        case ClientOpcode::CollectionsGetScopeID:
        case ClientOpcode::SetDriftCounterState_Unsupported:
        case ClientOpcode::GetAdjustedTime_Unsupported:
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
        case ClientOpcode::Scrub:
        case ClientOpcode::IsaslRefresh:
        case ClientOpcode::SslCertsRefresh:
        case ClientOpcode::GetCmdTimer:
        case ClientOpcode::SetCtrlToken:
        case ClientOpcode::GetCtrlToken:
        case ClientOpcode::UpdateExternalUserPermissions:
        case ClientOpcode::RbacRefresh:
        case ClientOpcode::AuthProvider:
        case ClientOpcode::DropPrivilege:
        case ClientOpcode::AdjustTimeofday:
        case ClientOpcode::EwouldblockCtl:
        case ClientOpcode::GetErrorMap:
        case ClientOpcode::RangeScanCreate:
        case ClientOpcode::RangeScanContinue:
        case ClientOpcode::RangeScanCancel:
        case ClientOpcode::Invalid:
            return false;

        case ClientOpcode::Getq:
        case ClientOpcode::Getkq:
        case ClientOpcode::Setq:
        case ClientOpcode::Addq:
        case ClientOpcode::Replaceq:
        case ClientOpcode::Deleteq:
        case ClientOpcode::Incrementq:
        case ClientOpcode::Decrementq:
        case ClientOpcode::Quitq:
        case ClientOpcode::Flushq:
        case ClientOpcode::Appendq:
        case ClientOpcode::Prependq:
        case ClientOpcode::Gatq:
        case ClientOpcode::Rsetq_Unsupported:
        case ClientOpcode::Rappendq_Unsupported:
        case ClientOpcode::Rprependq_Unsupported:
        case ClientOpcode::Rdeleteq_Unsupported:
        case ClientOpcode::Rincrq_Unsupported:
        case ClientOpcode::Rdecrq_Unsupported:
        case ClientOpcode::GetqMeta:
        case ClientOpcode::SetqWithMeta:
        case ClientOpcode::AddqWithMeta:
        case ClientOpcode::DelqWithMeta:
            return true;
        }
    } else {
        switch (getServerOpcode()) {
        case ServerOpcode::ClustermapChangeNotification:
        case ServerOpcode::Authenticate:
        case ServerOpcode::ActiveExternalUsers:
        case ServerOpcode::GetAuthorization:
            return false;
        }
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

std::string printableString(cb::const_byte_buffer buffer) {
    std::string ret;
    ret.reserve(buffer.size() + 9);
    ret.append("<ud>");
    std::copy(buffer.begin(), buffer.end(), std::back_inserter(ret));
    ret.append("</ud>");

    for (auto& ii : ret) {
        if (!std::isgraph(ii)) {
            ii = '.';
        }
    }

    return ret;
}

nlohmann::json Request::toJSON(bool validated) const {
    if (!validated && !isValid()) {
        throw std::logic_error("Request::toJSON(): Invalid packet");
    }

    nlohmann::json ret;
    auto m = Magic(magic);
    ret["magic"] = ::to_string(m);

    if (is_client_magic(m)) {
        ret["opcode"] = ::to_string(getClientOpcode());

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
                            cb::durability::Requirements(buffer).to_json();
                    break;
                case request::FrameInfoId::DcpStreamId:
                    frameid["dcp stream id"] = ntohs(
                            *reinterpret_cast<const uint16_t*>(buffer.data()));
                    break;
                case request::FrameInfoId::OpenTracingContext:
                    frameid["OpenTelemetry context"] = printableString(buffer);
                    break;
                case request::FrameInfoId::Impersonate:
                    if (buffer[0] == '^') {
                        frameid["euid"]["user"] =
                                std::string{reinterpret_cast<const char*>(
                                                    buffer.data() + 1),
                                            buffer.size() - 1};
                        frameid["euid"]["domain"] = "external";
                    } else {
                        frameid["euid"]["user"] = std::string{
                                reinterpret_cast<const char*>(buffer.data()),
                                buffer.size()};
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
        ret["opcode"] = ::to_string(getServerOpcode());
    }

    if (validated) {
        ret["key"] = printableString(getKey());
    }

    ret["keylen"] = getKeylen();
    ret["extlen"] = getExtlen();
    ret["datatype"] = ::toJSON(getDatatype());
    ret["vbucket"] = getVBucket().get();
    ret["bodylen"] = getBodylen();
    ret["opaque"] = getOpaque();
    ret["cas"] = getCas();

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
    case FrameInfoId::OpenTracingContext:
        return "OpenTracingContext";
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
