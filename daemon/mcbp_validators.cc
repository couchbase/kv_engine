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

#include "mcbp_validators.h"
#include "buckets.h"
#include "connection.h"
#include "cookie.h"
#include "enginemap.h"
#include "front_end_thread.h"
#include "memcached.h"
#include "network_interface_description.h"
#include "subdocument_validators.h"
#include "tls_configuration.h"
#include "xattr/utils.h"
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <memcached/collections.h>
#include <memcached/dcp.h>
#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <string_view>

using cb::mcbp::Status;

static bool is_valid_xattr_blob(Cookie& cookie,
                                const cb::mcbp::Request& request) {
    if (!mcbp::datatype::is_xattr(uint8_t(request.getDatatype()))) {
        // no xattr segment
        return true;
    }

    return cookie.getConnection().getThread().isXattrBlobValid(
            cookie.getInflatedInputPayload());
}

bool is_document_key_valid(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto& key = req.getKey();
    if (!cookie.getConnection().isCollectionsSupported()) {
        return true;
    }

    // Note:: key maximum length is checked in McbpValidator::verify_header
    // A document key must be 2 bytes minimum
    if (key.size() <= 1) {
        cookie.setErrorContext("Key length must be >= 2");
        return false; // 0 and 1 are invalid
    }

    // Next the validation depends on the collection-ID, default collection
    // has legacy data so has different rules.
    auto leb =
            cb::mcbp::unsigned_leb128<CollectionIDType>::decodeCanonical(key);

    // Failed to decode
    if (!leb.second.data()) {
        // To get detailed error context, decode via the version which tolerates
        // non-canonical encodings but will fail for bad input (no stop-byte)
        auto check =
                cb::mcbp::unsigned_leb128<CollectionIDType>::decodeNoThrow(key);

        // If the decode was fine, the issue is no-stop byte found
        if (check.second.data()) {
            cookie.setErrorContext("Key contains non-canonical leb128");
        } else {
            cookie.setErrorContext("No stop-byte found");
        }
        return false;
    }
    if (leb.second.empty()) {
        cookie.setErrorContext("No logical key found");
        return false;
    }
    // The range of collections permissible on the wire is restricted (no
    // internally reserved values are allowed)
    if (CollectionID::isReserved(leb.first)) {
        cookie.setErrorContext("Invalid collection-id:" +
                               std::to_string(leb.first));
        return false;
    }

    // The maximum length depends on the collection-ID
    const auto maxLen = ((leb.first == CollectionID::Default)
                                 ? KEY_MAX_LENGTH
                                 : MaxCollectionsLogicalKeyLen);
    bool rv = leb.second.size() <= maxLen;
    if (!rv) {
        cookie.setErrorContext("Logical key exceeds " + std::to_string(maxLen));
    }
    return rv;
}

static inline bool may_accept_dcp_deleteV2(const Cookie& cookie) {
    return cookie.getConnection().isDcpDeleteV2();
}

static inline std::string get_peer_description(const Cookie& cookie) {
    return cookie.getConnection().getDescription();
}

using ExpectedKeyLen = McbpValidator::ExpectedKeyLen;
using ExpectedValueLen = McbpValidator::ExpectedValueLen;
using ExpectedCas = McbpValidator::ExpectedCas;

/**
 * Verify the header meets basic sanity checks and fields length
 * match the provided expected lengths.
 *
 * NOTE: the packet framing is already verified _before_ this method
 *       is called (in Connection::isPacketAvailable())
 */
Status McbpValidator::verify_header(Cookie& cookie,
                                    uint8_t expected_extlen,
                                    ExpectedKeyLen expected_keylen,
                                    ExpectedValueLen expected_valuelen,
                                    ExpectedCas expected_cas,
                                    uint8_t expected_datatype_mask) {
    const auto& header = cookie.getHeader();
    auto& connection = cookie.getConnection();

    if (!mcbp::datatype::is_valid(header.getDatatype())) {
        cookie.setErrorContext("Request datatype invalid");
        return Status::Einval;
    }

    if ((expected_extlen == 0) && (header.getExtlen() != 0)) {
        cookie.setErrorContext("Request must not include extras");
        return Status::Einval;
    }
    if ((expected_extlen != 0) && (header.getExtlen() != expected_extlen)) {
        cookie.setErrorContext("Request must include extras of length " +
                               std::to_string(expected_extlen));
        return Status::Einval;
    }

    switch (expected_keylen) {
    case ExpectedKeyLen::Zero:
        if (header.getKeylen() != 0) {
            cookie.setErrorContext("Request must not include key");
            return Status::Einval;
        }
        break;
    case ExpectedKeyLen::NonZero:
        if (header.getKeylen() == 0) {
            cookie.setErrorContext("Request must include key");
            return Status::Einval;
        }
        // Fall-through to check against max key length
    case ExpectedKeyLen::Any:
        const auto maxKeyLen = connection.isCollectionsSupported()
                                       ? MaxCollectionsKeyLen
                                       : KEY_MAX_LENGTH;

        if (header.getKeylen() > maxKeyLen) {
            cookie.setErrorContext("Key length exceeds " +
                                   std::to_string(maxKeyLen));
            return cb::mcbp::Status::Einval;
        }
        break;
    }

    const auto& request = header.getRequest();
    const auto value = request.getValue();

    switch (expected_valuelen) {
    case ExpectedValueLen::Zero:
        if (!value.empty()) {
            cookie.setErrorContext("Request must not include value");
            return Status::Einval;
        }
        break;
    case ExpectedValueLen::NonZero:
        if (value.empty()) {
            cookie.setErrorContext("Request must include value");
            return Status::Einval;
        }
        break;
    case ExpectedValueLen::Any:
        break;
    }

    switch (expected_cas) {
    case ExpectedCas::NotSet:
        if (header.getCas() != 0) {
            cookie.setErrorContext("Request CAS must not be set");
            return Status::Einval;
        }
        break;
    case ExpectedCas::Set:
        if (header.getCas() == 0) {
            cookie.setErrorContext("Request CAS must be set");
            return Status::Einval;
        }
        break;
    case ExpectedCas::Any:
        break;
    }

    if ((~expected_datatype_mask) & header.getDatatype()) {
        cookie.setErrorContext("Request datatype invalid");
        return Status::Einval;
    }

    if (!connection.isDatatypeEnabled(header.getDatatype())) {
        uint8_t result = 0;
        const auto datatypes = header.getDatatype();
        for (int ii = 0; ii < 8; ++ii) {
            const auto bit = uint8_t(1 << ii);
            if ((bit & datatypes) != bit) {
                continue;
            }
            if (mcbp::datatype::is_valid(protocol_binary_datatype_t(ii))) {
                if (!connection.isDatatypeEnabled(bit)) {
                    result |= bit;
                }
            }
        }

        cookie.setErrorContext(
                "Datatype (" +
                mcbp::datatype::to_string(protocol_binary_datatype_t(result)) +
                ") not enabled for the connection");
        return Status::Einval;
    }

    if (!cookie.inflateInputPayload(header)) {
        // Error reason already set
        return Status::Einval;
    }

    if (!is_valid_xattr_blob(cookie, request)) {
        cookie.setErrorContext("The provided xattr segment is not valid");
        return Status::XattrEinval;
    }

    // Validate the frame id's
    auto status = Status::Success;
    auto opcode = request.getClientOpcode();

    try {
        request.parseFrameExtras([&status, &cookie, &opcode](
                                         cb::mcbp::request::FrameInfoId id,
                                         cb::const_byte_buffer data) -> bool {
            switch (id) {
            case cb::mcbp::request::FrameInfoId::Barrier:
                if (!data.empty()) {
                    cookie.setErrorContext("Barrier should not contain value");
                    status = Status::Einval;
                    // terminate parsing
                    return false;
                } else {
                    cookie.setBarrier();
                }
                return true;
            case cb::mcbp::request::FrameInfoId::DurabilityRequirement:
                try {
                    cb::durability::Requirements req(data);
                    if (!cb::mcbp::is_durability_supported(opcode)) {
                        status = Status::Einval;
                        cookie.setErrorContext(
                                R"(The requested command does not support durability requirements)");
                        // terminate parsing
                        return false;
                    }
                    return true;
                } catch (const std::exception& exception) {
                    // According to the spec the size may be 1 byte
                    // indicating the level and 2 optional bytes indicating
                    // the timeout.
                    if (data.size() == 1 || data.size() == 3) {
                        status = Status::DurabilityInvalidLevel;
                    } else {
                        status = Status::Einval;
                    }
                    std::string msg(exception.what());
                    // trim off the exception prefix
                    const std::string prefix{"Requirements(): "};
                    if (msg.find(prefix) == 0) {
                        msg = msg.substr(prefix.size());
                    }
                    cookie.setErrorContext(msg);
                    return false;
                }
            case cb::mcbp::request::FrameInfoId::DcpStreamId:
                if (data.size() != sizeof(cb::mcbp::DcpStreamId)) {
                    status = Status::Einval;
                    cookie.setErrorContext("DcpStreamId invalid size:" +
                                           std::to_string(data.size()));
                    return false;
                }
                return true;
            case cb::mcbp::request::FrameInfoId::OpenTracingContext:
                if (data.empty()) {
                    status = Status::Einval;
                    cookie.setErrorContext("OpenTracingContext cannot be empty");
                    return false;
                } else {
                    // Ideally we should only validate the packet here,
                    // but given that we've parsed the packet and found all
                    // the data I need I can might as well store it to avoid
                    // having to parse it again just to pick out the value
                    cookie.setOpenTracingContext(data);
                }
                return true;
            case cb::mcbp::request::FrameInfoId::Impersonate:
                if (data.empty()) {
                    cookie.setErrorContext("Impersonated user must be set");
                    status = Status::Einval;
                    // terminate parsing
                    return false;
                }

                if (data.front() == '^') {
                    status = cookie.setEffectiveUser(
                            {std::string{reinterpret_cast<const char*>(
                                                 data.data() + 1),
                                         data.size() - 1},
                             cb::rbac::Domain::External});
                } else {
                    status = cookie.setEffectiveUser(
                            {std::string{
                                     reinterpret_cast<const char*>(data.data()),
                                     data.size()},
                             cb::rbac::Domain::Local});
                }

                return status == Status::Success;
            case cb::mcbp::request::FrameInfoId::PreserveTtl:
                if (data.empty()) {
                    if (cb::mcbp::is_preserve_ttl_supported(opcode)) {
                        cookie.setPreserveTtl(true);
                    } else {
                        status = Status::NotSupported;
                        cookie.setErrorContext(
                                "This command does not support PreserveTtl");
                    }
                } else {
                    status = Status::Einval;
                    cookie.setErrorContext(
                            "PreserveTtl should not contain value");
                }
                return status == Status::Success;
            case cb::mcbp::request::FrameInfoId::ImpersonateExtraPrivilege:
                if (data.empty()) {
                    cookie.setErrorContext("Privilege name must be set");
                    status = Status::Einval;
                    return false;
                }
                try {
                    cookie.addImposedUserExtraPrivilege(
                            cb::rbac::to_privilege(std::string{
                                    reinterpret_cast<const char*>(data.data()),
                                    data.size()}));
                } catch (const std::invalid_argument&) {
                    cookie.setErrorContext("Failed to look up the privilege");
                    status = Status ::Einval;
                    return false;
                }
                return true; // continue parsing
            } // switch (id)
            status = Status::UnknownFrameInfo;
            return false;
        });
    } catch (const std::overflow_error&) {
        status = Status::Einval;
        cookie.setErrorContext("Invalid encoding in FrameExtras");
    }

    if (status != Status::Success) {
        return status;
    }

    // We need to look up SID and CID if this command is related to a
    // collection. Looking up the SID caused a perf regression (MB-39594) and
    // we don't really need to look up the SID if the privilege context
    // include all scopes and collections. If the command is executed with
    // a different user we don't have that users privilege context yet, so
    // we would have to look up SID and CID (MB-47904)
    if (connection.getBucket().type != BucketType::NoBucket &&
        cb::mcbp::is_collection_command(request.getClientOpcode()) &&
        connection.getBucket().supports(cb::engine::Feature::Collections) &&
        (cookie.getPrivilegeContext().hasScopePrivileges() ||
         cookie.getEffectiveUser().has_value())) {
        // verify that we can map the connection to sid. To make our
        // unit tests easier lets go through the connection
        auto key = cookie.getRequestKey();
        ScopeID sid;
        uint64_t manifestUid = 0;

        if (key.getCollectionID().isDefaultCollection()) {
            sid = ScopeID{ScopeID::Default};
        } else {
            auto vbid = request.getVBucket();
            auto res = connection.getBucket().getEngine().get_scope_id(
                    cookie, key.getCollectionID(), vbid);
            if (res.result == cb::engine_errc::success) {
                manifestUid = res.getManifestId();
                sid = res.getScopeId();
            } else if (res.result == cb::engine_errc::unknown_collection) {
                // Could not get the collection's scope - an unknown collection
                // against the manifest with id stored in res.first.
                cookie.setUnknownCollectionErrorContext(res.getManifestId());
                return Status::UnknownCollection;
            } else {
                return cb::mcbp::to_status(res.result);
            }
        }
        cookie.setCurrentCollectionInfo(
                sid, key.getCollectionID(), manifestUid);
    }

    return Status::Success;
}

/******************************************************************************
 *                         Package validators                                 *
 *****************************************************************************/

/**
 * Verify that the cookie meets the common DCP restrictions:
 *
 * a) The connected engine supports DCP
 * b) The connection cannot be set into the unordered execution mode.
 *
 * @param cookie The command cookie
 */
static Status verify_common_dcp_restrictions(Cookie& cookie) {
    const auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    if (!dcp) {
        cookie.setErrorContext("Attached bucket does not support DCP");
        return Status::NotSupported;
    }

    if (connection.allowUnorderedExecution()) {
        LOG_WARNING(
                "DCP on a connection with unordered execution is currently "
                "not supported: {}",
                get_peer_description(cookie));
        cookie.setErrorContext(
                "DCP on connections with unordered execution is not supported");
        return Status::NotSupported;
    }

    using cb::mcbp::ClientOpcode;
    const auto opcode = cookie.getRequest().getClientOpcode();
    if (opcode == ClientOpcode::DcpOpen) {
        if (connection.isDCP()) {
            LOG_DEBUG("{}: Can't do DCP OPEN twice", cookie.getConnectionId());
            cookie.setErrorContext(
                    "The connection is already opened as a DCP connection");
            return Status::Einval;
        }
    } else if (opcode != ClientOpcode::GetFailoverLog) {
        if (!connection.isDCP()) {
            LOG_DEBUG("{}: Can't send {} on connection before DCP open",
                      cookie.getConnectionId(),
                      to_string(cookie.getRequest().getClientOpcode()));
            cookie.setErrorContext(
                    "The command can only be sent on a DCP connection");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status dcp_open_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpOpenPayload;

    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(DcpOpenPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Any,
            ExpectedCas::Any,
            PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    // Validate the flags.
    const auto mask =
            DcpOpenPayload::Producer | DcpOpenPayload::IncludeXattrs |
            DcpOpenPayload::NoValue | DcpOpenPayload::IncludeDeleteTimes |
            DcpOpenPayload::NoValueWithUnderlyingDatatype |
            DcpOpenPayload::PiTR | DcpOpenPayload::IncludeDeletedUserXattrs;

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<DcpOpenPayload>();
    const auto flags = payload.getFlags();

    if (flags & ~mask) {
        LOG_INFO(
                "Client trying to open dcp stream with unknown flags ({:x}) {}",
                flags,
                get_peer_description(cookie));
        cookie.setErrorContext("Request contains invalid flags");
        return Status::Einval;
    }

    if ((flags & DcpOpenPayload::Producer) == 0 &&
        (flags & DcpOpenPayload::PiTR) == DcpOpenPayload::PiTR) {
        cookie.setErrorContext("PiTR require Producer to be set");
        return Status::Einval;
    }

    if ((flags & DcpOpenPayload::NoValue) &&
        (flags & DcpOpenPayload::NoValueWithUnderlyingDatatype)) {
        LOG_INFO(
                "Invalid flags combination ({:x}) specified for a DCP "
                "consumer {} - cannot specify NO_VALUE with "
                "NO_VALUE_WITH_UNDERLYING_DATATYPE",
                flags,
                get_peer_description(cookie));
        cookie.setErrorContext(
                "Request contains invalid flags combination (NO_VALUE && "
                "NO_VALUE_WITH_UNDERLYING_DATATYPE)");
        return Status::Einval;
    }

    if ((flags & DcpOpenPayload::IncludeDeletedUserXattrs) &&
        !(flags & DcpOpenPayload::IncludeXattrs)) {
        LOG_INFO(
                "Invalid DcpOpen flags combination ({:x}) specified for {} - "
                "Must specify IncludeXattrs for IncludeDeletedUserXattrs",
                flags,
                get_peer_description(cookie));
        cookie.setErrorContext(
                "Request contains invalid flags combination - "
                "IncludeDeletedUserXattrs but not IncludeXattrs");
        return Status::Einval;
    }

    // Validate the value. If non-empty must be a JSON payload.
    const auto value = cookie.getHeader().getValueString();
    const auto datatype = cookie.getHeader().getDatatype();
    if (value.empty()) {
        if (datatype != PROTOCOL_BINARY_RAW_BYTES) {
            cookie.setErrorContext("datatype should be set to RAW");
            return Status::Einval;
        }
    } else {
        if (datatype != PROTOCOL_BINARY_DATATYPE_JSON) {
            cookie.setErrorContext(
                    "datatype should be set to JSON for non-empty value");
            return Status::Einval;
        }
        try {
            auto json = nlohmann::json::parse(value);
            if (!json.is_object()) {
                cookie.setErrorContext("value must be JSON of type object");
                return Status::Einval;
            }
            for (const auto& kv : json.items()) {
                if (kv.key() == "consumer_name") {
                    if (flags & DcpOpenPayload::Producer) {
                        cookie.setErrorContext(
                                "consumer_name only valid for Consumer "
                                "connections");
                        return Status::Einval;
                    }
                    if (!kv.value().is_string()) {
                        cookie.setErrorContext(
                                "consumer_name must be a string");
                        return Status::Einval;
                    }
                } else {
                    cookie.setErrorContext("Unsupported JSON property " +
                                           kv.key());
                    return Status::Einval;
                }
            }
        } catch (const std::exception&) {
            cookie.setErrorContext("value is not valid JSON");
            return Status::Einval;
        }
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_add_stream_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpAddStreamPayload;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(DcpAddStreamPayload),
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    auto& req = cookie.getRequest();
    const auto& payload = req.getCommandSpecifics<DcpAddStreamPayload>();

    const uint32_t flags = payload.getFlags();
    const auto mask =
            DCP_ADD_STREAM_FLAG_TAKEOVER | DCP_ADD_STREAM_FLAG_DISKONLY |
            DCP_ADD_STREAM_FLAG_TO_LATEST | DCP_ADD_STREAM_ACTIVE_VB_ONLY |
            DCP_ADD_STREAM_FLAG_FROM_LATEST;

    if (flags & ~mask) {
        if (flags & DCP_ADD_STREAM_FLAG_NO_VALUE) {
            // MB-22525 The NO_VALUE flag should be passed to DCP_OPEN
            LOG_INFO("Client trying to add stream with NO VALUE {}",
                     get_peer_description(cookie));
            cookie.setErrorContext(
                    "DCP_ADD_STREAM_FLAG_NO_VALUE{8} flag is no longer used");
        } else {
            LOG_INFO("Client trying to add stream with unknown flags ({:x}) {}",
                     flags,
                     get_peer_description(cookie));
            cookie.setErrorContext("Request contains invalid flags");
        }
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_close_stream_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_get_failover_log_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_stream_req_validator(Cookie& cookie) {
    auto vlen = cookie.getConnection().isCollectionsSupported()
                        ? ExpectedValueLen::Any
                        : ExpectedValueLen::Zero;

    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::DcpStreamReqPayload),
            ExpectedKeyLen::Zero,
            vlen,
            ExpectedCas::Any,
            PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_stream_end_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::DcpStreamEndPayload),
            ExpectedKeyLen::Zero,
            ExpectedValueLen::Zero,
            ExpectedCas::Any,
            PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_snapshot_marker_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();

    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    using cb::mcbp::request::DcpSnapshotMarkerV2_0Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2xPayload;
    using cb::mcbp::request::DcpSnapshotMarkerV2xVersion;

    // Validate our extras length is correct
    if (!(header.getExtlen() == sizeof(DcpSnapshotMarkerV1Payload) ||
          header.getExtlen() == sizeof(DcpSnapshotMarkerV2xPayload))) {
        return Status::Einval;
    }

    // If v2.x validate the value length is expected
    auto expectedValueLen = ExpectedValueLen::Zero;
    if (header.getExtlen() == sizeof(DcpSnapshotMarkerV2xPayload)) {
        const auto& payload =
                header.getRequest()
                        .getCommandSpecifics<DcpSnapshotMarkerV2xPayload>();
        size_t expectedLen = sizeof(DcpSnapshotMarkerV2_0Value);
        expectedValueLen = ExpectedValueLen::Any;
        if (payload.getVersion() != DcpSnapshotMarkerV2xVersion::Zero) {
            cookie.setErrorContext(
                    "Unsupported dcp snapshot version:" +
                    std::to_string(uint32_t(payload.getVersion())));
            return Status::Einval;
        }

        if (header.getValue().size() != expectedLen) {
            cookie.setErrorContext("valuelen not expected:" +
                                   std::to_string(expectedLen));
            return Status::Einval;
        }
    }

    // Pass the extras len in because we will check it manually as it is
    // variable length
    auto status = McbpValidator::verify_header(cookie,
                                               header.getExtlen(),
                                               ExpectedKeyLen::Zero,
                                               expectedValueLen,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_system_event_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpSystemEventPayload;
    auto status =
            McbpValidator::verify_header(cookie,
                                         sizeof(DcpSystemEventPayload),
                                         ExpectedKeyLen::Any,
                                         ExpectedValueLen::Any,
                                         ExpectedCas::Any,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<DcpSystemEventPayload>();

    if (!payload.isValidEvent()) {
        cookie.setErrorContext("Invalid system event id");
        return Status::Einval;
    }

    if (!payload.isValidVersion()) {
        cookie.setErrorContext("Invalid system event version");
        return Status::Einval;
    }

    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_mutation_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpMutationPayload;

    auto status =
            McbpValidator::verify_header(cookie,
                                         sizeof(DcpMutationPayload),
                                         ExpectedKeyLen::NonZero,
                                         ExpectedValueLen::Any,
                                         ExpectedCas::Any,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
    }

    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<DcpMutationPayload>();
    if (payload.getBySeqno() == 0) {
        cookie.setErrorContext("Invalid seqno(0) for DCP mutation");
        return Status::Einval;
    }

    return verify_common_dcp_restrictions(cookie);
}

/// @return true if the datatype is valid for a deletion
static bool valid_dcp_delete_datatype(protocol_binary_datatype_t datatype) {
    // MB-29040: Allowing xattr + JSON. A bug in the producer means
    // it may send XATTR|JSON (with snappy possible). These are now allowed
    // so rebalance won't be failed and the consumer will sanitise the faulty
    // documents.
    // MB-31141: Allowing RAW+Snappy. A bug in delWithMeta has allowed us to
    // create deletes with a non-zero value tagged as RAW, which when snappy
    // is enabled gets DCP shipped as RAW+Snappy.
    std::array<const protocol_binary_datatype_t, 6> valid = {
            {PROTOCOL_BINARY_RAW_BYTES,
             PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_SNAPPY,
             PROTOCOL_BINARY_DATATYPE_XATTR,
             PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_SNAPPY,
             PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON,
             PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_SNAPPY |
                     PROTOCOL_BINARY_DATATYPE_JSON}};
    for (auto d : valid) {
        if (datatype == d) {
            return true;
        }
    }
    return false;
}

static Status dcp_deletion_validator(Cookie& cookie) {
    const size_t expectedExtlen =
            may_accept_dcp_deleteV2(cookie)
                    ? sizeof(cb::mcbp::request::DcpDeletionV2Payload)
                    : sizeof(cb::mcbp::request::DcpDeletionV1Payload);

    auto status =
            McbpValidator::verify_header(cookie,
                                         gsl::narrow<uint8_t>(expectedExtlen),
                                         ExpectedKeyLen::NonZero,
                                         ExpectedValueLen::Any,
                                         ExpectedCas::Any,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
    }

    if (!valid_dcp_delete_datatype(cookie.getHeader().getDatatype())) {
        cookie.setErrorContext("Request datatype invalid");
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_expiration_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::DcpExpirationPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Any,
            ExpectedCas::Any,
            McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_set_vbucket_state_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpSetVBucketState;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(DcpSetVBucketState),
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<DcpSetVBucketState>();
    if (!payload.isValid()) {
        cookie.setErrorContext("Request body state invalid");
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_noop_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_buffer_acknowledgement_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::DcpBufferAckPayload),
            ExpectedKeyLen::Zero,
            ExpectedValueLen::Zero,
            ExpectedCas::Any,
            PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_control_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static bool isValidDurabilityLevel(cb::durability::Level lvl) {
    switch (lvl) {
    case cb::durability::Level::None:
        return false;
    case cb::durability::Level::Majority:
    case cb::durability::Level::MajorityAndPersistOnMaster:
    case cb::durability::Level::PersistToMajority:
        return true;
    }
    return false;
}

static Status dcp_prepare_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpPreparePayload;
    auto status =
            McbpValidator::verify_header(cookie,
                                         sizeof(DcpPreparePayload),
                                         ExpectedKeyLen::NonZero,
                                         ExpectedValueLen::Any,
                                         ExpectedCas::Set,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
    }

    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<DcpPreparePayload>();

    if (payload.getBySeqno() == 0) {
        cookie.setErrorContext("Invalid seqno(0) for DCP prepare");
        return Status::Einval;
    }

    if (!isValidDurabilityLevel(payload.getDurabilityLevel())) {
        cookie.setErrorContext("Invalid durability specifier");
        return Status::Einval;
    }

    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_seqno_acknowledged_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpSeqnoAcknowledgedPayload;
    auto status =
            McbpValidator::verify_header(cookie,
                                         sizeof(DcpSeqnoAcknowledgedPayload),
                                         ExpectedKeyLen::Zero,
                                         ExpectedValueLen::Zero,
                                         ExpectedCas::NotSet,
                                         PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_commit_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpCommitPayload;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(DcpCommitPayload),
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_abort_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpAbortPayload;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(DcpAbortPayload),
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status update_user_permissions_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::NonZero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status configuration_refresh_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status auth_provider_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status drop_privilege_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_cluster_config_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status set_cluster_config_validator(Cookie& cookie) {
    using cb::mcbp::request::SetClusterConfigPayload;

    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(SetClusterConfigPayload),
            ExpectedKeyLen::Any,
            ExpectedValueLen::NonZero,
            ExpectedCas::Any,
            PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON);

    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<SetClusterConfigPayload>();
    if (payload.getRevision() < 1) {
        cookie.setErrorContext("Revision number must not be less than 1");
        return Status::Einval;
    }

    if (payload.getEpoch() < 1 && payload.getEpoch() != -1) {
        cookie.setErrorContext("Epoch must not be less than 1 (or -1)");
        return Status::Einval;
    }
    return Status::Success;
}

static Status verbosity_validator(Cookie& cookie) {
    return McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::VerbosityPayload),
            ExpectedKeyLen::Zero,
            ExpectedValueLen::Zero,
            ExpectedCas::NotSet,
            PROTOCOL_BINARY_RAW_BYTES);
}

static Status hello_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Any,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    auto& req = cookie.getRequest();
    auto value = req.getValue();
    if ((value.size() % 2) != 0) {
        cookie.setErrorContext("Request value must be of even length");
        return Status::Einval;
    }

    return Status::Success;
}

static Status version_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status quit_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status sasl_list_mech_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status sasl_auth_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Any,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status noop_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status flush_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();
    uint8_t extlen = header.getExtlen();

    if (extlen != 0 && extlen != 4) {
        cookie.setErrorContext("Request extras must be of length 0 or 4");
        return Status::Einval;
    }
    // We've already checked extlen so pass actual extlen as expected extlen
    auto status = McbpValidator::verify_header(cookie,
                                               extlen,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    if (extlen == 4) {
        auto& req = header.getRequest();
        auto extdata = req.getExtdata();
        if (*reinterpret_cast<const uint32_t*>(extdata.data()) != 0) {
            cookie.setErrorContext("Delayed flush no longer supported");
            return Status::NotSupported;
        }
    }

    return Status::Success;
}

static Status add_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must have extras and key, may have value */
    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::MutationPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Any,
            ExpectedCas::NotSet,
            expected_datatype_mask);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status set_replace_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must have extras and key, may have value */
    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::MutationPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Any,
            ExpectedCas::Any,
            expected_datatype_mask);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status append_prepend_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must not have extras, must have key, may have value */
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::Any,
                                               expected_datatype_mask);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status gat_validator(Cookie& cookie) {
    auto status =
            McbpValidator::verify_header(cookie,
                                         sizeof(cb::mcbp::request::GatPayload),
                                         ExpectedKeyLen::NonZero,
                                         ExpectedValueLen::Zero,
                                         ExpectedCas::NotSet,
                                         PROTOCOL_BINARY_RAW_BYTES);

    if (status != Status::Success) {
        return status;
    }

    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status delete_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status stat_validator(Cookie& cookie) {
    auto ret = McbpValidator::verify_header(
            cookie,
            0,
            ExpectedKeyLen::Any,
            ExpectedValueLen::Any,
            ExpectedCas::NotSet,
            PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON);
    if (ret != Status::Success) {
        return ret;
    }

    const auto& req = cookie.getRequest();
    auto value = req.getValueString();
    if (!value.empty()) {
        // The value must be JSON
        if ((uint8_t(req.getDatatype()) & PROTOCOL_BINARY_DATATYPE_JSON) == 0) {
            cookie.setErrorContext("Datatype must be JSON");
            return Status::Einval;
        }

        // Validate that the value is JSON
        try {
            const auto json = nlohmann::json::parse(value);
        } catch (const std::exception&) {
            cookie.setErrorContext("value is not valid JSON");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status arithmetic_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::ArithmeticPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Zero,
            ExpectedCas::NotSet,
            PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_cmd_timer_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        1,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status set_ctrl_token_validator(Cookie& cookie) {
    using cb::mcbp::request::SetCtrlTokenPayload;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(SetCtrlTokenPayload),
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<SetCtrlTokenPayload>();
    if (payload.getCas() == mcbp::cas::Wildcard) {
        cookie.setErrorContext("New CAS must be set");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_ctrl_token_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status ioctl_get_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status ioctl_set_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Any,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status audit_put_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        4,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::NonZero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status audit_config_reload_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status config_reload_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status config_validate_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::NonZero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status observe_seqno_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (cookie.getHeader().getBodylen() != 8) {
        cookie.setErrorContext("Request value must be of length 8");
        return Status::Einval;
    }
    return Status::Success;
}

/**
 * The create bucket contains message have the following format:
 *    key: bucket name
 *    body: module\nconfig
 */
static Status create_bucket_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    const auto& req = cookie.getRequest();
    const auto k = req.getKey();
    auto error = BucketValidator::validateBucketName(
            {reinterpret_cast<const char*>(k.data()), k.size()});
    if (!error.empty()) {
        cookie.setErrorContext(error);
        return Status::Einval;
    }

    const auto v = req.getValue();
    std::string value(reinterpret_cast<const char*>(v.data()), v.size());
    auto marker = value.find('\0');
    if (marker != std::string::npos) {
        value.resize(marker);
    }

    switch (module_to_bucket_type(value)) {
    case BucketType::Unknown:
        cookie.setErrorContext("Unknown bucket type");
        return Status::Einval;

    case BucketType::NoBucket:
        cookie.setErrorContext(
                "The internal bucket type nobucket.so can't be created");
        return Status::NotSupported;

    case BucketType::Memcached:
    case BucketType::Couchbase:
    case BucketType::EWouldBlock:
        break;
    }

    return Status::Success;
}

static Status list_bucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::Any,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status delete_bucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Any,
                                        ExpectedCas::Any,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status select_bucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::Any,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_all_vb_seqnos_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();

    // We check extlen below so pass actual extlen as expected_extlen to bypass
    // the check in McbpValidator::verify_header
    auto status = McbpValidator::verify_header(cookie,
                                               header.getExtlen(),
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    auto extras = header.getRequest().getExtdata();

    if (!extras.empty()) {
        // extlen is optional, and if non-zero it contains the vbucket
        // state to report and potentially also a collection ID
        if (extras.size() != sizeof(RequestedVBState) &&
            extras.size() !=
                    sizeof(RequestedVBState) + sizeof(CollectionIDType)) {
            cookie.setErrorContext("Request extras must be of length 0 or " +
                                   std::to_string(sizeof(RequestedVBState)) +
                                   " or " +
                                   std::to_string(sizeof(RequestedVBState) +
                                                  sizeof(CollectionIDType)));
            return Status::Einval;
        }
        RequestedVBState state;

        // vbucket state will be the first part of extras
        auto extrasState = extras.substr(0, sizeof(RequestedVBState));
        std::copy(extrasState.begin(),
                  extrasState.end(),
                  reinterpret_cast<uint8_t*>(&state));
        state = static_cast<RequestedVBState>(ntohl(static_cast<int>(state)));
        if (state < RequestedVBState::Alive || state > RequestedVBState::Dead) {
            cookie.setErrorContext("Request vbucket state invalid");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status shutdown_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::Set,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_meta_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();

    // We check extlen below so pass actual extlen as expected_extlen to bypass
    // the check in McbpValidator::verify_header
    auto status = McbpValidator::verify_header(cookie,
                                               header.getExtlen(),
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    auto extras = header.getRequest().getExtdata();

    if (extras.size() > 1) {
        cookie.setErrorContext("Request extras must be of length 0 or 1");
        return Status::Einval;
    }

    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    if (extras.size() == 1) {
        if (extras[0] > 2) {
            // 1 == return conflict resolution mode
            // 2 == return datatype
            cookie.setErrorContext("Request extras invalid");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status mutate_with_meta_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();

    // We check extlen below so pass actual extlen as expected_extlen to bypass
    // the check in McbpValidator::verify_header
    auto status =
            McbpValidator::verify_header(cookie,
                                         header.getExtlen(),
                                         ExpectedKeyLen::NonZero,
                                         ExpectedValueLen::Any,
                                         ExpectedCas::Any,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    // revid_nbytes, flags and exptime is mandatory fields.. and we need a key
    // extlen, the size dicates what is encoded.
    switch (header.getExtlen()) {
    case 24: // no nmeta and no options
    case 26: // nmeta
    case 28: // options (4-byte field)
    case 30: // options and nmeta (options followed by nmeta)
        break;
    default:
        cookie.setErrorContext("Request extras invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_errmap_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    const auto& request = cookie.getRequest();
    if (request.getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    if (request.getValue().size() !=
        sizeof(cb::mcbp::request::GetErrmapPayload)) {
        cookie.setErrorContext("Request value must be of length 2");
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_locked_validator(Cookie& cookie) {
    const auto extlen = cookie.getHeader().getExtlen();

    // We check extlen below so pass actual extlen as expected extlen to bypass
    // the check in verify header
    auto status = McbpValidator::verify_header(cookie,
                                               extlen,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }
    if (extlen != 0 && extlen != sizeof(cb::mcbp::request::GetLockedPayload)) {
        cookie.setErrorContext("Request extras must be of length 0 or 4");
        return Status::Einval;
    }

    return Status::Success;
}

static Status unlock_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Set,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status evict_key_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status collections_set_manifest_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (cookie.getRequest().getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    return Status::Success;
}

static Status collections_get_manifest_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (cookie.getRequest().getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    return Status::Success;
}

static Status collections_get_id_validator(Cookie& cookie) {
    // Temporarily support the input in the key or the value.
    // For GA we will only support input in the value
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Any,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    const auto& header = cookie.getHeader();
    // now expect value or key, but not both
    if (header.getKeylen() && header.getValue().size()) {
        cookie.setErrorContext("Cannot set both key and value");
        return Status::Einval;
    }

    if (header.getRequest().getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    return Status::Success;
}

static Status adjust_timeofday_validator(Cookie& cookie) {
    using cb::mcbp::request::AdjustTimePayload;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(AdjustTimePayload),
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    // The method should only be available for unit tests
    if (getenv("MEMCACHED_UNIT_TESTS") == nullptr) {
        cookie.setErrorContext("Only available for unit tests");
        return Status::NotSupported;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<AdjustTimePayload>();
    if (!payload.isValid()) {
        cookie.setErrorContext("Unexpected value for TimeType");
    }

    return Status::Success;
}

static Status ewb_validator(Cookie& cookie) {
    auto status =
            McbpValidator::verify_header(cookie,
                                         sizeof(cb::mcbp::request::EWB_Payload),
                                         ExpectedKeyLen::Any,
                                         ExpectedValueLen::Zero,
                                         ExpectedCas::NotSet,
                                         PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    // The method should only be available for unit tests
    if (getenv("MEMCACHED_UNIT_TESTS") == nullptr) {
        cookie.setErrorContext("Only available for unit tests");
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status scrub_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_random_key_validator(Cookie& cookie) {
    uint8_t extras = 0;
    // client is required to send the collection-id
    if (cookie.getConnection().isCollectionsSupported()) {
        extras = sizeof(CollectionIDType);
    }
    return McbpValidator::verify_header(cookie,
                                        extras,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

/**
 * The SetVbucket may contain 3 different encodings:
 *
 * 1) extras contain the vbucket state in 4 bytes (pre MadHatter). Dataype
 *    must be RAW
 * 2) body may contain the vbucket state in 4 bytes (ns_server in pre
 *    MadHatter). Datatype must be RAW
 * 3) 1 byte extras containing the VBUCKET state.
 *    There might be a body, which has to have the DATATYPE bit set to
 *    JSON. Introduced in MadHatter. (The executor needs to validate
 *    the case that it is valid JSON as otherwise we'd have to parse
 *    it just to throw it away)
 */
static Status set_vbucket_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();
    auto status = McbpValidator::verify_header(
            cookie,
            header.getExtlen(),
            ExpectedKeyLen::Zero,
            ExpectedValueLen::Any,
            ExpectedCas::Any,
            PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    vbucket_state_t state;
    auto extras = header.getExtdata();
    auto value = header.getValue();
    auto datatype = header.getDatatype();

    if (extras.size() == 1) {
        // This is the new-style setVbucket
        state = static_cast<vbucket_state_t>(extras.front());
        if (value.empty()) {
            if (datatype != PROTOCOL_BINARY_RAW_BYTES) {
                cookie.setErrorContext("datatype should be set to RAW");
                return Status::Einval;
            }
        } else {
            if (datatype != PROTOCOL_BINARY_DATATYPE_JSON) {
                cookie.setErrorContext("datatype should be set to JSON");
                return Status::Einval;
            }
        }
    } else if (extras.size() == sizeof(vbucket_state_t) && value.empty() &&
               datatype == PROTOCOL_BINARY_RAW_BYTES) {
        state = vbucket_state_t(
                ntohl(*reinterpret_cast<const uint32_t*>(extras.data())));
    } else if (extras.empty() && value.size() == sizeof(vbucket_state_t) &&
               datatype == PROTOCOL_BINARY_RAW_BYTES) {
        state = vbucket_state_t(
                ntohl(*reinterpret_cast<const uint32_t*>(value.data())));
    } else {
        // packet is incorrect
        cookie.setErrorContext(
                "Invalid format. Use 1 byte state in extras and an optional "
                "JSON value");
        return Status::Einval;
    }

    if (!is_valid_vbucket_state_t(state)) {
        cookie.setErrorContext("The provided vbucket state is invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status del_vbucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Any,
                                        ExpectedCas::Any,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_vbucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status start_stop_persistence_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status enable_disable_traffic_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status ifconfig_validator(Cookie& cookie) {
    const auto status =
            McbpValidator::verify_header(cookie,
                                         0,
                                         ExpectedKeyLen::NonZero,
                                         ExpectedValueLen::Any,
                                         ExpectedCas::NotSet,
                                         PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto& req = cookie.getRequest();

    const auto keybuf = req.getKey();
    const std::string_view key{reinterpret_cast<const char*>(keybuf.data()),
                               keybuf.size()};
    const auto value = req.getValueString();
    if (key == "list") {
        if (!value.empty()) {
            cookie.setErrorContext("Request must not include value");
            return Status::Einval;
        }
        return Status::Success;
    }

    if (key == "tls") {
        if (!value.empty()) {
            try {
                TlsConfiguration::validate(nlohmann::json::parse(value));
            } catch (const std::invalid_argument& e) {
                cookie.setErrorContext(e.what());
                return Status::Einval;
            } catch (const std::exception&) {
                cookie.setErrorContext("value is not valid JSON");
                return Status::Einval;
            }
        }

        return Status::Success;
    }

    if (value.empty()) { // Define and Delete must have value
        cookie.setErrorContext("Request must include value");
        return Status::Einval;
    }

    if (key == "define") {
        try {
            const auto descr =
                    NetworkInterfaceDescription(nlohmann::json::parse(value));
        } catch (const std::invalid_argument& e) {
            cookie.setErrorContext(e.what());
            return Status::Einval;
        } catch (const std::exception&) {
            cookie.setErrorContext("value is not valid JSON");
            return Status::Einval;
        }

        return Status::Success;
    }

    if (key == "delete") {
        return Status::Success;
    }

    cookie.setErrorContext(R"(Key must be "define", "delete", "list", "tls")");
    return Status::Einval;
}

static Status get_keys_validator(Cookie& cookie) {
    const auto extlen = cookie.getHeader().getExtlen();
    auto status = McbpValidator::verify_header(cookie,
                                               extlen,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    if (extlen != 0 && extlen != sizeof(uint32_t)) {
        cookie.setErrorContext(
                "Expected 4 bytes of extras containing the number of keys to "
                "get");
        return Status::Einval;
    }

    return Status::Success;
}

static Status set_param_validator(Cookie& cookie) {
    using cb::mcbp::request::SetParamPayload;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(SetParamPayload),
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);

    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<SetParamPayload>();
    if (!payload.validate()) {
        cookie.setErrorContext("Invalid param type specified");
    }

    return Status::Success;
}

static Status return_meta_validator(Cookie& cookie) {
    using cb::mcbp::request::ReturnMetaPayload;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(ReturnMetaPayload),
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    if (!is_document_key_valid(cookie)) {
        return Status::Einval;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<ReturnMetaPayload>();
    switch (payload.getMutationType()) {
    case cb::mcbp::request::ReturnMetaType::Set:
    case cb::mcbp::request::ReturnMetaType::Add:
    case cb::mcbp::request::ReturnMetaType::Del:
        return Status::Success;
    }

    cookie.setErrorContext("Invalid mode");
    return Status::Einval;
}

static Status seqno_persistence_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        8,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status last_closed_checkpoint_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status create_checkpoint_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status compact_db_validator(Cookie& cookie) {
    using cb::mcbp::request::CompactDbPayload;

    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(CompactDbPayload),
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<CompactDbPayload>();
    if (!payload.validate()) {
        cookie.setErrorContext("Padding bytes should be set to 0");
        return Status::Einval;
    }

    return Status::Success;
}

static Status observe_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::NonZero,
                                        ExpectedCas::NotSet,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status not_supported_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();
    auto status = McbpValidator::verify_header(cookie,
                                               header.getExtlen(),
                                               ExpectedKeyLen::Any,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::Any,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    return Status::NotSupported;
}

Status McbpValidator::validate(ClientOpcode command, Cookie& cookie) {
    const auto idx = std::underlying_type<ClientOpcode>::type(command);
    if (validators[idx]) {
        return validators[idx](cookie);
    }
    return Status::UnknownCommand;
}

void McbpValidator::setup(ClientOpcode command, Status (*f)(Cookie&)) {
    validators[std::underlying_type<ClientOpcode>::type(command)] = f;
}

McbpValidator::McbpValidator() {
    setup(cb::mcbp::ClientOpcode::DcpOpen, dcp_open_validator);
    setup(cb::mcbp::ClientOpcode::DcpAddStream, dcp_add_stream_validator);
    setup(cb::mcbp::ClientOpcode::DcpCloseStream, dcp_close_stream_validator);
    setup(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
          dcp_snapshot_marker_validator);
    setup(cb::mcbp::ClientOpcode::DcpDeletion, dcp_deletion_validator);
    setup(cb::mcbp::ClientOpcode::DcpExpiration, dcp_expiration_validator);
    setup(cb::mcbp::ClientOpcode::DcpGetFailoverLog,
          dcp_get_failover_log_validator);
    setup(cb::mcbp::ClientOpcode::DcpMutation, dcp_mutation_validator);
    setup(cb::mcbp::ClientOpcode::DcpSetVbucketState,
          dcp_set_vbucket_state_validator);
    setup(cb::mcbp::ClientOpcode::DcpNoop, dcp_noop_validator);
    setup(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
          dcp_buffer_acknowledgement_validator);
    setup(cb::mcbp::ClientOpcode::DcpControl, dcp_control_validator);
    setup(cb::mcbp::ClientOpcode::DcpStreamEnd, dcp_stream_end_validator);
    setup(cb::mcbp::ClientOpcode::DcpStreamReq, dcp_stream_req_validator);
    setup(cb::mcbp::ClientOpcode::DcpSystemEvent, dcp_system_event_validator);
    setup(cb::mcbp::ClientOpcode::DcpPrepare, dcp_prepare_validator);
    setup(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged,
          dcp_seqno_acknowledged_validator);
    setup(cb::mcbp::ClientOpcode::DcpCommit, dcp_commit_validator);
    setup(cb::mcbp::ClientOpcode::DcpAbort, dcp_abort_validator);
    setup(cb::mcbp::ClientOpcode::IsaslRefresh,
          configuration_refresh_validator);
    setup(cb::mcbp::ClientOpcode::SslCertsRefresh,
          configuration_refresh_validator);
    setup(cb::mcbp::ClientOpcode::Verbosity, verbosity_validator);
    setup(cb::mcbp::ClientOpcode::Hello, hello_validator);
    setup(cb::mcbp::ClientOpcode::Version, version_validator);
    setup(cb::mcbp::ClientOpcode::Quit, quit_validator);
    setup(cb::mcbp::ClientOpcode::Quitq, quit_validator);
    setup(cb::mcbp::ClientOpcode::SaslListMechs, sasl_list_mech_validator);
    setup(cb::mcbp::ClientOpcode::SaslAuth, sasl_auth_validator);
    setup(cb::mcbp::ClientOpcode::SaslStep, sasl_auth_validator);
    setup(cb::mcbp::ClientOpcode::Noop, noop_validator);
    setup(cb::mcbp::ClientOpcode::Flush, flush_validator);
    setup(cb::mcbp::ClientOpcode::Flushq, flush_validator);
    setup(cb::mcbp::ClientOpcode::Get, get_validator);
    setup(cb::mcbp::ClientOpcode::Getq, get_validator);
    setup(cb::mcbp::ClientOpcode::Getk, get_validator);
    setup(cb::mcbp::ClientOpcode::Getkq, get_validator);
    setup(cb::mcbp::ClientOpcode::Gat, gat_validator);
    setup(cb::mcbp::ClientOpcode::Gatq, gat_validator);
    setup(cb::mcbp::ClientOpcode::Touch, gat_validator);
    setup(cb::mcbp::ClientOpcode::Delete, delete_validator);
    setup(cb::mcbp::ClientOpcode::Deleteq, delete_validator);
    setup(cb::mcbp::ClientOpcode::Stat, stat_validator);
    setup(cb::mcbp::ClientOpcode::Increment, arithmetic_validator);
    setup(cb::mcbp::ClientOpcode::Incrementq, arithmetic_validator);
    setup(cb::mcbp::ClientOpcode::Decrement, arithmetic_validator);
    setup(cb::mcbp::ClientOpcode::Decrementq, arithmetic_validator);
    setup(cb::mcbp::ClientOpcode::GetCmdTimer, get_cmd_timer_validator);
    setup(cb::mcbp::ClientOpcode::SetCtrlToken, set_ctrl_token_validator);
    setup(cb::mcbp::ClientOpcode::GetCtrlToken, get_ctrl_token_validator);
    setup(cb::mcbp::ClientOpcode::IoctlGet, ioctl_get_validator);
    setup(cb::mcbp::ClientOpcode::IoctlSet, ioctl_set_validator);
    setup(cb::mcbp::ClientOpcode::AuditPut, audit_put_validator);
    setup(cb::mcbp::ClientOpcode::AuditConfigReload,
          audit_config_reload_validator);
    setup(cb::mcbp::ClientOpcode::ConfigReload, config_reload_validator);
    setup(cb::mcbp::ClientOpcode::ConfigValidate, config_validate_validator);
    setup(cb::mcbp::ClientOpcode::Shutdown, shutdown_validator);
    setup(cb::mcbp::ClientOpcode::ObserveSeqno, observe_seqno_validator);
    setup(cb::mcbp::ClientOpcode::GetAdjustedTime_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::SetDriftCounterState_Unsupported,
          not_supported_validator);

    setup(cb::mcbp::ClientOpcode::SubdocGet, subdoc_get_validator);
    setup(cb::mcbp::ClientOpcode::SubdocExists, subdoc_exists_validator);
    setup(cb::mcbp::ClientOpcode::SubdocDictAdd, subdoc_dict_add_validator);
    setup(cb::mcbp::ClientOpcode::SubdocDictUpsert,
          subdoc_dict_upsert_validator);
    setup(cb::mcbp::ClientOpcode::SubdocDelete, subdoc_delete_validator);
    setup(cb::mcbp::ClientOpcode::SubdocReplace, subdoc_replace_validator);
    setup(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
          subdoc_array_push_last_validator);
    setup(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
          subdoc_array_push_first_validator);
    setup(cb::mcbp::ClientOpcode::SubdocArrayInsert,
          subdoc_array_insert_validator);
    setup(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
          subdoc_array_add_unique_validator);
    setup(cb::mcbp::ClientOpcode::SubdocCounter, subdoc_counter_validator);
    setup(cb::mcbp::ClientOpcode::SubdocMultiLookup,
          subdoc_multi_lookup_validator);
    setup(cb::mcbp::ClientOpcode::SubdocMultiMutation,
          subdoc_multi_mutation_validator);
    setup(cb::mcbp::ClientOpcode::SubdocGetCount, subdoc_get_count_validator);
    setup(cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
          subdoc_replace_body_with_xattr_validator);

    setup(cb::mcbp::ClientOpcode::Setq, set_replace_validator);
    setup(cb::mcbp::ClientOpcode::Set, set_replace_validator);
    setup(cb::mcbp::ClientOpcode::Addq, add_validator);
    setup(cb::mcbp::ClientOpcode::Add, add_validator);
    setup(cb::mcbp::ClientOpcode::Replaceq, set_replace_validator);
    setup(cb::mcbp::ClientOpcode::Replace, set_replace_validator);
    setup(cb::mcbp::ClientOpcode::Appendq, append_prepend_validator);
    setup(cb::mcbp::ClientOpcode::Append, append_prepend_validator);
    setup(cb::mcbp::ClientOpcode::Prependq, append_prepend_validator);
    setup(cb::mcbp::ClientOpcode::Prepend, append_prepend_validator);
    setup(cb::mcbp::ClientOpcode::CreateBucket, create_bucket_validator);
    setup(cb::mcbp::ClientOpcode::ListBuckets, list_bucket_validator);
    setup(cb::mcbp::ClientOpcode::DeleteBucket, delete_bucket_validator);
    setup(cb::mcbp::ClientOpcode::SelectBucket, select_bucket_validator);
    setup(cb::mcbp::ClientOpcode::GetAllVbSeqnos, get_all_vb_seqnos_validator);

    setup(cb::mcbp::ClientOpcode::EvictKey, evict_key_validator);
    setup(cb::mcbp::ClientOpcode::Scrub, scrub_validator);

    setup(cb::mcbp::ClientOpcode::GetMeta, get_meta_validator);
    setup(cb::mcbp::ClientOpcode::GetqMeta, get_meta_validator);
    setup(cb::mcbp::ClientOpcode::SetWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::SetqWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::AddWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::AddqWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::DelWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::DelqWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::GetErrorMap, get_errmap_validator);
    setup(cb::mcbp::ClientOpcode::GetLocked, get_locked_validator);
    setup(cb::mcbp::ClientOpcode::UnlockKey, unlock_validator);
    setup(cb::mcbp::ClientOpcode::UpdateExternalUserPermissions,
          update_user_permissions_validator);
    setup(cb::mcbp::ClientOpcode::RbacRefresh, configuration_refresh_validator);
    setup(cb::mcbp::ClientOpcode::AuthProvider, auth_provider_validator);
    setup(cb::mcbp::ClientOpcode::DropPrivilege, drop_privilege_validator);
    setup(cb::mcbp::ClientOpcode::GetClusterConfig,
          get_cluster_config_validator);
    setup(cb::mcbp::ClientOpcode::SetClusterConfig,
          set_cluster_config_validator);
    setup(cb::mcbp::ClientOpcode::GetFailoverLog,
          dcp_get_failover_log_validator);
    setup(cb::mcbp::ClientOpcode::CollectionsSetManifest,
          collections_set_manifest_validator);
    setup(cb::mcbp::ClientOpcode::CollectionsGetManifest,
          collections_get_manifest_validator);
    setup(cb::mcbp::ClientOpcode::CollectionsGetID,
          collections_get_id_validator);
    setup(cb::mcbp::ClientOpcode::CollectionsGetScopeID,
          collections_get_id_validator); // same rules as GetID
    setup(cb::mcbp::ClientOpcode::AdjustTimeofday, adjust_timeofday_validator);
    setup(cb::mcbp::ClientOpcode::EwouldblockCtl, ewb_validator);
    setup(cb::mcbp::ClientOpcode::GetRandomKey, get_random_key_validator);
    setup(cb::mcbp::ClientOpcode::SetVbucket, set_vbucket_validator);
    setup(cb::mcbp::ClientOpcode::DelVbucket, del_vbucket_validator);
    setup(cb::mcbp::ClientOpcode::GetVbucket, get_vbucket_validator);
    setup(cb::mcbp::ClientOpcode::StopPersistence,
          start_stop_persistence_validator);
    setup(cb::mcbp::ClientOpcode::StartPersistence,
          start_stop_persistence_validator);
    setup(cb::mcbp::ClientOpcode::EnableTraffic,
          enable_disable_traffic_validator);
    setup(cb::mcbp::ClientOpcode::DisableTraffic,
          enable_disable_traffic_validator);
    setup(cb::mcbp::ClientOpcode::Ifconfig, ifconfig_validator);
    setup(cb::mcbp::ClientOpcode::GetKeys, get_keys_validator);
    setup(cb::mcbp::ClientOpcode::SetParam, set_param_validator);
    setup(cb::mcbp::ClientOpcode::GetReplica, get_validator);
    setup(cb::mcbp::ClientOpcode::ReturnMeta, return_meta_validator);
    setup(cb::mcbp::ClientOpcode::SeqnoPersistence,
          seqno_persistence_validator);
    setup(cb::mcbp::ClientOpcode::LastClosedCheckpoint,
          last_closed_checkpoint_validator);
    setup(cb::mcbp::ClientOpcode::CreateCheckpoint,
          create_checkpoint_validator);
    setup(cb::mcbp::ClientOpcode::CompactDb, compact_db_validator);
    setup(cb::mcbp::ClientOpcode::Observe, observe_validator);

    // Add a validator which returns not supported (we won't execute
    // these either as the executor would have returned not supported).
    // They're added to make it easier to see which opcodes we currently
    // don't have a proper validator for
    setup(cb::mcbp::ClientOpcode::Rget_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rset_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rsetq_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rappend_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rappendq_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rprepend_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rprependq_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rdelete_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rdeleteq_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rincr_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rincrq_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rdecr_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rdecrq_Unsupported, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapConnect_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapConnect_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapMutation_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapDelete_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapFlush_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapOpaque_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapVbucketSet_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapCheckpointStart_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapCheckpointEnd_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::DeregisterTapClient_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::DcpFlush_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::ResetReplicationChain_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::NotifyVbucketUpdate_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::SnapshotVbStates_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::VbucketBatchCount_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::ChangeVbFilter_Unsupported,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::CheckpointPersistence_Unsupported,
          not_supported_validator);
}
