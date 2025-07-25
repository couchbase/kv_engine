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
#include "settings.h"
#include "subdocument_validators.h"
#include "tls_configuration.h"
#include "xattr/utils.h"

#include <cbcrypto/key_store.h>
#include <dek/manager.h>
#include <logger/logger.h>
#include <memcached/collections.h>
#include <memcached/dcp.h>
#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>
#include <serverless/config.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/fusion_utilities.h>
#include <utilities/json_utilities.h>
#include <utilities/throttle_utilities.h>
#include <string_view>

using cb::mcbp::Status;

static bool is_valid_xattr_blob(Cookie& cookie,
                                const cb::mcbp::Request& request) {
    if (!cb::mcbp::datatype::is_xattr(uint8_t(request.getDatatype()))) {
        // no xattr segment
        return true;
    }

    return cookie.getConnection().getThread().isXattrBlobValid(
            cookie.getInflatedInputPayload());
}

bool McbpValidator::is_document_key_valid(Cookie& cookie) {
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

static bool may_accept_dcp_deleteV2(const Cookie& cookie) {
    return cookie.getConnection().isDcpDeleteV2();
}

using ExpectedKeyLen = McbpValidator::ExpectedKeyLen;
using ExpectedValueLen = McbpValidator::ExpectedValueLen;
using ExpectedCas = McbpValidator::ExpectedCas;
using GeneratesDocKey = McbpValidator::GeneratesDocKey;

static Status setCurrentCollectionInfo(Cookie& cookie, CollectionID cid) {
    ScopeID sid;
    uint64_t manifestUid = 0;
    bool metered = false;
    bool systemCollection = false;

    // Note: It is only safe to skip the default collection lookup when not
    // deployed as "serverless" (we need the correct metering state when
    // serverless is enabled).
    if (cid.isDefaultCollection() && !cb::serverless::isEnabled()) {
        sid = ScopeID{ScopeID::Default};
    } else {
        auto vbid = cookie.getRequest().getVBucket();
        auto res = cookie.getConnection()
                           .getBucket()
                           .getEngine()
                           .get_collection_meta(cookie, cid, vbid);
        if (res.result == cb::engine_errc::success) {
            manifestUid = res.getManifestId();
            sid = res.getScopeId();
            metered = res.isMetered();
            systemCollection = res.isSystemCollection();
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
            sid, cid, manifestUid, metered, systemCollection);
    return Status::Success;
}

static Status parseFrameExtras(Cookie& cookie,
                               const cb::mcbp::Request& request) {
    Status status = Status::Success;
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
                }
                cookie.setBarrier();
                return true;
            case cb::mcbp::request::FrameInfoId::DurabilityRequirement:
                try {
                    cb::durability::Requirements req(data);
                    if (!req.isValid()) {
                        status = Status::Einval;
                        cookie.setErrorContext(
                                "Invalid durability requirements");
                        return false;
                    }
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
                    if (msg.starts_with(prefix)) {
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
        cookie.setErrorContext("Invalid encoding in FrameExtras");
        return Status::Einval;
    }
    return status;
}

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
                                    GeneratesDocKey generates_dockey,
                                    uint8_t expected_datatype_mask) {
    const auto& header = cookie.getHeader();
    auto& connection = cookie.getConnection();

    if (!cb::mcbp::datatype::is_valid(header.getDatatype())) {
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
            return Status::Einval;
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
            if (cb::mcbp::datatype::is_valid(protocol_binary_datatype_t(ii))) {
                if (!connection.isDatatypeEnabled(bit)) {
                    result |= bit;
                }
            }
        }

        cookie.setErrorContext("Datatype (" +
                               cb::mcbp::datatype::to_string(
                                       protocol_binary_datatype_t(result)) +
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

    // Validate the frame id's?
    if (!request.getFramingExtras().empty()) {
        auto status = parseFrameExtras(cookie, request);
        if (status != Status::Success) {
            return status;
        }
    }

    if (generates_dockey == GeneratesDocKey::Yes &&
        !is_document_key_valid(cookie)) {
        // setErrorContext done within is_document_key_valid
        return Status::Einval;
    }

    // We need to set the scope and collection identifiers into the Cookie
    // The following code-block exists to populate into the Cookie all of the
    // data required to traverse the privileges hierarchy, that is we may need
    // to index into the scope privileges and then into the collection
    // privileges. Because the incoming "is_collection_command" operations only
    // include the collection, this code is doing a look-up of the collection
    // using the get_collection_meta.
    if (connection.getBucket().isCollectionCapable() &&
        is_collection_command(request.getClientOpcode())) {
        auto status = setCurrentCollectionInfo(
                cookie, cookie.getRequestKey().getCollectionID());
        if (status != Status::Success) {
            return status;
        }
    }

    const auto ingress_status =
            connection.getBucket().data_ingress_status.load();
    if (ingress_status != Status::Success &&
        cb::mcbp::is_client_writing_data(request.getClientOpcode())) {
        return ingress_status;
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
        LOG_WARNING_CTX(
                "DCP on a connection with unordered execution is currently not "
                "supported",
                {"description", cookie.getConnection().getDescription()});
        cookie.setErrorContext(
                "DCP on connections with unordered execution is not supported");
        return Status::NotSupported;
    }

    using cb::mcbp::ClientOpcode;
    const auto opcode = cookie.getRequest().getClientOpcode();
    if (opcode == ClientOpcode::DcpOpen) {
        if (connection.isDCP()) {
            cookie.setErrorContext(
                    "The connection is already opened as a DCP connection");
            return Status::Einval;
        }
    } else if (opcode != ClientOpcode::GetFailoverLog) {
        if (!connection.isDCP()) {
            cookie.setErrorContext(
                    "The command can only be sent on a DCP connection");
            return Status::Einval;
        }
    }

    return Status::Success;
}

/**
 * Verify that the request meets common restrictions between AddStream and
 * StreamRequest.
 * @param cookie Request to check
 * @return Status::Success if checks pass, otherwise sets the cookie's error
 *         context to a string explaining the failure and returns non-Success.
 */
static Status verify_common_dcp_stream_restrictions(
        Cookie& cookie, cb::mcbp::DcpAddStreamFlag flags) {
    using namespace cb::mcbp;

    constexpr auto mask =
            DcpAddStreamFlag::TakeOver | DcpAddStreamFlag::DiskOnly |
            DcpAddStreamFlag::ToLatest | DcpAddStreamFlag::ActiveVbOnly |
            DcpAddStreamFlag::StrictVbUuid | DcpAddStreamFlag::FromLatest |
            DcpAddStreamFlag::IgnorePurgedTombstones;
    const auto unknown = flags & ~mask;

    if (unknown != DcpAddStreamFlag::None) {
        if (isFlagSet(unknown, DcpAddStreamFlag::NoValue)) {
            // MB-22525 The NO_VALUE flag should be passed to DCP_OPEN
            if (cookie.getConnection().isAuthenticated()) {
                LOG_INFO_CTX("Client trying to add stream with NO VALUE",
                             {"description",
                              cookie.getConnection().getDescription()});
            }
            cookie.setErrorContext(
                    "DCP_ADD_STREAM_FLAG_NO_VALUE{8} flag is no longer used");
        } else {
            if (cookie.getConnection().isAuthenticated()) {
                LOG_INFO_CTX("Client trying to add stream with unknown flags",
                             {"flags", flags},
                             {"description",
                              cookie.getConnection().getDescription()});
            }
            cookie.setErrorContext(
                    fmt::format("Request contains invalid flags: {}", flags));
        }
        return Status::Einval;
    }
    return Status::Success;
}

static Status dcp_open_validator(Cookie& cookie) {
    using cb::mcbp::DcpOpenFlag;
    using cb::mcbp::request::DcpOpenPayload;

    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(DcpOpenPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Any,
            ExpectedCas::Any,
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    // Validate the flags.
    constexpr auto mask =
            ~(DcpOpenFlag::Producer | DcpOpenFlag::IncludeXattrs |
              DcpOpenFlag::NoValue | DcpOpenFlag::IncludeDeleteTimes |
              DcpOpenFlag::NoValueWithUnderlyingDatatype |
              DcpOpenFlag::IncludeDeletedUserXattrs);

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<DcpOpenPayload>();
    const auto flags = payload.getFlags();
    const auto unknown = flags & mask;

    if (unknown != DcpOpenFlag::None) {
        if (cookie.getConnection().isAuthenticated()) {
            LOG_INFO_CTX(
                    "Client trying to open dcp stream with unknown flags",
                    {"unknown", flags},
                    {"description", cookie.getConnection().getDescription()});
        }
        cookie.setErrorContext(
                fmt::format("Request contains invalid flags: {}", unknown));
        return Status::Einval;
    }

    if (isFlagSet(flags, DcpOpenFlag::NoValue) &&
        isFlagSet(flags, DcpOpenFlag::NoValueWithUnderlyingDatatype)) {
        if (cookie.getConnection().isAuthenticated()) {
            LOG_INFO_CTX(
                    "Invalid flags combination specified for a DCP consumer - "
                    "cannot specify NO_VALUE with "
                    "NO_VALUE_WITH_UNDERLYING_DATATYPE",
                    {"flags", flags},
                    {"description", cookie.getConnection().getDescription()});
        }
        cookie.setErrorContext(
                "Request contains invalid flags combination (NO_VALUE && "
                "NO_VALUE_WITH_UNDERLYING_DATATYPE)");
        return Status::Einval;
    }

    if (isFlagSet(flags, DcpOpenFlag::IncludeDeletedUserXattrs) &&
        !isFlagSet(flags, DcpOpenFlag::IncludeXattrs)) {
        if (cookie.getConnection().isAuthenticated()) {
            LOG_INFO_CTX(
                    "Invalid DcpOpen flags combination specified - Must "
                    "specify IncludeXattrs for IncludeDeletedUserXattrs",
                    {"flags", flags},
                    {"description", cookie.getConnection().getDescription()});
        }
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
                    if (isFlagSet(flags, DcpOpenFlag::Producer)) {
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
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    auto& req = cookie.getRequest();
    const auto& payload = req.getCommandSpecifics<DcpAddStreamPayload>();
    status = verify_common_dcp_stream_restrictions(cookie, payload.getFlags());
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_close_stream_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::Any,
                                               GeneratesDocKey::No,
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
                                               GeneratesDocKey::No,
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
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    auto& req = cookie.getRequest();
    const auto& payload =
            req.getCommandSpecifics<cb::mcbp::request::DcpStreamReqPayload>();
    status = verify_common_dcp_stream_restrictions(cookie, payload.getFlags());
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
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    return verify_common_dcp_restrictions(cookie);
}

// Producer can send v1, v2.0 or v2.2 formats.
static Status dcp_snapshot_marker_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();

    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    using cb::mcbp::request::DcpSnapshotMarkerV2_0Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2_2Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2xPayload;
    using cb::mcbp::request::DcpSnapshotMarkerV2xVersion;

    // Validate our extras length is correct
    if (!(header.getExtlen() == sizeof(DcpSnapshotMarkerV1Payload) ||
          header.getExtlen() == sizeof(DcpSnapshotMarkerV2xPayload))) {
        return Status::Einval;
    }

    // If v2.x validate the value length is expected, 2.0 or 2.2
    auto expectedValueLen = ExpectedValueLen::Zero;
    if (header.getExtlen() == sizeof(DcpSnapshotMarkerV2xPayload)) {
        const auto& payload =
                header.getRequest()
                        .getCommandSpecifics<DcpSnapshotMarkerV2xPayload>();
        expectedValueLen = ExpectedValueLen::Any;
        if (payload.getVersion() != DcpSnapshotMarkerV2xVersion::Zero &&
            payload.getVersion() != DcpSnapshotMarkerV2xVersion::Two) {
            cookie.setErrorContext(
                    "Unsupported dcp snapshot version:" +
                    std::to_string(uint32_t(payload.getVersion())));
            return Status::Einval;
        }

        size_t expectedLen = sizeof(DcpSnapshotMarkerV2_0Value);
        if (payload.getVersion() == DcpSnapshotMarkerV2xVersion::Two) {
            expectedLen = sizeof(DcpSnapshotMarkerV2_2Value);
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
                                               GeneratesDocKey::No,
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
                                         GeneratesDocKey::No,
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
                                         GeneratesDocKey::Yes,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
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
                                         GeneratesDocKey::Yes,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
    }

    if (!valid_dcp_delete_datatype(cookie.getHeader().getDatatype())) {
        cookie.setErrorContext("Request datatype invalid");
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
            GeneratesDocKey::Yes,
            McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
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
                                               GeneratesDocKey::No,
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
                                               GeneratesDocKey::No,
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
            GeneratesDocKey::No,
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
                                               GeneratesDocKey::No,
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
                                         GeneratesDocKey::Yes,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
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
                                         GeneratesDocKey::No,
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
                                               GeneratesDocKey::No,
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
                                               GeneratesDocKey::No,
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
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status configuration_refresh_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status auth_provider_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status drop_privilege_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_cluster_config_validator(Cookie& cookie) {
    if (cookie.getRequest().getExtdata().empty()) {
        return McbpValidator::verify_header(cookie,
                                            0,
                                            ExpectedKeyLen::Zero,
                                            ExpectedValueLen::Zero,
                                            ExpectedCas::NotSet,
                                            GeneratesDocKey::No,
                                            PROTOCOL_BINARY_RAW_BYTES);
    }
    using cb::mcbp::request::GetClusterConfigPayload;
    const auto status =
            McbpValidator::verify_header(cookie,
                                         sizeof(GetClusterConfigPayload),
                                         ExpectedKeyLen::Zero,
                                         ExpectedValueLen::Zero,
                                         ExpectedCas::NotSet,
                                         GeneratesDocKey::No,
                                         PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    try {
        cookie.getRequest()
                .getCommandSpecifics<GetClusterConfigPayload>()
                .validate();
    } catch (const std::exception&) {
        cookie.setErrorContext("Revision number must not be less than 1");
        return Status::Einval;
    }

    return Status::Success;
}

static Status set_cluster_config_validator(Cookie& cookie) {
    using cb::mcbp::request::SetClusterConfigPayload;

    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(SetClusterConfigPayload),
            ExpectedKeyLen::Any,
            ExpectedValueLen::NonZero,
            ExpectedCas::NotSet,
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON);

    if (status != Status::Success) {
        return status;
    }

    try {
        cookie.getRequest()
                .getCommandSpecifics<SetClusterConfigPayload>()
                .validate();
    } catch (const std::exception&) {
        cookie.setErrorContext("Revision number must not be less than 1");
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
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES);
}

static Status hello_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Any,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
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
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status quit_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status sasl_list_mech_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status sasl_auth_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Any,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status noop_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status add_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must have extras and key, may have value */
    return McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::MutationPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Any,
            ExpectedCas::NotSet,
            GeneratesDocKey::Yes,
            expected_datatype_mask);
}

static Status set_replace_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must have extras and key, may have value */
    return McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::MutationPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Any,
            ExpectedCas::Any,
            GeneratesDocKey::Yes,
            expected_datatype_mask);
}

static Status append_prepend_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must not have extras, must have key, may have value */
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Any,
                                        ExpectedCas::Any,
                                        GeneratesDocKey::Yes,
                                        expected_datatype_mask);
}

static Status get_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::Yes,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status getex_validator(Cookie& cookie) {
    const auto ret = McbpValidator::verify_header(cookie,
                                                  0,
                                                  ExpectedKeyLen::NonZero,
                                                  ExpectedValueLen::Zero,
                                                  ExpectedCas::NotSet,
                                                  GeneratesDocKey::Yes,
                                                  PROTOCOL_BINARY_RAW_BYTES);
    if (ret != Status::Success) {
        return ret;
    }
    if (!cookie.getConnection().isXattrEnabled()) {
        cookie.setErrorContext("Xattr support must be enabled");
        return Status::NotSupported;
    }
    if (!cookie.getConnection().isDatatypeEnabled(
                PROTOCOL_BINARY_DATATYPE_JSON)) {
        cookie.setErrorContext("JSON support must be enabled");
        return Status::NotSupported;
    }
    if (!cookie.getConnection().supportsSnappyEverywhere()) {
        cookie.setErrorContext("SnappyEverywhere support must be enabled");
        return Status::NotSupported;
    }
    return Status::Success;
}

static Status gat_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        sizeof(cb::mcbp::request::GatPayload),
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::Yes,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status delete_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::Any,
                                        GeneratesDocKey::Yes,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status stat_validator(Cookie& cookie) {
    auto ret = McbpValidator::verify_header(
            cookie,
            0,
            ExpectedKeyLen::Any,
            ExpectedValueLen::Any,
            ExpectedCas::NotSet,
            GeneratesDocKey::No,
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
    return McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::ArithmeticPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Zero,
            ExpectedCas::NotSet,
            GeneratesDocKey::Yes,
            PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_cmd_timer_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        1,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status set_ctrl_token_validator(Cookie& cookie) {
    using cb::mcbp::request::SetCtrlTokenPayload;
    auto status = McbpValidator::verify_header(cookie,
                                               sizeof(SetCtrlTokenPayload),
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<SetCtrlTokenPayload>();
    if (payload.getCas() == cb::mcbp::cas::Wildcard) {
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
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status ioctl_get_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status ioctl_set_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Any,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status audit_put_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        4,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::NonZero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status audit_config_reload_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status config_reload_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status config_validate_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::NonZero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status observe_seqno_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::Any,
                                               GeneratesDocKey::No,
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
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    const auto& req = cookie.getRequest();
    auto error = BucketValidator::validateBucketName(req.getKeyString());
    if (!error.empty()) {
        cookie.setErrorContext(error);
        return Status::Einval;
    }

    std::string value(req.getValueString());
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

    case BucketType::ClusterConfigOnly:
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
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status delete_bucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Any,
                                        ExpectedCas::Any,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status select_bucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::Any,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status pause_bucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status resume_bucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
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
                                               GeneratesDocKey::No,
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
        std::ranges::copy(extrasState, reinterpret_cast<uint8_t*>(&state));
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
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status set_bucket_throttle_properties_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);

    if (status != Status::Success) {
        return status;
    }

    auto payload = cookie.getHeader().getValueString();
    if (payload.size() > 1024) {
        // we don't want to go off json parsing incredible json payloads.
        // the document should be < 100 bytes...
        cookie.setErrorContext("Unexpected payload");
        return Status::Einval;
    }

    try {
        const cb::throttle::SetThrottleLimitPayload limits =
                nlohmann::json::parse(cookie.getHeader().getValueString());
        if (limits.reserved > limits.hard_limit) {
            cookie.setErrorContext("reserved can't exceed hard limit");
            return Status::Einval;
        }
    } catch (const std::exception& exception) {
        cookie.setErrorContext(fmt::format(
                "Invalid payload for SetBucketThrottleProperties: {}",
                exception.what()));
        return Status::Einval;
    }

    return Status::Success;
}

static Status set_node_throttle_properties_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    auto payload = cookie.getHeader().getValueString();
    if (payload.size() > 1024) {
        // we don't want to go off json parsing incredible json payloads.
        // the document should be < 100 bytes...
        cookie.setErrorContext("Unexpected payload");
        return Status::Einval;
    }

    try {
        const cb::throttle::SetNodeThrottleLimitPayload limits =
                nlohmann::json::parse(cookie.getHeader().getValueString());
        (void)limits;
    } catch (const std::exception& exception) {
        cookie.setErrorContext(
                fmt::format("Invalid payload for SetNodeThrottleProperties: {}",
                            exception.what()));
        return Status::Einval;
    }
    return Status::Success;
}

static Status set_bucket_data_limit_exceeded_validator(Cookie& cookie) {
    using cb::mcbp::request::SetBucketDataLimitExceededPayload;
    auto ret = McbpValidator::verify_header(
            cookie,
            sizeof(SetBucketDataLimitExceededPayload),
            ExpectedKeyLen::NonZero,
            ExpectedValueLen::Zero,
            ExpectedCas::NotSet,
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES);
    if (ret != Status::Success) {
        return ret;
    }

    const auto& payload =
            cookie.getRequest()
                    .getCommandSpecifics<SetBucketDataLimitExceededPayload>();
    switch (payload.getStatus()) {
    case Status::Success:
    case Status::BucketSizeLimitExceeded:
    case Status::BucketResidentRatioTooLow:
    case Status::BucketDataSizeTooBig:
    case Status::BucketDiskSpaceTooLow:
        break;
    default:
        cookie.setErrorContext("Unknown status code provided");
        return Status::Einval;
    }

    return Status::Success;
}

static Status set_active_encryption_key_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    auto key = cookie.getHeader().getKeyString();
    if (key.front() == '@') {
        try {
            cb::dek::to_entity(key);
        } catch (const std::exception& exception) {
            cookie.setErrorContext(fmt::format("Invalid entity provided: {}",
                                               exception.what()));
            return Status::Einval;
        }
    }
    try {
        auto payload = cookie.getHeader().getValueString();
        const auto json = nlohmann::json::parse(payload);
        if (!json.is_object()) {
            cookie.setErrorContext("The provided value is not a JSON object");
            return Status::Einval;
        }

        // Allow the old way of specifying the key store until
        // ns_server has been updated to use the new format. This chunk
        // should be removed once they've merged their code
        if (json.contains("active") || json.contains("keys")) {
            cb::crypto::KeyStore ks = nlohmann::json::parse(payload);
            return Status::Success;
        }

        if (json.contains("unavailable")) {
            if (!json["unavailable"].is_array()) {
                cookie.setErrorContext(
                        "The provided value must contain an array of "
                        "unavailable keys");
                return Status::Einval;
            }
            for (const auto& unavailabe : json["unavailable"]) {
                if (!unavailabe.is_string()) {
                    cookie.setErrorContext(
                            "The provided value must contain an array of "
                            "unavailable keys as strings");
                    return Status::Einval;
                }
                if (unavailabe.get<std::string>() ==
                    cb::crypto::DataEncryptionKey::UnencryptedKeyId) {
                    cookie.setErrorContext(
                            "It does not make sense to mark the unencrypted "
                            "key as unavailable");
                    return Status::Einval;
                }
            }
        }

        if (!json.contains("keystore")) {
            cookie.setErrorContext(
                    "The provided value must contain a keystore object");
            return Status::Einval;
        }

        cb::crypto::KeyStore ks = json["keystore"];
    } catch (const std::exception& exception) {
        cookie.setErrorContext(
                fmt::format("Failed to decode active encryption key info: {}",
                            exception.what()));
        return Status::Einval;
    }

    return Status::Success;
}

static Status prune_encryption_keys_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::NonZero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    auto payload = cookie.getHeader().getValueString();
    try {
        std::vector<std::string> keys = nlohmann::json::parse(payload);
    } catch (const std::exception& exception) {
        LOG_ERROR_CTX("Failed to decode the array of keys to prune",
                      {"conn_id", cookie.getConnectionId()},
                      {"error", exception.what()});
        cookie.setErrorContext(
                fmt::format("Failed to decode the array of keys to prune: {}",
                            exception.what()));
        return Status::Einval;
    }

    try {
        switch (cb::dek::to_entity(cookie.getRequest().getKeyString())) {
        case cb::dek::Entity::Logs:
        case cb::dek::Entity::Audit:
            break;

        default:
            cookie.setErrorContext(
                    fmt::format("Invalid entity provided. Must be {} or {}",
                                cb::dek::Entity::Logs,
                                cb::dek::Entity::Audit));
            return Status::Einval;
        }
    } catch (const std::exception& exception) {
        LOG_ERROR_CTX("Invalid entity provided",
                      {"conn_id", cookie.getConnectionId()},
                      {"error", exception.what()});
        cookie.setErrorContext(
                fmt::format("Invalid entity provided: {}", exception.what()));
        return Status::Einval;
    }

    return Status::Success;
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
                                               GeneratesDocKey::Yes,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    auto extras = header.getRequest().getExtdata();

    if (extras.size() > 1) {
        cookie.setErrorContext("Request extras must be of length 0 or 1");
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
                                         GeneratesDocKey::Yes,
                                         McbpValidator::AllSupportedDatatypes);
    if (status != Status::Success) {
        return status;
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
                                               GeneratesDocKey::No,
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
                                               GeneratesDocKey::Yes,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    if (extlen != 0 && extlen != sizeof(cb::mcbp::request::GetLockedPayload)) {
        cookie.setErrorContext("Request extras must be of length 0 or 4");
        return Status::Einval;
    }

    return Status::Success;
}

static Status unlock_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::Set,
                                        GeneratesDocKey::Yes,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status evict_key_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::NonZero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::Yes,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status collections_set_manifest_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    if (cookie.getRequest().getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    if (!Settings::instance().isCollectionsEnabled()) {
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status collections_get_manifest_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Zero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
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
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }
    const auto& header = cookie.getHeader();
    // now expect value or key, but not both
    if (!header.getKey().empty() && !header.getValue().empty()) {
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
                                               GeneratesDocKey::No,
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
                                         GeneratesDocKey::No,
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

static Status get_random_key_validator(Cookie& cookie) {
    const auto extdata = cookie.getRequest().getExtdata();
    const auto status = McbpValidator::verify_header(
            cookie,
            gsl::narrow_cast<uint8_t>(extdata.size()),
            ExpectedKeyLen::Zero,
            ExpectedValueLen::Zero,
            ExpectedCas::NotSet,
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
    }

    if (cookie.getConnection().isCollectionsSupported()) {
        if (extdata.size() != sizeof(cb::mcbp::request::GetRandomKeyPayload) &&
            extdata.size() !=
                    sizeof(cb::mcbp::request::GetRandomKeyPayloadV2)) {
            cookie.setErrorContext(
                    "Extras should contain collection id (and optionally flag "
                    "to request xattrs)");
            return Status::Einval;
        }
    } else if (!extdata.empty()) {
        cookie.setErrorContext(
                "Extras should be empty for clients without collections "
                "support");
        return Status::Einval;
    }

    return status;
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
            1,
            ExpectedKeyLen::Zero,
            ExpectedValueLen::Any,
            ExpectedCas::NotSet,
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto extras = header.getExtdata();
    const auto value = header.getValue();
    const auto datatype = header.getDatatype();

    const auto state = static_cast<vbucket_state_t>(extras.front());
    if (value.empty()) {
        if (datatype != PROTOCOL_BINARY_RAW_BYTES) {
            cookie.setErrorContext("Datatype should be set to RAW");
            return Status::Einval;
        }
    } else if (datatype != PROTOCOL_BINARY_DATATYPE_JSON) {
        cookie.setErrorContext("Datatype should be set to JSON");
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
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_vbucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status start_stop_persistence_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status enable_disable_traffic_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status ifconfig_validator(Cookie& cookie) {
    const auto status =
            McbpValidator::verify_header(cookie,
                                         0,
                                         ExpectedKeyLen::NonZero,
                                         ExpectedValueLen::Any,
                                         ExpectedCas::NotSet,
                                         GeneratesDocKey::No,
                                         PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto& req = cookie.getRequest();

    const std::string_view key{req.getKeyString()};
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
                                               GeneratesDocKey::Yes,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
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
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_RAW_BYTES);

    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<SetParamPayload>();
    if (!payload.validate()) {
        cookie.setErrorContext("Invalid param type specified");
    }

    if (payload.getParamType() == SetParamPayload::Type::Replication) {
        return Status::KeyEnoent;
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
                                               GeneratesDocKey::Yes,
                                               PROTOCOL_BINARY_RAW_BYTES);
    if (status != Status::Success) {
        return status;
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
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status compact_db_validator(Cookie& cookie) {
    using cb::mcbp::request::CompactDbPayload;

    auto status = McbpValidator::verify_header(
            cookie,
            sizeof(CompactDbPayload),
            ExpectedKeyLen::Zero,
            ExpectedValueLen::Any,
            ExpectedCas::NotSet,
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto& payload =
            cookie.getRequest().getCommandSpecifics<CompactDbPayload>();
    if (!payload.validate()) {
        cookie.setErrorContext("Padding bytes should be set to 0");
        return Status::Einval;
    }

    auto value = cookie.getRequest().getValueString();
    if (!value.empty()) {
        if (cookie.getRequest().getDatatype() != cb::mcbp::Datatype::JSON) {
            cookie.setErrorContext("Datatype should be JSON");
            return Status::Einval;
        }
        if (!cookie.getConnection().getThread().isValidJson(cookie, value)) {
            cookie.setErrorContext("Value should be JSON");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status observe_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::NonZero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status not_supported_validator(Cookie&) {
    return Status::NotSupported;
}

static Status create_range_scan_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::Any,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    // create value really must be JSON!
    if (header.getDatatype() != PROTOCOL_BINARY_DATATYPE_JSON) {
        cookie.setErrorContext("Datatype must be JSON");
        return Status::Einval;
    }

    // Only checking we can parse the input, no field/value checks here, except
    // for the collection read below
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(header.getRequest().getValueString());
    } catch (const nlohmann::json::exception& e) {
        cookie.setErrorContext(
                fmt::format("Failed nlohmann::json::parse {}", e.what()));
        return Status::Einval;
    }

    // The following code is required to setup the cookie's collection info so
    // that auth checks can be applied. This command is against a collection
    // and the client must have appropriate access for the collection/scope
    auto collection = cb::getOptionalJsonObject(
            parsed, "collection", nlohmann::json::value_t::string);

    CollectionID cid = CollectionID::Default;
    if (collection) {
        try {
            cid = CollectionID(collection.value().get<std::string>());
        } catch (const std::exception&) {
            cookie.setErrorContext(
                    "Invalid formatting of the collection field");
            return Status::Einval;
        }
    }

    return setCurrentCollectionInfo(cookie, cid);
}

static Status continue_range_scan_validator(Cookie& cookie) {
    return McbpValidator::verify_header(
            cookie,
            sizeof(cb::mcbp::request::RangeScanContinuePayload),
            ExpectedKeyLen::Zero,
            ExpectedValueLen::Zero,
            ExpectedCas::NotSet,
            GeneratesDocKey::No,
            PROTOCOL_BINARY_RAW_BYTES);
}

static Status cancel_range_scan_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        sizeof(cb::rangescan::Id),
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status get_file_fragment_validator(Cookie& cookie) {
    const auto status =
            McbpValidator::verify_header(cookie,
                                         0,
                                         ExpectedKeyLen::NonZero,
                                         ExpectedValueLen::NonZero,
                                         ExpectedCas::NotSet,
                                         GeneratesDocKey::No,
                                         PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    try {
        const auto json =
                nlohmann::json::parse(cookie.getRequest().getValueString());
        if (!json.contains("id") || !json["id"].is_number()) {
            cookie.setErrorContext(
                    "id must be provided and must be a numeric value");
            return Status::Einval;
        }
        if (!json.contains("offset") || !json["offset"].is_string()) {
            cookie.setErrorContext(
                    "offset be provided and must be a string value");
            return Status::Einval;
        }
        if (!json.contains("length") || !json["length"].is_string()) {
            cookie.setErrorContext(
                    "length be provided and must be a string value");
            return Status::Einval;
        }
        if (json.contains("checksum") && !json["checksum"].is_string()) {
            cookie.setErrorContext(
                    "If checksum is provided it has to be a string");
            return Status::Einval;
        }
    } catch (const std::exception&) {
        cookie.setErrorContext("Invalid json provided");
        return Status::Einval;
    }

    return Status::Success;
}

static Status prepare_snapshot_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status release_snapshot_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Any,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status download_snapshot_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto& request = cookie.getHeader();
    if (!cb::mcbp::datatype::is_json(request.getDatatype())) {
        cookie.setErrorContext("Datatype must be JSON");
        return Status::Einval;
    }

    if (!cookie.isValidJson(request.getValueString())) {
        cookie.setErrorContext("Provided value is not JSON");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_fusion_storage_snapshot_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto value = cookie.getRequest().getValueString();
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(value);
    } catch (const nlohmann::json::exception& e) {
        const auto msg = fmt::format(
                "get_fusion_storage_snapshot_validator: Invalid json '{}' {}",
                value,
                e.what());
        cookie.setErrorContext(msg);
        return Status::Einval;
    }

    if (!json.contains("snapshotUuid")) {
        cookie.setErrorContext(
                "get_fusion_storage_snapshot_validator: Missing snapshotUuid");
        return Status::Einval;
    }
    if (!json["snapshotUuid"].is_string()) {
        cookie.setErrorContext(
                "get_fusion_storage_snapshot_validator: snapshotUuid not "
                "string");
        return Status::Einval;
    }

    if (!json.contains("validity")) {
        cookie.setErrorContext(
                "get_fusion_storage_snapshot_validator: Missing validity");
        return Status::Einval;
    }
    const auto validity = json["validity"];
    if (!validity.is_number_integer() || validity.get<int64_t>() < 0) {
        cookie.setErrorContext(
                "get_fusion_storage_snapshot_validator: validity not positive "
                "integer");
        return Status::Einval;
    }

    return status;
}

static Status release_fusion_storage_snapshot_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto value = cookie.getRequest().getValueString();
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(value);
    } catch (const nlohmann::json::exception& e) {
        const auto msg = fmt::format(
                "release_fusion_storage_snapshot_validator: Invalid json '{}' "
                "{}",
                value,
                e.what());
        cookie.setErrorContext(msg);
        return Status::Einval;
    }

    if (!json.contains("snapshotUuid")) {
        cookie.setErrorContext(
                "release_fusion_storage_snapshot_validator: Missing "
                "snapshotUuid");
        return Status::Einval;
    }
    if (!json["snapshotUuid"].is_string()) {
        cookie.setErrorContext(
                "release_fusion_storage_snapshot_validator: snapshotUuid not "
                "string");
        return Status::Einval;
    }

    return status;
}

static Status mount_vbucket_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto value = cookie.getRequest().getValueString();
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(value);
    } catch (const nlohmann::json::exception& e) {
        const auto msg =
                fmt::format("mount_vbucket_validator: Invalid json '{}' {}",
                            value,
                            e.what());
        cookie.setErrorContext(msg);
        return Status::Einval;
    }

    if (!json.contains("mountPaths")) {
        cookie.setErrorContext("mount_vbucket_validator: Missing mountPaths");
        return Status::Einval;
    }

    if (!json["mountPaths"].is_array()) {
        cookie.setErrorContext(
                "mount_vbucket_validator: mountPaths not an array");
        return Status::Einval;
    }

    try {
        std::vector<std::string> paths = json["mountPaths"];
    } catch (const std::exception& e) {
        const auto msg =
                fmt::format("mount_vbucket_validator: Invalid json '{}' {}",
                            value,
                            e.what());
        cookie.setErrorContext(msg);
        return Status::Einval;
    }

    return status;
}

static Status unmount_vbucket_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status sync_fusion_logstore_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}


static Status start_fusion_uploader_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto value = cookie.getRequest().getValueString();
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(value);
    } catch (const nlohmann::json::exception& e) {
        const auto msg = fmt::format(
                "start_fusion_uploader_validator: Invalid json '{}' {}",
                value,
                e.what());
        cookie.setErrorContext(msg);
        return Status::Einval;
    }

    if (!json.contains("term")) {
        cookie.setErrorContext("start_fusion_uploader_validator: Missing term");
        return Status::Einval;
    }

    if (!json["term"].is_string()) {
        cookie.setErrorContext(
                "start_fusion_uploader_validator: term not string");
        return Status::Einval;
    }

    const std::string term = json["term"];
    uint64_t term_value = 0;
    if (!safe_strtoull(term, term_value)) {
        cookie.setErrorContext(
                "start_fusion_uploader_validator: term is not a valid number");
        return Status::Einval;
    }
    return Status::Success;
}

static Status stop_fusion_uploader_validator(Cookie& cookie) {
    return McbpValidator::verify_header(cookie,
                                        0,
                                        ExpectedKeyLen::Zero,
                                        ExpectedValueLen::Zero,
                                        ExpectedCas::NotSet,
                                        GeneratesDocKey::No,
                                        PROTOCOL_BINARY_RAW_BYTES);
}

static Status set_chronicle_auth_token_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto value = cookie.getRequest().getValueString();
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(value);
    } catch (const nlohmann::json::exception& e) {
        // Note: Don't log the full payload
        const auto msg = fmt::format(
                "set_chronicle_auth_token_validator: Invalid json format {}",
                e.what());
        cookie.setErrorContext(msg);
        return Status::Einval;
    }

    if (!json.contains("token")) {
        cookie.setErrorContext(
                "set_chronicle_auth_token_validator: Missing token");
        return Status::Einval;
    }
    if (!json["token"].is_string()) {
        cookie.setErrorContext(
                "set_chronicle_auth_token_validator: token not string");
        return Status::Einval;
    }

    return status;
}

Status delete_fusion_namespace_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto value = cookie.getRequest().getValueString();
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(value);
    } catch (const nlohmann::json::exception& e) {
        // Note: Don't log the full payload
        const auto msg = fmt::format(
                "delete_fusion_namespace_validator: Invalid json format {}",
                e.what());
        cookie.setErrorContext(msg);
        return Status::Einval;
    }

    if (!json.contains("logstore_uri")) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: Missing logstore_uri");
        return Status::Einval;
    }
    if (!json["logstore_uri"].is_string()) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: logstore_uri not string");
        return Status::Einval;
    }

    if (!json.contains("metadatastore_uri")) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: Missing "
                "metadatastore_uri");
        return Status::Einval;
    }
    if (!json["metadatastore_uri"].is_string()) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: metadatastore_uri not "
                "string");
        return Status::Einval;
    }

    if (!json.contains("metadatastore_auth_token")) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: Missing "
                "metadatastore_auth_token");
        return Status::Einval;
    }
    if (!json["metadatastore_auth_token"].is_string()) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: metadatastore_auth_token "
                "not string");
        return Status::Einval;
    }

    if (!json.contains("namespace")) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: Missing namespace");
        return Status::Einval;
    }
    if (!json["namespace"].is_string()) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: namespace not string");
        return Status::Einval;
    }

    const std::string_view ns = json["namespace"].get<std::string_view>();
    if (!isValidFusionNamespace(ns)) {
        cookie.setErrorContext(
                "delete_fusion_namespace_validator: namespace must be in "
                "format 'kv/bucketName/bucketUUID'");
        return Status::Einval;
    }
    return status;
}

Status get_fusion_namespaces_validator(Cookie& cookie) {
    auto status = McbpValidator::verify_header(cookie,
                                               0,
                                               ExpectedKeyLen::Zero,
                                               ExpectedValueLen::NonZero,
                                               ExpectedCas::NotSet,
                                               GeneratesDocKey::No,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
    if (status != Status::Success) {
        return status;
    }

    const auto value = cookie.getRequest().getValueString();
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(value);
    } catch (const nlohmann::json::exception& e) {
        // Note: Don't log the full payload
        const auto msg = fmt::format(
                "get_fusion_namespaces_validator: Invalid json format {}",
                e.what());
        cookie.setErrorContext(msg);
        return Status::Einval;
    }

    if (!json.contains("metadatastore_uri")) {
        cookie.setErrorContext(
                "get_fusion_namespaces_validator: Missing "
                "metadatastore_uri");
        return Status::Einval;
    }
    if (!json["metadatastore_uri"].is_string()) {
        cookie.setErrorContext(
                "get_fusion_namespaces_validator: metadatastore_uri not "
                "string");
        return Status::Einval;
    }

    if (!json.contains("metadatastore_auth_token")) {
        cookie.setErrorContext(
                "get_fusion_namespaces_validator: Missing "
                "metadatastore_auth_token");
        return Status::Einval;
    }
    if (!json["metadatastore_auth_token"].is_string()) {
        cookie.setErrorContext(
                "get_fusion_namespaces_validator: metadatastore_auth_token "
                "not string");
        return Status::Einval;
    }

    return status;
}

Status McbpValidator::validate(ClientOpcode command, Cookie& cookie) {
    const auto idx = std::underlying_type_t<ClientOpcode>(command);
    if (validators[idx]) {
        return validators[idx](cookie);
    }
    return Status::UnknownCommand;
}

void McbpValidator::setup(ClientOpcode command, Status (*f)(Cookie&)) {
    validators[std::underlying_type_t<ClientOpcode>(command)] = f;
}

McbpValidator::McbpValidator() {
    setup(ClientOpcode::DcpOpen, dcp_open_validator);
    setup(ClientOpcode::DcpAddStream, dcp_add_stream_validator);
    setup(ClientOpcode::DcpCloseStream, dcp_close_stream_validator);
    setup(ClientOpcode::DcpSnapshotMarker, dcp_snapshot_marker_validator);
    setup(ClientOpcode::DcpDeletion, dcp_deletion_validator);
    setup(ClientOpcode::DcpExpiration, dcp_expiration_validator);
    setup(ClientOpcode::DcpGetFailoverLog, dcp_get_failover_log_validator);
    setup(ClientOpcode::DcpMutation, dcp_mutation_validator);
    setup(ClientOpcode::DcpSetVbucketState, dcp_set_vbucket_state_validator);
    setup(ClientOpcode::DcpNoop, dcp_noop_validator);
    setup(ClientOpcode::DcpBufferAcknowledgement,
          dcp_buffer_acknowledgement_validator);
    setup(ClientOpcode::DcpControl, dcp_control_validator);
    setup(ClientOpcode::DcpStreamEnd, dcp_stream_end_validator);
    setup(ClientOpcode::DcpStreamReq, dcp_stream_req_validator);
    setup(ClientOpcode::DcpSystemEvent, dcp_system_event_validator);
    setup(ClientOpcode::DcpPrepare, dcp_prepare_validator);
    setup(ClientOpcode::DcpSeqnoAcknowledged, dcp_seqno_acknowledged_validator);
    setup(ClientOpcode::DcpCommit, dcp_commit_validator);
    setup(ClientOpcode::DcpAbort, dcp_abort_validator);
    setup(ClientOpcode::IsaslRefresh, configuration_refresh_validator);
    setup(ClientOpcode::Verbosity, verbosity_validator);
    setup(ClientOpcode::Hello, hello_validator);
    setup(ClientOpcode::Version, version_validator);
    setup(ClientOpcode::Quit, quit_validator);
    setup(ClientOpcode::SaslListMechs, sasl_list_mech_validator);
    setup(ClientOpcode::SaslAuth, sasl_auth_validator);
    setup(ClientOpcode::SaslStep, sasl_auth_validator);
    setup(ClientOpcode::Noop, noop_validator);
    setup(ClientOpcode::GetEx, getex_validator);
    setup(ClientOpcode::GetExReplica, getex_validator);
    setup(ClientOpcode::Get, get_validator);
    setup(ClientOpcode::Gat, gat_validator);
    setup(ClientOpcode::Touch, gat_validator);
    setup(ClientOpcode::Delete, delete_validator);
    setup(ClientOpcode::Stat, stat_validator);
    setup(ClientOpcode::Increment, arithmetic_validator);
    setup(ClientOpcode::Decrement, arithmetic_validator);
    setup(ClientOpcode::GetCmdTimer, get_cmd_timer_validator);
    setup(ClientOpcode::SetCtrlToken, set_ctrl_token_validator);
    setup(ClientOpcode::GetCtrlToken, get_ctrl_token_validator);
    setup(ClientOpcode::IoctlGet, ioctl_get_validator);
    setup(ClientOpcode::IoctlSet, ioctl_set_validator);
    setup(ClientOpcode::AuditPut, audit_put_validator);
    setup(ClientOpcode::AuditConfigReload, audit_config_reload_validator);
    setup(ClientOpcode::ConfigReload, config_reload_validator);
    setup(ClientOpcode::ConfigValidate, config_validate_validator);
    setup(ClientOpcode::Shutdown, shutdown_validator);

    setup(ClientOpcode::SetBucketDataLimitExceeded,
          set_bucket_data_limit_exceeded_validator);

    setup(ClientOpcode::SetActiveEncryptionKeys,
          set_active_encryption_key_validator);
    setup(ClientOpcode::PruneEncryptionKeys, prune_encryption_keys_validator);

    if (cb::serverless::isEnabled()) {
        setup(ClientOpcode::SetBucketThrottleProperties,
              set_bucket_throttle_properties_validator);
        setup(ClientOpcode::SetNodeThrottleProperties,
              set_node_throttle_properties_validator);
    } else {
        setup(ClientOpcode::SetBucketThrottleProperties,
              not_supported_validator);
        setup(ClientOpcode::SetNodeThrottleProperties, not_supported_validator);
    }

    setup(ClientOpcode::ObserveSeqno, observe_seqno_validator);
    setup(ClientOpcode::GetAdjustedTime_Unsupported, not_supported_validator);
    setup(ClientOpcode::SetDriftCounterState_Unsupported,
          not_supported_validator);

    setup(ClientOpcode::SubdocGet, subdoc_get_validator);
    setup(ClientOpcode::SubdocExists, subdoc_exists_validator);
    setup(ClientOpcode::SubdocDictAdd, subdoc_dict_add_validator);
    setup(ClientOpcode::SubdocDictUpsert, subdoc_dict_upsert_validator);
    setup(ClientOpcode::SubdocDelete, subdoc_delete_validator);
    setup(ClientOpcode::SubdocReplace, subdoc_replace_validator);
    setup(ClientOpcode::SubdocArrayPushLast, subdoc_array_push_last_validator);
    setup(ClientOpcode::SubdocArrayPushFirst,
          subdoc_array_push_first_validator);
    setup(ClientOpcode::SubdocArrayInsert, subdoc_array_insert_validator);
    setup(ClientOpcode::SubdocArrayAddUnique,
          subdoc_array_add_unique_validator);
    setup(ClientOpcode::SubdocCounter, subdoc_counter_validator);
    setup(ClientOpcode::SubdocMultiLookup, subdoc_multi_lookup_validator);
    setup(ClientOpcode::SubdocMultiMutation, subdoc_multi_mutation_validator);
    setup(ClientOpcode::SubdocGetCount, subdoc_get_count_validator);
    setup(ClientOpcode::SubdocReplaceBodyWithXattr,
          subdoc_replace_body_with_xattr_validator);

    setup(ClientOpcode::Set, set_replace_validator);
    setup(ClientOpcode::Add, add_validator);
    setup(ClientOpcode::Replace, set_replace_validator);
    setup(ClientOpcode::Append, append_prepend_validator);
    setup(ClientOpcode::Prepend, append_prepend_validator);
    setup(ClientOpcode::CreateBucket, create_bucket_validator);
    setup(ClientOpcode::ListBuckets, list_bucket_validator);
    setup(ClientOpcode::DeleteBucket, delete_bucket_validator);
    setup(ClientOpcode::SelectBucket, select_bucket_validator);
    setup(ClientOpcode::PauseBucket, pause_bucket_validator);
    setup(ClientOpcode::ResumeBucket, resume_bucket_validator);
    setup(ClientOpcode::GetAllVbSeqnos, get_all_vb_seqnos_validator);

    setup(ClientOpcode::EvictKey, evict_key_validator);
    setup(ClientOpcode::Scrub_Unsupported, not_supported_validator);
    setup(ClientOpcode::Flush_Unsupported, not_supported_validator);
    setup(ClientOpcode::Flushq_Unsupported, not_supported_validator);

    if (cb::serverless::isEnabled()) {
        // No need to start allowing quiet commands in a serverless
        // deployment
        setup(ClientOpcode::Quitq, not_supported_validator);
        setup(ClientOpcode::Getq, not_supported_validator);
        setup(ClientOpcode::Getk, not_supported_validator);
        setup(ClientOpcode::Getkq, not_supported_validator);
        setup(ClientOpcode::Gatq, not_supported_validator);
        setup(ClientOpcode::Deleteq, not_supported_validator);
        setup(ClientOpcode::Incrementq, not_supported_validator);
        setup(ClientOpcode::Decrementq, not_supported_validator);
        setup(ClientOpcode::Setq, not_supported_validator);
        setup(ClientOpcode::Addq, not_supported_validator);
        setup(ClientOpcode::Replaceq, not_supported_validator);
        setup(ClientOpcode::Appendq, not_supported_validator);
        setup(ClientOpcode::Prependq, not_supported_validator);
        setup(ClientOpcode::GetqMeta, not_supported_validator);
        setup(ClientOpcode::SetqWithMeta, not_supported_validator);
        setup(ClientOpcode::AddqWithMeta, not_supported_validator);
        setup(ClientOpcode::DelqWithMeta, not_supported_validator);
    } else {
        setup(ClientOpcode::Quitq, quit_validator);
        setup(ClientOpcode::Getq, get_validator);
        setup(ClientOpcode::Getk, get_validator);
        setup(ClientOpcode::Getkq, get_validator);
        setup(ClientOpcode::Gatq, gat_validator);
        setup(ClientOpcode::Deleteq, delete_validator);
        setup(ClientOpcode::Incrementq, arithmetic_validator);
        setup(ClientOpcode::Decrementq, arithmetic_validator);
        setup(ClientOpcode::Setq, set_replace_validator);
        setup(ClientOpcode::Addq, add_validator);
        setup(ClientOpcode::Replaceq, set_replace_validator);
        setup(ClientOpcode::Appendq, append_prepend_validator);
        setup(ClientOpcode::Prependq, append_prepend_validator);
        setup(ClientOpcode::GetqMeta, get_meta_validator);
        setup(ClientOpcode::SetqWithMeta, mutate_with_meta_validator);
        setup(ClientOpcode::AddqWithMeta, mutate_with_meta_validator);
        setup(ClientOpcode::DelqWithMeta, mutate_with_meta_validator);
    }

    setup(ClientOpcode::GetMeta, get_meta_validator);
    setup(ClientOpcode::SetWithMeta, mutate_with_meta_validator);
    setup(ClientOpcode::AddWithMeta, mutate_with_meta_validator);
    setup(ClientOpcode::DelWithMeta, mutate_with_meta_validator);
    setup(ClientOpcode::GetErrorMap, get_errmap_validator);
    setup(ClientOpcode::GetLocked, get_locked_validator);
    setup(ClientOpcode::UnlockKey, unlock_validator);
    setup(ClientOpcode::UpdateExternalUserPermissions,
          update_user_permissions_validator);
    setup(ClientOpcode::RbacRefresh, configuration_refresh_validator);
    setup(ClientOpcode::AuthProvider, auth_provider_validator);
    setup(ClientOpcode::DropPrivilege, drop_privilege_validator);
    setup(ClientOpcode::GetClusterConfig, get_cluster_config_validator);
    setup(ClientOpcode::SetClusterConfig, set_cluster_config_validator);
    setup(ClientOpcode::GetFailoverLog, dcp_get_failover_log_validator);
    setup(ClientOpcode::CollectionsSetManifest,
          collections_set_manifest_validator);
    setup(ClientOpcode::CollectionsGetManifest,
          collections_get_manifest_validator);
    setup(ClientOpcode::CollectionsGetID, collections_get_id_validator);
    setup(ClientOpcode::CollectionsGetScopeID,
          collections_get_id_validator); // same rules as GetID
    setup(ClientOpcode::AdjustTimeofday, adjust_timeofday_validator);
    setup(ClientOpcode::EwouldblockCtl, ewb_validator);
    setup(ClientOpcode::GetRandomKey, get_random_key_validator);
    setup(ClientOpcode::SetVbucket, set_vbucket_validator);
    setup(ClientOpcode::DelVbucket, del_vbucket_validator);
    setup(ClientOpcode::GetVbucket, get_vbucket_validator);
    setup(ClientOpcode::StopPersistence, start_stop_persistence_validator);
    setup(ClientOpcode::StartPersistence, start_stop_persistence_validator);
    setup(ClientOpcode::EnableTraffic, enable_disable_traffic_validator);
    setup(ClientOpcode::DisableTraffic, enable_disable_traffic_validator);
    setup(ClientOpcode::Ifconfig, ifconfig_validator);
    setup(ClientOpcode::GetKeys, get_keys_validator);
    setup(ClientOpcode::SetParam, set_param_validator);
    setup(ClientOpcode::GetReplica, get_validator);
    setup(ClientOpcode::ReturnMeta, return_meta_validator);
    setup(ClientOpcode::SeqnoPersistence, seqno_persistence_validator);
    setup(ClientOpcode::CompactDb, compact_db_validator);
    setup(ClientOpcode::Observe, observe_validator);

    // Add a validator which returns not supported (we won't execute
    // these either as the executor would have returned not supported).
    // They're added to make it easier to see which opcodes we currently
    // don't have a proper validator for
    setup(ClientOpcode::Rget_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rset_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rsetq_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rappend_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rappendq_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rprepend_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rprependq_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rdelete_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rdeleteq_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rincr_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rincrq_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rdecr_Unsupported, not_supported_validator);
    setup(ClientOpcode::Rdecrq_Unsupported, not_supported_validator);
    setup(ClientOpcode::TapConnect_Unsupported, not_supported_validator);
    setup(ClientOpcode::TapConnect_Unsupported, not_supported_validator);
    setup(ClientOpcode::TapMutation_Unsupported, not_supported_validator);
    setup(ClientOpcode::TapDelete_Unsupported, not_supported_validator);
    setup(ClientOpcode::TapFlush_Unsupported, not_supported_validator);
    setup(ClientOpcode::TapOpaque_Unsupported, not_supported_validator);
    setup(ClientOpcode::TapVbucketSet_Unsupported, not_supported_validator);
    setup(ClientOpcode::TapCheckpointStart_Unsupported,
          not_supported_validator);
    setup(ClientOpcode::TapCheckpointEnd_Unsupported, not_supported_validator);
    setup(ClientOpcode::DeregisterTapClient_Unsupported,
          not_supported_validator);
    setup(ClientOpcode::DcpFlush_Unsupported, not_supported_validator);
    setup(ClientOpcode::ResetReplicationChain_Unsupported,
          not_supported_validator);
    setup(ClientOpcode::NotifyVbucketUpdate_Unsupported,
          not_supported_validator);
    setup(ClientOpcode::SnapshotVbStates_Unsupported, not_supported_validator);
    setup(ClientOpcode::VbucketBatchCount_Unsupported, not_supported_validator);
    setup(ClientOpcode::ChangeVbFilter_Unsupported, not_supported_validator);
    setup(ClientOpcode::CheckpointPersistence_Unsupported,
          not_supported_validator);
    setup(ClientOpcode::CreateCheckpoint_Unsupported, not_supported_validator);
    setup(ClientOpcode::LastClosedCheckpoint_Unsupported,
          not_supported_validator);
    setup(ClientOpcode::SslCertsRefresh_Unsupported, not_supported_validator);

    setup(ClientOpcode::RangeScanCreate, create_range_scan_validator);
    setup(ClientOpcode::RangeScanContinue, continue_range_scan_validator);
    setup(ClientOpcode::RangeScanCancel, cancel_range_scan_validator);

    setup(ClientOpcode::GetFileFragment, get_file_fragment_validator);
    setup(ClientOpcode::PrepareSnapshot, prepare_snapshot_validator);
    setup(ClientOpcode::ReleaseSnapshot, release_snapshot_validator);
    setup(ClientOpcode::DownloadSnapshot, download_snapshot_validator);

    setup(ClientOpcode::GetFusionStorageSnapshot,
          get_fusion_storage_snapshot_validator);
    setup(ClientOpcode::ReleaseFusionStorageSnapshot,
          release_fusion_storage_snapshot_validator);
    setup(ClientOpcode::MountFusionVbucket, mount_vbucket_validator);
    setup(ClientOpcode::UnmountFusionVbucket, unmount_vbucket_validator);
    setup(ClientOpcode::SyncFusionLogstore, sync_fusion_logstore_validator);
    setup(ClientOpcode::StartFusionUploader, start_fusion_uploader_validator);
    setup(ClientOpcode::StopFusionUploader, stop_fusion_uploader_validator);
    setup(ClientOpcode::SetChronicleAuthToken,
          set_chronicle_auth_token_validator);
    setup(ClientOpcode::DeleteFusionNamespace,
          delete_fusion_namespace_validator);
    setup(ClientOpcode::GetFusionNamespaces, get_fusion_namespaces_validator);
}
