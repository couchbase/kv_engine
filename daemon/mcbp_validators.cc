/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "config.h"

#include "buckets.h"
#include "connection.h"
#include "cookie.h"
#include "mcbp_validators.h"
#include "memcached.h"
#include "subdocument_validators.h"
#include "xattr/utils.h"
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>
#include <platform/compress.h>
#include <platform/string_hex.h>

using Status = cb::mcbp::Status;

static inline bool may_accept_xattr(const Cookie& cookie) {
    auto& req = cookie.getHeader();
    if (mcbp::datatype::is_xattr(req.getDatatype())) {
        return cookie.getConnection().isXattrEnabled();
    }

    return true;
}

bool is_document_key_valid(const Cookie& cookie) {
    const auto& req = cookie.getRequest(Cookie::PacketContent::Header);
    if (cookie.getConnection().isCollectionsSupported()) {
        const auto& key = req.getKey();
        auto stopByte = cb::mcbp::unsigned_leb128_get_stop_byte_index(key);
        // 1. CID is leb128 encode, key must then be 1 byte of key and 1 byte of
        //    leb128 minimum
        // 2. Secondly - require that the leb128 and key are encoded, i.e. we
        //    expect that the leb128 stop byte is not the last byte of the key.
        return req.getKeylen() > 1 && stopByte && (key.size() - 1) > *stopByte;
    }
    return req.getKeylen() > 0;
}

static inline bool may_accept_dcp_deleteV2(const Cookie& cookie) {
    return cookie.getConnection().isDcpDeleteV2();
}

static inline std::string get_peer_description(const Cookie& cookie) {
    return cookie.getConnection().getDescription();
}

enum class ExpectedKeyLen { Zero, NonZero, Any };
enum class ExpectedValueLen { Zero, NonZero, Any };
enum class ExpectedCas { Set, NotSet, Any };

/**
 * Verify the header meets basic sanity checks and fields length
 * match the provided expected lengths.
 */
static bool verify_header(
        Cookie& cookie,
        uint8_t expected_extlen,
        ExpectedKeyLen expected_keylen,
        ExpectedValueLen expected_valuelen,
        ExpectedCas expected_cas = ExpectedCas::Any,
        uint8_t expected_datatype_mask = mcbp::datatype::highest) {
    const auto& header = cookie.getHeader();

    if (!header.isValid()) {
        cookie.setErrorContext("Request header invalid");
        return false;
    }
    if (!mcbp::datatype::is_valid(header.getDatatype())) {
        cookie.setErrorContext("Request datatype invalid");
        return false;
    }

    if ((expected_extlen == 0) && (header.getExtlen() != 0)) {
        cookie.setErrorContext("Request must not include extras");
        return false;
    }
    if ((expected_extlen != 0) && (header.getExtlen() != expected_extlen)) {
        cookie.setErrorContext("Request must include extras of length " +
                               std::to_string(expected_extlen));
        return false;
    }

    switch (expected_keylen) {
    case ExpectedKeyLen::Zero:
        if (header.getKeylen() != 0) {
            cookie.setErrorContext("Request must not include key");
            return false;
        }
        break;
    case ExpectedKeyLen::NonZero:
        if (header.getKeylen() == 0) {
            cookie.setErrorContext("Request must include key");
            return false;
        }
        break;
    case ExpectedKeyLen::Any:
        break;
    }

    uint32_t valuelen =
            header.getBodylen() - header.getKeylen() - header.getExtlen();
    switch (expected_valuelen) {
    case ExpectedValueLen::Zero:
        if (valuelen != 0) {
            cookie.setErrorContext("Request must not include value");
            return false;
        }
        break;
    case ExpectedValueLen::NonZero:
        if (valuelen == 0) {
            cookie.setErrorContext("Request must include value");
            return false;
        }
        break;
    case ExpectedValueLen::Any:
        break;
    }

    switch (expected_cas) {
    case ExpectedCas::NotSet:
        if (header.getCas() != 0) {
            cookie.setErrorContext("Request CAS must not be set");
            return false;
        }
        break;
    case ExpectedCas::Set:
        if (header.getCas() == 0) {
            cookie.setErrorContext("Request CAS must be set");
            return false;
        }
        break;
    case ExpectedCas::Any:
        break;
    }

    if ((~expected_datatype_mask) & header.getDatatype()) {
        cookie.setErrorContext("Request datatype invalid");
        return false;
    }

    return true;
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
 * In the future it should be extended to verify that the various DCP
 * commands is only sent on a connection which is set up as a DCP
 * connection (except the initial OPEN etc)
 *
 * @param cookie The command cookie
 */
static Status verify_common_dcp_restrictions(Cookie& cookie) {
    auto* dcp = cookie.getConnection().getBucket().getDcpIface();
    if (!dcp) {
        cookie.setErrorContext("Attached bucket does not support DCP");
        return Status::NotSupported;
    }

    const auto& connection = cookie.getConnection();
    if (connection.allowUnorderedExecution()) {
        LOG_WARNING(
                "DCP on a connection with unordered execution is currently "
                "not supported: {}",
                get_peer_description(cookie));
        cookie.setErrorContext(
                "DCP on connections with unordered execution is not supported");
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status dcp_open_validator(Cookie& cookie) {
    using cb::mcbp::request::DcpOpenPayload;

    if (!verify_header(cookie,
                       sizeof(DcpOpenPayload),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    const auto mask = DCP_OPEN_PRODUCER | DCP_OPEN_NOTIFIER |
                      DCP_OPEN_INCLUDE_XATTRS | DCP_OPEN_NO_VALUE |
                      DCP_OPEN_INCLUDE_DELETE_TIMES;

    auto ext = cookie.getHeader().getExtdata();
    const auto* payload = reinterpret_cast<const DcpOpenPayload*>(ext.data());
    const auto flags = payload->getFlags();

    if (flags & ~mask) {
        LOG_INFO(
                "Client trying to open dcp stream with unknown flags ({:x}) {}",
                flags,
                get_peer_description(cookie));
        cookie.setErrorContext("Request contains invalid flags");
        return Status::Einval;
    }

    if ((flags & DCP_OPEN_NOTIFIER) && (flags & ~DCP_OPEN_NOTIFIER)) {
        LOG_INFO(
                "Invalid flags combination ({:x}) specified for a DCP "
                "consumer {}",
                flags,
                get_peer_description(cookie));
        cookie.setErrorContext("Request contains invalid flags combination");
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_add_stream_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto extras = req.getExtdata();
    const uint32_t flags =
            ntohl(*reinterpret_cast<const uint32_t*>(extras.data()));
    const auto mask = DCP_ADD_STREAM_FLAG_TAKEOVER |
                      DCP_ADD_STREAM_FLAG_DISKONLY |
                      DCP_ADD_STREAM_FLAG_LATEST |
                      DCP_ADD_STREAM_ACTIVE_VB_ONLY;

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
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_get_failover_log_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_stream_req_validator(Cookie& cookie) {
    auto vlen = cookie.getConnection().isCollectionsSupported()
                        ? ExpectedValueLen::Any
                        : ExpectedValueLen::Zero;

    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::DcpStreamReqPayload),
                       ExpectedKeyLen::Zero,
                       vlen,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_stream_end_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::DcpStreamEndPayload),
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_snapshot_marker_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       20,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_system_event_validator(Cookie& cookie) {
    // keylen + bodylen > ??
    auto req = static_cast<protocol_binary_request_dcp_system_event*>(
            cookie.getPacketAsVoidPtr());

    if (!verify_header(
                cookie,
                protocol_binary_request_dcp_system_event::getExtrasLength(),
                ExpectedKeyLen::Any,
                ExpectedValueLen::Any)) {
        return Status::Einval;
    }

    if (!mcbp::systemevent::validate_id(ntohl(req->message.body.event))) {
        cookie.setErrorContext("Invalid system event id");
        return Status::Einval;
    }

    if (!mcbp::systemevent::validate_version(req->message.body.version)) {
        cookie.setErrorContext("Invalid system event version");
        return Status::Einval;
    }

    return verify_common_dcp_restrictions(cookie);
}

static bool is_valid_xattr_blob(const cb::mcbp::Request& request) {
    auto value = request.getValue();
    cb::compression::Buffer buffer;
    cb::const_char_buffer xattr{reinterpret_cast<const char*>(value.data()),
                                value.size()};
    if (mcbp::datatype::is_snappy(uint8_t(request.getDatatype()))) {
        // Inflate the xattr data and validate that.
        if (!cb::compression::inflate(
                    cb::compression::Algorithm::Snappy, xattr, buffer)) {
            return false;
        }
        xattr = buffer;
    }

    return cb::xattr::validate(xattr);
}

static Status dcp_mutation_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       protocol_binary_request_dcp_mutation::getExtrasLength(),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any)) {
        return Status::Einval;
    }

    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    if (!may_accept_xattr(cookie)) {
        cookie.setErrorContext("Connection not Xattr enabled");
        return Status::Einval;
    }

    auto& header = cookie.getHeader();
    const auto datatype = header.getDatatype();

    if (mcbp::datatype::is_xattr(datatype) &&
        !is_valid_xattr_blob(header.getRequest())) {
        cookie.setErrorContext("Xattr blob not valid");
        return Status::XattrEinval;
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

    if (!verify_header(cookie,
                       gsl::narrow<uint8_t>(expectedExtlen),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any)) {
        return Status::Einval;
    }

    if (!valid_dcp_delete_datatype(cookie.getHeader().getDatatype())) {
        cookie.setErrorContext("Request datatype invalid");
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_expiration_validator(Cookie& cookie) {
    if (!verify_header(
                cookie,
                gsl::narrow<uint8_t>(protocol_binary_request_dcp_expiration::
                                             getExtrasLength()),
                ExpectedKeyLen::NonZero,
                ExpectedValueLen::Zero,
                ExpectedCas::Any,
                PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_set_vbucket_state_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       1,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto extras = cookie.getHeader().getRequest().getExtdata();
    if (extras[0] < 1 || extras[0] > 4) {
        cookie.setErrorContext("Request body state invalid");
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_noop_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_buffer_acknowledgement_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_control_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status update_user_permissions_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status configuration_refresh_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status auth_provider_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status drop_privilege_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_cluster_config_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status set_cluster_config_validator(Cookie& cookie) {
    if (!verify_header(
                cookie,
                0,
                ExpectedKeyLen::Zero,
                ExpectedValueLen::NonZero,
                ExpectedCas::Any,
                PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_JSON)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status verbosity_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::VerbosityPayload),
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status hello_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto value = req.getValue();
    if ((value.size() % 2) != 0) {
        cookie.setErrorContext("Request value must be of even length");
        return Status::Einval;
    }

    return Status::Success;
}

static Status version_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status quit_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status sasl_list_mech_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status sasl_auth_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status noop_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status flush_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();
    uint8_t extlen = header.getExtlen();

    if (extlen != 0 && extlen != 4) {
        cookie.setErrorContext("Request extras must be of length 0 or 4");
        return Status::Einval;
    }
    // We've already checked extlen so pass actual extlen as expected extlen
    if (!verify_header(cookie,
                       extlen,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
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
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::MutationPayload),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       expected_datatype_mask)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return Status::Success;
}

static Status set_replace_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must have extras and key, may have value */
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::MutationPayload),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       expected_datatype_mask)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return Status::Success;
}

static Status append_prepend_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must not have extras, must have key, may have value */
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       expected_datatype_mask)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status gat_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::GatPayload),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status delete_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status stat_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status arithmetic_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::ArithmeticPayload),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_cmd_timer_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       1,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status set_ctrl_token_validator(Cookie& cookie) {
    using cb::mcbp::request::SetCtrlTokenPayload;
    if (!verify_header(cookie,
                       sizeof(SetCtrlTokenPayload),
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto extras = cookie.getRequest(Cookie::PacketContent::Full).getExtdata();
    auto* payload = reinterpret_cast<const SetCtrlTokenPayload*>(extras.data());
    if (payload->getCas() == 0) {
        cookie.setErrorContext("New CAS must be set");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_ctrl_token_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status ioctl_get_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getKeylen() > IOCTL_KEY_LENGTH) {
        cookie.setErrorContext("Request key length exceeds maximum");
        return Status::Einval;
    }

    return Status::Success;
}

static Status ioctl_set_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto req = cookie.getHeader().getRequest();

    if (req.getKey().size() > IOCTL_KEY_LENGTH) {
        cookie.setErrorContext("Request key length exceeds maximum");
        return Status::Einval;
    }

    if (req.getValue().size() > IOCTL_VAL_LENGTH) {
        cookie.setErrorContext("Request value length exceeds maximum");
        return Status::Einval;
    }

    return Status::Success;
}

static Status audit_put_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status audit_config_reload_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status config_reload_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status config_validate_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    auto& header = cookie.getHeader();
    const auto bodylen = header.getBodylen();

    if (bodylen > CONFIG_VALIDATE_MAX_LENGTH) {
        cookie.setErrorContext("Request value length exceeds maximum");
        return Status::Einval;
    }
    return Status::Success;
}

static Status observe_seqno_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getBodylen() != 8) {
        cookie.setErrorContext("Request value must be of length 8");
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_adjusted_time_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status set_drift_counter_state_validator(Cookie& cookie) {
    constexpr uint8_t expected_extlen = sizeof(uint8_t) + sizeof(int64_t);

    if (!verify_header(cookie,
                       expected_extlen,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
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
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getKeylen() > MAX_BUCKET_NAME_LENGTH) {
        cookie.setErrorContext("Request key length exceeds maximum");
        return Status::Einval;
    }

    return Status::Success;
}

static Status list_bucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status delete_bucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status select_bucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getKeylen() > 1023) {
        cookie.setErrorContext("Request key length exceeds maximum");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_all_vb_seqnos_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();

    // We check extlen below so pass actual extlen as expected_extlen to bypass
    // the check in verify_header
    if (!verify_header(cookie,
                       header.getExtlen(),
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto extras = header.getRequest().getExtdata();

    if (!extras.empty()) {
        // extlen is optional, and if non-zero it contains the vbucket
        // state to report
        if (extras.size() != sizeof(vbucket_state_t)) {
            cookie.setErrorContext("Request extras must be of length 0 or " +
                                   std::to_string(sizeof(vbucket_state_t)));
            return Status::Einval;
        }
        vbucket_state_t state;
        std::copy(extras.begin(),
                  extras.end(),
                  reinterpret_cast<uint8_t*>(&state));
        state = static_cast<vbucket_state_t>(ntohl(state));
        if (!is_valid_vbucket_state_t(state)) {
            cookie.setErrorContext("Request vbucket state invalid");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status shutdown_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Set,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_meta_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();

    // We check extlen below so pass actual extlen as expected_extlen to bypass
    // the check in verify_header
    if (!verify_header(cookie,
                       header.getExtlen(),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto extras = header.getRequest().getExtdata();

    if (extras.size() > 1) {
        cookie.setErrorContext("Request extras must be of length 0 or 1");
        return Status::Einval;
    }

    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
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
    // the check in verify_header
    if (!verify_header(cookie,
                       header.getExtlen(),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    if (!may_accept_xattr(cookie)) {
        cookie.setErrorContext("Connection not Xattr enabled");
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

    if (mcbp::datatype::is_xattr(header.getDatatype()) &&
        !is_valid_xattr_blob(header.getRequest())) {
        cookie.setErrorContext("Xattr blob invalid");
        return Status::XattrEinval;
    }

    return Status::Success;
}

static Status get_errmap_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto& request = cookie.getHeader().getRequest();
    if (request.getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    if (request.getValue().size() != 2) {
        cookie.setErrorContext("Request value must be of length 2");
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_locked_validator(Cookie& cookie) {
    const auto extlen = cookie.getHeader().getExtlen();

    // We check extlen below so pass actual extlen as expected extlen to bypass
    // the check in verify header
    if (!verify_header(cookie,
                       extlen,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    if (extlen != 0 && extlen != sizeof(cb::mcbp::request::GetLockedPayload)) {
        cookie.setErrorContext("Request extras must be of length 0 or 4");
        return Status::Einval;
    }

    return Status::Success;
}

static Status unlock_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Set,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status evict_key_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return Status::Success;
}

static Status collections_set_manifest_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getRequest().getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    // We could do these tests before checking the packet, but
    // it feels cleaner to validate the packet first.
    auto* engine = cookie.getConnection().getBucket().getEngine();
    if (engine == nullptr || engine->collections.set_manifest == nullptr) {
        cookie.setErrorContext("Attached bucket does not support collections");
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status collections_get_manifest_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getRequest().getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    // We could do these tests before checking the packet, but
    // it feels cleaner to validate the packet first.
    auto* engine = cookie.getConnection().getBucket().getEngine();
    if (engine == nullptr || engine->collections.get_manifest == nullptr) {
        cookie.setErrorContext("Attached bucket does not support collections");
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status adjust_timeofday_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::AdjustTimePayload),
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    // The method should only be available for unit tests
    if (getenv("MEMCACHED_UNIT_TESTS") == nullptr) {
        cookie.setErrorContext("Only available for unit tests");
        return Status::NotSupported;
    }

    auto extras = cookie.getHeader().getExtdata();
    using cb::mcbp::request::AdjustTimePayload;
    auto* payload = reinterpret_cast<const AdjustTimePayload*>(extras.data());
    if (!payload->isValid()) {
        cookie.setErrorContext("Unexpected value for TimeType");
    }

    return Status::Success;
}

static Status ewb_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::EWB_Payload),
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    // The method should only be available for unit tests
    if (getenv("MEMCACHED_UNIT_TESTS") == nullptr) {
        cookie.setErrorContext("Only available for unit tests");
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status scrub_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_random_key_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status set_vbucket_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();
    if (!verify_header(cookie,
                       header.getExtlen(),
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto& request = header.getRequest();
    auto extras = request.getExtdata();
    static_assert(sizeof(vbucket_state_t) == 4,
                  "Unexpected size for vbucket_state_t");
    if (extras.size() == sizeof(vbucket_state_t)) {
        if (!request.getValue().empty()) {
            cookie.setErrorContext("No value should be present");
            return Status::Einval;
        }
    } else {
        // MB-31867: ns_server encodes this in the value field. Fall back
        //           and check if it contains the value
        auto value = request.getValue();
        if (value.size() != sizeof(vbucket_state_t)) {
            cookie.setErrorContext(
                    "Expected 4 bytes of extras containing the new state");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status del_vbucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_vbucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status start_stop_persistence_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status enable_disable_traffic_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_keys_validator(Cookie& cookie) {
    const auto extlen = cookie.getHeader().getExtlen();
    if (!verify_header(cookie,
                       extlen,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
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
    if (!verify_header(cookie,
                       sizeof(SetParamPayload),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto extras = cookie.getHeader().getExtdata();
    auto* payload = reinterpret_cast<const SetParamPayload*>(extras.data());
    if (!payload->validate()) {
        cookie.setErrorContext("Invalid param type specified");
    }

    return Status::Success;
}

static Status return_meta_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       sizeof(cb::mcbp::request::ReturnMetaPayload),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    using cb::mcbp::request::ReturnMetaPayload;
    using cb::mcbp::request::ReturnMetaType;
    auto* payload = reinterpret_cast<const ReturnMetaPayload*>(
            cookie.getHeader().getRequest().getExtdata().data());

    switch (payload->getMutationType()) {
    case ReturnMetaType::Set:
    case ReturnMetaType::Add:
    case ReturnMetaType::Del:
        return Status::Success;
    }

    cookie.setErrorContext("Invalid mode");
    return Status::Einval;
}

static Status seqno_persistence_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       8,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status last_closed_checkpoint_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status create_checkpoint_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status chekpoint_persistence_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();

    if (!verify_header(cookie,
                       header.getExtlen(),
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto& req = header.getRequest();
    if (req.getExtdata().size() == sizeof(uint64_t)) {
        if (!req.getValue().empty()) {
            cookie.setErrorContext("Missing checkpoint id");
            return Status::Einval;
        }
    } else {
        if (req.getValue().size() != sizeof(uint64_t)) {
            cookie.setErrorContext("Missing checkpoint id");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status compact_db_validator(Cookie& cookie) {
    using cb::mcbp::request::CompactDbPayload;

    if (!verify_header(cookie,
                       sizeof(CompactDbPayload),
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    const auto extras = cookie.getHeader().getRequest().getExtdata();
    const auto* payload =
            reinterpret_cast<const CompactDbPayload*>(extras.data());
    if (!payload->validate()) {
        cookie.setErrorContext("Padding bytes should be set to 0");
        return Status::Einval;
    }

    return Status::Success;
}

static Status not_supported_validator(Cookie& cookie) {
    auto& header = cookie.getHeader();
    if (!verify_header(cookie,
                       header.getExtlen(),
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::NotSupported;
}

Status McbpValidator::validate(ClientOpcode command, Cookie& cookie) {
    const auto idx = std::underlying_type<ClientOpcode>::type(command);
    if (validators[idx]) {
        return validators[idx](cookie);
    }
    return Status::Success;
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
    setup(cb::mcbp::ClientOpcode::GetAdjustedTime, get_adjusted_time_validator);
    setup(cb::mcbp::ClientOpcode::SetDriftCounterState,
          set_drift_counter_state_validator);

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
    setup(cb::mcbp::ClientOpcode::CheckpointPersistence,
          chekpoint_persistence_validator);
    setup(cb::mcbp::ClientOpcode::CompactDb, compact_db_validator);

    // Add a validator which returns not supported (we won't execute
    // these either as the executor would have returned not supported
    // They're added sto make it easier to see which opcodes we currently
    // don't have a proper validator for
    setup(cb::mcbp::ClientOpcode::Rget, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rset, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rsetq, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rappend, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rappendq, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rprepend, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rprependq, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rdelete, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rdeleteq, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rincr, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rincrq, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rdecr, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::Rdecrq, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapConnect, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapConnect, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapMutation, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapDelete, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapFlush, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapOpaque, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapVbucketSet, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapCheckpointStart, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::TapCheckpointEnd, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::DeregisterTapClient, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::ResetReplicationChain,
          not_supported_validator);
    setup(cb::mcbp::ClientOpcode::NotifyVbucketUpdate, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::SnapshotVbStates, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::VbucketBatchCount, not_supported_validator);
    setup(cb::mcbp::ClientOpcode::ChangeVbFilter, not_supported_validator);
}
