/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
#include "mcbp_validators.h"
#include <memcached/protocol_binary.h>

#include "buckets.h"
#include "memcached.h"
#include "subdocument_validators.h"
#include "xattr_utils.h"

/******************************************************************************
 *                         Package validators                                 *
 *****************************************************************************/
static protocol_binary_response_status dcp_open_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_open*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 8 ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        /* INCORRECT FORMAT */
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_add_stream_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_add_stream*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != htonl(4) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        /* INCORRECT FORMAT */
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_close_stream_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_close_stream*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        /* INCORRECT FORMAT */
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_get_failover_log_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_get_failover_log*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_stream_req_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_stream_req*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 5*sizeof(uint64_t) + 2*sizeof(uint32_t) ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        /* INCORRECT FORMAT */
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_stream_end_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_stream_end*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != htonl(4) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_snapshot_marker_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_snapshot_marker*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 20 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != htonl(20) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static bool is_valid_xattr_blob(const protocol_binary_request_header& header) {
    const uint32_t extlen{header.request.extlen};
    const uint32_t keylen{ntohs(header.request.keylen)};
    const uint32_t bodylen{ntohl(header.request.bodylen)};

    auto* ptr = reinterpret_cast<const char*>(header.bytes);
    ptr += sizeof(header.bytes) + extlen + keylen;

    cb::const_char_buffer xattr{ptr, bodylen - keylen - extlen};
    return cb::xattr::validate(xattr);
}

static protocol_binary_response_status dcp_mutation_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_mutation*>(McbpConnection::getPacket(cookie));
    const auto datatype = req->message.header.request.datatype;

    const uint32_t extlen{req->message.header.request.extlen};
    const uint32_t keylen{ntohs(req->message.header.request.keylen)};
    const uint32_t bodylen{ntohl(req->message.header.request.bodylen)};

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        extlen != (2*sizeof(uint64_t) + 3 * sizeof(uint32_t) + sizeof(uint16_t)) + sizeof(uint8_t) ||
        keylen == 0 || bodylen == 0 || (keylen + extlen) > bodylen ||
        !mcbp::datatype::is_valid(datatype)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    if (mcbp::datatype::is_xattr(datatype) &&
        !is_valid_xattr_blob(req->message.header)) {
        return PROTOCOL_BINARY_RESPONSE_XATTR_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_deletion_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_deletion*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != (2*sizeof(uint64_t) + sizeof(uint16_t)) ||
        req->message.header.request.keylen == 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_expiration_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_deletion*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t bodylen = ntohl(req->message.header.request.bodylen) - klen;
    bodylen -= req->message.header.request.extlen;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != (2*sizeof(uint64_t) + sizeof(uint16_t)) ||
        req->message.header.request.keylen == 0 ||
        bodylen != 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_flush_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_flush*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_set_vbucket_state_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_set_vbucket_state*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 1 ||
        req->message.header.request.keylen != 0 ||
        ntohl(req->message.header.request.bodylen) != 1 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    if (req->message.body.state < 1 || req->message.body.state > 4) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_noop_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_noop*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_buffer_acknowledgement_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_buffer_acknowledgement*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != ntohl(4) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status dcp_control_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_dcp_control*>(McbpConnection::getPacket(cookie));
    uint16_t nkey = ntohs(req->message.header.request.keylen);
    uint32_t nval = ntohl(req->message.header.request.bodylen) - nkey;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 || nkey == 0 || nval == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status isasl_refresh_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status ssl_certs_refresh_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status verbosity_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.cas != 0 ||
        ntohl(req->message.header.request.bodylen) != 4 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status hello_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint32_t len = ntohl(req->message.header.request.bodylen);
    len -= ntohs(req->message.header.request.keylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 || (len % 2) != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status version_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status quit_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.header.request.cas != 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status sasl_list_mech_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status sasl_auth_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status noop_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.header.request.cas != 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status flush_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint8_t extlen = req->message.header.request.extlen;
    uint32_t bodylen = ntohl(req->message.header.request.bodylen);

    if (extlen != 0 && extlen != 4) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    if (bodylen != extlen) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    if (extlen == 4) {
        // We don't support delayed flush anymore
        auto *req = reinterpret_cast<protocol_binary_request_flush*>(McbpConnection::getPacket(cookie));
        if (req->message.body.expiration != 0) {
            return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
        }
    }

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.header.request.cas != 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status add_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    /* Must have extras and key, may have value */
    auto datatype = req->message.header.request.datatype;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 8 ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.cas != 0 ||
        mcbp::datatype::is_xattr(datatype) ||
        !mcbp::datatype::is_valid(datatype)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status set_replace_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    /* Must have extras and key, may have value */
    auto datatype = req->message.header.request.datatype;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 8 ||
        req->message.header.request.keylen == 0 ||
        mcbp::datatype::is_xattr(datatype) ||
        !mcbp::datatype::is_valid(datatype)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status append_prepend_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    /* Must not have extras, must have key, may have value */
    auto datatype = req->message.header.request.datatype;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen == 0 ||
        mcbp::datatype::is_xattr(datatype) ||
        !mcbp::datatype::is_valid(datatype)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status get_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        klen == 0 || klen != blen ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.header.request.cas != 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status delete_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        klen == 0 || klen != blen ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status stat_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 || klen != blen ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status arithmetic_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    uint8_t extlen = req->message.header.request.extlen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        extlen != 20 || klen == 0 || (klen + extlen) != blen ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status get_cmd_timer_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    uint8_t extlen = req->message.header.request.extlen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        extlen != 1 || (klen + extlen) != blen ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status set_ctrl_token_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_set_ctrl_token*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != sizeof(uint64_t) ||
        req->message.header.request.keylen != 0 ||
        ntohl(req->message.header.request.bodylen) != sizeof(uint64_t) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.body.new_cas == 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status get_ctrl_token_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status init_complete_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status ioctl_get_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_ioctl_get*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        klen == 0 || klen != blen || klen > IOCTL_KEY_LENGTH ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status ioctl_set_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_ioctl_set*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen) - klen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.cas != 0 ||
        klen == 0 || klen > IOCTL_KEY_LENGTH ||
        vallen > IOCTL_VAL_LENGTH ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status audit_put_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_audit_put*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.cas != 0 ||
        ntohl(req->message.header.request.bodylen) <= 4 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status audit_config_reload_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status observe_seqno_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint32_t bodylen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        bodylen != 8 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status get_adjusted_time_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_get_adjusted_time*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status set_drift_counter_state_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_set_drift_counter_state*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != sizeof(uint8_t) + sizeof(int64_t) ||
        req->message.header.request.keylen != 0 ||
        ntohl(req->message.header.request.bodylen) != sizeof(uint8_t) + sizeof(int64_t) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

/**
 * The create bucket contains message have the following format:
 *    key: bucket name
 *    body: module\nconfig
 */
static protocol_binary_response_status create_bucket_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    uint8_t extlen = req->message.header.request.extlen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        extlen != 0 || klen == 0 || klen > MAX_BUCKET_NAME_LENGTH ||
        /* The packet needs a body with the information of the bucket
         * to create
         */
        (blen - klen) == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status list_bucket_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status delete_bucket_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.bodylen == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status select_bucket_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        klen != blen || req->message.header.request.extlen != 0 ||
        klen > 1023 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status get_all_vb_seqnos_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_get_all_vb_seqnos*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    uint8_t extlen = req->message.header.request.extlen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        klen != 0 || extlen != blen ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    if (extlen != 0) {
        // extlen is optional, and if non-zero it contains the vbucket
        // state to report
        if (extlen != sizeof(vbucket_state_t)) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        vbucket_state_t state;
        memcpy(&state, &req->message.body.state, sizeof(vbucket_state_t));
        state = static_cast<vbucket_state_t>(ntohl(state));
        if (!is_valid_vbucket_state_t(state)) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status shutdown_validator(const Cookie& cookie)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status tap_validator(const Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_tap_no_extras*>(McbpConnection::getPacket(cookie));
    auto bodylen = ntohl(req->message.header.request.bodylen);
    auto enginelen = ntohs(req->message.body.tap.enginespecific_length);
    if (sizeof(req->message.body) > bodylen ||
        enginelen > bodylen) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status get_collections_validator(const Cookie& cookie) {
    /*
       This function will check key for collection
       cookie->getBucket().isKeyValidCollection() ...
    */

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status mutate_with_meta_validator(const Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_get_meta*>(McbpConnection::getPacket(cookie));

    const uint32_t extlen = req->message.header.request.extlen;
    const uint32_t keylen = ntohs(req->message.header.request.keylen);
    const uint32_t bodylen = ntohl(req->message.header.request.bodylen);
    const auto datatype = req->message.header.request.datatype;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        keylen == 0 || (keylen + extlen) > bodylen ||
        !mcbp::datatype::is_valid(datatype)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    // revid_nbytes, flags and exptime is mandatory fields.. and we need a key
    // extlen, the size dicates what is encoded.
    switch (extlen) {
    case 24: // no nmeta and no options
    case 26: // nmeta
    case 28: // options (4-byte field)
    case 30: // options and nmeta (options followed by nmeta)
        break;
    default:
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    if (mcbp::datatype::is_xattr(datatype) &&
        !is_valid_xattr_blob(req->message.header)) {
        return PROTOCOL_BINARY_RESPONSE_XATTR_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

void McbpValidatorChains::initializeMcbpValidatorChains(McbpValidatorChains& chains) {
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_OPEN, dcp_open_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_ADD_STREAM, dcp_add_stream_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM, dcp_close_stream_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_snapshot_marker_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_DELETION, dcp_deletion_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_EXPIRATION, dcp_expiration_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_FLUSH, dcp_flush_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG, dcp_get_failover_log_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_MUTATION, dcp_mutation_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE, dcp_set_vbucket_state_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_NOOP, dcp_noop_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT, dcp_buffer_acknowledgement_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_CONTROL, dcp_control_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_STREAM_END, dcp_stream_end_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ, dcp_stream_req_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_ISASL_REFRESH, isasl_refresh_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH, ssl_certs_refresh_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_VERBOSITY, verbosity_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_HELLO, hello_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_VERSION, version_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_QUIT, quit_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_QUITQ, quit_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SASL_LIST_MECHS, sasl_list_mech_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SASL_AUTH, sasl_auth_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SASL_STEP, sasl_auth_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_NOOP, noop_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_FLUSH, flush_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_FLUSHQ, flush_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GET, get_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GETQ, get_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GETK, get_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GETKQ, get_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DELETE, delete_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DELETEQ, delete_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_STAT, stat_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_INCREMENT, arithmetic_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_INCREMENTQ, arithmetic_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DECREMENT, arithmetic_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DECREMENTQ, arithmetic_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GET_CMD_TIMER, get_cmd_timer_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN, set_ctrl_token_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN, get_ctrl_token_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_INIT_COMPLETE, init_complete_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_IOCTL_GET, ioctl_get_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_IOCTL_SET, ioctl_set_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_AUDIT_PUT, audit_put_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD, audit_config_reload_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SHUTDOWN, shutdown_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_OBSERVE_SEQNO, observe_seqno_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, get_adjusted_time_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE, set_drift_counter_state_validator);

    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_GET, subdoc_get_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, subdoc_exists_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD, subdoc_dict_add_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT, subdoc_dict_upsert_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, subdoc_delete_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, subdoc_replace_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, subdoc_array_push_last_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, subdoc_array_push_first_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT, subdoc_array_insert_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE, subdoc_array_add_unique_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER, subdoc_counter_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP, subdoc_multi_lookup_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION, subdoc_multi_mutation_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT, subdoc_get_count_validator);

    chains.push_unique(PROTOCOL_BINARY_CMD_SETQ, set_replace_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SET, set_replace_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_ADDQ, add_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_ADD, add_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_REPLACEQ, set_replace_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_REPLACE, set_replace_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_APPENDQ, append_prepend_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_APPEND, append_prepend_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_PREPENDQ, append_prepend_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_PREPEND, append_prepend_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_CREATE_BUCKET, create_bucket_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_LIST_BUCKETS, list_bucket_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DELETE_BUCKET, delete_bucket_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SELECT_BUCKET, select_bucket_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS, get_all_vb_seqnos_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_TAP_MUTATION, tap_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END, tap_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START, tap_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_TAP_DELETE, tap_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_TAP_FLUSH, tap_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_TAP_OPAQUE, tap_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET, tap_validator);

    chains.push_unique(PROTOCOL_BINARY_CMD_GET_META, get_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GETQ_META, get_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SET_WITH_META, mutate_with_meta_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_SETQ_WITH_META, mutate_with_meta_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_ADD_WITH_META, mutate_with_meta_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_ADDQ_WITH_META, mutate_with_meta_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DEL_WITH_META, mutate_with_meta_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_DELQ_WITH_META, mutate_with_meta_validator);
}

/**
 * Add relevant collections validators to KV opcodes.
 */
void McbpValidatorChains::enableCollections(McbpValidatorChains& chains) {
    // example of how this will work
    chains.push_unique(PROTOCOL_BINARY_CMD_GET, get_collections_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GETQ, get_collections_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GETK, get_collections_validator);
    chains.push_unique(PROTOCOL_BINARY_CMD_GETKQ, get_collections_validator);
    // TODO: set/delete etc...
}
