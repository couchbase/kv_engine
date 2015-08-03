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

#include <mutex>
#include <vector>

static std::vector<mcbp_package_validate> validators;

static void initialize();

mcbp_package_validate *get_mcbp_validators(void) {
    static std::mutex mutex;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (validators.size() != 0x100) {
            initialize();
        }
    }

    return validators.data();
}

/******************************************************************************
 *                         Package validators                                 *
 *****************************************************************************/
static int dcp_open_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_open*>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 8 ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        /* INCORRECT FORMAT */
        return -1;
    }

    return 0;
}

static int dcp_add_stream_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_add_stream *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != htonl(4) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        /* INCORRECT FORMAT */
        return -1;
    }

    return 0;
}

static int dcp_close_stream_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_close_stream *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        /* INCORRECT FORMAT */
        return -1;
    }

    return 0;
}

static int dcp_get_failover_log_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_get_failover_log *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int dcp_stream_req_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_stream_req *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 5*sizeof(uint64_t) + 2*sizeof(uint32_t) ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        /* INCORRECT FORMAT */
        return -1;
    }
    return 0;
}

static int dcp_stream_end_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_stream_end *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != htonl(4) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int dcp_snapshot_marker_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_snapshot_marker *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 20 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != htonl(20) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int dcp_mutation_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_mutation *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != (2*sizeof(uint64_t) + 3 * sizeof(uint32_t) + sizeof(uint16_t)) + sizeof(uint8_t) ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.bodylen == 0) {
        return -1;
    }

    return 0;
}

static int dcp_deletion_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_deletion *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != (2*sizeof(uint64_t) + sizeof(uint16_t)) ||
        req->message.header.request.keylen == 0) {
        return -1;
    }

    return 0;
}

static int dcp_expiration_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_deletion *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t bodylen = ntohl(req->message.header.request.bodylen) - klen;
    bodylen -= req->message.header.request.extlen;
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != (2*sizeof(uint64_t) + sizeof(uint16_t)) ||
        req->message.header.request.keylen == 0 ||
        bodylen != 0) {
        return -1;
    }

    return 0;
}

static int dcp_flush_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_flush *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int dcp_set_vbucket_state_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_set_vbucket_state *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 1 ||
        req->message.header.request.keylen != 0 ||
        ntohl(req->message.header.request.bodylen) != 1 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    if (req->message.body.state < 1 || req->message.body.state > 4) {
        return -1;
    }

    return 0;
}

static int dcp_noop_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_noop *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int dcp_buffer_acknowledgement_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_buffer_acknowledgement *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != ntohl(4) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int dcp_control_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_dcp_control *>(packet);
    uint16_t nkey = ntohs(req->message.header.request.keylen);
    uint32_t nval = ntohl(req->message.header.request.bodylen) - nkey;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 || nkey == 0 || nval == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int isasl_refresh_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int ssl_certs_refresh_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int verbosity_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.cas != 0 ||
        ntohl(req->message.header.request.bodylen) != 4 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int hello_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint32_t len = ntohl(req->message.header.request.bodylen);
    len -= ntohs(req->message.header.request.keylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 || (len % 2) != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int version_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int quit_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.header.request.cas != 0) {
        return -1;
    }

    return 0;
}

static int sasl_list_mech_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int sasl_auth_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int noop_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.header.request.cas != 0) {
        return -1;
    }

    return 0;
}

static int flush_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint8_t extlen = req->message.header.request.extlen;
    uint32_t bodylen = ntohl(req->message.header.request.bodylen);

    if (extlen != 0 && extlen != 4) {
        return -1;
    }

    if (bodylen != extlen) {
        return -1;
    }

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.header.request.cas != 0) {
        return -1;
    }

    return 0;
}

static int add_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    /* Must have extras and key, may have value */

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 8 ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.cas != 0) {
        return -1;
    }
    return 0;
}

static int set_replace_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    /* Must have extras and key, may have value */

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 8 ||
        req->message.header.request.keylen == 0) {
        return -1;
    }
    return 0;
}

static int append_prepend_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    /* Must not have extras, must have key, may have value */

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen == 0) {
        return -1;
    }
    return 0;
}

static int get_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        klen == 0 || klen != blen ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.header.request.cas != 0) {
        return -1;
    }

    return 0;
}

static int delete_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        klen == 0 || klen != blen ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int stat_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 || klen != blen ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int arithmetic_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    uint8_t extlen = req->message.header.request.extlen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        extlen != 20 || klen == 0 || (klen + extlen) != blen ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int get_cmd_timer_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    uint8_t extlen = req->message.header.request.extlen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        extlen != 1 || (klen + extlen) != blen ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int set_ctrl_token_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_set_ctrl_token *>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != sizeof(uint64_t) ||
        req->message.header.request.keylen != 0 ||
        ntohl(req->message.header.request.bodylen) != sizeof(uint64_t) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES ||
        req->message.body.new_cas == 0) {
        return -1;
    }

    return 0;
}

static int get_ctrl_token_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int init_complete_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int ioctl_get_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_ioctl_get *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        klen == 0 || klen != blen || klen > IOCTL_KEY_LENGTH ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int ioctl_set_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_ioctl_set *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.cas != 0 ||
        klen == 0 || klen > IOCTL_KEY_LENGTH ||
        vallen > IOCTL_VAL_LENGTH ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int assume_role_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 || klen != blen  ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int audit_put_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_audit_put *>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 4 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.cas != 0 ||
        ntohl(req->message.header.request.bodylen) <= 4 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }
    return 0;
}

static int audit_config_reload_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }
    return 0;
}

static int observe_seqno_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras *>(packet);
    uint32_t bodylen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        bodylen != 8 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }
    return 0;
}

static int get_adjusted_time_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_get_adjusted_time*>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }
    return 0;
}

static int set_drift_counter_state_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_set_drift_counter_state*>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.extlen != sizeof(uint8_t) + sizeof(int64_t) ||
        req->message.header.request.keylen != 0 ||
        ntohl(req->message.header.request.bodylen) != sizeof(uint8_t) + sizeof(int64_t) ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }
    return 0;
}

/**
 * The create bucket contains message have the following format:
 *    key: bucket name
 *    body: module\nconfig
 */
static int create_bucket_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(packet);

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
        return -1;
    }

    return 0;
}

static int list_bucket_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.keylen != 0 ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.bodylen != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int delete_bucket_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(packet);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        req->message.header.request.keylen == 0 ||
        req->message.header.request.extlen != 0 ||
        req->message.header.request.bodylen == 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int select_bucket_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_no_extras*>(packet);

    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        klen != blen || req->message.header.request.extlen != 0 ||
        klen > 1023 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    return 0;
}

static int get_all_vb_seqnos_validator(void *packet)
{
    auto req = static_cast<protocol_binary_request_get_all_vb_seqnos*>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    uint8_t extlen = req->message.header.request.extlen;

    if (req->message.header.request.magic != PROTOCOL_BINARY_REQ ||
        klen != 0 || extlen != blen ||
        req->message.header.request.cas != 0 ||
        req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        return -1;
    }

    if (extlen != 0) {
        // extlen is optional, and if non-zero it contains the vbucket
        // state to report
        if (extlen != sizeof(vbucket_state_t)) {
            return -1;
        }
        vbucket_state_t state;
        memcpy(&state, &req->message.body.state, sizeof(vbucket_state_t));
        state = static_cast<vbucket_state_t>(ntohl(state));
        if (!is_valid_vbucket_state_t(state)) {
            return -1;
        }
    }

    return 0;
}

static int null_validator(void *) {
    return 0;
}

static void initialize() {
    validators.reserve(0x100);
    for (int ii = 0; ii < 0x100; ++ii) {
        validators.push_back(null_validator);
    }
    validators[PROTOCOL_BINARY_CMD_DCP_OPEN] = dcp_open_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = dcp_add_stream_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = dcp_close_stream_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = dcp_snapshot_marker_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_DELETION] = dcp_deletion_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = dcp_expiration_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_FLUSH] = dcp_flush_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = dcp_get_failover_log_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_MUTATION] = dcp_mutation_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = dcp_set_vbucket_state_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_NOOP] = dcp_noop_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = dcp_buffer_acknowledgement_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_CONTROL] = dcp_control_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = dcp_stream_end_validator;
    validators[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = dcp_stream_req_validator;
    validators[PROTOCOL_BINARY_CMD_ISASL_REFRESH] = isasl_refresh_validator;
    validators[PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH] = ssl_certs_refresh_validator;
    validators[PROTOCOL_BINARY_CMD_VERBOSITY] = verbosity_validator;
    validators[PROTOCOL_BINARY_CMD_HELLO] = hello_validator;
    validators[PROTOCOL_BINARY_CMD_VERSION] = version_validator;
    validators[PROTOCOL_BINARY_CMD_QUIT] = quit_validator;
    validators[PROTOCOL_BINARY_CMD_QUITQ] = quit_validator;
    validators[PROTOCOL_BINARY_CMD_SASL_LIST_MECHS] = sasl_list_mech_validator;
    validators[PROTOCOL_BINARY_CMD_SASL_AUTH] = sasl_auth_validator;
    validators[PROTOCOL_BINARY_CMD_SASL_STEP] = sasl_auth_validator;
    validators[PROTOCOL_BINARY_CMD_NOOP] = noop_validator;
    validators[PROTOCOL_BINARY_CMD_FLUSH] = flush_validator;
    validators[PROTOCOL_BINARY_CMD_FLUSHQ] = flush_validator;
    validators[PROTOCOL_BINARY_CMD_GET] = get_validator;
    validators[PROTOCOL_BINARY_CMD_GETQ] = get_validator;
    validators[PROTOCOL_BINARY_CMD_GETK] = get_validator;
    validators[PROTOCOL_BINARY_CMD_GETKQ] = get_validator;
    validators[PROTOCOL_BINARY_CMD_DELETE] = delete_validator;
    validators[PROTOCOL_BINARY_CMD_DELETEQ] = delete_validator;
    validators[PROTOCOL_BINARY_CMD_STAT] = stat_validator;
    validators[PROTOCOL_BINARY_CMD_INCREMENT] = arithmetic_validator;
    validators[PROTOCOL_BINARY_CMD_INCREMENTQ] = arithmetic_validator;
    validators[PROTOCOL_BINARY_CMD_DECREMENT] = arithmetic_validator;
    validators[PROTOCOL_BINARY_CMD_DECREMENTQ] = arithmetic_validator;
    validators[PROTOCOL_BINARY_CMD_GET_CMD_TIMER] = get_cmd_timer_validator;
    validators[PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN] = set_ctrl_token_validator;
    validators[PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN] = get_ctrl_token_validator;
    validators[PROTOCOL_BINARY_CMD_INIT_COMPLETE] = init_complete_validator;
    validators[PROTOCOL_BINARY_CMD_IOCTL_GET] = ioctl_get_validator;
    validators[PROTOCOL_BINARY_CMD_IOCTL_SET] = ioctl_set_validator;
    validators[PROTOCOL_BINARY_CMD_ASSUME_ROLE] = assume_role_validator;
    validators[PROTOCOL_BINARY_CMD_AUDIT_PUT] = audit_put_validator;
    validators[PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD] = audit_config_reload_validator;
    validators[PROTOCOL_BINARY_CMD_OBSERVE_SEQNO] = observe_seqno_validator;
    validators[PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME] = get_adjusted_time_validator;
    validators[PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE] = set_drift_counter_state_validator;

#ifndef BUILDING_VALIDATORS_TEST
    validators[PROTOCOL_BINARY_CMD_SUBDOC_GET] = subdoc_get_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_EXISTS] = subdoc_exists_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD] = subdoc_dict_add_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT] = subdoc_dict_upsert_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_DELETE] = subdoc_delete_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_REPLACE] = subdoc_replace_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST] = subdoc_array_push_last_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST] = subdoc_array_push_first_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT] = subdoc_array_insert_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE] = subdoc_array_add_unique_validator;
    validators[PROTOCOL_BINARY_CMD_SUBDOC_COUNTER] = subdoc_counter_validator;
#endif

    validators[PROTOCOL_BINARY_CMD_SETQ] = set_replace_validator;
    validators[PROTOCOL_BINARY_CMD_SET] = set_replace_validator;
    validators[PROTOCOL_BINARY_CMD_ADDQ] = add_validator;
    validators[PROTOCOL_BINARY_CMD_ADD] = add_validator;
    validators[PROTOCOL_BINARY_CMD_REPLACEQ] = set_replace_validator;
    validators[PROTOCOL_BINARY_CMD_REPLACE] = set_replace_validator;
    validators[PROTOCOL_BINARY_CMD_APPENDQ] = append_prepend_validator;
    validators[PROTOCOL_BINARY_CMD_APPEND] = append_prepend_validator;
    validators[PROTOCOL_BINARY_CMD_PREPENDQ] = append_prepend_validator;
    validators[PROTOCOL_BINARY_CMD_PREPEND] = append_prepend_validator;
    validators[PROTOCOL_BINARY_CMD_CREATE_BUCKET] = create_bucket_validator;
    validators[PROTOCOL_BINARY_CMD_LIST_BUCKETS] = list_bucket_validator;
    validators[PROTOCOL_BINARY_CMD_DELETE_BUCKET] = delete_bucket_validator;
    validators[PROTOCOL_BINARY_CMD_SELECT_BUCKET] = select_bucket_validator;
    validators[PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS] = get_all_vb_seqnos_validator;
}
