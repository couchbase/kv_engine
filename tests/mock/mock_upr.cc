/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include <stdlib.h>

#include "item.h"
#include "mock_upr.h"

uint8_t upr_last_op;
uint8_t upr_last_status;
uint16_t upr_last_vbucket;
uint32_t upr_last_opaque;
uint32_t upr_last_flags;
uint32_t upr_last_stream_opaque;
uint32_t upr_last_locktime;
uint64_t upr_last_cas;
uint64_t upr_last_start_seqno;
uint64_t upr_last_end_seqno;
uint64_t upr_last_vbucket_uuid;
uint64_t upr_last_high_seqno;
uint64_t upr_last_byseqno;
uint64_t upr_last_revseqno;
const void *upr_last_meta;
uint16_t upr_last_nmeta;
std::string upr_last_key;

extern "C" {

ENGINE_ERROR_CODE mock_upr_add_failover_log(vbucket_failover_t* entry,
                                            size_t nentries,
                                            const void *cookie) {
    (void) entry;
    (void) nentries;
    (void) cookie;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_get_failover_log(const void *cookie,
                                               uint32_t opaque,
                                               uint16_t vbucket) {
    (void) cookie;
    (void) opaque;
    (void) vbucket;
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE mock_stream_req(const void *cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint32_t flags,
                                        uint64_t start_seqno,
                                        uint64_t end_seqno,
                                        uint64_t vbucket_uuid,
                                        uint64_t high_seqno) {
    (void) cookie;
    upr_last_op = PROTOCOL_BINARY_CMD_UPR_STREAM_REQ;
    upr_last_opaque = opaque;
    upr_last_vbucket = vbucket;
    upr_last_flags = flags;
    upr_last_start_seqno = start_seqno;
    upr_last_end_seqno = end_seqno;
    upr_last_vbucket_uuid = vbucket_uuid;
    upr_last_high_seqno = high_seqno;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_add_stream_rsp(const void *cookie,
                                             uint32_t opaque,
                                             uint32_t stream_opaque,
                                             uint8_t status) {
    (void) cookie;
    upr_last_op = PROTOCOL_BINARY_CMD_UPR_ADD_STREAM;
    upr_last_opaque = opaque;
    upr_last_stream_opaque = stream_opaque;
    upr_last_status = status;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_stream_end(const void *cookie,
                                         uint32_t opaque,
                                         uint16_t vbucket,
                                         uint32_t flags) {
    (void) cookie;
    upr_last_op = PROTOCOL_BINARY_CMD_UPR_STREAM_END;
    upr_last_opaque = opaque;
    upr_last_vbucket = vbucket;
    upr_last_flags = flags;
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE mock_marker(const void *cookie,
                                     uint32_t opaque,
                                     uint16_t vbucket) {
    (void) cookie;
    upr_last_op = PROTOCOL_BINARY_CMD_UPR_SNAPSHOT_MARKER;
    upr_last_opaque = opaque;
    upr_last_vbucket = vbucket;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_mutation(const void* cookie,
                                       uint32_t opaque,
                                       item *itm,
                                       uint16_t vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t lock_time,
                                       const void *meta,
                                       uint16_t nmeta) {
    (void) cookie;
    upr_last_op = PROTOCOL_BINARY_CMD_UPR_MUTATION;
    upr_last_opaque = opaque;
    upr_last_key.assign(reinterpret_cast<Item*>(itm)->getKey().c_str());
    upr_last_vbucket = vbucket;
    upr_last_byseqno = by_seqno;
    upr_last_revseqno = rev_seqno;
    upr_last_locktime = lock_time;
    upr_last_meta = meta;
    upr_last_nmeta = nmeta;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_deletion(const void* cookie,
                                       uint32_t opaque,
                                       const void *key,
                                       uint16_t nkey,
                                       uint64_t cas,
                                       uint16_t vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       const void *meta,
                                       uint16_t nmeta) {
    (void) cookie;
    upr_last_op = PROTOCOL_BINARY_CMD_UPR_DELETION;
    upr_last_opaque = opaque;
    upr_last_key.assign(static_cast<const char*>(key), nkey);
    upr_last_cas = cas;
    upr_last_vbucket = vbucket;
    upr_last_byseqno = by_seqno;
    upr_last_revseqno = rev_seqno;
    upr_last_meta = meta;
    upr_last_nmeta = nmeta;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_expiration(const void* cookie,
                                         uint32_t opaque,
                                         const void *key,
                                         uint16_t nkey,
                                         uint64_t cas,
                                         uint16_t vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         const void *meta,
                                         uint16_t nmeta) {
    (void) cookie;
    (void) opaque;
    (void) key;
    (void) nkey;
    (void) cas;
    (void) vbucket;
    (void) by_seqno;
    (void) rev_seqno;
    (void)meta;
    (void)nmeta;
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE mock_flush(const void* cookie,
                                    uint32_t opaque,
                                    uint16_t vbucket) {
    (void) cookie;
    (void) opaque;
    (void) vbucket;
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE mock_set_vbucket_state(const void* cookie,
                                                uint32_t opaque,
                                                uint16_t vbucket,
                                                vbucket_state_t state) {
    (void) cookie;
    (void) opaque;
    (void) vbucket;
    (void) state;
    return ENGINE_ENOTSUP;
}

}

struct upr_message_producers* get_upr_producers() {
    upr_message_producers* producers =
        (upr_message_producers*)malloc(sizeof(upr_message_producers));

    producers->get_failover_log = mock_get_failover_log;
    producers->stream_req = mock_stream_req;
    producers->add_stream_rsp = mock_add_stream_rsp;
    producers->stream_end = mock_stream_end;
    producers->marker = mock_marker;
    producers->mutation = mock_mutation;
    producers->deletion = mock_deletion;
    producers->expiration = mock_expiration;
    producers->flush = mock_flush;
    producers->set_vbucket_state = mock_set_vbucket_state;

    return producers;
}
