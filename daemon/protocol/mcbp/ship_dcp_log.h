/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#pragma once

#include <memcached/dcp.h>

class Cookie;
void ship_dcp_log(Cookie& c);

ENGINE_ERROR_CODE dcp_message_marker_response(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint8_t status);

ENGINE_ERROR_CODE dcp_message_set_vbucket_state_response(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint8_t status);

ENGINE_ERROR_CODE dcp_message_stream_end(gsl::not_null<const void*> void_cookie,
                                         uint32_t opaque,
                                         uint16_t vbucket,
                                         uint32_t flags);

ENGINE_ERROR_CODE dcp_message_marker(gsl::not_null<const void*> void_cookie,
                                     uint32_t opaque,
                                     uint16_t vbucket,
                                     uint64_t start_seqno,
                                     uint64_t end_seqno,
                                     uint32_t flags);

ENGINE_ERROR_CODE dcp_message_mutation(gsl::not_null<const void*> void_cookie,
                                       uint32_t opaque,
                                       item* it,
                                       uint16_t vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t lock_time,
                                       const void* meta,
                                       uint16_t nmeta,
                                       uint8_t nru,
                                       uint8_t collection_len);

ENGINE_ERROR_CODE dcp_message_deletion_v1(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        item* it,
        uint16_t vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        const void* meta,
        uint16_t nmeta);

ENGINE_ERROR_CODE dcp_message_deletion_v2(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        gsl::not_null<item*> it,
        uint16_t vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t delete_time,
        uint8_t collection_len);

ENGINE_ERROR_CODE dcp_message_expiration(gsl::not_null<const void*> void_cookie,
                                         uint32_t opaque,
                                         item* it,
                                         uint16_t vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         const void* meta,
                                         uint16_t nmeta,
                                         uint8_t collection_len);

ENGINE_ERROR_CODE dcp_message_flush(gsl::not_null<const void*> void_cookie,
                                    uint32_t opaque,
                                    uint16_t vbucket);

ENGINE_ERROR_CODE dcp_message_set_vbucket_state(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket,
        vbucket_state_t state);

ENGINE_ERROR_CODE dcp_message_noop(gsl::not_null<const void*> void_cookie,
                                   uint32_t opaque);

ENGINE_ERROR_CODE dcp_message_buffer_acknowledgement(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket,
        uint32_t buffer_bytes);

ENGINE_ERROR_CODE dcp_message_control(gsl::not_null<const void*> void_cookie,
                                      uint32_t opaque,
                                      const void* key,
                                      uint16_t nkey,
                                      const void* value,
                                      uint32_t nvalue);

ENGINE_ERROR_CODE dcp_message_system_event(gsl::not_null<const void*> cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket,
                                           mcbp::systemevent::id event,
                                           uint64_t bySeqno,
                                           cb::const_byte_buffer key,
                                           cb::const_byte_buffer eventData);

ENGINE_ERROR_CODE dcp_message_get_error_map(gsl::not_null<const void*> cookie,
                                            uint32_t opaque,
                                            uint16_t version);
