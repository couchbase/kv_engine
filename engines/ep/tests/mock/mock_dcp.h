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

#pragma once

#include "config.h"

#include <memcached/engine.h>
#include <memcached/dcp.h>

extern std::vector<std::pair<uint64_t, uint64_t> > dcp_failover_log;

ENGINE_ERROR_CODE mock_dcp_add_failover_log(vbucket_failover_t* entry,
                                            size_t nentries,
                                            gsl::not_null<const void*> cookie);

void clear_dcp_data();

class MockDcpMessageProducers : public dcp_message_producers {
public:
    MockDcpMessageProducers(EngineIface* engine = nullptr);

    ENGINE_ERROR_CODE get_failover_log(uint32_t opaque,
                                       uint16_t vbucket) override;

    ENGINE_ERROR_CODE stream_req(uint32_t opaque,
                                 uint16_t vbucket,
                                 uint32_t flags,
                                 uint64_t start_seqno,
                                 uint64_t end_seqno,
                                 uint64_t vbucket_uuid,
                                 uint64_t snap_start_seqno,
                                 uint64_t snap_end_seqno) override;

    ENGINE_ERROR_CODE add_stream_rsp(uint32_t opaque,
                                     uint32_t stream_opaque,
                                     uint8_t status) override;

    ENGINE_ERROR_CODE marker_rsp(uint32_t opaque, uint8_t status) override;

    ENGINE_ERROR_CODE set_vbucket_state_rsp(uint32_t opaque,
                                            uint8_t status) override;

    ENGINE_ERROR_CODE stream_end(uint32_t opaque,
                                 uint16_t vbucket,
                                 uint32_t flags) override;

    ENGINE_ERROR_CODE marker(uint32_t opaque,
                             uint16_t vbucket,
                             uint64_t start_seqno,
                             uint64_t end_seqno,
                             uint32_t flags) override;

    ENGINE_ERROR_CODE mutation(uint32_t opaque,
                               item* itm,
                               uint16_t vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t lock_time,
                               const void* meta,
                               uint16_t nmeta,
                               uint8_t nru) override;

    ENGINE_ERROR_CODE deletion(uint32_t opaque,
                               item* itm,
                               uint16_t vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               const void* meta,
                               uint16_t nmeta) override;

    ENGINE_ERROR_CODE deletion_v2(uint32_t opaque,
                                  gsl::not_null<item*> itm,
                                  uint16_t vbucket,
                                  uint64_t by_seqno,
                                  uint64_t rev_seqno,
                                  uint32_t delete_time) override;

    ENGINE_ERROR_CODE expiration(uint32_t opaque,
                                 item* itm,
                                 uint16_t vbucket,
                                 uint64_t by_seqno,
                                 uint64_t rev_seqno,
                                 const void* meta,
                                 uint16_t nmeta) override;

    ENGINE_ERROR_CODE set_vbucket_state(uint32_t opaque,
                                        uint16_t vbucket,
                                        vbucket_state_t state) override;
    ENGINE_ERROR_CODE noop(uint32_t opaque) override;
    ENGINE_ERROR_CODE buffer_acknowledgement(uint32_t opaque,
                                             uint16_t vbucket,
                                             uint32_t buffer_bytes) override;
    ENGINE_ERROR_CODE control(uint32_t opaque,
                              const void* key,
                              uint16_t nkey,
                              const void* value,
                              uint32_t nvalue) override;
    ENGINE_ERROR_CODE get_error_map(uint32_t opaque, uint16_t version) override;
    // Change the status code returned from mutation() to the specified value.
    void setMutationStatus(ENGINE_ERROR_CODE code);

protected:
    /// Helper method for deletion / deletion_v2
    ENGINE_ERROR_CODE deletionInner(uint32_t opaque,
                                    item* itm,
                                    uint16_t vbucket,
                                    uint64_t by_seqno,
                                    uint64_t rev_seqno,
                                    const void* meta,
                                    uint16_t nmeta,
                                    uint32_t deleteTime,
                                    uint32_t extlen);

    ENGINE_ERROR_CODE mutationStatus = ENGINE_SUCCESS;
};
