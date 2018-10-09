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

#include "test_helpers.h"

#include "dcp/consumer.h"
#include "dcp/response.h"

extern uint8_t dcp_last_op;

void handleProducerResponseIfStepBlocked(DcpConsumer& consumer) {
    // MB-29441: Added a call to DcpConsumer::handleGetErrorMap() in
    // DcpConsumer::step().
    // We need to call DcpConsumer::handleResponse() to notify (set a flag)
    // that the GetErrorMap response has been received. The next calls to
    // step() would block forever in DcpConsumer::handleGetErrorMap() otherwise
    if (dcp_last_op == PROTOCOL_BINARY_CMD_GET_ERROR_MAP) {
        protocol_binary_response_header resp{};
        resp.response.opcode = PROTOCOL_BINARY_CMD_GET_ERROR_MAP;
        resp.response.status = ntohs(PROTOCOL_BINARY_RESPONSE_SUCCESS);
        consumer.handleResponse(&resp);
    }
}

std::unique_ptr<MutationResponse> makeMutation(uint64_t seqno,
                                               uint16_t vbid,
                                               const std::string& value,
                                               uint64_t opaque) {
    uint8_t ext_meta[EXT_META_LEN] = {PROTOCOL_BINARY_RAW_BYTES};
    queued_item qi(new Item(makeStoredDocKey("key_" + std::to_string(seqno)),
                            0 /*flags*/,
                            0 /*expiry*/,
                            value.c_str(),
                            value.size(),
                            ext_meta,
                            1 /*ext_len*/,
                            0 /*cas*/,
                            seqno,
                            vbid));
    return std::make_unique<MutationResponse>(
            std::move(qi), opaque, IncludeValue::Yes, IncludeXattrs::Yes);
}
