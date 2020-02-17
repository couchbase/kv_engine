/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "testapp.h"
#include "testapp_assert_helper.h"

#include <mcbp/protocol/framebuilder.h>
#include <memcached/util.h>

std::vector<uint8_t> mcbp_arithmetic_command(cb::mcbp::ClientOpcode cmd,
                                             std::string_view key,
                                             uint64_t delta,
                                             uint64_t initial,
                                             uint32_t exp) {
    using namespace cb::mcbp;
    using request::ArithmeticPayload;

    ArithmeticPayload extras;
    extras.setDelta(delta);
    extras.setInitial(initial);
    extras.setExpiration(exp);

    std::vector<uint8_t> buffer(sizeof(Request) + sizeof(extras) + key.size());
    RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(Magic::ClientRequest);
    builder.setOpcode(cmd);
    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(&extras), sizeof(extras)});
    builder.setOpaque(0xdeadbeef);
    builder.setKey(key);
    return buffer;
}

std::vector<uint8_t> mcbp_storage_command(cb::mcbp::ClientOpcode cmd,
                                          std::string_view key,
                                          std::string_view value,
                                          uint32_t flags,
                                          uint32_t exp) {
    using namespace cb::mcbp;
    std::vector<uint8_t> buffer;
    size_t size = sizeof(Request) + key.size() + value.size();
    if (cmd != ClientOpcode::Append && cmd != ClientOpcode::Appendq &&
        cmd != ClientOpcode::Prepend && cmd != ClientOpcode::Prependq) {
        size += sizeof(request::MutationPayload);
    }
    buffer.resize(size);
    FrameBuilder<Request> builder({buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cmd);
    builder.setOpaque(0xdeadbeef);

    if (cmd != ClientOpcode::Append && cmd != ClientOpcode::Appendq &&
        cmd != ClientOpcode::Prepend && cmd != ClientOpcode::Prependq) {
        request::MutationPayload extras;
        extras.setFlags(flags);
        extras.setExpiration(exp);
        builder.setExtras(extras.getBuffer());
    }
    builder.setKey(key);
    builder.setValue(value);
    return buffer;
}

/* Validate the specified response header against the expected cmd and status.
 */
void mcbp_validate_response_header(protocol_binary_response_no_extras* response,
                                   cb::mcbp::ClientOpcode cmd,
                                   cb::mcbp::Status status) {
    mcbp_validate_response_header(
            response->message.header.response, cmd, status);
}

void mcbp_validate_response_header(cb::mcbp::Response& response,
                                   cb::mcbp::ClientOpcode cmd,
                                   cb::mcbp::Status status) {
    if (status == cb::mcbp::Status::UnknownCommand) {
        if (response.getStatus() == cb::mcbp::Status::NotSupported) {
            response.setStatus(cb::mcbp::Status::UnknownCommand);
        }
    }
    bool mutation_seqno_enabled =
            enabled_hello_features.count(cb::mcbp::Feature::MUTATION_SEQNO) > 0;
    EXPECT_TRUE(mcbp_validate_response_header(
            response, cmd, status, mutation_seqno_enabled));
}

::testing::AssertionResult mcbp_validate_response_header(
        const cb::mcbp::Response& response,
        cb::mcbp::ClientOpcode cmd,
        cb::mcbp::Status status,
        bool mutation_seqno_enabled) {
    AssertHelper result;

    TESTAPP_EXPECT_EQ(
            result, cb::mcbp::Magic::ClientResponse, response.getMagic());
    TESTAPP_EXPECT_EQ(result, cmd, response.getClientOpcode());
    TESTAPP_EXPECT_EQ(result, status, response.getStatus());
    TESTAPP_EXPECT_EQ(result, 0xdeadbeef, response.getOpaque());

    if (status == cb::mcbp::Status::Success) {
        switch (cmd) {
        case cb::mcbp::ClientOpcode::Addq:
        case cb::mcbp::ClientOpcode::Appendq:
        case cb::mcbp::ClientOpcode::Decrementq:
        case cb::mcbp::ClientOpcode::Deleteq:
        case cb::mcbp::ClientOpcode::Flushq:
        case cb::mcbp::ClientOpcode::Incrementq:
        case cb::mcbp::ClientOpcode::Prependq:
        case cb::mcbp::ClientOpcode::Quitq:
        case cb::mcbp::ClientOpcode::Replaceq:
        case cb::mcbp::ClientOpcode::Setq:
            result.fail("Quiet command shouldn't return on success");
            break;
        default:
            break;
        }

        switch (cmd) {
        case cb::mcbp::ClientOpcode::Add:
        case cb::mcbp::ClientOpcode::Replace:
        case cb::mcbp::ClientOpcode::Set:
        case cb::mcbp::ClientOpcode::Append:
        case cb::mcbp::ClientOpcode::Prepend:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16u, response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 16u, response.getBodylen());
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 0u, response.getBodylen());
            }
            TESTAPP_EXPECT_NE(result, response.getCas(), 0u);
            break;
        case cb::mcbp::ClientOpcode::Flush:
        case cb::mcbp::ClientOpcode::Noop:
        case cb::mcbp::ClientOpcode::Quit:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, response.getExtlen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));
            TESTAPP_EXPECT_EQ(result, 0u, response.getBodylen());
            break;
        case cb::mcbp::ClientOpcode::Delete:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16u, response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 16u, response.getBodylen());
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 0u, response.getBodylen());
            }
            break;
        case cb::mcbp::ClientOpcode::Decrement:
        case cb::mcbp::ClientOpcode::Increment:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));
            /* extlen is permitted to be either zero, or 16 if MUTATION_SEQNO
             * is enabled. Similary, bodylen must be either 8 or 24. */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16, response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 24u, response.getBodylen());
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 8u, response.getBodylen());
            }
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::Stat:
            TESTAPP_EXPECT_EQ(result, 0, response.getExtlen());
            /* key and value exists in all packets except in the terminating */
            TESTAPP_EXPECT_EQ(result, 0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::Version:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, response.getExtlen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));
            TESTAPP_EXPECT_NE(result, 0u, response.getBodylen());
            TESTAPP_EXPECT_EQ(result, 0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::Get:
        case cb::mcbp::ClientOpcode::Getq:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 4, response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::Getk:
        case cb::mcbp::ClientOpcode::Getkq:
            TESTAPP_EXPECT_NE(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 4, response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::SubdocGet:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            TESTAPP_EXPECT_NE(result, 0u, response.getBodylen());
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::SubdocExists:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, response.getExtlen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));
            TESTAPP_EXPECT_EQ(result, 0u, response.getBodylen());
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::SubdocDictAdd:
        case cb::mcbp::ClientOpcode::SubdocDictUpsert:
        case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
        case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
        case cb::mcbp::ClientOpcode::SubdocArrayInsert:
        case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));

            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16, response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 16u, response.getBodylen());
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 0u, response.getBodylen());
            }
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::SubdocCounter:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.

            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16, response.getExtlen());
                TESTAPP_EXPECT_GT(result, response.getBodylen(), 16u);
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, response.getExtlen());
                TESTAPP_EXPECT_NE(result, 0u, response.getBodylen());
            }
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::SubdocMultiLookup:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, response.getExtlen());
            // Datatype of a multipath body is RAW_BYTES, as the body is
            // a binary structure packing multiple (JSON) fragments.
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));
            TESTAPP_EXPECT_NE(result, 0u, response.getBodylen());
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::SubdocMultiMutation:
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
            // Datatype of a multipath body is RAW_BYTES, as the body is
            // a binary structure packing multiple results.
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(response.getDatatype()));
            /* extlen is either zero, or 16 if MUTATION_SEQNO is enabled.
             * bodylen is at least as big as extlen.
             */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16, response.getExtlen());
                TESTAPP_EXPECT_GE(result, response.getBodylen(), 16u);
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, response.getExtlen());
                TESTAPP_EXPECT_GE(result, response.getBodylen(), 0u);
            }
            TESTAPP_EXPECT_NE(result, 0u, response.getCas());
            break;
        default:
            /* Undefined command code */
            break;
        }
    } else if (status == cb::mcbp::Status::SubdocMultiPathFailure) {
        // Subdoc: Even though the some paths may have failed; actual document
        // was successfully accessed so CAS may be valid.
    } else if (status == cb::mcbp::Status::SubdocSuccessDeleted) {
        TESTAPP_EXPECT_NE(result, 0u, response.getCas());
    } else {
        TESTAPP_EXPECT_EQ(result, 0u, response.getCas());
        TESTAPP_EXPECT_EQ(result, 0, response.getExtlen());
        if (cmd != cb::mcbp::ClientOpcode::Getk) {
            TESTAPP_EXPECT_EQ(result, 0, response.getKeylen());
        }
    }
    return result.result();
}
