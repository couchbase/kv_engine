/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp.h"
#include "testapp_assert_helper.h"

#include <mcbp/protocol/framebuilder.h>
#include <memcached/util.h>

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
