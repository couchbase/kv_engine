/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_binprot.h"
#include "testapp.h"
#include <mcbp/protocol/framebuilder.h>
#include <memcached/util.h>

static void mcbp_validate_response_header(const cb::mcbp::Response& response,
                                          cb::mcbp::ClientOpcode cmd,
                                          cb::mcbp::Status status,
                                          bool mutation_seqno_enabled);

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
    mcbp_validate_response_header(
            response, cmd, status, mutation_seqno_enabled);
}

static void mcbp_validate_response_header(const cb::mcbp::Response& response,
                                          cb::mcbp::ClientOpcode cmd,
                                          cb::mcbp::Status status,
                                          bool mutation_seqno_enabled) {
    ASSERT_EQ(cb::mcbp::Magic::ClientResponse, response.getMagic());
    ASSERT_EQ(cmd, response.getClientOpcode());
    ASSERT_EQ(status, response.getStatus());
    ASSERT_EQ(0xdeadbeef, response.getOpaque());

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
            FAIL() << "Quiet command shouldn't return on success";
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
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                ASSERT_EQ(16u, response.getExtlen());
                ASSERT_EQ(16u, response.getBodylen());
            } else {
                ASSERT_EQ(0u, response.getExtlen());
                ASSERT_EQ(0u, response.getBodylen());
            }
            ASSERT_NE(0, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::Flush:
        case cb::mcbp::ClientOpcode::Noop:
        case cb::mcbp::ClientOpcode::Quit:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(0, response.getExtlen());
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));
            ASSERT_EQ(0u, response.getBodylen());
            break;
        case cb::mcbp::ClientOpcode::Delete:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                ASSERT_EQ(16u, response.getExtlen());
                ASSERT_EQ(16u, response.getBodylen());
            } else {
                ASSERT_EQ(0u, response.getExtlen());
                ASSERT_EQ(0u, response.getBodylen());
            }
            break;
        case cb::mcbp::ClientOpcode::Decrement:
        case cb::mcbp::ClientOpcode::Increment:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));
            /* extlen is permitted to be either zero, or 16 if MUTATION_SEQNO
             * is enabled. Similary, bodylen must be either 8 or 24. */
            if (mutation_seqno_enabled) {
                ASSERT_EQ(16, response.getExtlen());
                ASSERT_EQ(24u, response.getBodylen());
            } else {
                ASSERT_EQ(0u, response.getExtlen());
                ASSERT_EQ(8u, response.getBodylen());
            }
            ASSERT_NE(0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::Stat:
            ASSERT_EQ(0, response.getExtlen());
            /* key and value exists in all packets except in the terminating */
            ASSERT_EQ(0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::Version:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(0, response.getExtlen());
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));
            ASSERT_NE(0u, response.getBodylen());
            ASSERT_EQ(0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::Get:
        case cb::mcbp::ClientOpcode::Getq:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(4, response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            ASSERT_NE(0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::Getk:
        case cb::mcbp::ClientOpcode::Getkq:
            ASSERT_NE(0, response.getKeylen());
            ASSERT_EQ(4, response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            ASSERT_NE(0u, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::SubdocGet:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(0, response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            ASSERT_NE(0u, response.getBodylen());
            ASSERT_NE(0u, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::SubdocExists:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(0, response.getExtlen());
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));
            ASSERT_EQ(0u, response.getBodylen());
            ASSERT_NE(0u, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::SubdocDictAdd:
        case cb::mcbp::ClientOpcode::SubdocDictUpsert:
        case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
        case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
        case cb::mcbp::ClientOpcode::SubdocArrayInsert:
        case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));

            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                ASSERT_EQ(16, response.getExtlen());
                ASSERT_EQ(16u, response.getBodylen());
            } else {
                ASSERT_EQ(0u, response.getExtlen());
                ASSERT_EQ(0u, response.getBodylen());
            }
            ASSERT_NE(0u, response.getCas());
            break;
        case cb::mcbp::ClientOpcode::SubdocCounter:
            ASSERT_EQ(0, response.getKeylen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.

            if (mutation_seqno_enabled) {
                ASSERT_EQ(16, response.getExtlen());
                ASSERT_GT(response.getBodylen(), 16u);
            } else {
                ASSERT_EQ(0u, response.getExtlen());
                ASSERT_NE(0u, response.getBodylen());
            }
            ASSERT_NE(0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::SubdocMultiLookup:
            ASSERT_EQ(0, response.getKeylen());
            ASSERT_EQ(0, response.getExtlen());
            // Datatype of a multipath body is RAW_BYTES, as the body is
            // a binary structure packing multiple (JSON) fragments.
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));
            ASSERT_NE(0u, response.getBodylen());
            ASSERT_NE(0u, response.getCas());
            break;

        case cb::mcbp::ClientOpcode::SubdocMultiMutation:
            ASSERT_EQ(0, response.getKeylen());
            // Datatype of a multipath body is RAW_BYTES, as the body is
            // a binary structure packing multiple results.
            ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES,
                      uint8_t(response.getDatatype()));
            /* extlen is either zero, or 16 if MUTATION_SEQNO is enabled.
             * bodylen is at least as big as extlen.
             */
            if (mutation_seqno_enabled) {
                ASSERT_EQ(16, response.getExtlen());
                ASSERT_GE(response.getBodylen(), 16u);
            } else {
                ASSERT_EQ(0u, response.getExtlen());
                ASSERT_GE(response.getBodylen(), 0u);
            }
            ASSERT_NE(0u, response.getCas());
            break;
        default:
            /* Undefined command code */
            break;
        }
    } else if (status == cb::mcbp::Status::SubdocMultiPathFailure) {
        // Subdoc: Even though the some paths may have failed; actual document
        // was successfully accessed so CAS may be valid.
    } else if (status == cb::mcbp::Status::SubdocSuccessDeleted) {
        ASSERT_NE(0u, response.getCas());
    } else {
        ASSERT_EQ(0u, response.getCas());
        ASSERT_EQ(0, response.getExtlen());
        if (cmd != cb::mcbp::ClientOpcode::Getk) {
            ASSERT_EQ(0, response.getKeylen());
        }
    }
}
