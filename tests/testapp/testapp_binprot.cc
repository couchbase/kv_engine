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
#include "config.h"
#include "testapp.h"
#include "testapp_assert_helper.h"

#include <include/memcached/util.h>

/*
 * Set the read/write commands differently than the default values
 * so that we can verify that the override works
 */
static cb::mcbp::ClientOpcode read_command = cb::mcbp::ClientOpcode::Invalid;
static cb::mcbp::ClientOpcode write_command = cb::mcbp::ClientOpcode::Invalid;

off_t mcbp_raw_command(char* buf,
                       size_t bufsz,
                       cb::mcbp::ClientOpcode cmd,
                       const void* key,
                       size_t keylen,
                       const void* dta,
                       size_t dtalen) {
    /* all of the storage commands use the same command layout */
    off_t key_offset;
    protocol_binary_request_no_extras* request =
        reinterpret_cast<protocol_binary_request_no_extras*>(buf);
    EXPECT_GE(bufsz, (sizeof(*request) + keylen + dtalen));

    memset(request, 0, sizeof(*request));
    if (cmd == read_command || cmd == write_command) {
        request->message.header.request.extlen = 8;
    } else if (cmd == cb::mcbp::ClientOpcode::AuditPut) {
        request->message.header.request.extlen = 4;
    } else if (cmd == cb::mcbp::ClientOpcode::EwouldblockCtl) {
        request->message.header.request.extlen = 12;
    } else if (cmd == cb::mcbp::ClientOpcode::SetCtrlToken) {
        request->message.header.request.extlen = 8;
    }
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.setOpcode(cmd);
    request->message.header.request.keylen = htons((uint16_t)keylen);
    request->message.header.request.bodylen = htonl(
        (uint32_t)(keylen + dtalen + request->message.header.request.extlen));
    request->message.header.request.opaque = 0xdeadbeef;

    key_offset = sizeof(protocol_binary_request_no_extras) +
                 request->message.header.request.extlen;

    if (key != NULL) {
        memcpy(buf + key_offset, key, keylen);
    }
    if (dta != NULL) {
        memcpy(buf + key_offset + keylen, dta, dtalen);
    }

    return (off_t)(sizeof(*request) + keylen + dtalen +
                   request->message.header.request.extlen);
}

off_t mcbp_arithmetic_command(char* buf,
                              size_t bufsz,
                              cb::mcbp::ClientOpcode cmd,
                              const void* key,
                              size_t keylen,
                              uint64_t delta,
                              uint64_t initial,
                              uint32_t exp) {
    off_t key_offset;
    protocol_binary_request_incr* request =
        reinterpret_cast<protocol_binary_request_incr*>(buf);
    cb_assert(bufsz > sizeof(*request) + keylen);

    memset(request, 0, sizeof(*request));
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.setOpcode(cmd);
    request->message.header.request.keylen = htons((uint16_t)keylen);
    request->message.header.request.extlen = 20;
    request->message.header.request.bodylen = htonl((uint32_t)(keylen + 20));
    request->message.header.request.opaque = 0xdeadbeef;
    request->message.body.delta = htonll(delta);
    request->message.body.initial = htonll(initial);
    request->message.body.expiration = htonl(exp);

    key_offset = sizeof(protocol_binary_request_no_extras) + 20;

    memcpy(buf + key_offset, key, keylen);
    return (off_t)(key_offset + keylen);
}

size_t mcbp_storage_command(Frame& frame,
                            cb::mcbp::ClientOpcode cmd,
                            const std::string& id,
                            const std::vector<uint8_t>& value,
                            uint32_t flags,
                            uint32_t exp) {
    frame.reset();
    // A storage command consists of a 24 byte memcached header, then 4
    // bytes flags and 4 bytes expiration time.
    frame.payload.resize(24 + 4 + 4 + id.size() + value.size());
    auto size = mcbp_storage_command(
        reinterpret_cast<char*>(frame.payload.data()), frame.payload.size(),
        cmd, id.data(), id.size(), value.data(), value.size(), flags, exp);
    frame.payload.resize(size);
    return size;
}

size_t mcbp_storage_command(char* buf,
                            size_t bufsz,
                            cb::mcbp::ClientOpcode cmd,
                            const void* key,
                            size_t keylen,
                            const void* dta,
                            size_t dtalen,
                            uint32_t flags,
                            uint32_t exp) {
    /* all of the storage commands use the same command layout */
    size_t key_offset;

    auto* request = reinterpret_cast<protocol_binary_request_set*>(buf);
    cb_assert(bufsz >= sizeof(*request) + keylen + dtalen);

    memset(request, 0, sizeof(*request));
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.setOpcode(cmd);
    request->message.header.request.keylen = htons((uint16_t)keylen);
    request->message.header.request.opaque = 0xdeadbeef;
    key_offset = sizeof(protocol_binary_request_no_extras);

    if (cmd != cb::mcbp::ClientOpcode::Append &&
        cmd != cb::mcbp::ClientOpcode::Prepend) {
        request->message.header.request.extlen = 8;
        request->message.header.request.bodylen = htonl(
            (uint32_t)(keylen + 8 + dtalen));
        request->message.body.flags = htonl(flags);
        request->message.body.expiration = htonl(exp);
        key_offset += 8;
    } else {
        request->message.header.request.bodylen = htonl(
            (uint32_t)(keylen + dtalen));
    }

    memcpy(buf + key_offset, key, keylen);
    if (dta != nullptr) {
        memcpy(buf + key_offset + keylen, dta, dtalen);
    }

    return key_offset + keylen + dtalen;
}

/* Validate the specified response header against the expected cmd and status.
 */
void mcbp_validate_response_header(protocol_binary_response_no_extras* response,
                                   cb::mcbp::ClientOpcode cmd,
                                   cb::mcbp::Status status) {
    auto* header = &response->message.header;
    if (status == cb::mcbp::Status::UnknownCommand) {
        if (header->response.getStatus() == cb::mcbp::Status::NotSupported) {
            header->response.setStatus(cb::mcbp::Status::UnknownCommand);
        }
    }
    bool mutation_seqno_enabled =
            enabled_hello_features.count(cb::mcbp::Feature::MUTATION_SEQNO) > 0;
    EXPECT_TRUE(mcbp_validate_response_header(
            header, cmd, status, mutation_seqno_enabled));
}

void mcbp_validate_arithmetic(const protocol_binary_response_incr* incr,
                              uint64_t expected) {
    const uint8_t* ptr = incr->bytes + sizeof(incr->message.header) +
                         incr->message.header.response.getExtlen();
    const uint64_t result = ntohll(*(uint64_t*)ptr);
    EXPECT_EQ(expected, result);

    /* Check for extras - if present should be {vbucket_uuid, seqno) pair for
     * mutation seqno support. */
    if (incr->message.header.response.getExtlen() != 0) {
        EXPECT_EQ(16, incr->message.header.response.getExtlen());
    }
}

::testing::AssertionResult mcbp_validate_response_header(
        const protocol_binary_response_header* header,
        cb::mcbp::ClientOpcode cmd,
        cb::mcbp::Status status,
        bool mutation_seqno_enabled) {
    AssertHelper result;

    TESTAPP_EXPECT_EQ(
            result, PROTOCOL_BINARY_RES, uint8_t(header->response.getMagic()));
    TESTAPP_EXPECT_EQ(result, cmd, header->response.getClientOpcode());
    TESTAPP_EXPECT_EQ(result, status, header->response.getStatus());
    TESTAPP_EXPECT_EQ(result, 0xdeadbeef, header->response.getOpaque());

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
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16u, header->response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 16u, header->response.getBodylen());
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getBodylen());
            }
            TESTAPP_EXPECT_NE(result, header->response.cas, 0u);
            break;
        case cb::mcbp::ClientOpcode::Flush:
        case cb::mcbp::ClientOpcode::Noop:
        case cb::mcbp::ClientOpcode::Quit:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, header->response.getExtlen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));
            TESTAPP_EXPECT_EQ(result, 0u, header->response.getBodylen());
            break;
        case cb::mcbp::ClientOpcode::Delete:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16u, header->response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 16u, header->response.getBodylen());
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getBodylen());
            }
            break;
        case cb::mcbp::ClientOpcode::Decrement:
        case cb::mcbp::ClientOpcode::Increment:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));
            /* extlen is permitted to be either zero, or 16 if MUTATION_SEQNO
             * is enabled. Similary, bodylen must be either 8 or 24. */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16, header->response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 24u, header->response.getBodylen());
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 8u, header->response.getBodylen());
            }
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;

        case cb::mcbp::ClientOpcode::Stat:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getExtlen());
            /* key and value exists in all packets except in the terminating */
            TESTAPP_EXPECT_EQ(result, 0u, header->response.cas);
            break;

        case cb::mcbp::ClientOpcode::Version:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, header->response.getExtlen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));
            TESTAPP_EXPECT_NE(result, 0u, header->response.getBodylen());
            TESTAPP_EXPECT_EQ(result, 0u, header->response.cas);
            break;

        case cb::mcbp::ClientOpcode::Get:
        case cb::mcbp::ClientOpcode::Getq:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 4, header->response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;

        case cb::mcbp::ClientOpcode::Getk:
        case cb::mcbp::ClientOpcode::Getkq:
            TESTAPP_EXPECT_NE(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 4, header->response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;
        case cb::mcbp::ClientOpcode::SubdocGet:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, header->response.getExtlen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.
            TESTAPP_EXPECT_NE(result, 0u, header->response.getBodylen());
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;
        case cb::mcbp::ClientOpcode::SubdocExists:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, header->response.getExtlen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));
            TESTAPP_EXPECT_EQ(result, 0u, header->response.getBodylen());
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;
        case cb::mcbp::ClientOpcode::SubdocDictAdd:
        case cb::mcbp::ClientOpcode::SubdocDictUpsert:
        case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
        case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
        case cb::mcbp::ClientOpcode::SubdocArrayInsert:
        case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));

            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16, header->response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 16u, header->response.getBodylen());
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getExtlen());
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getBodylen());
            }
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;
        case cb::mcbp::ClientOpcode::SubdocCounter:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            // Datatype depends on the document fetched / if Hello::JSON
            // negotiated - should be checked by caller.

            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16, header->response.getExtlen());
                TESTAPP_EXPECT_GT(result, header->response.getBodylen(), 16u);
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getExtlen());
                TESTAPP_EXPECT_NE(result, 0u, header->response.getBodylen());
            }
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;

        case cb::mcbp::ClientOpcode::SubdocMultiLookup:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            TESTAPP_EXPECT_EQ(result, 0, header->response.getExtlen());
            // Datatype of a multipath body is RAW_BYTES, as the body is
            // a binary structure packing multiple (JSON) fragments.
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));
            TESTAPP_EXPECT_NE(result, 0u, header->response.getBodylen());
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;

        case cb::mcbp::ClientOpcode::SubdocMultiMutation:
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
            // Datatype of a multipath body is RAW_BYTES, as the body is
            // a binary structure packing multiple results.
            TESTAPP_EXPECT_EQ(result,
                              PROTOCOL_BINARY_RAW_BYTES,
                              uint8_t(header->response.getDatatype()));
            /* extlen is either zero, or 16 if MUTATION_SEQNO is enabled.
             * bodylen is at least as big as extlen.
             */
            if (mutation_seqno_enabled) {
                TESTAPP_EXPECT_EQ(result, 16, header->response.getExtlen());
                TESTAPP_EXPECT_GE(result, header->response.getBodylen(), 16u);
            } else {
                TESTAPP_EXPECT_EQ(result, 0u, header->response.getExtlen());
                TESTAPP_EXPECT_GE(result, header->response.getBodylen(), 0u);
            }
            TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
            break;
        default:
            /* Undefined command code */
            break;
        }
    } else if (status == cb::mcbp::Status::SubdocMultiPathFailure) {
        // Subdoc: Even though the some paths may have failed; actual document
        // was successfully accessed so CAS may be valid.
    } else if (status == cb::mcbp::Status::SubdocSuccessDeleted) {
        TESTAPP_EXPECT_NE(result, 0u, header->response.cas);
    } else {
        TESTAPP_EXPECT_EQ(result, 0u, header->response.cas);
        TESTAPP_EXPECT_EQ(result, 0, header->response.getExtlen());
        if (cmd != cb::mcbp::ClientOpcode::Getk) {
            TESTAPP_EXPECT_EQ(result, 0, header->response.getKeylen());
        }
    }
    return result.result();
}
