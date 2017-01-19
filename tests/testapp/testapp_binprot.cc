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

#include <utilities/protocol2text.h>
#include <include/memcached/util.h>

/*
 * Set the read/write commands differently than the default values
 * so that we can verify that the override works
 */
static uint8_t read_command = 0xe1;
static uint8_t write_command = 0xe2;

off_t mcbp_raw_command(Frame& frame,
                       uint8_t cmd,
                       const void* key,
                       size_t keylen,
                       const void* data,
                       size_t datalen) {
    frame.reset();
    // 255 is the max amount of data that may fit in extdata. The underlying
    // mcbp_raw_command method magically injects extdata depending on the
    // opcode called, so just make room for it..
    frame.payload.resize(keylen + datalen + 255);
    off_t size = mcbp_raw_command(reinterpret_cast<char*>(frame.payload.data()),
                                  frame.payload.size(), cmd,
                                  key, keylen, data, datalen);
    frame.payload.resize(size);
    return size;
}

off_t mcbp_raw_command(char* buf,
                       size_t bufsz,
                       uint8_t cmd,
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
    } else if (cmd == PROTOCOL_BINARY_CMD_AUDIT_PUT) {
        request->message.header.request.extlen = 4;
    } else if (cmd == PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL) {
        request->message.header.request.extlen = 12;
    } else if (cmd == PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN) {
        request->message.header.request.extlen = 8;
    }
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.opcode = cmd;
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

off_t mcbp_flush_command(char* buf, size_t bufsz, uint8_t cmd, uint32_t exptime,
                         bool use_extra) {
    off_t size;
    protocol_binary_request_flush* request =
        reinterpret_cast<protocol_binary_request_flush*>(buf);
    cb_assert(bufsz > sizeof(*request));

    memset(request, 0, sizeof(*request));
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.opcode = cmd;

    size = sizeof(protocol_binary_request_no_extras);
    if (use_extra) {
        request->message.header.request.extlen = 4;
        request->message.body.expiration = htonl(exptime);
        request->message.header.request.bodylen = htonl(4);
        size += 4;
    }

    request->message.header.request.opaque = 0xdeadbeef;

    return size;
}

off_t mcbp_arithmetic_command(char* buf,
                              size_t bufsz,
                              uint8_t cmd,
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
    request->message.header.request.opcode = cmd;
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

size_t mcbp_storage_command(Frame &frame,
                            uint8_t cmd,
                            const std::string &id,
                            const std::vector<uint8_t> &value,
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
                            uint8_t cmd,
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
    request->message.header.request.opcode = cmd;
    request->message.header.request.keylen = htons((uint16_t)keylen);
    request->message.header.request.opaque = 0xdeadbeef;
    key_offset = sizeof(protocol_binary_request_no_extras);

    if (cmd != PROTOCOL_BINARY_CMD_APPEND && cmd != PROTOCOL_BINARY_CMD_PREPEND) {
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
                                   uint8_t cmd, uint16_t status) {

    auto* header = &response->message.header;
    if (status == PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND) {
        if (header->response.status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED) {
            header->response.status = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        }
    }
    bool mutation_seqno_enabled =
            enabled_hello_features.count(mcbp::Feature::MUTATION_SEQNO) > 0;
    EXPECT_TRUE(mcbp_validate_response_header(header,
                                              protocol_binary_command(cmd),
                                              status,
                                              mutation_seqno_enabled));
}

void mcbp_validate_arithmetic(const protocol_binary_response_incr* incr,
                              uint64_t expected) {
    const uint8_t* ptr = incr->bytes
                         + sizeof(incr->message.header)
                         + incr->message.header.response.extlen;
    const uint64_t result = ntohll(*(uint64_t*)ptr);
    EXPECT_EQ(expected, result);

    /* Check for extras - if present should be {vbucket_uuid, seqno) pair for
     * mutation seqno support. */
    if (incr->message.header.response.extlen != 0) {
        EXPECT_EQ(16, incr->message.header.response.extlen);
    }
}

::testing::AssertionResult mcbp_validate_response_header(const protocol_binary_response_header* header,
                                                         protocol_binary_command cmd, uint16_t status,
                                                         bool mutation_seqno_enabled) {
    AssertHelper result;

    //TODO(mnunberg) - replace with TESTAPP_EXPECT*. This undef/redefine is done
    //to make the diff smaller.

#undef EXPECT_EQ
#define EXPECT_EQ(a, b) result.eq(#a, #b, a, b)

#undef EXPECT_NE
#define EXPECT_NE(a, b) result.ne(#a, #b, a, b)

#undef EXPECT_GT
#define EXPECT_GT(a, b) result.gt(#a, #b, a, b)

#undef EXPECT_GE
#define EXPECT_GE(a, b) result.ge(#a, #b, a, b)

    EXPECT_EQ(PROTOCOL_BINARY_RES, header->response.magic);
    EXPECT_EQ(static_cast<protocol_binary_command>(cmd),
              header->response.opcode);
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, header->response.datatype);
    EXPECT_EQ(static_cast<protocol_binary_response_status>(status),
              header->response.status);
    EXPECT_EQ(0xdeadbeef, header->response.opaque);

    //TODO: Shouldn't this be header->response.status?
    if (status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        switch (cmd) {
        case PROTOCOL_BINARY_CMD_ADDQ:
        case PROTOCOL_BINARY_CMD_APPENDQ:
        case PROTOCOL_BINARY_CMD_DECREMENTQ:
        case PROTOCOL_BINARY_CMD_DELETEQ:
        case PROTOCOL_BINARY_CMD_FLUSHQ:
        case PROTOCOL_BINARY_CMD_INCREMENTQ:
        case PROTOCOL_BINARY_CMD_PREPENDQ:
        case PROTOCOL_BINARY_CMD_QUITQ:
        case PROTOCOL_BINARY_CMD_REPLACEQ:
        case PROTOCOL_BINARY_CMD_SETQ:
            result.fail("Quiet command shouldn't return on success");
            break;
        default:
            break;
        }

        switch (cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
        case PROTOCOL_BINARY_CMD_REPLACE:
        case PROTOCOL_BINARY_CMD_SET:
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_PREPEND:
            EXPECT_EQ(0, header->response.keylen);
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                EXPECT_EQ(16u, header->response.extlen);
                EXPECT_EQ(16u, header->response.bodylen);
            } else {
                EXPECT_EQ(0u, header->response.extlen);
                EXPECT_EQ(0u, header->response.bodylen);
            }
            EXPECT_NE(header->response.cas, 0u);
            break;
        case PROTOCOL_BINARY_CMD_FLUSH:
        case PROTOCOL_BINARY_CMD_NOOP:
        case PROTOCOL_BINARY_CMD_QUIT:
            EXPECT_EQ(0, header->response.keylen);
            EXPECT_EQ(0, header->response.extlen);
            EXPECT_EQ(0u, header->response.bodylen);
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
            EXPECT_EQ(0, header->response.keylen);
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                EXPECT_EQ(16u, header->response.extlen);
                EXPECT_EQ(16u, header->response.bodylen);
            } else {
                EXPECT_EQ(0u, header->response.extlen);
                EXPECT_EQ(0u, header->response.bodylen);
            }
            break;
        case PROTOCOL_BINARY_CMD_DECREMENT:
        case PROTOCOL_BINARY_CMD_INCREMENT:
            EXPECT_EQ(0, header->response.keylen);
            /* extlen is permitted to be either zero, or 16 if MUTATION_SEQNO
             * is enabled. Similary, bodylen must be either 8 or 24. */
            if (mutation_seqno_enabled) {
                EXPECT_EQ(16, header->response.extlen);
                EXPECT_EQ(24u, header->response.bodylen);
            } else {
                EXPECT_EQ(0u, header->response.extlen);
                EXPECT_EQ(8u, header->response.bodylen);
            }
            EXPECT_NE(0u, header->response.cas);
            break;

        case PROTOCOL_BINARY_CMD_STAT:
            EXPECT_EQ(0, header->response.extlen);
            /* key and value exists in all packets except in the terminating */
            EXPECT_EQ(0u, header->response.cas);
            break;

        case PROTOCOL_BINARY_CMD_VERSION:
            EXPECT_EQ(0, header->response.keylen);
            EXPECT_EQ(0, header->response.extlen);
            EXPECT_NE(0u, header->response.bodylen);
            EXPECT_EQ(0u, header->response.cas);
            break;

        case PROTOCOL_BINARY_CMD_GET:
        case PROTOCOL_BINARY_CMD_GETQ:
            EXPECT_EQ(0, header->response.keylen);
            EXPECT_EQ(4, header->response.extlen);
            EXPECT_NE(0u, header->response.cas);
            break;

        case PROTOCOL_BINARY_CMD_GETK:
        case PROTOCOL_BINARY_CMD_GETKQ:
            EXPECT_NE(0, header->response.keylen);
            EXPECT_EQ(4, header->response.extlen);
            EXPECT_NE(0u, header->response.cas);
            break;
        case PROTOCOL_BINARY_CMD_SUBDOC_GET:
            EXPECT_EQ(0, header->response.keylen);
            EXPECT_EQ(0, header->response.extlen);
            EXPECT_NE(0u, header->response.bodylen);
            EXPECT_NE(0u, header->response.cas);
            break;
        case PROTOCOL_BINARY_CMD_SUBDOC_EXISTS:
            EXPECT_EQ(0, header->response.keylen);
            EXPECT_EQ(0, header->response.extlen);
            EXPECT_EQ(0u, header->response.bodylen);
            EXPECT_NE(0u, header->response.cas);
            break;
        case PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD:
        case PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE:
            EXPECT_EQ(0, header->response.keylen);
            /* extlen/bodylen are permitted to be either zero, or 16 if
             * MUTATION_SEQNO is enabled.
             */
            if (mutation_seqno_enabled) {
                EXPECT_EQ(16, header->response.extlen);
                EXPECT_EQ(16u, header->response.bodylen);
            } else {
                EXPECT_EQ(0u, header->response.extlen);
                EXPECT_EQ(0u, header->response.bodylen);
            }
            EXPECT_NE(0u, header->response.cas);
            break;
        case PROTOCOL_BINARY_CMD_SUBDOC_COUNTER:
            EXPECT_EQ(0, header->response.keylen);
            if (mutation_seqno_enabled) {
                EXPECT_EQ(16, header->response.extlen);
                EXPECT_GT(header->response.bodylen, 16u);
            } else {
                EXPECT_EQ(0u, header->response.extlen);
                EXPECT_NE(0u, header->response.bodylen);
            }
            EXPECT_NE(0u, header->response.cas);
            break;

        case PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP:
            EXPECT_EQ(0, header->response.keylen);
            EXPECT_EQ(0, header->response.extlen);
            EXPECT_NE(0u, header->response.bodylen);
            EXPECT_NE(0u, header->response.cas);
            break;

        case PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION:
            EXPECT_EQ(0, header->response.keylen);
            /* extlen is either zero, or 16 if MUTATION_SEQNO is enabled.
             * bodylen is at least as big as extlen.
             */
            if (mutation_seqno_enabled) {
                EXPECT_EQ(16, header->response.extlen);
                EXPECT_GE(header->response.bodylen, 16u);
            } else {
                EXPECT_EQ(0u, header->response.extlen);
                EXPECT_GE(header->response.bodylen, 0u);
            }
            EXPECT_NE(0u, header->response.cas);
            break;
        default:
            /* Undefined command code */
            break;
        }
    } else if (status == PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE) {
        // Subdoc: Even though the some paths may have failed; actual document
        // was successfully accessed so CAS may be valid.
    } else if (status == PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED) {
        EXPECT_NE(0u, header->response.cas);
    } else {
        EXPECT_EQ(0u, header->response.cas);
        EXPECT_EQ(0, header->response.extlen);
        if (cmd != PROTOCOL_BINARY_CMD_GETK) {
            EXPECT_EQ(0, header->response.keylen);
        }
    }
    return result.result();
}
