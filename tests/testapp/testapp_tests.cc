/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <evutil.h>
#include <snappy-c.h>
#include <valgrind/valgrind.h>

#include <gtest/gtest.h>
#include <atomic>
#include <algorithm>
#include <string>
#include <vector>

#include "testapp.h"
#include "testapp_subdoc_common.h"

#include <memcached/config_parser.h>
#include <memcached/util.h>
#include <platform/backtrace.h>
#include <platform/cb_malloc.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <platform/platform.h>
#include <platform/socket.h>
#include <fstream>
#include <gsl/gsl>
#include "daemon/topkeys.h"
#include "memcached/openssl.h"
#include "utilities.h"

// Note: retained as a seperate function as other tests call this.
void test_noop(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_NOOP,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_NOOP,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, Noop) {
    test_noop();
}

void test_quit_impl(uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  cmd, NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_QUIT) {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_QUIT,
                                      cb::mcbp::Status::Success);
    }

    /* Socket should be closed now, read should return 0 */
    EXPECT_EQ(0, phase_recv(buffer.bytes, sizeof(buffer.bytes)));

    reconnect_to_server();
}

TEST_P(McdTestappTest, Quit) {
    test_quit_impl(PROTOCOL_BINARY_CMD_QUIT);
}

TEST_P(McdTestappTest, QuitQ) {
    test_quit_impl(PROTOCOL_BINARY_CMD_QUITQ);
}

void test_set_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    uint64_t value = 0xdeadbeefdeadcafe;
    size_t len = mcbp_storage_command(send.bytes, sizeof(send.bytes), cmd,
                                      key, strlen(key), &value, sizeof(value),
                                      0, 0);

    /* Set should work over and over again */
    int ii;
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_SET) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(
                    &receive.response, cmd, cb::mcbp::Status::Success);
        }
    }

    if (cmd == PROTOCOL_BINARY_CMD_SETQ) {
        return test_noop();
    }

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_SET) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(
                &receive.response, cmd, cb::mcbp::Status::Success);
        EXPECT_NE(receive.response.message.header.response.cas,
                  send.request.message.header.request.cas);
    } else {
        return test_noop();
    }
}

TEST_P(McdTestappTest, Set) {
    test_set_impl("test_set", PROTOCOL_BINARY_CMD_SET);
}

TEST_P(McdTestappTest, SetQ) {
    test_set_impl("test_setq", PROTOCOL_BINARY_CMD_SETQ);
}

static void test_add_impl(const char *key, uint8_t cmd) {
    uint64_t value = 0xdeadbeefdeadcafe;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_storage_command(send.bytes, sizeof(send.bytes), cmd, key,
                                      strlen(key), &value, sizeof(value),
                                      0, 0);

    /* Add should only work the first time */
    int ii;
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (ii == 0) {
            if (cmd == PROTOCOL_BINARY_CMD_ADD) {
                safe_recv_packet(receive.bytes, sizeof(receive.bytes));
                mcbp_validate_response_header(
                        &receive.response, cmd, cb::mcbp::Status::Success);
            }
        } else {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(
                    &receive.response, cmd, cb::mcbp::Status::KeyEexists);
        }
    }

    /* And verify that it doesn't work with the "correct" CAS */
    /* value */
    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(
            &receive.response, cmd, cb::mcbp::Status::KeyEexists);

    delete_object(key);
}

TEST_P(McdTestappTest, Add) {
    test_add_impl("test_add", PROTOCOL_BINARY_CMD_ADD);
}

TEST_P(McdTestappTest, AddQ) {
    test_add_impl("test_addq", PROTOCOL_BINARY_CMD_ADDQ);
}

static void test_replace_impl(const char* key, uint8_t cmd) {
    uint64_t value = 0xdeadbeefdeadcafe;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    int ii;
    size_t len = mcbp_storage_command(send.bytes, sizeof(send.bytes), cmd,
                                      key, strlen(key), &value, sizeof(value),
                                      0, 0);
    safe_send(send.bytes, len, false);
    ASSERT_TRUE(safe_recv_packet(receive.bytes, sizeof(receive.bytes)));
    mcbp_validate_response_header(
            &receive.response, cmd, cb::mcbp::Status::KeyEnoent);
    len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_ADD,
                               key, strlen(key), &value, sizeof(value), 0, 0);
    safe_send(send.bytes, len, false);
    ASSERT_TRUE(safe_recv_packet(receive.bytes, sizeof(receive.bytes)));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_ADD,
                                  cb::mcbp::Status::Success);

    len = mcbp_storage_command(send.bytes, sizeof(send.bytes), cmd,
                               key, strlen(key), &value, sizeof(value), 0, 0);
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_REPLACE) {
            ASSERT_TRUE(safe_recv_packet(receive.bytes, sizeof(receive.bytes)));
            mcbp_validate_response_header(&receive.response,
                                          PROTOCOL_BINARY_CMD_REPLACE,
                                          cb::mcbp::Status::Success);
        }
    }

    if (cmd == PROTOCOL_BINARY_CMD_REPLACEQ) {
        test_noop();
    }

    delete_object(key);
}

TEST_P(McdTestappTest, Replace) {
    test_replace_impl("test_replace", PROTOCOL_BINARY_CMD_REPLACE);
}

TEST_P(McdTestappTest, ReplaceQ) {
    test_replace_impl("test_replaceq", PROTOCOL_BINARY_CMD_REPLACEQ);
}

static void test_delete_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes), cmd,
                                  key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(
            &receive.response, cmd, cb::mcbp::Status::KeyEnoent);
    len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_ADD,
                               key, strlen(key), NULL, 0, 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_ADD,
                                  cb::mcbp::Status::Success);

    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           cmd, key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);

    if (cmd == PROTOCOL_BINARY_CMD_DELETE) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response,
                                      PROTOCOL_BINARY_CMD_DELETE,
                                      cb::mcbp::Status::Success);
    }

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(
            &receive.response, cmd, cb::mcbp::Status::KeyEnoent);
}

TEST_P(McdTestappTest, Delete) {
    test_delete_impl("test_delete", PROTOCOL_BINARY_CMD_DELETE);
}

TEST_P(McdTestappTest, DeleteQ) {
    test_delete_impl("test_deleteq", PROTOCOL_BINARY_CMD_DELETEQ);
}

static void test_delete_cas_impl(const char *key, bool bad) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len;
    len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_SET,
                               key, strlen(key), NULL, 0, 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SET,
                                  cb::mcbp::Status::Success);
    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_DELETE, key, strlen(key), NULL,
                           0);

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    if (bad) {
        ++send.request.message.header.request.cas;
    }
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    if (bad) {
        mcbp_validate_response_header(&receive.response,
                                      PROTOCOL_BINARY_CMD_DELETE,
                                      cb::mcbp::Status::KeyEexists);
    } else {
        mcbp_validate_response_header(&receive.response,
                                      PROTOCOL_BINARY_CMD_DELETE,
                                      cb::mcbp::Status::Success);
    }
}


TEST_P(McdTestappTest, DeleteCAS) {
    test_delete_cas_impl("test_delete_cas", false);
}

TEST_P(McdTestappTest, DeleteBadCAS) {
    test_delete_cas_impl("test_delete_bad_cas", true);
}

TEST_P(McdTestappTest, DeleteMutationSeqno) {
    /* Enable mutation seqno support, then call the normal delete test. */
    set_mutation_seqno_feature(true);
    test_delete_impl("test_delete_mutation_seqno", PROTOCOL_BINARY_CMD_DELETE);
    set_mutation_seqno_feature(false);
}

static void test_get_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    int ii;
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes), cmd,
                                  key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(
            &receive.response, cmd, cb::mcbp::Status::KeyEnoent);

    len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_ADD,
                               key, strlen(key), NULL, 0,
                               0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_ADD,
                                  cb::mcbp::Status::Success);

    /* run a little pipeline test ;-) */
    len = 0;
    for (ii = 0; ii < 10; ++ii) {
        union {
            protocol_binary_request_no_extras request;
            char bytes[1024];
        } temp;
        size_t l = mcbp_raw_command(temp.bytes, sizeof(temp.bytes),
                                    cmd, key, strlen(key), NULL, 0);
        memcpy(send.bytes + len, temp.bytes, l);
        len += l;
    }

    safe_send(send.bytes, len, false);
    for (ii = 0; ii < 10; ++ii) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(
                &receive.response, cmd, cb::mcbp::Status::Success);
    }

    delete_object(key);
}

TEST_P(McdTestappTest, Get) {
    test_get_impl("test_get", PROTOCOL_BINARY_CMD_GET);
}

TEST_P(McdTestappTest, GetK) {
    test_get_impl("test_getk", PROTOCOL_BINARY_CMD_GETK);
}

static void test_getq_impl(const char *key, uint8_t cmd) {
    const char *missing = "test_getq_missing";
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, temp, receive;
    size_t len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                                      PROTOCOL_BINARY_CMD_ADD,
                                      key, strlen(key), NULL, 0,
                                      0, 0);
    size_t len2 = mcbp_raw_command(temp.bytes, sizeof(temp.bytes), cmd,
                                   missing, strlen(missing), NULL, 0);
    /* I need to change the first opaque so that I can separate the two
     * return packets */
    temp.request.message.header.request.opaque = 0xfeedface;
    memcpy(send.bytes + len, temp.bytes, len2);
    len += len2;

    len2 = mcbp_raw_command(temp.bytes, sizeof(temp.bytes), cmd,
                            key, strlen(key), NULL, 0);
    memcpy(send.bytes + len, temp.bytes, len2);
    len += len2;

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_ADD,
                                  cb::mcbp::Status::Success);
    /* The first GETQ shouldn't return anything */
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(
            &receive.response, cmd, cb::mcbp::Status::Success);

    delete_object(key);
}

TEST_P(McdTestappTest, GetQ) {
    test_getq_impl("test_getq", PROTOCOL_BINARY_CMD_GETQ);
}

TEST_P(McdTestappTest, GetKQ) {
    test_getq_impl("test_getkq", PROTOCOL_BINARY_CMD_GETKQ);
}

static void test_incr_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_incr response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                         key, strlen(key), 1, 0, 0);

    int ii;
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(
                    &receive.response_header, cmd, cb::mcbp::Status::Success);
            mcbp_validate_arithmetic(&receive.response, ii);
        }
    }

    if (cmd == PROTOCOL_BINARY_CMD_INCREMENTQ) {
        test_noop();
    }

    delete_object(key);
}

TEST_P(McdTestappTest, Incr) {
    test_incr_impl("test_incr", PROTOCOL_BINARY_CMD_INCREMENT);
}

TEST_P(McdTestappTest, IncrQ) {
    test_incr_impl("test_incrq", PROTOCOL_BINARY_CMD_INCREMENTQ);
}

static void test_incr_invalid_cas_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_incr response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                         key, strlen(key), 1, 0, 0);

    send.request.message.header.request.cas = 5;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(
            &receive.response_header, cmd, cb::mcbp::Status::Einval);
}

TEST_P(McdTestappTest, InvalidCASIncr) {
    test_incr_invalid_cas_impl("test_incr", PROTOCOL_BINARY_CMD_INCREMENT);
}

TEST_P(McdTestappTest, InvalidCASIncrQ) {
    test_incr_invalid_cas_impl("test_incrq", PROTOCOL_BINARY_CMD_INCREMENTQ);
}

TEST_P(McdTestappTest, InvalidCASDecr) {
    test_incr_invalid_cas_impl("test_decr", PROTOCOL_BINARY_CMD_DECREMENT);
}

TEST_P(McdTestappTest, InvalidCASDecrQ) {
    test_incr_invalid_cas_impl("test_decrq", PROTOCOL_BINARY_CMD_DECREMENTQ);
}

static void test_decr_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_decr response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                         key, strlen(key), 1, 9, 0);

    int ii;
    for (ii = 9; ii >= 0; --ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_DECREMENT) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(
                    &receive.response_header, cmd, cb::mcbp::Status::Success);
            mcbp_validate_arithmetic(&receive.response, ii);
        }
    }

    /* decr on 0 should not wrap */
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_DECREMENT) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(
                &receive.response_header, cmd, cb::mcbp::Status::Success);
        mcbp_validate_arithmetic(&receive.response, 0);
    } else {
        test_noop();
    }

    delete_object(key);
}

TEST_P(McdTestappTest, Decr) {
    test_decr_impl("test_decr",PROTOCOL_BINARY_CMD_DECREMENT);
}

TEST_P(McdTestappTest, DecrQ) {
    test_decr_impl("test_decrq", PROTOCOL_BINARY_CMD_DECREMENTQ);
}

TEST_P(McdTestappTest, IncrMutationSeqno) {
    /* Enable mutation seqno support, then call the normal incr test. */
    set_mutation_seqno_feature(true);
    test_incr_impl("test_incr_mutation_seqno", PROTOCOL_BINARY_CMD_INCREMENT);
    set_mutation_seqno_feature(false);
}

TEST_P(McdTestappTest, DecrMutationSeqno) {
    /* Enable mutation seqno support, then call the normal decr test. */
    set_mutation_seqno_feature(true);
    test_decr_impl("test_decr_mutation_seqno", PROTOCOL_BINARY_CMD_DECREMENT);
    set_mutation_seqno_feature(false);
}

TEST_P(McdTestappTest, Version) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_VERSION,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_VERSION,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, CAS) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    uint64_t value = 0xdeadbeefdeadcafe;
    size_t len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                                      PROTOCOL_BINARY_CMD_SET,
                                      "FOO", 3, &value, sizeof(value), 0, 0);

    send.request.message.header.request.cas = 0x7ffffff;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SET,
                                  cb::mcbp::Status::KeyEnoent);

    send.request.message.header.request.cas = 0x0;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SET,
                                  cb::mcbp::Status::Success);

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SET,
                                  cb::mcbp::Status::Success);

    send.request.message.header.request.cas = receive.response.message.header.response.cas - 1;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SET,
                                  cb::mcbp::Status::KeyEexists);

    // Cleanup
    delete_object("FOO");
}

void test_concat_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    const char *value = "world";
    char *ptr;
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes), cmd,
                                  key, strlen(key), value, strlen(value));


    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(
            &receive.response, cmd, cb::mcbp::Status::NotStored);

    len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_ADD,
                               key, strlen(key), value, strlen(value), 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_ADD,
                                  cb::mcbp::Status::Success);

    len = mcbp_raw_command(send.bytes, sizeof(send.bytes), cmd,
                           key, strlen(key), value, strlen(value));
    safe_send(send.bytes, len, false);

    if (cmd == PROTOCOL_BINARY_CMD_APPEND || cmd == PROTOCOL_BINARY_CMD_PREPEND) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(
                &receive.response, cmd, cb::mcbp::Status::Success);
    } else {
        len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_NOOP,
                               NULL, 0, NULL, 0);
        safe_send(send.bytes, len, false);
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response,
                                      PROTOCOL_BINARY_CMD_NOOP,
                                      cb::mcbp::Status::Success);
    }

    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_GETK,
                           key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_GETK,
                                  cb::mcbp::Status::Success);

    EXPECT_EQ(strlen(key),
              receive.response.message.header.response.getKeylen());
    EXPECT_EQ((strlen(key) + 2 * strlen(value) + 4),
              receive.response.message.header.response.getBodylen());

    ptr = receive.bytes;
    ptr += sizeof(receive.response);
    ptr += 4;

    EXPECT_EQ(0, memcmp(ptr, key, strlen(key)));
    ptr += strlen(key);
    EXPECT_EQ(0, memcmp(ptr, value, strlen(value)));
    ptr += strlen(value);
    EXPECT_EQ(0, memcmp(ptr, value, strlen(value)));

    // Cleanup
    delete_object(key);
}

TEST_P(McdTestappTest, Append) {
    test_concat_impl("test_append", PROTOCOL_BINARY_CMD_APPEND);
}

TEST_P(McdTestappTest, Prepend) {
    test_concat_impl("test_prepend", PROTOCOL_BINARY_CMD_PREPEND);
}

TEST_P(McdTestappTest, AppendQ) {
    test_concat_impl("test_appendq", PROTOCOL_BINARY_CMD_APPENDQ);
}

TEST_P(McdTestappTest, PrependQ) {
    test_concat_impl("test_prependq", PROTOCOL_BINARY_CMD_PREPENDQ);
}

TEST_P(McdTestappTest, Stat) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    do {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_STAT,
                                      cb::mcbp::Status::Success);
    } while (buffer.response.message.header.response.getKeylen() != 0);
}

TEST_P(McdTestappTest, StatConnections) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[2048];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  "connections", strlen("connections"), NULL, 0);

    safe_send(buffer.bytes, len, false);
    do {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_STAT,
                                      cb::mcbp::Status::Success);
    } while (buffer.response.message.header.response.getKeylen() != 0);
}

std::atomic<bool> hickup_thread_running;

static void binary_hickup_recv_verification_thread(void *arg) {
    protocol_binary_response_no_extras *response =
            reinterpret_cast<protocol_binary_response_no_extras*>(cb_malloc(65*1024));
    if (response != NULL) {
        while (safe_recv_packet(response, 65*1024)) {
            /* Just validate the packet format */
            mcbp_validate_response_header(
                    response,
                    uint8_t(response->message.header.response
                                    .getClientOpcode()),
                    response->message.header.response.getStatus());
        }
        cb_free(response);
    }
    hickup_thread_running = false;
    set_allow_closed_read(false);
}

static void test_pipeline_hickup_chunk(void *buffer, size_t buffersize) {
    off_t offset = 0;
    char *key[256] = {0};
    uint64_t value = 0xfeedfacedeadbeef;

    while (hickup_thread_running &&
           offset + sizeof(protocol_binary_request_no_extras) < buffersize) {
        union {
            protocol_binary_request_no_extras request;
            char bytes[65 * 1024];
        } command;
        uint8_t cmd = (uint8_t)(rand() & 0xff);
        size_t len;
        size_t keylen = (rand() % 250) + 1;

        switch (cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
        case PROTOCOL_BINARY_CMD_ADDQ:
        case PROTOCOL_BINARY_CMD_REPLACE:
        case PROTOCOL_BINARY_CMD_REPLACEQ:
        case PROTOCOL_BINARY_CMD_SET:
        case PROTOCOL_BINARY_CMD_SETQ:
            len = mcbp_storage_command(command.bytes, sizeof(command.bytes),
                                       cmd, key, keylen, &value, sizeof(value),
                                       0, 0);
            break;
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_APPENDQ:
        case PROTOCOL_BINARY_CMD_PREPEND:
        case PROTOCOL_BINARY_CMD_PREPENDQ:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes), cmd,
                                   key, keylen, &value, sizeof(value));
            break;
        case PROTOCOL_BINARY_CMD_NOOP:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes), cmd,
                                   NULL, 0, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
        case PROTOCOL_BINARY_CMD_DELETEQ:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes), cmd,
                                   key, keylen, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_DECREMENT:
        case PROTOCOL_BINARY_CMD_DECREMENTQ:
        case PROTOCOL_BINARY_CMD_INCREMENT:
        case PROTOCOL_BINARY_CMD_INCREMENTQ:
            len = mcbp_arithmetic_command(command.bytes, sizeof(command.bytes),
                                          cmd,
                                          key, keylen, 1, 0, 0);
            break;
        case PROTOCOL_BINARY_CMD_VERSION:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes),
                                   PROTOCOL_BINARY_CMD_VERSION,
                                   NULL, 0, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_GET:
        case PROTOCOL_BINARY_CMD_GETK:
        case PROTOCOL_BINARY_CMD_GETKQ:
        case PROTOCOL_BINARY_CMD_GETQ:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes), cmd,
                                   key, keylen, NULL, 0);
            break;

        case PROTOCOL_BINARY_CMD_STAT:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes),
                                   PROTOCOL_BINARY_CMD_STAT,
                                   NULL, 0, NULL, 0);
            break;

        default:
            /* don't run commands we don't know */
            continue;
        }

        if ((len + offset) < buffersize) {
            memcpy(((char*)buffer) + offset, command.bytes, len);
            offset += (off_t)len;
        } else {
            break;
        }
    }
    safe_send(buffer, offset, true);
}

TEST_P(McdTestappTest, PipelineHickup)
{
    std::vector<char> buffer(65 * 1024);
    int ii;
    cb_thread_t tid;
    int ret;
    size_t len;

    set_allow_closed_read(true);
    hickup_thread_running = true;
    if ((ret = cb_create_thread(&tid, binary_hickup_recv_verification_thread,
                                NULL, 0)) != 0) {
        FAIL() << "Can't create thread: " << strerror(ret);
    }

    /* Allow the thread to start */
#ifdef WIN32
    Sleep(1);
#else
    usleep(250);
#endif

    for (ii = 0; ii < 2; ++ii) {
        test_pipeline_hickup_chunk(buffer.data(), buffer.size());
    }

    /* send quit to shut down the read thread ;-) */
    len = mcbp_raw_command(buffer.data(), buffer.size(),
                           PROTOCOL_BINARY_CMD_QUIT,
                           NULL, 0, NULL, 0);
    safe_send(buffer.data(), len, false);

    cb_join_thread(tid);

    reconnect_to_server();
}

TEST_P(McdTestappTest, IOCTL_Get) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    sasl_auth("@admin", "password");

    /* NULL key is invalid. */
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_IOCTL_GET, NULL, 0, NULL,
                                  0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_IOCTL_GET,
                                  cb::mcbp::Status::Einval);
}

TEST_P(McdTestappTest, IOCTL_Set) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    sasl_auth("@admin", "password");

    /* NULL key is invalid. */
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_IOCTL_SET, NULL, 0, NULL,
                                  0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_IOCTL_SET,
                                  cb::mcbp::Status::Einval);
    reconnect_to_server();
    sasl_auth("@admin", "password");

    /* Very long (> IOCTL_KEY_LENGTH) is invalid. */
    {
        char long_key[128 + 1] = {0};
        len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                               PROTOCOL_BINARY_CMD_IOCTL_SET, long_key,
                               sizeof(long_key), NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_IOCTL_SET,
                                      cb::mcbp::Status::Einval);
        reconnect_to_server();
        sasl_auth("@admin", "password");
    }

    /* release_free_memory always returns OK, regardless of how much was freed.*/
    {
        char cmd[] = "release_free_memory";
        len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                               PROTOCOL_BINARY_CMD_IOCTL_SET, cmd, strlen(cmd),
                               NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_IOCTL_SET,
                                      cb::mcbp::Status::Success);
    }
}

TEST_P(McdTestappTest, IOCTL_Tracing) {
    auto& conn = getAdminConnection();
    conn.authenticate("@admin", "password", "PLAIN");

    // Disable trace so that we start from a known status
    conn.ioctl_set("trace.stop", {});

    // Ensure that trace isn't running
    auto value = conn.ioctl_get("trace.status");
    EXPECT_EQ("disabled", value);

    // Specify config
    const std::string config{"buffer-mode:ring;buffer-size:2000000;"
                                 "enabled-categories:*"};
    conn.ioctl_set("trace.config", config);

    // Try to read it back and check that setting the config worked
    // Phosphor rebuilds the string and adds the disabled categories
    EXPECT_EQ(config + ";disabled-categories:", conn.ioctl_get("trace.config"));

    // Start the trace
    conn.ioctl_set("trace.start", {});

    // Ensure that it's running
    value = conn.ioctl_get("trace.status");
    EXPECT_EQ("enabled", value);

    // Stop the tracing
    conn.ioctl_set("trace.stop", {});

    // Ensure that it stopped
    value = conn.ioctl_get("trace.status");
    EXPECT_EQ("disabled", value);

    // get the data
    auto uuid = conn.ioctl_get("trace.dump.begin");

    const std::string chunk_key = "trace.dump.chunk?id=" + uuid;
    std::string dump;
    std::string chunk;

    do {
        chunk = conn.ioctl_get(chunk_key);
        dump += chunk;
    } while (chunk.size() > 0);

    conn.ioctl_set("trace.dump.clear", uuid);

    // Difficult to tell what's been written to the buffer so just check
    // that it's valid JSON and that the traceEvents array is present
    auto json = nlohmann::json::parse(dump);
    EXPECT_TRUE(json["traceEvents"].is_array());
}

TEST_P(McdTestappTest, Config_ValidateCurrentConfig) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[2048];
    } buffer;
    sasl_auth("@admin", "password");

    /* identity config is valid. */
    const auto config_string = memcached_cfg.dump();
    size_t len = mcbp_raw_command(buffer.bytes,
                                  sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  NULL,
                                  0,
                                  config_string.data(),
                                  config_string.size());

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, Config_Validate_Empty) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    sasl_auth("@admin", "password");

    /* empty config is invalid */
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                                  NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  cb::mcbp::Status::Einval);
}

TEST_P(McdTestappTest, Config_ValidateInvalidJSON) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    sasl_auth("@admin", "password");

    /* non-JSON config is invalid */
    char non_json[] = "[something which isn't JSON]";
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                                  non_json, strlen(non_json));

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  cb::mcbp::Status::Einval);
}

TEST_P(McdTestappTest, Config_ValidateThreadsNotDynamic) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    sasl_auth("@admin", "password");

    /* 'threads' cannot be changed */
    nlohmann::json json;
    json["threads"] = 99;
    const auto dyn_string = json.dump();
    size_t len = mcbp_raw_command(buffer.bytes,
                                  sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  NULL,
                                  0,
                                  dyn_string.data(),
                                  dyn_string.size());

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  cb::mcbp::Status::Einval);
}

TEST_P(McdTestappTest, Config_ValidateInterface) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[2048];
    } buffer;
    sasl_auth("@admin", "password");

    /* 'interfaces' - should be able to change max connections */
    auto dynamic = generate_config();
    dynamic["interfaces"][0]["maxconn"] = Testapp::MAX_CONNECTIONS * 2;
    const auto dyn_string = dynamic.dump();
    size_t len = mcbp_raw_command(buffer.bytes,
                                  sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  NULL,
                                  0,
                                  dyn_string.data(),
                                  dyn_string.size());

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, Config_Reload) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    sasl_auth("@admin", "password");

    if (getProtocolParam() != TransportProtocols::McbpPlain) {
        return;
    }

    /* reload identity config */
    {
        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      cb::mcbp::Status::Success);
    }

    /* Change max_conns on first interface. */
    {
        auto dynamic = generate_config(ssl_port);
        dynamic["interfaces"][0]["maxconn"] = Testapp::MAX_CONNECTIONS * 2;
        const auto dyn_string = dynamic.dump();

        write_config_to_file(dyn_string, config_file);

        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      cb::mcbp::Status::Success);
    }

    /* Change backlog on first interface. */
    {
        auto dynamic = generate_config(ssl_port);
        dynamic["interfaces"][0]["maxconn"] = Testapp::BACKLOG * 2;
        const auto dyn_string = dynamic.dump();
        write_config_to_file(dyn_string, config_file);

        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      cb::mcbp::Status::Success);
    }

    /* Change tcp_nodelay on first interface. */
    {
        auto dynamic = generate_config(ssl_port);
        dynamic["interfaces"][0]["tcp_nodelay"] = false;
        const auto dyn_string = dynamic.dump();

        write_config_to_file(dyn_string, config_file);

        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      cb::mcbp::Status::Success);
    }

    /* Check that invalid (corrupted) file is rejected (and we don't
       leak any memory in the process). */
    {
        auto dynamic = generate_config(ssl_port);
        auto dyn_string = dynamic.dump();
        // Corrupt the JSON by replacing first opening brace '{' with
        // a closing '}'.
        dyn_string.front() = '}';

        write_config_to_file(dyn_string, config_file);

        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      cb::mcbp::Status::Success);
    }

    /* Restore original configuration. */
    auto dynamic = generate_config(ssl_port);
    const auto dyn_string = dynamic.dump();

    write_config_to_file(dyn_string, config_file);

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                                  NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, Config_Reload_SSL) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    if (getProtocolParam() != TransportProtocols::McbpSsl) {
        return;
    }
    sasl_auth("@admin", "password");

    /* Change ssl cert/key on second interface. */
    const std::string cwd = cb::io::getcwd();
    const std::string pem_path = cwd + CERTIFICATE_PATH("testapp2.pem");
    const std::string cert_path = cwd + CERTIFICATE_PATH("testapp2.cert");
    auto dynamic = generate_config(ssl_port);
    dynamic["interfaces"][1]["ssl"]["key"] = pem_path;
    dynamic["interfaces"][1]["ssl"]["cert"] = cert_path;
    const auto dyn_string = dynamic.dump();
    write_config_to_file(dyn_string, config_file);

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                                  NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, Audit_Put) {
    union {
        protocol_binary_request_audit_put request;
        protocol_binary_response_audit_put response;
        char bytes[1024];
    }buffer;
    sasl_auth("@admin", "password");

    buffer.request.message.body.id = 0;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_AUDIT_PUT, NULL, 0,
                                  "{}", 2);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_AUDIT_PUT,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, Audit_ConfigReload) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    }buffer;
    sasl_auth("@admin", "password");

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                                  cb::mcbp::Status::Success);
}


TEST_P(McdTestappTest, Verbosity) {
    union {
        protocol_binary_request_verbosity request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    sasl_auth("@admin", "password");

    int ii;
    for (ii = 10; ii > -1; --ii) {
        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_VERBOSITY,
                                      NULL, 0, NULL, 0);
        buffer.request.message.header.request.extlen = 4;
        buffer.request.message.header.request.bodylen = ntohl(4);
        buffer.request.message.body.level = (uint32_t)ntohl(ii);
        safe_send(buffer.bytes, len + sizeof(4), false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_VERBOSITY,
                                      cb::mcbp::Status::Success);
    }
}

TEST_P(McdTestappTest, Hello) {
    union {
        protocol_binary_request_hello request;
        protocol_binary_response_hello response;
        char bytes[1024];
    } buffer;
    const char *useragent = "hello world";
    uint16_t features[6];
    uint16_t *ptr;
    size_t len;

    features[0] = htons(uint16_t(cb::mcbp::Feature::SNAPPY));
    features[1] = htons(uint16_t(cb::mcbp::Feature::JSON));
    features[2] = htons(uint16_t(cb::mcbp::Feature::TCPNODELAY));
    features[3] = htons(uint16_t(cb::mcbp::Feature::MUTATION_SEQNO));
    features[4] = htons(uint16_t(cb::mcbp::Feature::XATTR));
    features[5] = htons(uint16_t(cb::mcbp::Feature::SELECT_BUCKET));

    memset(buffer.bytes, 0, sizeof(buffer.bytes));

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_HELLO,
                           useragent, strlen(useragent), features,
                           sizeof(features));

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_HELLO,
                                  cb::mcbp::Status::Success);

    EXPECT_EQ(12u, buffer.response.message.header.response.getBodylen());
    ptr = (uint16_t*)(buffer.bytes + sizeof(buffer.response));

    std::vector<cb::mcbp::Feature> enabled;
    enabled.push_back(cb::mcbp::Feature(ntohs(ptr[0])));
    enabled.push_back(cb::mcbp::Feature(ntohs(ptr[1])));
    enabled.push_back(cb::mcbp::Feature(ntohs(ptr[2])));
    enabled.push_back(cb::mcbp::Feature(ntohs(ptr[3])));
    enabled.push_back(cb::mcbp::Feature(ntohs(ptr[4])));
    enabled.push_back(cb::mcbp::Feature(ntohs(ptr[5])));

    EXPECT_NE(
            enabled.end(),
            std::find(
                    enabled.begin(), enabled.end(), cb::mcbp::Feature::SNAPPY));
    EXPECT_NE(
            enabled.end(),
            std::find(enabled.begin(), enabled.end(), cb::mcbp::Feature::JSON));
    EXPECT_NE(enabled.end(),
              std::find(enabled.begin(),
                        enabled.end(),
                        cb::mcbp::Feature::TCPNODELAY));
    EXPECT_NE(enabled.end(),
              std::find(enabled.begin(),
                        enabled.end(),
                        cb::mcbp::Feature::MUTATION_SEQNO));
    EXPECT_NE(
            enabled.end(),
            std::find(
                    enabled.begin(), enabled.end(), cb::mcbp::Feature::XATTR));
    EXPECT_NE(enabled.end(),
              std::find(enabled.begin(),
                        enabled.end(),
                        cb::mcbp::Feature::SELECT_BUCKET));

    features[0] = 0xffff;
    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_HELLO,
                           useragent, strlen(useragent), features,
                           2);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_HELLO,
                                  cb::mcbp::Status::Success);
    EXPECT_EQ(0u, buffer.response.message.header.response.getBodylen());

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_HELLO,
                           useragent, strlen(useragent), features,
                           sizeof(features) - 1);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_HELLO,
                                  cb::mcbp::Status::Einval);
}

// Test to ensure that if a Tap Connect is requested we respond with
// cb::mcbp::Status::NotSupported
TEST_P(McdTestappTest, TapConnect) {
    union {
        protocol_binary_request_tap_connect request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    memset(buffer.bytes, 0, sizeof(buffer.bytes));
    auto len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                PROTOCOL_BINARY_CMD_TAP_CONNECT,
                                NULL, 0, NULL,0);

     safe_send(buffer.bytes, len, false);
     safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
     mcbp_validate_response_header(&buffer.response,
                                   PROTOCOL_BINARY_CMD_TAP_CONNECT,
                                   cb::mcbp::Status::NotSupported);
     EXPECT_EQ(0u, buffer.response.message.header.response.getBodylen());
}

enum class Conversion { Yes, No };

static void get_object_w_datatype(const std::string& key,
                                  cb::const_char_buffer data,
                                  cb::mcbp::Datatype datatype,
                                  Conversion conversion) {
    protocol_binary_response_no_extras response;
    protocol_binary_request_no_extras request;
    uint32_t flags;
    uint32_t len;

    memset(request.bytes, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    request.message.header.request.keylen = htons((uint16_t)key.size());
    request.message.header.request.bodylen = htonl((uint32_t)key.size());

    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_send(key.data(), key.size(), false);

    safe_recv(&response.bytes, sizeof(response.bytes));
    if (response.message.header.response.getStatus() !=
        cb::mcbp::Status::Success) {
        std::cerr << "Failed to retrieve object!: "
                  << to_string(response.message.header.response.getStatus())
                  << std::endl;
        abort();
    }

    len = response.message.header.response.getBodylen();
    cb_assert(len > 4);
    safe_recv(&flags, sizeof(flags));
    len -= 4;
    std::vector<char> body(len);
    safe_recv(body.data(), len);

    if (conversion == Conversion::Yes) {
        ASSERT_EQ(cb::mcbp::Datatype::Raw,
                  response.message.header.response.getDatatype());
    } else {
        ASSERT_EQ(datatype,
                  cb::mcbp::Datatype(
                          response.message.header.response.getDatatype()));
    }

    ASSERT_EQ(len, data.size());
    ASSERT_EQ(0, memcmp(data.data(), body.data(), body.size()));
}

TEST_P(McdTestappTest, DatatypeJSON) {
    const std::string body{R"({ "value" : 1234123412 })"};
    set_datatype_feature(true);
    // The server will set the datatype to JSON
    store_object_w_datatype("myjson", body, 0, 0, cb::mcbp::Datatype::Raw);

    get_object_w_datatype(
            "myjson", body, cb::mcbp::Datatype::JSON, Conversion::No);

    set_datatype_feature(false);
    get_object_w_datatype(
            "myjson", body, cb::mcbp::Datatype::Raw, Conversion::Yes);
}

TEST_P(McdTestappTest, DatatypeJSONWithoutSupport) {
    const std::string body{R"({ "value" : 1234123412 })"};
    set_datatype_feature(false);
    // The server will set the datatype to JSON
    store_object_w_datatype("myjson", body, 0, 0, cb::mcbp::Datatype::Raw);

    get_object_w_datatype(
            "myjson", body, cb::mcbp::Datatype::Raw, Conversion::No);

    set_datatype_feature(true);
    get_object_w_datatype(
            "myjson", body, cb::mcbp::Datatype::JSON, Conversion::No);
}

TEST_P(McdTestappTest, DatatypeCompressed) {
    const std::string inflated{"aaaaaaaaabbbbbbbccccccdddddd"};
    cb::compression::Buffer deflated;
    cb::compression::deflate(
            cb::compression::Algorithm::Snappy, inflated, deflated);

    setCompressionMode("passive");

    set_datatype_feature(true);
    store_object_w_datatype(
            "mycompressed", deflated, 0, 0, cb::mcbp::Datatype::Snappy);

    get_object_w_datatype("mycompressed",
                          deflated,
                          cb::mcbp::Datatype::Snappy,
                          Conversion::No);

    set_datatype_feature(false);
    get_object_w_datatype(
            "mycompressed", inflated, cb::mcbp::Datatype::Raw, Conversion::No);
}

TEST_P(McdTestappTest, DatatypeInvalid) {
    protocol_binary_request_no_extras request;
    union {
        protocol_binary_response_no_extras response;
        char buffer[1024];
    } res;

    set_datatype_feature(false);

    memset(request.bytes, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_NOOP;
    request.message.header.request.datatype = 1;

    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_recv_packet(res.buffer, sizeof(res.buffer));

    auto code = res.response.message.header.response.getStatus();
    ASSERT_EQ(cb::mcbp::Status::Einval, code);

    reconnect_to_server();

    set_datatype_feature(false);
    request.message.header.request.datatype = 4;
    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_recv_packet(res.buffer, sizeof(res.buffer));
    code = res.response.message.header.response.getStatus();
    EXPECT_EQ(cb::mcbp::Status::Einval, code);

    reconnect_to_server();
}

static uint64_t get_session_ctrl_token(void) {
    union {
        protocol_binary_request_get_ctrl_token request;
        protocol_binary_response_get_ctrl_token response;
        char bytes[1024];
    } buffer;
    uint64_t ret;

    memset(buffer.bytes, 0, sizeof(buffer));
    buffer.request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    buffer.request.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN;

    safe_send(buffer.bytes, sizeof(buffer.request), false);
    safe_recv_packet(&buffer.response, sizeof(buffer.bytes));

    cb_assert(buffer.response.message.header.response.getStatus() ==
              cb::mcbp::Status::Success);

    ret = ntohll(buffer.response.message.header.response.cas);
    cb_assert(ret != 0);

    return ret;
}

static void prepare_set_session_ctrl_token(protocol_binary_request_set_ctrl_token *req,
                                           uint64_t old, uint64_t new_cas)
{
    memset(req, 0, sizeof(*req));
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN;
    req->message.header.request.extlen = sizeof(uint64_t);
    req->message.header.request.bodylen = htonl(sizeof(uint64_t));
    req->message.header.request.cas = htonll(old);
    req->message.body.new_cas = htonll(new_cas);
}

TEST_P(McdTestappTest, SessionCtrlToken) {
    union {
        protocol_binary_request_set_ctrl_token request;
        protocol_binary_response_set_ctrl_token response;
        char bytes[1024];
    } buffer;

    sasl_auth("@admin", "password");

    uint64_t old_token = get_session_ctrl_token();
    uint64_t new_token = 0x0102030405060708;

    /* Validate that you may successfully set the token to a legal value */
    prepare_set_session_ctrl_token(&buffer.request, old_token, new_token);
    safe_send(buffer.bytes, sizeof(buffer.request), false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));

    cb_assert(buffer.response.message.header.response.getStatus() ==
              cb::mcbp::Status::Success);
    cb_assert(new_token == ntohll(buffer.response.message.header.response.cas));
    old_token = new_token;

    /* Validate that you can't set it to 0 */
    prepare_set_session_ctrl_token(&buffer.request, old_token, 0);
    safe_send(buffer.bytes, sizeof(buffer.request), false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));
    cb_assert(buffer.response.message.header.response.getStatus() ==
              cb::mcbp::Status::Einval);
    reconnect_to_server();
    sasl_auth("@admin", "password");

    cb_assert(old_token == get_session_ctrl_token());

    /* Validate that you can't set it by providing an incorrect cas */
    prepare_set_session_ctrl_token(&buffer.request, old_token + 1, new_token - 1);
    safe_send(buffer.bytes, sizeof(buffer.request), false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));

    cb_assert(buffer.response.message.header.response.getStatus() ==
              cb::mcbp::Status::KeyEexists);
    cb_assert(new_token == ntohll(buffer.response.message.header.response.cas));
    cb_assert(new_token == get_session_ctrl_token());

    /* Validate that you may set it by overriding the cas with 0 */
    prepare_set_session_ctrl_token(&buffer.request, 0, 0xdeadbeef);
    safe_send(buffer.bytes, sizeof(buffer.request), false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));
    cb_assert(buffer.response.message.header.response.getStatus() ==
              cb::mcbp::Status::Success);
    cb_assert(0xdeadbeef == ntohll(buffer.response.message.header.response.cas));
    cb_assert(0xdeadbeef == get_session_ctrl_token());
}

TEST_P(McdTestappTest, MB_10114) {
    char value[100000] = {0};
    const char *key = "mb-10114";
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[100100];
    } send, receive;
    size_t len;

    // Disable ewouldblock_engine - not wanted / needed for this MB regression test.
    ewouldblock_engine_disable();

    store_document(key, "world");
    do {
        len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_APPEND,
                               key, strlen(key), value, sizeof(value));
        safe_send(send.bytes, len, false);
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    } while (receive.response.message.header.response.getStatus() ==
             cb::mcbp::Status::Success);

    EXPECT_EQ(cb::mcbp::Status::E2big,
              receive.response.message.header.response.getStatus());

    /* We should be able to delete it */
    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_DELETE,
                           key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_DELETE,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, DCP_Noop) {
    // TODO: For ep-engine (which supports DCP), actualy test it (instead of
    // skipping the default-engine style test).
    TESTAPP_SKIP_IF_SUPPORTED(PROTOCOL_BINARY_CMD_DCP_NOOP);

    union {
        protocol_binary_request_dcp_noop request;
        protocol_binary_response_dcp_noop response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_DCP_NOOP,
                                  NULL, 0, NULL, 0);

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_NOOP,
                                  cb::mcbp::Status::NotSupported);
    reconnect_to_server();

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_NOOP,
                           "d", 1, "f", 1);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_NOOP,
                                  cb::mcbp::Status::Einval);
}

TEST_P(McdTestappTest, DCP_BufferAck) {
    // TODO: For ep-engine (which supports DCP), actualy test it (instead of
    // skipping the default-engine style test).
    TESTAPP_SKIP_IF_SUPPORTED(PROTOCOL_BINARY_CMD_DCP_NOOP);

    union {
        protocol_binary_request_dcp_buffer_acknowledgement request;
        protocol_binary_response_dcp_noop response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                                  NULL, 0, "asdf", 4);
    buffer.request.message.header.request.extlen = 4;

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(
            &buffer.response,
            PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
            cb::mcbp::Status::NotSupported);
    reconnect_to_server();

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                           "d", 1, "ffff", 4);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(
            &buffer.response,
            PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
            cb::mcbp::Status::Einval);
    reconnect_to_server();

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                           NULL, 0, "fff", 3);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(
            &buffer.response,
            PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
            cb::mcbp::Status::Einval);
}

TEST_P(McdTestappTest, DCP_Control) {
    // TODO: For ep-engine (which supports DCP), actualy test it (instead of
    // skipping the default-engine style test).
    TESTAPP_SKIP_IF_SUPPORTED(PROTOCOL_BINARY_CMD_DCP_NOOP);

    union {
        protocol_binary_request_dcp_control request;
        protocol_binary_response_dcp_control response;
        char bytes[1024];
    } buffer;

    size_t len;

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_CONTROL,
                           "foo", 3, "bar", 3);

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                  cb::mcbp::Status::NotSupported);
    reconnect_to_server();

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_CONTROL,
                           NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                  cb::mcbp::Status::Einval);
    reconnect_to_server();

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_CONTROL,
                           NULL, 0, "fff", 3);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                  cb::mcbp::Status::Einval);
    reconnect_to_server();

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_CONTROL,
                           "foo", 3, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                  cb::mcbp::Status::Einval);
}

/* expiry, wait1 and wait2 need to be crafted so that
   1. sleep(wait1) and key exists
   2. sleep(wait2) and key should now have expired.
*/
static void test_expiry(const char* key, time_t expiry,
                        time_t wait1, int clock_shift) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;

    uint64_t value = 0xdeadbeefdeadcafe;
    size_t len = 0;
    len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_SET,
                               key, strlen(key), &value, sizeof(value),
                               0, (uint32_t)expiry);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SET,
                                  cb::mcbp::Status::Success);

    adjust_memcached_clock(clock_shift, TimeType::TimeOfDay);

    memset(send.bytes, 0, 1024);
    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_GET,
                           key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_GET,
                                  cb::mcbp::Status::Success);
}

TEST_P(McdTestappTest, ExpiryRelativeWithClockChangeBackwards) {
    /*
       Just test for MB-11548
       120 second expiry.
       Set clock back by some amount that's before the time we started memcached.
       wait 2 seconds (allow mc time to tick)
       (defect was that time went negative and expired keys immediatley)
    */
    time_t now = time(0);
    test_expiry("test_expiry_relative_with_clock_change_backwards",
                120, 2, (int)(0 - ((now - get_server_start_time()) * 2)));
}

void McdTestappTest::test_set_huge_impl(const char* key,
                                        uint8_t cmd,
                                        cb::mcbp::Status result,
                                        bool pipeline,
                                        int iterations,
                                        int message_size) {
    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    /* some error case may return a body in the response */
    char receive[sizeof(protocol_binary_response_no_extras) + 32];
    const size_t len = message_size + sizeof(protocol_binary_request_set) + strlen(key);
    std::vector<char> set_message(len);
    char* message = set_message.data() + (sizeof(protocol_binary_request_set) + strlen(key));
    int ii;
    memset(message, 0xb0, message_size);

    cb_assert(len == mcbp_storage_command(set_message.data(), len, cmd, key,
                                          strlen(key), NULL, message_size,
                                          0, 0));

    for (ii = 0; ii < iterations; ++ii) {
        safe_send(set_message.data(), len, false);
        if (!pipeline) {
            if (cmd == PROTOCOL_BINARY_CMD_SET) {
                safe_recv_packet(&receive, sizeof(receive));
                mcbp_validate_response_header(
                    (protocol_binary_response_no_extras*)receive, cmd, result);
            }
        }
    }

    if (pipeline && cmd == PROTOCOL_BINARY_CMD_SET) {
        for (ii = 0; ii < iterations; ++ii) {
            safe_recv_packet(&receive, sizeof(receive));
            mcbp_validate_response_header(
                (protocol_binary_response_no_extras*)receive, cmd, result);
        }
    }
}

TEST_P(McdTestappTest, SetHuge) {
    test_set_huge_impl("test_set_huge",
                       PROTOCOL_BINARY_CMD_SET,
                       cb::mcbp::Status::Success,
                       false,
                       10,
                       1023 * 1024);
}

TEST_P(McdTestappTest, SetE2BIG) {
    test_set_huge_impl(
            "test_set_e2big",
            PROTOCOL_BINARY_CMD_SET,
            cb::mcbp::Status::E2big,
            false,
            1,
            gsl::narrow<int>(GetTestBucket().getMaximumDocSize()) + 1);
}

TEST_P(McdTestappTest, SetQHuge) {
    test_set_huge_impl("test_setq_huge",
                       PROTOCOL_BINARY_CMD_SETQ,
                       cb::mcbp::Status::Success,
                       false,
                       10,
                       1023 * 1024);
}

TEST_P(McdTestappTest, PipelineHuge) {
    test_set_huge_impl("test_pipeline_huge",
                       PROTOCOL_BINARY_CMD_SET,
                       cb::mcbp::Status::Success,
                       true,
                       200,
                       1023 * 1024);
}

#ifndef THREAD_SANITIZER
// These tests are disabled under valgrind as they take a lot
// of time and don't really expose any new features in the server

/* support set, get, delete */
void test_pipeline_impl(int cmd,
                        cb::mcbp::Status result,
                        const char* key_root,
                        uint32_t messages_in_stream,
                        size_t value_size) {
    size_t largest_protocol_packet = sizeof(protocol_binary_request_set); /* set has the largest protocol message */
    size_t key_root_len = strlen(key_root);
    size_t key_digit_len = 5; /*append 00001, 00002 etc.. to key_root */
    const size_t buffer_len = (largest_protocol_packet + key_root_len +
                               key_digit_len + value_size) * messages_in_stream;
    size_t out_message_len = 0, in_message_len = 0, send_len = 0, receive_len = 0;
    std::vector<uint8_t> buffer(buffer_len); /* space for creating and receiving a stream */
    std::vector<char> key(key_root_len + key_digit_len + 1); /* space for building keys */
    uint8_t* current_message = buffer.data();
    int session = 0; /* something to stick in opaque */

    session = rand() % 100;

    cb_assert(messages_in_stream <= 99999);

    /* now figure out the correct send and receive lengths */
    if (cmd == PROTOCOL_BINARY_CMD_SET) {
        /* set, sends key and a value */
        out_message_len = sizeof(protocol_binary_request_set) + key_root_len + key_digit_len + value_size;
        /* receives a plain response, no extra */
        in_message_len = sizeof(protocol_binary_response_no_extras);
    } else if (cmd == PROTOCOL_BINARY_CMD_GET) {
        /* get sends key */
        out_message_len = sizeof(protocol_binary_request_get) + key_root_len + key_digit_len;

        if (result == cb::mcbp::Status::Success) {
            /* receives a response + flags + value */
            in_message_len = sizeof(protocol_binary_response_no_extras) + 4 + value_size;
        } else {
            /* receives a response + string error */
            in_message_len = sizeof(protocol_binary_response_no_extras) + 9;
        }
    } else if (cmd == PROTOCOL_BINARY_CMD_DELETE) {
        /* delete sends key */
        out_message_len = sizeof(protocol_binary_request_get) + key_root_len + key_digit_len;
        /* receives a plain response, no extra */
        in_message_len = sizeof(protocol_binary_response_no_extras);
    } else {
        FAIL() << "invalid cmd (" << cmd << ") in test_pipeline_impl";
    }

    send_len    = out_message_len * messages_in_stream;
    receive_len = in_message_len * messages_in_stream;

    /* entire buffer and thus any values are 0xaf */
    std::fill(buffer.begin(), buffer.end(), 0xaf);

    for (uint32_t ii = 0; ii < messages_in_stream; ii++) {
        snprintf(key.data(), key_root_len + key_digit_len + 1, "%s%05d", key_root, ii);
        if (PROTOCOL_BINARY_CMD_SET == cmd) {
            protocol_binary_request_set* this_req = (protocol_binary_request_set*)current_message;
            current_message += mcbp_storage_command((char*)current_message,
                                                    out_message_len, cmd,
                                                    key.data(),
                                                    strlen(key.data()),
                                                    NULL, value_size, 0, 0);
            this_req->message.header.request.opaque = htonl((session << 8) | ii);
        } else {
            protocol_binary_request_no_extras* this_req = (protocol_binary_request_no_extras*)current_message;
            current_message += mcbp_raw_command((char*)current_message,
                                                out_message_len, cmd,
                                                key.data(), strlen(key.data()),
                                                NULL, 0);
            this_req->message.header.request.opaque = htonl((session << 8) | ii);
        }
    }

    cb_assert(buffer.size() >= send_len);

    safe_send(buffer.data(), send_len, false);

    std::fill(buffer.begin(), buffer.end(), 0);

    /* and get it all back in the same buffer */
    cb_assert(buffer.size() >= receive_len);

    safe_recv(buffer.data(), receive_len);
    current_message = buffer.data();
    for (uint32_t ii = 0; ii < messages_in_stream; ii++) {
        auto* message = (protocol_binary_response_no_extras*)current_message;

        uint32_t bodylen = message->message.header.response.getBodylen();
        uint8_t extlen = message->message.header.response.getExtlen();
        auto status = message->message.header.response.getStatus();
        uint32_t opq = ntohl(message->message.header.response.getOpaque());

        cb_assert(status == result);
        cb_assert(opq == ((session << 8)|ii));

        /* a value? */
        if (bodylen != 0 && result == cb::mcbp::Status::Success) {
            uint8_t* value = current_message + sizeof(protocol_binary_response_no_extras) + extlen;
            for (size_t jj = 0; jj < value_size; jj++) {
                cb_assert(value[jj] == 0xaf);
            }
            current_message = current_message + bodylen + sizeof(protocol_binary_response_no_extras);
        } else {
            current_message = (uint8_t*)(message + 1);
        }
    }
}

TEST_P(McdTestappTest, PipelineSet) {
    if (RUNNING_ON_VALGRIND) {
        return;
    }

    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    /*
      MB-11203 would break at iteration 529 where we happen to send 57916 bytes in 1 pipe
      this triggered some edge cases in our SSL recv code.
    */
    for (int ii = 1; ii < 1000; ii++) {
        test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                           cb::mcbp::Status::Success,
                           "key_set_pipe",
                           100,
                           ii);
        test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                           cb::mcbp::Status::Success,
                           "key_set_pipe",
                           100,
                           ii);
    }
}

TEST_P(McdTestappTest, PipelineSetGetDel) {
    if (RUNNING_ON_VALGRIND) {
        return;
    }

    const char key_root[] = "key_set_get_del";

    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                       cb::mcbp::Status::Success,
                       key_root,
                       5000,
                       256);

    test_pipeline_impl(PROTOCOL_BINARY_CMD_GET,
                       cb::mcbp::Status::Success,
                       key_root,
                       5000,
                       256);

    test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                       cb::mcbp::Status::Success,
                       key_root,
                       5000,
                       256);
}

TEST_P(McdTestappTest, PipelineSetDel) {
    if (RUNNING_ON_VALGRIND) {
        return;
    }

    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                       cb::mcbp::Status::Success,
                       "key_root",
                       5000,
                       256);

    test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                       cb::mcbp::Status::Success,
                       "key_root",
                       5000,
                       256);
}
#endif

/* Send one character to the SSL port, then check memcached correctly closes
 * the connection (and doesn't hold it open for ever trying to read) more bytes
 * which will never come.
 */
TEST_P(McdTestappTest, MB_12762_SSLHandshakeHang) {

    // Requires SSL.
    if (!sock_is_ssl()) {
        return;
    }

    /* Setup: Close the existing (handshaked) SSL connection, and create a
     * 'plain' TCP connection to the SSL port - i.e. without any SSL handshake.
     */
    cb::net::closesocket(sock_ssl);
    sock_ssl = create_connect_plain_socket(ssl_port);

    /* Send a payload which is NOT a valid SSL handshake: */
    char buf[] = {'a', '\n'};
    ssize_t len = cb::net::send(sock_ssl, buf, sizeof(buf), 0);
    cb_assert(len == 2);

/* Done writing, close the socket for writing. This triggers the bug: a
 * conn_read -> conn_waiting -> conn_read_packet_header ... loop in memcached */
#if defined(WIN32)
    int res = shutdown(sock_ssl, SD_SEND);
#else
    int res = shutdown(sock_ssl, SHUT_WR);
#endif
    cb_assert(res == 0);

    /* Check status of the FD - expected to be ready (as it's just been closed
     * by peer), and should not have hit the timeout.
     */
    fd_set fdset;
#ifndef __clang_analyzer__
    /* FD_ZERO() is often implemented as inline asm(), which Clang
     * static analyzer cannot parse. */
    FD_ZERO(&fdset);
    FD_SET(sock_ssl, &fdset);
#endif
    struct timeval timeout = {0};
    timeout.tv_sec = 5;
    int ready_fds = select((int)(sock_ssl + 1), &fdset, NULL, NULL, &timeout);
    cb_assert(ready_fds == 1);

    /* Verify that attempting to read from the socket returns 0 (peer has
     * indeed closed the connection).
     */
    len = cb::net::recv(sock_ssl, buf, 1, 0);
    cb_assert(len == 0);

    /* Restore the SSL connection to a sane state :) */
    reconnect_to_server();
}

TEST_P(McdTestappTest, ExceedMaxPacketSize)
{
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    memset(send.bytes, 0, sizeof(send.bytes));

    mcbp_storage_command(send.bytes, sizeof(send.bytes),
                         PROTOCOL_BINARY_CMD_SET,
                         "key", 3, NULL, 0, 0, 0);
    send.request.message.header.request.bodylen = ntohl(31*1024*1024);
    safe_send(send.bytes, sizeof(send.bytes), false);

    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SET,
                                  cb::mcbp::Status::Einval);

    reconnect_to_server();
}

/**
 * Returns the current access count of the test key ("someval") as an integer
 * via the old string format.
 */
int get_topkeys_legacy_value(const std::string& wanted_key) {

    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[8192];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  "topkeys", strlen("topkeys"),
                                  NULL, 0);
    safe_send(buffer.bytes, len, false);

    // We expect a variable number of response packets (one per top key);
    // take them all off the wire, recording the one for our key which we
    // parse at the end.
    std::string value;
    while (true) {
        if (!safe_recv_packet(buffer.bytes, sizeof(buffer.bytes))) {
            ADD_FAILURE() << "Failed to receive topkeys packet";
            return -1;
        }
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_STAT,
                                      cb::mcbp::Status::Success);

        const char* key_ptr(
                buffer.bytes + sizeof(buffer.response) +
                buffer.response.message.header.response.getExtlen());
        size_t key_len(buffer.response.message.header.response.getKeylen());

        // A packet with key length zero indicates end of the stats.
        if (key_len == 0) {
            break;
        }

        const std::string key(key_ptr, key_len);
        if (key == wanted_key) {
            // Got our key. Save the value to one side; and finish consuming
            // the STAT reponse packets.
            EXPECT_EQ(0u, value.size())
                << "Unexpectedly found a second topkey for wanted key '" << wanted_key;

            const char* val_ptr(key_ptr + key_len);
            const size_t val_len(
                    buffer.response.message.header.response.getBodylen() -
                    key_len -
                    buffer.response.message.header.response.getExtlen());
            EXPECT_GT(val_len, 0u);
            value = std::string(val_ptr, val_len);
        }
    };

    if (value.size() > 0) {
        // Extract the 'get_hits' stat (which actually the aggregate of all
        // operations now).

        const std::string token("get_hits=");
        auto pos = value.find(token);
        EXPECT_NE(std::string::npos, pos)
            << "Failed to locate '" << token << "' substring in topkey '"
            << wanted_key << "' value '" << value << "'";

        // Move iterator to the other side of the equals sign (the value) and
        // erase before that point.
        pos += token.size();
        value.erase(0, pos);

        return std::stoi(value);
    } else {
        // If we got here then we failed to find the given key.
        return 0;
    }
}

/**
 * Accesses the current value of the key via the JSON formatted topkeys return.
 * @return True if the specified key was found (and sets count to the
 *         keys' access count) or false if not found.
 */
bool get_topkeys_json_value(const std::string& key, int& count) {

    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[10000];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  "topkeys_json", strlen("topkeys_json"),
                                  NULL, 0);
    safe_send(buffer.bytes, len, false);

    // Expect 1 valid packet followed by 1 null
    if (!safe_recv_packet(buffer.bytes, sizeof(buffer.bytes))) {
        ADD_FAILURE() << "Failed to recv topkeys_json response";
        return false;
    }
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_STAT,
                                  cb::mcbp::Status::Success);

    EXPECT_NE(0, buffer.response.message.header.response.getKeylen());

    const char* val_ptr = buffer.bytes +
                          (sizeof(buffer.response) +
                           buffer.response.message.header.response.getKeylen() +
                           buffer.response.message.header.response.getExtlen());
    const size_t vallen(buffer.response.message.header.response.getBodylen() -
                        buffer.response.message.header.response.getKeylen() -
                        buffer.response.message.header.response.getExtlen());
    EXPECT_GT(vallen, 0u);
    const std::string value(val_ptr, vallen);

    // Consume NULL stats packet.
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_STAT,
                                  cb::mcbp::Status::Success);
    EXPECT_EQ(0, buffer.response.message.header.response.getKeylen());

    // Check for response string
    auto json = nlohmann::json::parse(value);

    auto topkeys = json["topkeys"];
    EXPECT_TRUE(topkeys.is_array());

    // Search the array for the specified key's information.
    for (auto record : topkeys) {
        if (key == record["key"]) {
            count = record["access_count"].get<int>();
            return true;
        }
    }

    return false;
}

/**
 * Set a key a number of times and assert that the return value matches the
 * change after the number of set operations.
 */
static void test_set_topkeys(const std::string& key, const int operations) {

    // In theory we should start with no record of a key; but there's no
    // explicit way to clear topkeys; and a previous test run against the same
    // memcached instance may have used the same key.
    // Therefore for robustness don't assume the key doesn't exist; and fetch
    // the initial count.
    int initial_count = 0;
    get_topkeys_json_value(key, initial_count);

    int ii;
    size_t len;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    memset(buffer.bytes, 0, sizeof(buffer));

    /* Send CMD_SET for current key 'sum' number of times (and validate
     * response). */
    for (ii = 0; ii < operations; ii++) {
        len = mcbp_storage_command(buffer.bytes, sizeof(buffer.bytes),
                                   PROTOCOL_BINARY_CMD_SET, key.c_str(),
                                   key.length(),
                                   "foo", strlen("foo"), 0, 0);

        safe_send(buffer.bytes, len, false);

        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_SET,
                                      cb::mcbp::Status::Success);
    }

    EXPECT_EQ(initial_count + operations, get_topkeys_legacy_value(key));
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(initial_count + operations, json_value);
}


/**
 * Get a key a number of times and assert that the return value matches the
 * change after the number of get operations.
 */
static void test_get_topkeys(const std::string& key, int operations) {
    int initial_count = 0;
    ASSERT_TRUE(get_topkeys_json_value(key, initial_count));

    int ii;
    size_t len;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[2048];
    } buffer;
    memset(buffer.bytes, 0, sizeof(buffer));

    for (ii = 0; ii < operations; ii++) {
        len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                               PROTOCOL_BINARY_CMD_GET, key.c_str(),
                               key.length(),
                               NULL, 0);
        safe_send(buffer.bytes, len, false);

        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_GET,
                                      cb::mcbp::Status::Success);
    }

    const int expected_count = initial_count + operations;
    EXPECT_EQ(expected_count, get_topkeys_legacy_value(key))
        << "Unexpected topkeys legacy count for key:" << key;
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(expected_count, json_value);
}

/**
 * Delete a key and assert that the return value matches the change
 * after the delete operation.
 */
static void test_delete_topkeys(const std::string& key) {
    int initial_count = 0;
    ASSERT_TRUE(get_topkeys_json_value(key, initial_count));

    size_t len;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    memset(buffer.bytes, 0, sizeof(buffer));

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DELETE, key.c_str(),
                           key.length(),
                           NULL, 0);
    safe_send(buffer.bytes, len, false);

    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DELETE,
                                  cb::mcbp::Status::Success);

    EXPECT_EQ(initial_count + 1, get_topkeys_legacy_value(key))
        << "Unexpected topkeys legacy count for key:" << key;
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(initial_count + 1, json_value);
}


/**
 * Test for JSON document formatted topkeys (part of bucket_engine). Tests for
 * correct values when issuing CMD_SET, CMD_GET, and CMD_DELETE.
 */
TEST_P(McdTestappTest, test_topkeys) {

    /* Perform sets on a few different keys. */
    test_set_topkeys("key1", 1);
    test_set_topkeys("key2", 2);
    test_set_topkeys("key3", 3);

    test_get_topkeys("key1", 10);

    test_delete_topkeys("key1");
}

/**
 * Test that opcode 255 is rejected and the server doesn't crash
 */
TEST_P(McdTestappTest, test_MB_16333) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    memset(buffer.bytes, 0, sizeof(buffer));

    auto len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                255, NULL, 0, NULL, 0);
    safe_send(buffer.bytes, len, false);

    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(
            &buffer.response, 255, cb::mcbp::Status::UnknownCommand);
}

/**
 * Test that a bad SASL auth doesn't crash the server.
 * It should be rejected with EINVAL.
 */
TEST_P(McdTestappTest, test_MB_16197) {
    const char* chosenmech = "PLAIN";
    const char* data = "\0nouser\0nopassword";

    reconnect_to_server();

    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    // Bodylen deliberatley set to less than keylen.
    // This packet should be rejected.
    size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                   PROTOCOL_BINARY_CMD_SASL_AUTH,
                                   chosenmech, strlen(chosenmech)/*keylen*/,
                                   data, 1/*bodylen*/);

    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_SASL_AUTH,
                                  cb::mcbp::Status::Einval);
}

INSTANTIATE_TEST_CASE_P(
        Transport,
        McdTestappTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain,
                                             TransportProtocols::McbpSsl),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No)),
        McdTestappTest::PrintToStringCombinedName);
