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
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <evutil.h>
#include <valgrind/valgrind.h>

#include <folly/CPortability.h>
#include <folly/portability/GTest.h>
#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "testapp.h"
#include "testapp_subdoc_common.h"

#include "memcached/openssl.h"
#include "utilities.h"
#include <mcbp/protocol/framebuilder.h>
#include <memcached/config_parser.h>
#include <memcached/util.h>
#include <platform/backtrace.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <platform/socket.h>
#include <platform/string_hex.h>
#include <protocol/connection/frameinfo.h>
#include <fstream>
#include <gsl/gsl>

using namespace cb::mcbp;

// Note: retained as a seperate function as other tests call this.
void test_noop() {
    BinprotGenericCommand cmd(ClientOpcode::Noop);
    std::vector<uint8_t> blob;
    cmd.encode(blob);
    safe_send(blob);

    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Noop,
                                  Status::Success);
}

TEST_P(McdTestappTest, Noop) {
    test_noop();
}

void test_quit_impl(ClientOpcode cmd) {
    BinprotGenericCommand command(cmd);
    std::vector<uint8_t> blob;
    command.encode(blob);
    safe_send(blob);

    if (cmd == ClientOpcode::Quit) {
        ASSERT_TRUE(safe_recv_packet(blob));
        mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                      ClientOpcode::Quit,
                                      Status::Success);
    }

    /* Socket should be closed now, read should return 0 */
    EXPECT_EQ(0, phase_recv(blob.data(), blob.size()));
    reconnect_to_server();
}

TEST_P(McdTestappTest, Quit) {
    test_quit_impl(ClientOpcode::Quit);
}

TEST_P(McdTestappTest, QuitQ) {
    test_quit_impl(ClientOpcode::Quitq);
}

TEST_P(McdTestappTest, SetQ) {
    const std::string key = "test_setq";
    auto command = mcbp_storage_command(ClientOpcode::Setq, key, "value", 0, 0);

    /* Set should work over and over again */
    std::vector<uint8_t> blob;
    for (int ii = 0; ii < 10; ++ii) {
        safe_send(command);
    }

    return test_noop();
}

TEST_P(McdTestappTest, AddQ) {
    const std::string key = "test_addq";
    auto command = mcbp_storage_command(ClientOpcode::Addq, key, "value", 0, 0);

    /* Add should only work the first time */
    std::vector<uint8_t> blob;
    for (int ii = 0; ii < 10; ++ii) {
        safe_send(command);
        if (ii > 0) { // first time should succeed
            ASSERT_TRUE(safe_recv_packet(blob));
            auto& response = *reinterpret_cast<Response*>(blob.data());
            mcbp_validate_response_header(
                    response, ClientOpcode::Addq, Status::KeyEexists);
        }
    }

    delete_object(key);
}

TEST_P(McdTestappTest, ReplaceQ) {
    const std::string key = "test_replaceq";

    auto command =
            mcbp_storage_command(ClientOpcode::Replaceq, key, "value", 0, 0);
    safe_send(command);
    std::vector<uint8_t> blob;
    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Replaceq,
                                  Status::KeyEnoent);

    store_document(key, "foo");

    for (int ii = 0; ii < 10; ++ii) {
        safe_send(command);
    }

    test_noop();

    delete_object(key);
}

TEST_P(McdTestappTest, DeleteQ) {
    const std::string key = "test_deleteq";
    BinprotGenericCommand del(ClientOpcode::Deleteq, key);
    std::vector<uint8_t> blob;
    del.encode(blob);
    safe_send(blob);

    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Deleteq,
                                  Status::KeyEnoent);

    // Store a value we can delete
    store_document(key, "foo");

    del.encode(blob);
    safe_send(blob);

    // quiet delete should not return anything
    test_noop();

    // Deleting it one more time should fail with no key

    del.encode(blob);
    safe_send(blob);
    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Deleteq,
                                  Status::KeyEnoent);
}

static void test_delete_cas_impl(const char *key, bool bad) {
    BinprotMutationCommand mut;
    mut.setMutationType(MutationType::Set);
    mut.setKey(key);
    std::vector<uint8_t> blob;
    mut.encode(blob);
    safe_send(blob);

    ASSERT_TRUE(safe_recv_packet(blob));
    auto response = reinterpret_cast<Response*>(blob.data());
    mcbp_validate_response_header(
            *response, ClientOpcode::Set, Status::Success);

    BinprotGenericCommand del(ClientOpcode::Deleteq, key);
    // @todo fixme.. the send expects cas to already be in NBO
    del.setCas(htonll(response->getCas()) + (bad ? 1 : 0));
    del.encode(blob);
    safe_send(blob);

    if (bad) {
        ASSERT_TRUE(safe_recv_packet(blob));
        response = reinterpret_cast<Response*>(blob.data());
        mcbp_validate_response_header(
                *response, ClientOpcode::Deleteq, Status::KeyEexists);
    } else {
        test_noop();
    }
}

TEST_P(McdTestappTest, DeleteCAS) {
    test_delete_cas_impl("test_delete_cas", false);
}

TEST_P(McdTestappTest, DeleteBadCAS) {
    test_delete_cas_impl("test_delete_bad_cas", true);
}

TEST_P(McdTestappTest, GetK) {
    const std::string key = "test_getk";
    BinprotGenericCommand get(ClientOpcode::Getk, key);
    std::vector<uint8_t> blob;
    get.encode(blob);
    safe_send(blob);

    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Getk,
                                  Status::KeyEnoent);

    store_document(key, "value", 0xcafefeed);

    /* run a little pipeline test ;-) */
    blob.resize(0);
    for (int ii = 0; ii < 10; ++ii) {
        std::vector<uint8_t> buf;
        get.encode(buf);
        std::copy(buf.begin(), buf.end(), std::back_inserter(blob));
    }

    safe_send(blob);
    for (int ii = 0; ii < 10; ++ii) {
        ASSERT_TRUE(safe_recv_packet(blob));
        mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                      ClientOpcode::Getk,
                                      Status::Success);
        BinprotGetResponse rsp;
        rsp.assign(std::move(blob));
        EXPECT_EQ(0xcafefeed, rsp.getDocumentFlags());
    }

    delete_object(key);
}

static void test_getq_impl(const char* key, ClientOpcode cmd) {
    BinprotGenericCommand command(cmd, key);
    std::vector<uint8_t> blob;
    command.encode(blob);
    // I need to change the first opaque so that I can separate the two
    // return packets
    auto* request = reinterpret_cast<Request*>(blob.data());
    request->setOpaque(0xfeedface);
    safe_send(blob);
    // We should not get a reply with that...

    BinprotMutationCommand mt;
    mt.setMutationType(MutationType::Add);
    mt.setKey(key);
    mt.encode(blob);
    safe_send(blob);

    command.encode(blob);
    request = reinterpret_cast<Request*>(blob.data());
    request->setOpaque(1);
    safe_send(blob);

    // We've sent 3 packets, and we should get 2 responses.
    // First we'll get the add response, followed by the get.
    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Add,
                                  Status::Success);

    // Next we'll get the get
    ASSERT_TRUE(safe_recv_packet(blob));
    auto& response = *reinterpret_cast<Response*>(blob.data());
    EXPECT_EQ(1, response.getOpaque()) << cb::to_hex(response.getOpaque());
    response.setOpaque(0xdeadbeef);
    mcbp_validate_response_header(response, cmd, Status::Success);

    delete_object(key);
}

TEST_P(McdTestappTest, GetQ) {
    test_getq_impl("test_getq", ClientOpcode::Getq);
}

TEST_P(McdTestappTest, GetKQ) {
    test_getq_impl("test_getkq", ClientOpcode::Getkq);
}

TEST_P(McdTestappTest, IncrQ) {
    const std::string key = "test_incrq";
    const auto command =
            mcbp_arithmetic_command(ClientOpcode::Incrementq, key, 1, 0, 0);

    for (int ii = 0; ii < 10; ++ii) {
        safe_send(command);
    }

    test_noop();
    auto ret = fetch_value(key);
    EXPECT_EQ(Status::Success, ret.first);
    EXPECT_EQ(9, std::stoi(ret.second));
    delete_object(key);
}

TEST_P(McdTestappTest, DecrQ) {
    const std::string key = "test_decrq";
    const auto command =
            mcbp_arithmetic_command(ClientOpcode::Decrementq, key, 1, 9, 0);

    for (int ii = 10; ii >= 0; --ii) {
        safe_send(command);
    }

    test_noop();
    auto ret = fetch_value(key);
    EXPECT_EQ(Status::Success, ret.first);
    EXPECT_EQ(0, std::stoi(ret.second));
    delete_object(key);
}

TEST_P(McdTestappTest, Version) {
    BinprotGenericCommand cmd(ClientOpcode::Version);
    std::vector<uint8_t> blob;
    cmd.encode(blob);
    safe_send(blob);
    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Version,
                                  Status::Success);
}

void test_concat_impl(const std::string& key, ClientOpcode cmd) {
    auto command = mcbp_storage_command(cmd, key, "world", 0, 0);
    safe_send(command);
    std::vector<uint8_t> blob;
    safe_recv_packet(blob);

    mcbp_validate_response_header(
            *reinterpret_cast<Response*>(blob.data()), cmd, Status::NotStored);

    if (cmd == ClientOpcode::Appendq) {
        store_document(key, "hello");
        command = mcbp_storage_command(cmd, key, "world", 0, 0);
    } else {
        store_document(key, "world");
        command = mcbp_storage_command(cmd, key, "hello", 0, 0);
    }

    safe_send(command);

    // success should not return value
    test_noop();

    auto ret = fetch_value(key);
    EXPECT_EQ(Status::Success, ret.first);
    EXPECT_EQ("helloworld", ret.second);
    // Cleanup
    delete_object(key);
}

TEST_P(McdTestappTest, AppendQ) {
    test_concat_impl("test_appendq", ClientOpcode::Appendq);
}

TEST_P(McdTestappTest, PrependQ) {
    test_concat_impl("test_prependq", ClientOpcode::Prependq);
}

TEST_P(McdTestappTest, IOCTL_Set) {
    sasl_auth("@admin", "password");

    // release_free_memory always returns OK, regardless of how much was
    // freed.
    BinprotGenericCommand cmd(ClientOpcode::IoctlSet, "release_free_memory");
    std::vector<uint8_t> blob;
    cmd.encode(blob);

    safe_send(blob);
    safe_recv_packet(blob);
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  cb::mcbp::ClientOpcode::IoctlSet,
                                  cb::mcbp::Status::Success);
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

TEST_P(McdTestappTest, Config_Validate_Empty) {
    sasl_auth("@admin", "password");
    BinprotGenericCommand cmd(ClientOpcode::ConfigValidate);
    std::vector<uint8_t> blob;
    cmd.encode(blob);

    safe_send(blob);
    safe_recv_packet(blob);
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::ConfigValidate,
                                  Status::Einval);
}

TEST_P(McdTestappTest, Config_ValidateInvalidJSON) {
    sasl_auth("@admin", "password");

    BinprotGenericCommand cmd(ClientOpcode::ConfigValidate);
    cmd.setValue("This isn't JSON");
    std::vector<uint8_t> blob;
    cmd.encode(blob);

    safe_send(blob);
    safe_recv_packet(blob);
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::ConfigValidate,
                                  Status::Einval);
}

// Test to ensure that if a Tap Connect is requested we respond with
// Status::NotSupported
TEST_P(McdTestappTest, TapConnect) {
    std::vector<uint8_t> blob(1024);
    RequestBuilder builder({blob.data(), blob.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(ClientOpcode::TapConnect);
    builder.setOpaque(0xdeadbeef);
    safe_send(builder.getFrame()->getFrame());
    safe_recv_packet(blob);
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::TapConnect,
                                  Status::NotSupported);
}

static uint64_t get_session_ctrl_token() {
    Request request = {};
    request.setMagic(Magic::ClientRequest);
    request.setOpcode(ClientOpcode::GetCtrlToken);

    safe_send(request.getFrame());
    std::vector<uint8_t> blob;
    safe_recv_packet(blob);

    auto& response = *reinterpret_cast<Response*>(blob.data());
    cb_assert(response.getStatus() == Status::Success);

    const auto ret = response.getCas();
    cb_assert(ret != 0);

    return ret;
}

static size_t prepare_set_session_ctrl_token(cb::byte_buffer buf,
                                             uint64_t old,
                                             uint64_t new_cas) {
    request::SetCtrlTokenPayload payload;
    payload.setCas(new_cas);

    FrameBuilder<Request> builder(buf);
    builder.setMagic(Magic::ClientRequest);
    builder.setOpcode(ClientOpcode::SetCtrlToken);
    builder.setCas(old);
    builder.setExtras(payload.getBuffer());
    return builder.getFrame()->getFrame().size();
}

TEST_P(McdTestappTest, SessionCtrlToken) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        uint8_t bytes[1024];
    } buffer;

    sasl_auth("@admin", "password");

    uint64_t old_token = get_session_ctrl_token();
    uint64_t new_token = 0x0102030405060708;

    /* Validate that you may successfully set the token to a legal value */
    auto size = prepare_set_session_ctrl_token(
            {buffer.bytes, sizeof(buffer.bytes)}, old_token, new_token);
    safe_send(buffer.bytes, size, false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));

    cb_assert(buffer.response.message.header.response.getStatus() ==
              Status::Success);
    cb_assert(new_token == ntohll(buffer.response.message.header.response.cas));
    old_token = new_token;

    /* Validate that you can't set it to 0 */
    size = prepare_set_session_ctrl_token(
            {buffer.bytes, sizeof(buffer.bytes)}, old_token, 0);
    safe_send(buffer.bytes, size, false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));
    cb_assert(buffer.response.message.header.response.getStatus() ==
              Status::Einval);
    reconnect_to_server();
    sasl_auth("@admin", "password");

    cb_assert(old_token == get_session_ctrl_token());

    /* Validate that you can't set it by providing an incorrect cas */
    size = prepare_set_session_ctrl_token(
            {buffer.bytes, sizeof(buffer.bytes)}, old_token + 1, new_token - 1);
    safe_send(buffer.bytes, size, false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));

    cb_assert(buffer.response.message.header.response.getStatus() ==
              Status::KeyEexists);
    cb_assert(new_token == ntohll(buffer.response.message.header.response.cas));
    cb_assert(new_token == get_session_ctrl_token());

    /* Validate that you may set it by overriding the cas with 0 */
    size = prepare_set_session_ctrl_token(
            {buffer.bytes, sizeof(buffer.bytes)}, 0, 0xdeadbeef);
    safe_send(buffer.bytes, size, false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));
    cb_assert(buffer.response.message.header.response.getStatus() ==
              Status::Success);
    cb_assert(0xdeadbeef == ntohll(buffer.response.message.header.response.cas));
    cb_assert(0xdeadbeef == get_session_ctrl_token());
}

TEST_P(McdTestappTest, MB_10114) {
    // Disable ewouldblock_engine - not wanted / needed for this MB regression test.
    ewouldblock_engine_disable();

    char value[100000] = {0};
    const char* key = "mb-10114";
    auto command = mcbp_storage_command(
            ClientOpcode::Append, key, {value, sizeof(value)}, 0, 0);

    store_document(key, "world");
    std::vector<uint8_t> blob;
    do {
        safe_send(command);
        safe_recv_packet(blob);
    } while (reinterpret_cast<Response*>(blob.data())->getStatus() ==
             Status::Success);

    EXPECT_EQ(Status::E2big,
              reinterpret_cast<Response*>(blob.data())->getStatus());

    /* We should be able to delete it */
    delete_object(key, false);
}

/* expiry, wait1 and wait2 need to be crafted so that
   1. sleep(wait1) and key exists
   2. sleep(wait2) and key should now have expired.
*/
static void test_expiry(const char* key, time_t expiry,
                        time_t wait1, int clock_shift) {
    auto command = mcbp_storage_command(
            ClientOpcode::Set, key, "value", 0, (uint32_t)expiry);

    safe_send(command);
    std::vector<uint8_t> blob;
    safe_recv_packet(blob);
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Set,
                                  Status::Success);

    adjust_memcached_clock(clock_shift,
                           request::AdjustTimePayload::TimeType::TimeOfDay);

    auto ret = fetch_value(key);
    EXPECT_EQ(Status::Success, ret.first);
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

void McdTestappTest::test_set_huge_impl(const std::string& key,
                                        ClientOpcode cmd,
                                        Status result,
                                        size_t message_size) {
    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();
    std::vector<char> payload(message_size);
    auto command = mcbp_storage_command(
            cmd, key, {payload.data(), payload.size()}, 0, 0);

    safe_send(command);
    if (cmd == ClientOpcode::Set || result != Status::Success) {
        safe_recv_packet(payload);
        mcbp_validate_response_header(
                *reinterpret_cast<Response*>(payload.data()), cmd, result);
    } else {
        test_noop();
    }
}

TEST_P(McdTestappTest, SetHuge) {
    test_set_huge_impl("test_set_huge",
                       ClientOpcode::Set,
                       Status::Success,
                       GetTestBucket().getMaximumDocSize() - 256);
}

TEST_P(McdTestappTest, SetE2BIG) {
    test_set_huge_impl("test_set_e2big",
                       ClientOpcode::Set,
                       Status::E2big,
                       GetTestBucket().getMaximumDocSize() + 1);
}

TEST_P(McdTestappTest, SetQHuge) {
    test_set_huge_impl("test_setq_huge",
                       ClientOpcode::Setq,
                       Status::Success,
                       GetTestBucket().getMaximumDocSize() - 256);
}

TEST_P(McdTestappTest, SetQE2BIG) {
    test_set_huge_impl("test_set_e2big",
                       ClientOpcode::Setq,
                       Status::E2big,
                       GetTestBucket().getMaximumDocSize() + 1);
}

#if !defined(FOLLY_SANITIZE)
// These tests are disabled under Sanitizers as they take a lot
// of time and don't really expose any new features in the server

/* support set, get, delete */
void test_pipeline_impl(ClientOpcode cmd,
                        Status result,
                        const char* key_root,
                        uint32_t messages_in_stream,
                        size_t value_size) {
    size_t largest_protocol_packet =
            sizeof(Request) +
            sizeof(request::MutationPayload); /* set has the largest
                                                           protocol message */
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
    if (cmd == ClientOpcode::Set) {
        /* set, sends key and a value */
        out_message_len = sizeof(Request) + sizeof(request::MutationPayload) +
                          key_root_len + key_digit_len + value_size;
        /* receives a plain response, no extra */
        in_message_len = sizeof(protocol_binary_response_no_extras);
    } else if (cmd == ClientOpcode::Get) {
        /* get sends key */
        out_message_len = sizeof(Request) + key_root_len + key_digit_len;

        if (result == Status::Success) {
            /* receives a response + flags + value */
            in_message_len = sizeof(protocol_binary_response_no_extras) + 4 + value_size;
        } else {
            /* receives a response + string error */
            in_message_len = sizeof(protocol_binary_response_no_extras) + 9;
        }
    } else if (cmd == ClientOpcode::Delete) {
        /* delete sends key */
        out_message_len = sizeof(Request) + key_root_len + key_digit_len;
        /* receives a plain response, no extra */
        in_message_len = sizeof(protocol_binary_response_no_extras);
    } else {
        FAIL() << "invalid cmd (" << to_string(cmd)
               << ") in test_pipeline_impl";
    }

    send_len    = out_message_len * messages_in_stream;
    receive_len = in_message_len * messages_in_stream;

    // Fill the entire buffer with 0xfa (to make sure that we initialize
    // all fields in the stream)
    std::fill(buffer.begin(), buffer.end(), 0xfa);
    std::vector<uint8_t> value(value_size);
    // Use a value of 0xaf for all data we set (we verify the value of the
    // responses that they contain this pattern)
    std::fill(value.begin(), value.end(), 0xaf);

    for (uint32_t ii = 0; ii < messages_in_stream; ii++) {
        snprintf(key.data(), key_root_len + key_digit_len + 1, "%s%05d", key_root, ii);
        FrameBuilder<Request> builder({current_message, out_message_len});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cmd);
        builder.setOpaque(htonl((session << 8) | ii));
        if (ClientOpcode::Set == cmd) {
            request::MutationPayload extras;
            builder.setExtras(extras.getBuffer());
        }
        builder.setKey({key.data(), strlen(key.data())});
        if (ClientOpcode::Set == cmd) {
            builder.setValue(value);
        }
        current_message += builder.getFrame()->getFrame().size();
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
        if (bodylen != 0 && result == Status::Success) {
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
    // Skip on slow environments.
    if (RUNNING_ON_VALGRIND || (folly::kIsWindows && folly::kIsDebug)) {
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
        test_pipeline_impl(
                ClientOpcode::Set, Status::Success, "key_set_pipe", 100, ii);
        test_pipeline_impl(
                ClientOpcode::Delete, Status::Success, "key_set_pipe", 100, ii);
    }
}

TEST_P(McdTestappTest, PipelineSetGetDel) {
    // Skip on slow environments.
    if (RUNNING_ON_VALGRIND || (folly::kIsWindows && folly::kIsDebug)) {
        return;
    }

    const char key_root[] = "key_set_get_del";

    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    test_pipeline_impl(ClientOpcode::Set, Status::Success, key_root, 5000, 256);

    test_pipeline_impl(ClientOpcode::Get, Status::Success, key_root, 5000, 256);

    test_pipeline_impl(
            ClientOpcode::Delete, Status::Success, key_root, 5000, 256);
}

TEST_P(McdTestappTest, PipelineSetDel) {
    // Skip on slow environments.
    if (RUNNING_ON_VALGRIND || (folly::kIsWindows && folly::kIsDebug)) {
        return;
    }

    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    test_pipeline_impl(
            ClientOpcode::Set, Status::Success, "key_root", 5000, 256);

    test_pipeline_impl(
            ClientOpcode::Delete, Status::Success, "key_root", 5000, 256);
}
#endif // FOLLY_SANITIZE

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

TEST_P(McdTestappTest, ExceedMaxPacketSize) {
    Request request;
    request.setMagic(Magic::ClientRequest);
    request.setOpcode(ClientOpcode::Set);
    request.setExtlen(sizeof(request::MutationPayload));
    request.setKeylen(1);
    request.setBodylen(31 * 1024 * 1024);
    request.setOpaque(0xdeadbeef);
    safe_send(&request, sizeof(request), false);

    // the server will read the header, and figure out that the packet
    // is too big and close the socket
    std::vector<uint8_t> blob(1024);
    EXPECT_EQ(0, phase_recv(blob.data(), blob.size()));
    reconnect_to_server();
}

/**
 * Test that opcode 255 is rejected and the server doesn't crash
 */
TEST_P(McdTestappTest, test_MB_16333) {
    BinprotGenericCommand cmd(ClientOpcode::Invalid);
    std::vector<uint8_t> blob;
    cmd.encode(blob);
    safe_send(blob);
    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Invalid,
                                  Status::UnknownCommand);
}

/**
 * Test that a bad SASL auth doesn't crash the server.
 * It should be rejected with EINVAL.
 */
TEST_P(McdTestappTest, test_MB_16197) {
    reconnect_to_server();

    std::vector<uint8_t> buffer(1024);
    RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(Magic::ClientRequest);
    builder.setOpcode(ClientOpcode::SaslAuth);
    builder.setKey("PLAIN");
    // This is a malformed request where we don't contain a username at all
    builder.setValue({"\0", 1});
    builder.setOpaque(0xdeadbeef);
    safe_send(builder.getFrame()->getFrame());

    safe_recv_packet(buffer);
    mcbp_validate_response_header(*reinterpret_cast<Response*>(buffer.data()),
                                  ClientOpcode::SaslAuth,
                                  Status::Einval,
                                  false);
}

TEST_F(TestappTest, CollectionsSelectBucket) {
    auto& conn = getAdminConnection();

    // Create and select a bucket on which we will be able to hello collections
    ASSERT_NO_THROW(
            conn.createBucket("collections", "", BucketType::Couchbase));
    conn.selectBucket("collections");

    // Hello collections to enable collections for this connection
    BinprotHelloCommand cmd("Collections");
    cmd.enableFeature(cb::mcbp::Feature::Collections);
    const auto rsp = BinprotHelloResponse(conn.execute(cmd));
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    try {
        conn.selectBucket("default");
        if (!GetTestBucket().supportsCollections()) {
            FAIL() << "Select bucket did not throw a not supported error when"
                      "attempting to select a memcache bucket with a "
                      "collections enabled connections";
        }
    } catch (const ConnectionError& e) {
        if (!GetTestBucket().supportsCollections()) {
            EXPECT_EQ(cb::mcbp::Status::NotSupported, e.getReason());
        } else {
            FAIL() << std::string("Select bucket failed for unknown reason: ") +
                              to_string(e.getReason());
        }
    }
}

/**
 * Encode a Alt Request (0x08) HELO command with framing extras but
 * too short total length field.
 *
 * See MB-46853.
 */
class MalformedHelloCommand : public BinprotHelloCommand {
public:
    MalformedHelloCommand() : BinprotHelloCommand("client") {
        // To repro the issue, It does not matter _what_ frame infos are
        // present, as long as there are two and they are valid.
        addFrameInfo(BarrierFrameInfo());
        addFrameInfo(BarrierFrameInfo());

        // _NO_ Features are advertised; the value should be empty.
    }

    void encode(std::vector<uint8_t>& buf) const override {
        BinprotHelloCommand::encode(buf);
        // At this point
        //   BodyLen = frameExtras + extras + key + value
        //      8     =     2       +   0    +  6  +  0
        auto& hdr = *reinterpret_cast<cb::mcbp::Request*>(buf.data());
        auto realBodyLen = hdr.getBodylen();
        EXPECT_EQ(8, realBodyLen);
        // shorten the bodyLen by two bytes. This _should_ fail validation
        // because now frameExtras + extras + key > BodyLen.
        // This would mean they key would extend past the end of the
        // request.
        // MB-46853 - this was accepted because frameExtras were not counted
        // leaving extras + key <= BodyLen
        hdr.setBodylen(realBodyLen - 2);
    }
};

TEST_F(TestappTest, MB_46853_TotalBodyLengthValidation) {
    auto& conn = getConnection();

    MalformedHelloCommand cmd;
    // MB-46853: memcached segfault, connection reset.
    // Good behaviour: validation catches malformed packet,
    //                 memcached fine, connection reset.
    try {
        conn.execute(cmd);
        // connection should be closed, reading the response should throw.
        // Fail if that didn't happen.
        FAIL();
    } catch (const std::system_error& e) {
        EXPECT_EQ(ECONNRESET, e.code().value());
    }

    // this will fail if memcached died because of the previous request.
    conn.reconnect();

    BinprotHelloCommand followup("StillAlive?");
    const auto rsp = BinprotHelloResponse(conn.execute(followup));
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
}

INSTANTIATE_TEST_CASE_P(
        Transport,
        McdTestappTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain,
                                             TransportProtocols::McbpSsl),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No)),
        McdTestappTest::PrintToStringCombinedName);
