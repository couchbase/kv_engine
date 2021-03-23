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

#include "testapp.h"

#include <cstring>
#include <ctime>

#include <folly/portability/GTest.h>
#include <atomic>
#include <string>
#include <vector>

#include <mcbp/protocol/framebuilder.h>
#include <memcached/util.h>
#include <platform/socket.h>
#include <platform/string_hex.h>

using namespace cb::mcbp;

/**
 * Constructs a storage command using the give arguments into buf.
 *
 * @param cmd the command opcode to use
 * @param key the key to use
 * @param value the value for the key
 * @param flags the value to use for the flags
 * @param exp the expiry time
 */
static std::vector<uint8_t> mcbp_storage_command(cb::mcbp::ClientOpcode cmd,
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

class OldMemcachedTests : public McdTestappTest {
protected:
    /* Helpers for individual testcases */
    void test_set_huge_impl(const std::string& key,
                            cb::mcbp::ClientOpcode cmd,
                            cb::mcbp::Status result,
                            size_t message_size);
};

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

TEST_P(OldMemcachedTests, Noop) {
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
    EXPECT_EQ(0, cb::net::recv(sock, blob.data(), blob.size(), 0));
    reconnect_to_server();
}

TEST_P(OldMemcachedTests, Quit) {
    test_quit_impl(ClientOpcode::Quit);
}

TEST_P(OldMemcachedTests, QuitQ) {
    test_quit_impl(ClientOpcode::Quitq);
}

TEST_P(OldMemcachedTests, SetQ) {
    const std::string key = "test_setq";
    auto command = mcbp_storage_command(ClientOpcode::Setq, key, "value", 0, 0);

    /* Set should work over and over again */
    std::vector<uint8_t> blob;
    for (int ii = 0; ii < 10; ++ii) {
        safe_send(command);
    }

    return test_noop();
}

TEST_P(OldMemcachedTests, AddQ) {
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

TEST_P(OldMemcachedTests, ReplaceQ) {
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

TEST_P(OldMemcachedTests, DeleteQ) {
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

TEST_P(OldMemcachedTests, GetK) {
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

TEST_P(OldMemcachedTests, GetQ) {
    test_getq_impl("test_getq", ClientOpcode::Getq);
}

TEST_P(OldMemcachedTests, GetKQ) {
    test_getq_impl("test_getkq", ClientOpcode::Getkq);
}

static std::vector<uint8_t> mcbp_arithmetic_command(cb::mcbp::ClientOpcode cmd,
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
    builder.setExtras(extras.getBuffer());
    builder.setOpaque(0xdeadbeef);
    builder.setKey(key);
    return buffer;
}

TEST_P(OldMemcachedTests, IncrQ) {
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

TEST_P(OldMemcachedTests, DecrQ) {
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

TEST_P(OldMemcachedTests, Version) {
    const auto rsp = getConnection().execute(
            BinprotGenericCommand{ClientOpcode::Version});
    EXPECT_EQ(ClientOpcode::Version, rsp.getOp());
    EXPECT_TRUE(rsp.isSuccess());
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

TEST_P(OldMemcachedTests, AppendQ) {
    test_concat_impl("test_appendq", ClientOpcode::Appendq);
}

TEST_P(OldMemcachedTests, PrependQ) {
    test_concat_impl("test_prependq", ClientOpcode::Prependq);
}

void OldMemcachedTests::test_set_huge_impl(const std::string& key,
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

TEST_P(OldMemcachedTests, SetHuge) {
    test_set_huge_impl("test_set_huge",
                       ClientOpcode::Set,
                       Status::Success,
                       GetTestBucket().getMaximumDocSize() - 256);
}

TEST_P(OldMemcachedTests, SetE2BIG) {
    test_set_huge_impl("test_set_e2big",
                       ClientOpcode::Set,
                       Status::E2big,
                       GetTestBucket().getMaximumDocSize() + 1);
}

TEST_P(OldMemcachedTests, SetQHuge) {
    test_set_huge_impl("test_setq_huge",
                       ClientOpcode::Setq,
                       Status::Success,
                       GetTestBucket().getMaximumDocSize() - 256);
}

TEST_P(OldMemcachedTests, SetQE2BIG) {
    test_set_huge_impl("test_set_e2big",
                       ClientOpcode::Setq,
                       Status::E2big,
                       GetTestBucket().getMaximumDocSize() + 1);
}

INSTANTIATE_TEST_SUITE_P(
        Transport,
        OldMemcachedTests,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No)),
        OldMemcachedTests::PrintToStringCombinedName);
