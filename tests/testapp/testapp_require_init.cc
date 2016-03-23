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
#include <string.h>
#include <cerrno>
#include "testapp.h"

#ifdef USE_EXTENDED_ERROR_CODES

class RequireInitTest : public TestappTest {
public:
    RequireInitTest()
        : token(0xdeadbeef) {
    }

    static void SetUpTestCase() {
        // Do nothing.
        //
        // If we don't provide a SetUpTestCase we'll get the one from
        // TestappTest which will start the server for us (which is
        // what we want, but in this test we're going to experiment with
        // sending messages before we've told memcached we're done
    }

    virtual void SetUp() {

        memcached_cfg.reset(generate_config(0));
        cJSON_AddTrueToObject(memcached_cfg.get(), "require_init");
        start_memcached_server(memcached_cfg.get());

        if (HasFailure()) {
            server_pid = reinterpret_cast<pid_t>(-1);
        } else {
            CreateTestBucket();
        }

        ASSERT_NE(reinterpret_cast<pid_t>(-1), server_pid);

        setControlToken();
    }

    virtual void TearDown() {
        closesocket(sock);
        TestappTest::TearDownTestCase();
    }

    static void TearDownTestCase() {
        // Empty
    }

protected:
    /**
     * Set the session control token in memcached (this token is used
     * to validate the shutdown command)
     */
    void setControlToken() {
        auto& conn = connectionMap.getConnection(Protocol::Memcached, false);
        ASSERT_NO_THROW(conn.authenticate("_admin", "password", "PLAIN"));

        Frame frame;

        mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                         NULL, 0, NULL, 0);
        frame.payload.resize(32);
        char* ptr = reinterpret_cast<char*>(&token);
        memcpy(frame.payload.data() + 24, ptr, sizeof(token));

        conn.sendFrame(frame);

        conn.recvFrame(frame);
        EXPECT_EQ(uint8_t(PROTOCOL_BINARY_RES), frame.payload.at(0));

        auto* packet = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

        mcbp_validate_response_header(packet,
                                      PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
        // reconnect to get rid of admin privileges
        conn.reconnect();
    }

    void noop(bool success) {
        auto& conn = connectionMap.getConnection(Protocol::Memcached, false);
        conn.reconnect();

        Frame frame;
        mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_NOOP, NULL, 0, NULL, 0);
        conn.sendFrame(frame);

        conn.recvFrame(frame);
        EXPECT_EQ(uint8_t(PROTOCOL_BINARY_RES), frame.payload.at(0));

        auto* packet = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

        if (success) {
            mcbp_validate_response_header(packet, PROTOCOL_BINARY_CMD_NOOP,
                                          PROTOCOL_BINARY_RESPONSE_SUCCESS);
        } else {
            mcbp_validate_response_header(packet, PROTOCOL_BINARY_CMD_NOOP,
                                          PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED);
        }

    }

    uint64_t token;
};

TEST_F(RequireInitTest, NotYetInitialized) {
    noop(false);
}

TEST_F(RequireInitTest, UserPortsNotPresent) {
    // The SSL port is not marked as a management port
    EXPECT_THROW(connectionMap.getConnection(Protocol::Memcached, true,
                                             AF_INET),
                 std::runtime_error);

    EXPECT_THROW(connectionMap.getConnection(Protocol::Memcached, true,
                                             AF_INET6),
                 std::runtime_error);
}

TEST_F(RequireInitTest, InitializeNotAuthorized) {
    auto& conn = connectionMap.getConnection(Protocol::Memcached, false);

    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_INIT_COMPLETE, NULL, 0, NULL,
                     0);
    conn.sendFrame(frame);

    conn.recvFrame(frame);
    EXPECT_EQ(uint8_t(PROTOCOL_BINARY_RES), frame.payload.at(0));

    auto* packet = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());
    // This may look wrong to you, but until we're properly initialized
    // the error sent back to the client is that we're not initialized
    mcbp_validate_response_header(packet, PROTOCOL_BINARY_CMD_INIT_COMPLETE,
                                  PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED);

    // noop should still fail
    noop(false);
}

TEST_F(RequireInitTest, InitializeWrongToken) {
    auto& conn = connectionMap.getConnection(Protocol::Memcached, false);
    ASSERT_NO_THROW(conn.authenticate("_admin", "password", "PLAIN"));

    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_INIT_COMPLETE, NULL, 0, NULL,
                     0);
    uint64_t* cas = reinterpret_cast<uint64_t*>(frame.payload.data() + 16);
    *cas = 0xcafeeed;
    conn.sendFrame(frame);

    conn.recvFrame(frame);
    EXPECT_EQ(uint8_t(PROTOCOL_BINARY_RES), frame.payload.at(0));

    auto* packet = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

    mcbp_validate_response_header(packet, PROTOCOL_BINARY_CMD_INIT_COMPLETE,
                                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    // noop should still fail
    noop(false);
}

TEST_F(RequireInitTest, InitializeSuccess) {
    auto& conn = connectionMap.getConnection(Protocol::Memcached, false);
    ASSERT_NO_THROW(conn.authenticate("_admin", "password", "PLAIN"));

    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_INIT_COMPLETE, NULL, 0, NULL,
                     0);
    uint64_t* cas = reinterpret_cast<uint64_t*>(frame.payload.data() + 16);
    *cas = token;
    conn.sendFrame(frame);

    conn.recvFrame(frame);
    EXPECT_EQ(uint8_t(PROTOCOL_BINARY_RES), frame.payload.at(0));

    auto* packet = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

    mcbp_validate_response_header(packet, PROTOCOL_BINARY_CMD_INIT_COMPLETE,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    // We should get a new output file..
    FILE* fp;
    time_t start = time(NULL);

    while ((fp = fopen(portnumber_file.c_str(), "r")) == nullptr) {
        usleep(50);
        ASSERT_GE(start + 5, time(NULL)); // give up after 5 secs
    }

    fclose(fp);
    unique_cJSON_ptr portnumbers;
    ASSERT_NO_THROW(portnumbers = loadJsonFile(portnumber_file));
    connectionMap.initialize(portnumbers.get());
    // And now we should have the SSL connection available
    EXPECT_NO_THROW(connectionMap.getConnection(Protocol::Memcached, true,
                                             AF_INET));


    EXPECT_EQ(0, remove(portnumber_file.c_str()));

    // noop should work :D
    noop(true);
}

#endif
