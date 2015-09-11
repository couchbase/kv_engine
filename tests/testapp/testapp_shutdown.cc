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

class ShutdownTest : public TestappTest {
public:
    ShutdownTest()
        : token(0xdeadbeef) {
    }

    static void SetUpTestCase() {
        // Do nothing.
        //
        // If we don't provide a SetUpTestCase we'll get the one from
        // TestappTest which will start the server for us (which is
        // what we want, but in this test we're going to start try
        // to stop the server so we need to have each test case
        // start (and stop) the server for us...
    }

    virtual void SetUp() {
        TestappTest::SetUpTestCase();

        ASSERT_NE(reinterpret_cast<pid_t>(-1), server_pid);
        sock = connect_to_server_plain(port);
        ASSERT_NE(INVALID_SOCKET, sock);

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
        std::vector<char> message;
        message.resize(32);
        raw_command(message.data(), message.size(),
                    PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                    nullptr, 0, nullptr, 0);

        char* ptr = reinterpret_cast<char*>(&token);
        memcpy(message.data() + 24, ptr, sizeof(token));

        safe_send(message.data(), message.size(), false);
        uint8_t buffer[1024];
        safe_recv_packet(buffer, sizeof(buffer));
        auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(buffer);
        validate_response_header(rsp, PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);

    }

    /**
     * Assume the specified role
     */
    void assumeRole(const std::string& role) {
        std::vector<char> message;
        message.resize(24 + role.size());
        raw_command(message.data(), message.size(),
                    PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                    role.c_str(), role.length(), nullptr, 0);
        safe_send(message.data(), message.size(), false);
        uint8_t buffer[1024];
        safe_recv_packet(buffer, sizeof(buffer));
        auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(buffer);
        validate_response_header(rsp, PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);

    }

    /**
     * Send the shutdown message to the server and read the response
     * back and compare it with the expected result
     */
    void sendShutdown(protocol_binary_response_status status) {
        // build the shutdown packet
        std::vector<char> packet;
        packet.resize(24);
        raw_command(packet.data(), packet.size(), PROTOCOL_BINARY_CMD_SHUTDOWN,
                    nullptr, 0, nullptr, 0);
        char* ptr = reinterpret_cast<char*>(&token);
        memcpy(packet.data() + 16, ptr, sizeof(token));
        safe_send(packet.data(), packet.size(), false);

        uint8_t buffer[1024];
        safe_recv_packet(buffer, sizeof(buffer));
        auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(buffer);
        validate_response_header(rsp, PROTOCOL_BINARY_CMD_SHUTDOWN, status);
    }

    uint64_t token;
};

TEST_F(ShutdownTest, ShutdownNotAllowed) {
    assumeRole("statistics");
    sendShutdown(PROTOCOL_BINARY_RESPONSE_EACCESS);
}

TEST_F(ShutdownTest, ShutdownAllowed) {
    sendShutdown(PROTOCOL_BINARY_RESPONSE_SUCCESS);

#ifdef WIN32
    ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(server_pid, 60000));
    DWORD exit_code = NULL;
    GetExitCodeProcess(server_pid, &exit_code);
    EXPECT_EQ(0, exit_code);
#else
    /* Wait for the process to be gone... */
    int status;
    pid_t ret;
    int retry = 60;
    while ((ret = waitpid(server_pid, &status, 0)) != server_pid && retry > 0) {
        ASSERT_NE(reinterpret_cast<pid_t>(-1), ret)
            << "waitpid failed: " << strerror(errno);
        ASSERT_EQ(0, ret);
        --retry;
        sleep(1);
    }
    EXPECT_NE(0, retry);
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
#endif
    server_pid = reinterpret_cast<pid_t>(-1);
}
