/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#ifdef WIN32
#error "This file should not be included on windows"
#endif

#include <atomic>
#include <thread>
#include "testapp.h"
#include "testapp_client_test.h"
#include "saslauthd_mock.h"

static char cbauth_env_var[256];

std::unique_ptr<SaslauthdMock> authdMock;

// This class acts as a "guard". If we get an exception we can be sure that
// the thread will be joined, and that thread::~thread() won't abort because
// it wasn't join()d yet.
class MockAuthServer {
public:
    MockAuthServer() : thread([](){authdMock->processOne();}){
    }

    ~MockAuthServer() {
        thread.join();
    }

private:
    std::thread thread;
};

class SaslauthdTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        authdMock.reset(new SaslauthdMock);
        std::string env{"CBAUTH_SOCKPATH=" + authdMock->getSockfile() };
        if (env.size() + 1 > sizeof(cbauth_env_var)) {
            throw std::overflow_error("SaslauthdTest::SetUpTestCase: "
                                          "buffer for socket path is too"
                                          " small");
        }
        strcpy(cbauth_env_var, env.c_str());
        putenv(cbauth_env_var);
        TestappClientTest::SetUpTestCase();
    }

    static void TearDownTestCase() {
        unsetenv("CBAUTH_SOCKPATH");
        TestappClientTest::TearDownTestCase();
    }
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        SaslauthdTest,
    ::testing::Values(TransportProtocols::McbpPlain,
                      TransportProtocols::McbpIpv6Plain,
                      TransportProtocols::McbpSsl,
                      TransportProtocols::McbpIpv6Ssl
                     ),
    ::testing::PrintToStringParamName());

TEST_P(SaslauthdTest, TestSuccessfulSaslauthd) {
    MemcachedConnection& conn = getConnection();
    MockAuthServer saslauthd;
    conn.authenticate("superman", "<3LoisLane<3", "PLAIN");
}

TEST_P(SaslauthdTest, TestIncorrectSaslauthd) {
    MemcachedConnection& conn = getConnection();
    MockAuthServer saslauthd;

    EXPECT_THROW(conn.authenticate("superman", "Lane<3", "PLAIN"),
                 std::runtime_error);
}

TEST_P(SaslauthdTest, TestUnknownUser) {
    MemcachedConnection& conn = getConnection();
    MockAuthServer saslauthd;
    EXPECT_THROW(conn.authenticate("godzilla", "Lane<3", "PLAIN"),
                 std::runtime_error);
}

TEST_P(SaslauthdTest, TestKnownSaslauthdUnknownMech) {
    MemcachedConnection& conn = getConnection();
    EXPECT_THROW(conn.authenticate("superman", "<3LoisLane<3", "SCRAM-SHA1"),
                 std::runtime_error);
}
