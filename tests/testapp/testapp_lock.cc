/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "testapp_client_test.h"
#include <protocol/connection/client_mcbp_connection.h>

#include <algorithm>
#include <platform/compress.h>

class LockTest : public TestappClientTest {
protected:
    MemcachedBinprotConnection& getMcbpConnection() {
        return dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    }
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        LockTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

TEST_P(LockTest, LockNotSupported) {
    auto& conn = getMcbpConnection();

    try {
        conn.get_and_lock("foo", 0, 0);
        FAIL() << "default_bucket should not support lock";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotSupported());
    }
}

TEST_P(LockTest, UnlockNotSupported) {
    auto& conn = getMcbpConnection();

    try {
        conn.unlock("foo", 0, 0xdeadbeef);
        FAIL() << "default_bucket should not support unlock";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotSupported());
    }
}
