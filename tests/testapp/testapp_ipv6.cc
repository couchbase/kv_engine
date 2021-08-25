/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp.h"
#include "testapp_client_test.h"

/*
 * This test batch run basic tests over IPv6 (plain and SSL).
 *
 * Given that the only difference between IPv4 and IPv6 is how
 * the socket is being created, it isn't much value in running
 * all of the unit tests over both IPv4 and IPv6. This speeds
 * up the test execution.
 */

class IPv6Test : public TestappClientTest {
public:
    bool isTls() {
        switch (GetParam()) {
        case TransportProtocols::McbpPlain:
            return false;
        case TransportProtocols::McbpSsl:
            return true;
        }
        throw std::invalid_argument("IPv6Test::isTls: Invalid param provided");
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         IPv6Test,
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

/**
 * The test just tries to authenticate as @admin on the memcached connection
 *
 * In order to do that we'll do multiple roundtips to the server which
 * checks that we can acutally use an IPv6 connection
 */
TEST_P(IPv6Test, Authenticate) {
    if (!mcd_env->haveIPv6()) {
        GTEST_SKIP();
    }

    bool tls = isTls();
    std::unique_ptr<MemcachedConnection> connection;
    connectionMap.iterate([&connection, tls](const auto& c) {
        if (c.getFamily() == AF_INET6 && tls == c.isSsl()) {
            connection = c.clone();
        }
    });

    ASSERT_TRUE(connection) << "Failed to locate connection";
    connection->authenticate("@admin", "password");
}
