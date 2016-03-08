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

#include "testapp.h"
#include "testapp_client_test.h"
#include <protocol/connection/client_greenstack_connection.h>
#include <protocol/connection/client_mcbp_connection.h>

class RolesTest : public TestappClientTest {
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        RolesTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

TEST_P(RolesTest, AssumeNonexistingRole) {
    MemcachedConnection& conn = getConnection();

    // Adding it one more time should fail
    try {
        conn.assumeRole("NonexistingRole");
        FAIL();
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }
}

TEST_P(RolesTest, AssumeRole) {
    MemcachedConnection& conn = getConnection();
    ASSERT_NO_THROW(conn.assumeRole("statistics"));
    // I should be allowed to run stats
    ASSERT_NO_THROW(conn.stats(""));

    // I should not be allowed to store a document operation
    Document doc;
    doc.info.cas = Greenstack::CAS::Wildcard;
    doc.info.compression = Greenstack::Compression::None;
    doc.info.datatype = Greenstack::Datatype::Json;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    char* ptr = cJSON_Print(memcached_cfg.get());
    std::copy(ptr, ptr + strlen(ptr), std::back_inserter(doc.value));
    cJSON_Free(ptr);

    try {
        conn.mutate(doc, 0, Greenstack::MutationType::Add);
        FAIL() << "role should not have access to the add command";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied()) << error.what();
    }

    // Drop the role
    ASSERT_NO_THROW(conn.assumeRole(""));

    // This time add should succeed
    EXPECT_NO_THROW(conn.mutate(doc, 0, Greenstack::MutationType::Add));
}
