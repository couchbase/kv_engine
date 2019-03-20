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
#include <cbcrypto/cbcrypto.h>
#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <folly/portability/GTest.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <gsl/gsl>
#include <memory>

const char* cbpwfile = "cbsasl_test.pw";

char envptr[256]{"ISASL_PWFILE=cbsasl_test.pw"};

class SaslClientServerTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        FILE* fp = fopen(cbpwfile, "w");
        ASSERT_NE(nullptr, fp);

        // Create a password with a leading, middle and trailing space
        fprintf(fp, "mikewied  mik epw \n");
        ASSERT_EQ(0, fclose(fp));

        putenv(envptr);
        cb::sasl::server::initialize();
    }

    static void TearDownTestCase() {
        cb::sasl::server::shutdown();
        ASSERT_EQ(0, remove(cbpwfile));
    }

    // You may set addNonce to true to have it use a fixed nonce
    // for debugging purposes
    void test_auth(const char* mech) {
        cb::sasl::client::ClientContext client(
                []() -> std::string { return std::string{"mikewied"}; },
                []() -> std::string { return std::string{" mik epw "}; },
                mech);

        auto client_data = client.start();
        ASSERT_EQ(cb::sasl::Error::OK, client_data.first);

        cb::sasl::server::ServerContext server;

        auto server_data = server.start(
                client.getName(),
                "SCRAM-SHA512,SCRAM-SHA256,SCRAM-SHA1,CRAM-MD5,PLAIN",
                client_data.second);
        if (server_data.first == cb::sasl::Error::OK) {
            // Authentication success
            return;
        }

        ASSERT_EQ(cb::sasl::Error::CONTINUE, server_data.first);

        // jeg må da avslutte med en client step?
        do {
            client_data = client.step(server_data.second);
            ASSERT_EQ(cb::sasl::Error::CONTINUE, client_data.first);
            server_data = server.step(client_data.second);
        } while (server_data.first == cb::sasl::Error::CONTINUE);

        ASSERT_EQ(cb::sasl::Error::OK, server_data.first);
    }
};

TEST_F(SaslClientServerTest, PLAIN) {
    test_auth("PLAIN");
}

TEST_F(SaslClientServerTest, SCRAM_SHA1) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA1)) {
        test_auth("SCRAM-SHA1");
    }
}

TEST_F(SaslClientServerTest, SCRAM_SHA256) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA256)) {
        test_auth("SCRAM-SHA256");
    }
}

TEST_F(SaslClientServerTest, SCRAM_SHA512) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA512)) {
        test_auth("SCRAM-SHA512");
    }
}

TEST_F(SaslClientServerTest, AutoSelectMechamism) {
    test_auth("(SCRAM-SHA512,SCRAM-SHA256,SCRAM-SHA1,CRAM-MD5,PLAIN)");
}
