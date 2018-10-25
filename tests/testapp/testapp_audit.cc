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

/*
 * Tests that check certain events make it into the audit log.
 */

#include "testapp.h"

#include <platform/dirutils.h>
#include "testapp_client_test.h"
#include "memcached_audit_events.h"
#include "auditd/auditd_audit_events.h"

#include <string>
#include <fstream>


class AuditTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        reconfigure_client_cert_auth("disable", "", "", "");
        auto& logdir = mcd_env->getAuditLogDir();
        EXPECT_NO_THROW(cb::io::rmrf(logdir));
        cb::io::mkdirp(logdir);
        setEnabled(true);
    }

    void TearDown() override {
        reconfigure_client_cert_auth("disable", "", "", "");
        setEnabled(false);
        auto& logdir = mcd_env->getAuditLogDir();
        EXPECT_NO_THROW(cb::io::rmrf(mcd_env->getAuditLogDir()));
        cb::io::mkdirp(logdir);
        TestappClientTest::TearDown();
    }

    void setEnabled(bool mode) {
        auto& json = mcd_env->getAuditConfig();
        json["auditd_enabled"] = mode;
        try {
            mcd_env->rewriteAuditConfig();
        } catch (std::exception& e) {
            FAIL() << "Failed to toggle audit state: " << e.what();
        }

        auto& connection = getConnection();
        connection.authenticate("@admin", "password", "PLAIN");
        connection.reloadAuditConfiguration();
        connection.reconnect();
    }

    std::vector<unique_cJSON_ptr> readAuditData();

    bool searchAuditLogForID(int id,
                             const std::string& username = "",
                             const std::string& bucketname = "");
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        AuditTest,
                        ::testing::Values(TransportProtocols::McbpPlain),
                        ::testing::PrintToStringParamName());


std::vector<unique_cJSON_ptr> AuditTest::readAuditData() {
    std::vector<unique_cJSON_ptr> rval;
    auto files = cb::io::findFilesContaining(
        mcd_env->getAuditLogDir(),
        "audit.log");
    for (auto file : files) {
        std::ifstream auditData(file, std::ifstream::in);
        while (auditData.good()) {
            std::string line;
            std::getline(auditData, line);
            unique_cJSON_ptr jsonPtr(cJSON_Parse(line.c_str()));
            if (jsonPtr.get() != nullptr) {
                rval.push_back(std::move(jsonPtr));
            }
        }
    }
    return rval;
}

bool AuditTest::searchAuditLogForID(int id,
                                    const std::string& username,
                                    const std::string& bucketname) {
    // @todo loop up to 5 sec trying to get it..
    auto timeout = time(NULL) + 5;

    do {
        auto auditEntries = readAuditData();
        for (auto& entry : auditEntries) {
            cJSON* idEntry = cJSON_GetObjectItem(entry.get(), "id");
            if (idEntry && idEntry->valueint == id) {
                // This the type we're searching for..
                std::string user;
                std::string bucket;

                auto* obj = cJSON_GetObjectItem(entry.get(), "bucket");
                if (obj && obj->type == cJSON_String) {
                    bucket.assign(obj->valuestring);
                }

                obj = cJSON_GetObjectItem(entry.get(), "real_userid");
                if (obj) {
                    obj = cJSON_GetObjectItem(obj, "user");
                    if (obj && obj->type == cJSON_String) {
                        user.assign(obj->valuestring);
                    }
                }

                if (!username.empty()) {
                    if (user.empty()) {
                        // The entry did not contain a username!
                        return false;
                    }

                    if (user != username) {
                        // We found another user (needed to test authentication
                        // success ;)
                        continue;
                    }
                }

                if (!bucketname.empty()) {
                    if (bucket.empty()) {
                        // This entry did not contain a bucket entry
                        return false;
                    }

                    if (bucket != bucketname) {
                        continue;
                    }
                }

                return true;
            }
        }

        // Avoid busy-loop by backing off for 500 µsec
        usleep(500);
    } while (time(nullptr) < timeout);

    return false;
}

/**
 * Validate that a rejected illegal packet is audit logged.
 */
TEST_P(AuditTest, AuditIllegalPacket) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    uint64_t value = 0xdeadbeefdeadcafe;
    const std::string key("AuditTest::AuditIllegalPacket");
    size_t len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                                      PROTOCOL_BINARY_CMD_SET,
                                      key.c_str(), key.size(),
                                      &value, sizeof(value),
                                      0, 0);

    // Now make packet illegal. The validator for SET requires an extlen of
    // 8 bytes.. let's just include them in the key.
    auto& request = send.request.message.header.request;
    ASSERT_EQ(8, request.getExtlen());
    request.setKeylen(uint16_t(key.size() + 8));
    request.setExtlen(uint8_t(0));
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SET,
                                  cb::mcbp::Status::Einval);

    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_INVALID_PACKET));
}

/**
 * Validate that we log when we reconfigure
 */
TEST_P(AuditTest, AuditStartedStopped) {
    ASSERT_TRUE(searchAuditLogForID(AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON));
}

/**
 * Validate that a failed SASL auth is audit logged.
 */
TEST_P(AuditTest, AuditFailedAuth) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    // use a plain auth with an unknown user, easy failure to force.
    const char* chosenmech = "PLAIN";
    const char* data = "\0nouser\0nopassword";

    size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                   PROTOCOL_BINARY_CMD_SASL_AUTH,
                                   chosenmech, strlen(chosenmech),
                                   data, sizeof(data));

    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_SASL_AUTH,
                                  cb::mcbp::Status::AuthError);

    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                                    "nouser"));
}

TEST_P(AuditTest, AuditX509SuccessfulAuth) {
    reconfigure_client_cert_auth("enable", "subject.cn", "", " ");
    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);

    // The certificate will be accepted, so the connection is established
    // but the server will disconnect the client immediately
    connection.connect();

    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED,
                                    "Trond"));
}

TEST_P(AuditTest, AuditX509FailedAuth) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "Tr", "");
    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);

    // The certificate will be accepted, so the connection is established
    // but the server will disconnect the client immediately
    connection.connect();
    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                                    "unknown"));
}

TEST_P(AuditTest, AuditSelectBucket) {
    auto& conn = getAdminConnection();
    conn.createBucket("bucket-1", "", BucketType::Memcached);
    conn.selectBucket("bucket-1");

    ASSERT_TRUE(searchAuditLogForID(
            MEMCACHED_AUDIT_SELECT_BUCKET, "@admin", "bucket-1"));
}
