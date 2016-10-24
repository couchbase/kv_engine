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
    void SetUp() {
        TestappClientTest::SetUp();
        auto& logdir = mcd_env->getAuditLogDir();
        ASSERT_TRUE(cb::io::rmrf(logdir));
        cb::io::mkdirp(logdir);
        setEnabled(true);
    }

    void TearDown() {
        setEnabled(false);
        auto& logdir = mcd_env->getAuditLogDir();
        EXPECT_TRUE(cb::io::rmrf(mcd_env->getAuditLogDir()));
        cb::io::mkdirp(logdir);
        TestappClientTest::TearDown();
    }

    void setEnabled(bool mode) {
        auto* json = mcd_env->getAuditConfig();
        try {
            cJSON_ReplaceItemInObject(json, "auditd_enabled",
                                      mode ? cJSON_CreateTrue() :
                                             cJSON_CreateFalse());
            mcd_env->rewriteAuditConfig();
        } catch (std::exception& e) {
            FAIL() << "Failed to toggle audit state: " << e.what();
        }

        auto& connection = getConnection();
        connection.authenticate("_admin", "password", "PLAIN");
        connection.reloadAuditConfiguration();
        connection.reconnect();
    }

    std::vector<unique_cJSON_ptr> readAuditData();

    bool searchAuditLogForID(int id, const std::string& username = "");
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

bool AuditTest::searchAuditLogForID(int id, const std::string& username) {
    // @todo loop up to 5 sec trying to get it..
    auto timeout = time(NULL) + 5;

    do {
        auto auditEntries = readAuditData();
        for (auto& entry : auditEntries) {
            cJSON* idEntry = cJSON_GetObjectItem(entry.get(), "id");
            if (idEntry && idEntry->valueint == id) {
                if (!username.empty()) {
                    auto* ue = cJSON_GetObjectItem(entry.get(), "real_userid");
                    if (ue == nullptr) {
                        return false;
                    }
                    auto* user = cJSON_GetObjectItem(ue, "user");
                    return (user != nullptr && username == user->valuestring);
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
    std::string key("AuditTest::AuditIllegalPacket");
    size_t len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                                      PROTOCOL_BINARY_CMD_SET,
                                      key.c_str(), key.size(),
                                      &value, sizeof(value),
                                      0, 0);

    // Now make packet illegal.
    auto diff = ntohl(send.request.message.header.request.bodylen) - 1;
    send.request.message.header.request.bodylen = htonl(1);
    safe_send(send.bytes, len - diff, false);

    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);

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
    mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_SASL_AUTH,
                                  PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);

    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                                    "nouser"));
}
