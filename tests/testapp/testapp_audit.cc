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

#include "testapp_client_test.h"
#include "memcached_audit_events.h"
#include "auditd/auditd_audit_events.h"

#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

#include <protocol/connection/frameinfo.h>
#include <cctype>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

class AuditTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();

        // Create a copy of the audit events file so that we can modify
        // the events
        auto& json = mcd_env->getAuditConfig();
        descriptor_file = cb::io::mktemp("audit_events.json");
        org_descriptor_file = json["descriptors_path"].get<std::string>();

        auto content =
                cb::io::loadFile(org_descriptor_file + "/audit_events.json");
        std::ofstream copy(descriptor_file, std::ios::binary);
        copy.write(content.data(), content.size());
        copy.close();
        json["descriptors_path"] = descriptor_file;

        reconfigure_client_cert_auth("disable", "", "", "");
        auto& logdir = mcd_env->getAuditLogDir();
        EXPECT_NO_THROW(cb::io::rmrf(logdir));
        cb::io::mkdirp(logdir);
        setEnabled(true);
    }

    void TearDown() override {
        reconfigure_client_cert_auth("disable", "", "", "");
        auto& json = mcd_env->getAuditConfig();
        json["descriptors_path"] = org_descriptor_file;
        setEnabled(false);
        auto& logdir = mcd_env->getAuditLogDir();
        EXPECT_NO_THROW(cb::io::rmrf(mcd_env->getAuditLogDir()));
        cb::io::mkdirp(logdir);
        cb::io::rmrf(descriptor_file);
        TestappClientTest::TearDown();
    }

    void setEnabled(bool mode) {
        auto& json = mcd_env->getAuditConfig();
        json["auditd_enabled"] = mode;
        json["filtering_enabled"] = true;
        json["disabled_userids"][0] = {
                {"domain", to_string(cb::rbac::Domain::Local)},
                {"user", "MB33603"}};

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

    std::vector<nlohmann::json> readAuditData();

    std::vector<nlohmann::json> splitJsonData(const std::string& input);

    bool searchAuditLogForID(int id,
                             const std::string& username = "",
                             const std::string& bucketname = "");

    /**
     * Iterate over all of the entries found in the log file(s) over and
     * over until the callback method returns false.
     *
     * @param callback the callback containing the audit event
     */
    void iterate(const std::function<bool(const nlohmann::json&)>& callback);

protected:
    std::string descriptor_file;
    std::string org_descriptor_file;
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         AuditTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

std::vector<nlohmann::json> AuditTest::splitJsonData(const std::string& input) {
    std::vector<nlohmann::json> rval;
    std::istringstream content(input);
    while (content.good()) {
        std::string line;
        std::getline(content, line);
        while (!line.empty() && std::isspace(line.back())) {
            line.pop_back();
        }
        if (!line.empty()) {
            try {
                rval.emplace_back(nlohmann::json::parse(line));
            } catch (const nlohmann::json::exception&) {
                // Stop parsing this file
                if (!content.eof()) {
                    throw std::runtime_error(
                            "splitJsonData: Invalid last entry");
                }
                break;
            }
        }
    }
    return rval;
}

std::vector<nlohmann::json> AuditTest::readAuditData() {
    std::vector<nlohmann::json> rval;
    const auto files =
            cb::io::findFilesContaining(mcd_env->getAuditLogDir(), "audit.log");
    for (const auto& file : files) {
        auto entries = splitJsonData(cb::io::loadFile(file));
        std::move(entries.begin(), entries.end(), std::back_inserter(rval));
    }
    return rval;
}

void AuditTest::iterate(
        const std::function<bool(const nlohmann::json&)>& callback) {
    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(5);

    do {
        const auto auditEntries = readAuditData();
        for (auto& entry : auditEntries) {
            if (callback(entry)) {
                // We're done
                return;
                }
        }
        // Avoid busy-loop by backing off
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < timeout);

    FAIL() << "Timed out waiting for audit event";
}

bool AuditTest::searchAuditLogForID(int id,
                                    const std::string& username,
                                    const std::string& bucketname) {
    bool ret = false;
    iterate([&ret, id, username, bucketname](const nlohmann::json& entry) {
        if (entry["id"].get<int>() != id) {
            return false;
        }

        // This the type we're searching for..
        std::string user;
        std::string bucket;

        auto iter = entry.find("bucket");
        if (iter != entry.end()) {
            bucket.assign(iter->get<std::string>());
        }

        iter = entry.find("real_userid");
        if (iter != entry.end()) {
            auto u = iter->find("user");
            if (u != iter->end()) {
                user.assign(u->get<std::string>());
            }
        }

        if (!username.empty()) {
            if (user.empty()) {
                // The entry did not contain a username!
                ret = false;
                return true;
            }

            if (user != username) {
                // We found another user (needed to test authentication
                // success ;)
                return false;
            }
        }

        if (!bucketname.empty()) {
            if (bucket.empty()) {
                // This entry did not contain a bucket entry
                ret = false;
                return true;
            }

            if (bucket != bucketname) {
                return false;
            }
        }

        ret = true;
        return true;
    });

    return ret;
}

/**
 * Add a unit test to verify that we're able to successfully detect any
 * garbled entries at the end of the file
 */
TEST_P(AuditTest, splitJsonData) {
    // We should accept windows style new line
    EXPECT_EQ(4, splitJsonData("{}\r\n{}\r\n{}\r\n{}\r\n").size());
    // And unix style
    EXPECT_EQ(4, splitJsonData("{}\n{}\n{}\n{}\n").size());
    // We should be able to parse the data if it doesn't include a newline
    // at the end or leading / trailing while space and empty lines
    EXPECT_EQ(4, splitJsonData("{}\n{}\n {} \n\r\n{}").size());
    // We should allow (and ignore) a garbled entry at the end
    EXPECT_EQ(4, splitJsonData("{}\n{}\n {} \n\r\n{}\n{\"foo\"").size());
    // We should fail for garbled entries in the middle of the file
    EXPECT_THROW(splitJsonData("{\"Foo: false}\n{}"), std::runtime_error);
}

/**
 * Validate that a rejected illegal packet is audit logged.
 */
TEST_P(AuditTest, AuditIllegalPacket) {
    auto& conn = getConnection();
    // A set command should have 8 bytes of extra;
    auto rsp = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Set, "AuditTest::AuditIllegalPacket"});
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_INVALID_PACKET));
}

/**
 * Validate that a rejected illegal packet is audit logged.
 */
TEST_P(AuditTest, AuditIllegalFrame_MB31071) {
    std::vector<uint8_t> blob(300);
    std::fill(blob.begin(), blob.end(), 'a');

    safe_send(blob.data(), blob.size(), false);

    // This should terminate the conenction
    EXPECT_EQ(0, phase_recv(blob.data(), blob.size()));
    reconnect_to_server();

    // Wait up to 5 secs for the entry to appear
    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        for (auto& entry : readAuditData()) {
            auto iter = entry.find("id");
            ASSERT_NE(entry.end(), iter);
            if (iter->get<int>() == MEMCACHED_AUDIT_INVALID_PACKET) {
                // Ok, this is the entry types i want... is this the one with
                // the blob of 300 'a'?
                // The audit daemon dumps the first 256 bytes and tells us
                // the number it truncated. For simplicity we'll just search
                // for that...
                auto str = entry.dump();
                if (str.find(" [truncated 44 bytes]") != std::string::npos) {
                    return;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    } while (std::chrono::steady_clock::now() < timeout);
    FAIL() << "TImed out waiting for log entry to appear";
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
    BinprotSaslAuthCommand cmd;
    cmd.setChallenge({"\0nouser\0nopassword", 18});
    cmd.setMechanism("PLAIN");

    auto rsp = getConnection().execute(cmd);
    EXPECT_EQ(cb::mcbp::ClientOpcode::SaslAuth, rsp.getOp());
    EXPECT_EQ(cb::mcbp::Status::AuthError, rsp.getStatus());
    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                                    "nouser"));
}

TEST_P(AuditTest, AuditX509SuccessfulAuth) {
    reconfigure_client_cert_auth("enable", "subject.cn", "", " ");
    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);

    connection.connect();
    connection.listBuckets();

    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED,
                                    "Trond"));
}

TEST_P(AuditTest, AuditX509FailedAuth) {
    reconfigure_client_cert_auth("mandatory", "subject.cn", "Tr", "");
    MemcachedConnection connection("127.0.0.1", ssl_port, AF_INET, true);
    setClientCertData(connection);

    connection.connect();
    try {
        connection.listBuckets();
    } catch (const std::exception&) {
        // Ignore the exception as all we want to now is that we got the
        // authentication failed audit event
    }

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

TEST_P(AuditTest, AuditConfigReload) {
    auto& conn = getAdminConnection();
    auto rsp = conn.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::ConfigReload});
    EXPECT_TRUE(rsp.isSuccess());
}

TEST_P(AuditTest, AuditPut) {
    auto& conn = getAdminConnection();
    auto rsp = conn.execute(BinprotAuditPutCommand{0, R"({})"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getDataString();
}

/// Filtering failed to work for memcached generated events as the domain
/// for these events was hardcoded to "memcached"
TEST_P(AuditTest, MB33603_ValidDomainName) {
    auto& conn = getConnection();
    try {
        conn.authenticate("bubba", "invalid", "PLAIN");
        FAIL() << "Authentication should fail";
    } catch (const ConnectionError& error) {
        ASSERT_TRUE(error.isAuthError());
    }

    std::string domain;
    iterate([&domain](const nlohmann::json& entry) {
        if (entry["id"].get<int>() != MEMCACHED_AUDIT_AUTHENTICATION_FAILED) {
            return false;
        }

        if (entry["real_userid"]["user"] != "bubba") {
            return false;
        }

        domain = entry["real_userid"]["domain"].get<std::string>();
        return true;
    });

    EXPECT_EQ(to_string(cb::rbac::Domain::Local), domain);
}

TEST_P(AuditTest, MB33603_Filtering) {
    auto json = nlohmann::json::parse(cb::io::loadFile(descriptor_file));

    for (auto& module : json["modules"]) {
        for (auto& entry : module["events"]) {
            if (entry["id"].get<int>() ==
                MEMCACHED_AUDIT_AUTHENTICATION_FAILED) {
                entry["filtering_permitted"] = true;
            }
        }
    }

    auto content = json.dump(2);
    std::ofstream copy(descriptor_file, std::ios::binary);
    copy.write(content.data(), content.size());
    copy.close();
    setEnabled(true);

    auto& conn = getConnection();
    try {
        conn.authenticate("MB33603", "invalid", "PLAIN");
        FAIL() << "Authentication should fail";
    } catch (const ConnectionError& error) {
        ASSERT_TRUE(error.isAuthError());
    }

    // Perform a second invalid login (with a different username)
    // so that we know when we can stop looking at the audit trail and
    // verify that we haven't generated an entry for the user.

    try {
        conn.authenticate("MB33603_1", "invalid", "PLAIN");
        FAIL() << "Authentication should fail";
    } catch (const ConnectionError& error) {
        ASSERT_TRUE(error.isAuthError());
    }

    bool found = false;
    iterate([&found](const nlohmann::json& entry) {
        if (entry["id"].get<int>() != MEMCACHED_AUDIT_AUTHENTICATION_FAILED) {
            return false;
        }

        if (entry["real_userid"]["user"] == "MB33603") {
            found = true;
            return true;
        }

        return entry["real_userid"]["user"] == "MB33603_1";
    });

    EXPECT_FALSE(found)
            << "Filtering out memcached generated events don't work";
}

TEST_P(AuditTest, MB3750_AuditImpersonatedUser) {
    auto& conn = getAdminConnection();
    conn.selectBucket("default");

    // We should be allowed to fetch a document (should return enoent
    try {
        conn.get("MB3750_AuditImpersonatedUser", Vbid{0});
        FAIL() << "Document should not be here";
    } catch (const ConnectionError& error) {
        ASSERT_TRUE(error.isNotFound()) << "Document should not be there";
    }

    // Smith does not have access to the default bucket so trying to fetch
    // the document should fail with an access violation (and not that the
    // document isn't found).
    try {
        conn.get(
                "MB3750_AuditImpersonatedUser",
                Vbid{0},
                []() -> FrameInfoVector {
                    FrameInfoVector ret;
                    ret.emplace_back(std::make_unique<ImpersonateUserFrameInfo>(
                            "smith"));
                    return ret;
                });
        FAIL() << "Document should not be here";
    } catch (const ConnectionError& error) {
        ASSERT_TRUE(error.isAccessDenied())
                << "smith have no access to to default: " << error.what();
    }

    // Verify that the audit trail contains the effective user
    std::string user;
    std::string domain;
    iterate([&user, &domain](const nlohmann::json& entry) {
        if (entry["id"].get<int>() != MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE) {
            return false;
        }

        if (entry.find("effective_userid") == entry.end()) {
            return false;
        }

        user = entry["effective_userid"]["user"];
        domain = entry["effective_userid"]["domain"].get<std::string>();
        return true;
    });

    EXPECT_EQ("smith", user);
    EXPECT_EQ(to_string(cb::rbac::Domain::Local), domain);
}
