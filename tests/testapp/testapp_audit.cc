/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Tests that check certain events make it into the audit log.
 */

#include "testapp_audit.h"

#include "testapp_client_test.h"
#include <auditd/couchbase_audit_events.h>
#include <mcbp/codec/frameinfo.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <cctype>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

void AuditTest::SetUp() {
    TestappClientTest::SetUp();
    reconfigure_client_cert_auth("disabled", "", "", "");
    auto logdir = mcd_env->getAuditLogDir();
    std::filesystem::remove_all(logdir);
    std::filesystem::create_directories(logdir);
    setEnabled(true);
}

void AuditTest::TearDown() {
    reconfigure_client_cert_auth("disabled", "", "", "");
    setEnabled(false);
    std::filesystem::create_directories(mcd_env->getAuditLogDir());
    TestappClientTest::TearDown();
}

void AuditTest::setEnabled(bool mode) {
    auto& json = mcd_env->getAuditConfig();
    json["auditd_enabled"] = mode;
    json["filtering_enabled"] = true;
    json["disabled_userids"][0] = {{"domain", cb::rbac::Domain::Local},
                                   {"user", "MB33603"}};
    json["disabled_userids"][1] = {{"domain", cb::rbac::Domain::Local},
                                   {"user", "Jane"}};

    json["event_states"][std::to_string(MEMCACHED_AUDIT_DOCUMENT_READ)] =
            "enabled";
    json["event_states"][std::to_string(MEMCACHED_AUDIT_DOCUMENT_MODIFY)] =
            "enabled";
    json["event_states"][std::to_string(MEMCACHED_AUDIT_DOCUMENT_DELETE)] =
            "enabled";

    reconfigureAudit();
}

void AuditTest::reconfigureAudit() {
    try {
        mcd_env->rewriteAuditConfig();
    } catch (std::exception& e) {
        FAIL() << "Failed to toggle audit state: " << e.what();
    }
    adminConnection->reloadAuditConfiguration();
}

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         AuditTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

std::vector<nlohmann::json> AuditTest::readAuditData() {
    std::vector<nlohmann::json> rval;
    mcd_env->iterateAuditEvents([&rval](auto entry) {
        rval.emplace_back(entry);
        return false;
    });
    return rval;
}

void AuditTest::iterate(
        const std::function<bool(const nlohmann::json&)>& callback) {
    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(5);

    do {
        if (mcd_env->iterateAuditEvents(callback)) {
            return;
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
        if (entry.value("id", -1) != id) {
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

int AuditTest::getAuditCount(const std::vector<nlohmann::json>& entries,
                             int id) {
    return std::count_if(
            entries.begin(), entries.end(), [id](const auto& entry) {
                return entry.at("id").template get<int>() == id;
            });
}

/**
 * Validate the audit configured event
 */
TEST_P(AuditTest, AuditConfigured) {
    bool found = false;
    iterate([&found](const nlohmann::json& entry) -> bool {
        if (entry.value("id", 0) == AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON) {
            EXPECT_TRUE(entry["auditd_enabled"].get<bool>());
            // this is the entry I want
            EXPECT_EQ(2, entry["version"].get<int>());
            EXPECT_EQ("this_is_the_uuid", entry["uuid"].get<std::string>());
            EXPECT_EQ("local",
                      entry["real_userid"]["domain"].get<std::string>());
            EXPECT_EQ("@memcached",
                      entry["real_userid"]["user"].get<std::string>());
            found = true;
            return true;
        }
        return false;
    });

    EXPECT_TRUE(found) << "Timed out waiting for log entry to appear";
    found = false;
    setEnabled(false);
    iterate([&found](const nlohmann::json& entry) -> bool {
        if (entry.value("id", 0) == AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON) {
            // There should be one entry for it to be started, and one
            // for it to be shut down.
            if (!entry["auditd_enabled"].get<bool>()) {
                // this is the entry I want
                EXPECT_EQ(2, entry["version"].get<int>());
                EXPECT_EQ("this_is_the_uuid", entry["uuid"].get<std::string>());
                found = true;
                return true;
            }
        }
        return false;
    });

    EXPECT_TRUE(found) << "Timed out waiting for log entry to appear";
}

/**
 * Validate that we only create the audit log file in the situation
 * where we go from disabled to enabled. Configure is synchronous
 * so it is easy to test
 */
TEST_P(AuditTest, ValidateAuditLogFileCreated) {
    std::filesystem::path log_dir{mcd_env->getAuditLogDir()};
    auto log = log_dir / "current-audit.log";
    // Audit should be enabled so the file should be there
    EXPECT_TRUE(exists(log));
    EXPECT_TRUE(is_symlink(log));
    setEnabled(false);
    remove(log);

    // Reconfigure going from disabled to disabled should not cause
    // the audit file to be created
    setEnabled(false);
    EXPECT_FALSE(exists(log));

    // But if we start it the file should be created
    setEnabled(true);
    EXPECT_TRUE(exists(log));
}

/**
 * Validate that a rejected illegal packet is audit logged.
 */
TEST_P(AuditTest, AuditIllegalPacket) {
    auto& conn = getConnection();
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);

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

    auto socket = getConnection().releaseSocket();
    EXPECT_EQ(blob.size(), cb::net::send(socket, blob.data(), blob.size(), 0));

    // This should terminate the conenction
    EXPECT_EQ(0, cb::net::recv(socket, blob.data(), blob.size(), 0));
    cb::net::closesocket(socket);

    bool found = false;
    iterate([&found](const nlohmann::json& entry) -> bool {
        auto iter = entry.find("id");
        if (iter != entry.cend() &&
            iter->get<int>() == MEMCACHED_AUDIT_INVALID_PACKET) {
            // Ok, this is the entry types i want... is this the one with
            // the blob of 300 'a'?
            // The audit daemon dumps the first 256 bytes and tells us
            // the number it truncated. For simplicity we'll just search
            // for that...
            auto str = entry.dump();
            if (str.find(" [truncated 44 bytes]") != std::string::npos) {
                found = true;
                return true;
            }
        }
        return false;
    });

    EXPECT_TRUE(found) << "Timed out waiting for log entry to appear";
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
    reconfigure_client_cert_auth("enabled", "subject.cn", "", " ");
    std::unique_ptr<MemcachedConnection> conn;
    connectionMap.iterate([&conn](const MemcachedConnection& c) {
        if (!conn && c.isSsl()) {
            auto family = c.getFamily();
            conn = std::make_unique<MemcachedConnection>(
                    family == AF_INET ? "127.0.0.1" : "::1",
                    c.getPort(),
                    family,
                    true);
        }
    });

    ASSERT_TRUE(conn) << "Failed to locate a SSL port";
    setClientCertData(*conn, "trond");
    conn->connect();
    conn->listBuckets();

    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED,
                                    "Trond"));
}

TEST_P(AuditTest, AuditSelectBucket) {
    mcd_env->getTestBucket().createBucket("bucket-1", {}, *adminConnection);
    adminConnection->executeInBucket("bucket-1", [this](auto&) {
        ASSERT_TRUE(searchAuditLogForID(
                MEMCACHED_AUDIT_SELECT_BUCKET, "@admin", "bucket-1"));
    });
    adminConnection->deleteBucket("bucket-1");
}

// Each subdoc single-lookup should log one (and only one) DOCUMENT_READ
// event.
TEST_P(AuditTest, AuditSubdocLookup) {
    auto& conn = getConnection();
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);
    conn.store("doc", Vbid(0), "{\"foo\": 1}");
    BinprotSubdocCommand cmd(
            cb::mcbp::ClientOpcode::SubdocGet, "doc", "foo", "", {});
    ASSERT_EQ(cb::mcbp::Status::Success, conn.execute(cmd).getStatus());

    // Cleanup document; this also gives us a sentinel audit event to know that
    // all previous document audit events we are interested in have been
    // writen to disk.
    conn.remove("doc", Vbid(0));
    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_DOCUMENT_DELETE));

    // Should have one DOCUMENT_MODIFY (from initial create) and one
    // DOCUMENT_READ event.
    const auto entries = readAuditData();
    EXPECT_EQ(1, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_READ));
    EXPECT_EQ(1, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_MODIFY));
}

// Each subdoc single-mutation should log one (and only one) DOCUMENT_MODIFY
// event.
TEST_P(AuditTest, AuditSubdocMutation) {
    auto& conn = getConnection();
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);
    BinprotSubdocCommand cmd(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             "doc",
                             "foo",
                             "\"bar\"",
                             {},
                             cb::mcbp::subdoc::DocFlag::Mkdoc);
    ASSERT_EQ(cb::mcbp::Status::Success, conn.execute(cmd).getStatus());

    // Cleanup document; this also gives us a sentinel audit event to know that
    // all previous document audit events we are interested in have been
    // writen to disk.
    conn.remove("doc", Vbid(0));
    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_DOCUMENT_DELETE));

    // Should have one DOCUMENT_MODIFY event and no DOCUMENT_READ events.
    const auto entries = readAuditData();
    EXPECT_EQ(1, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_MODIFY));
    EXPECT_EQ(0, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_READ));
}

// Each subdoc multi-mutation should log one (and only one) DOCUMENT_MODIFY
// event.
TEST_P(AuditTest, AuditSubdocMultiMutation) {
    auto& conn = getConnection();
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);
    BinprotSubdocMultiMutationCommand cmd(
            "doc",
            {{cb::mcbp::ClientOpcode::SubdocDictUpsert, {}, "foo", "\"bar\""},
             {cb::mcbp::ClientOpcode::SubdocDictUpsert, {}, "foo2", "\"baz\""}},
            cb::mcbp::subdoc::DocFlag::Mkdoc);

    ASSERT_EQ(cb::mcbp::Status::Success, conn.execute(cmd).getStatus());

    // Cleanup document; this also gives us a sentinel audit event to know that
    // all previous document audit events we are interested in have been
    // writen to disk.
    conn.remove("doc", Vbid(0));
    ASSERT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_DOCUMENT_DELETE));

    // Should have one DOCUMENT_MODIFY event and no DOCUMENT_READ events.
    const auto entries = readAuditData();
    EXPECT_EQ(1, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_MODIFY));
    EXPECT_EQ(0, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_READ));
}

// Check that a delete via subdoc is audited correctly.
TEST_P(AuditTest, AuditSubdocMultiMutationDelete) {
    auto& conn = getConnection();
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);
    conn.store("doc", Vbid(0), "foo");

    BinprotSubdocMultiMutationCommand cmd(
            "doc",
            {{cb::mcbp::ClientOpcode::Delete, {}, {}, {}}},
            cb::mcbp::subdoc::DocFlag::None);

    ASSERT_EQ(cb::mcbp::Status::Success, conn.execute(cmd).getStatus());

    // Should have one DOCUMENT_MODIFY (when initial document was stored) and
    // one DOCUMENT_DELETE event.
    // Delete is last, so search / wait for that; then check entire contents.
    EXPECT_TRUE(searchAuditLogForID(MEMCACHED_AUDIT_DOCUMENT_DELETE));
    const auto entries = readAuditData();
    EXPECT_EQ(0, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_READ));
    EXPECT_EQ(1, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_MODIFY));
    EXPECT_EQ(1, getAuditCount(entries, MEMCACHED_AUDIT_DOCUMENT_DELETE));
}

TEST_P(AuditTest, AuditConfigReload) {
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::ConfigReload});
    EXPECT_TRUE(rsp.isSuccess());
}

TEST_P(AuditTest, AuditPut) {
    const auto event = nlohmann::json{
            {"real_userid", {{"domain", "external"}, {"user", "joe"}}},
            {"bucket", "foo"},
            {"timestamp", "2022-11-01T05:15:25.277211+01:00"},
            {"local", {{"ip", "::1"}, {"port", 1}}},
            {"remote", {{"ip", "::1"}, {"port", 1}}}};
    auto rsp = adminConnection->execute(BinprotAuditPutCommand{
            MEMCACHED_AUDIT_INVALID_PACKET, event.dump()});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getDataView();
}

#ifdef CB_DEVELOPMENT_ASSERTS
TEST_P(AuditTest, AuditPutMissingMandatoryField) {
    auto rsp = adminConnection->execute(BinprotAuditPutCommand{
            MEMCACHED_AUDIT_INVALID_PACKET,
            R"({"real_userid":{"domain":"external","user":"Joe"}})"});
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    auto json = rsp.getDataJson();
    EXPECT_EQ("Audit event is missing elements specified as mandatory",
              json["error"]["context"].get<std::string>());
    EXPECT_EQ(R"(["bucket","local","remote","timestamp"])",
              json["missing_elements"].dump());
}
#endif

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

    EXPECT_EQ(format_as(cb::rbac::Domain::Local), domain);
}

/// Verify that we honor filtering. We should filter out all events for Jane,
/// but all Lukes commands should be audited.
TEST_P(AuditTest, MB33603_Filtering) {
    Document doc;
    doc.info.id = "MB33603_Filtering";
    doc.value = "blah blah";

    auto jane = userConnection->clone();
    jane->authenticate("Jane");
    jane->selectBucket("default");
    // That should not generate an audit event
    jane->mutate(doc, Vbid{0}, MutationType::Set);

    // redo the mutation and verify that
    userConnection->mutate(doc, Vbid{0}, MutationType::Set);

    bool found = false;
    iterate([&found, &doc](const nlohmann::json& entry) {
        if (entry["id"].get<int>() != MEMCACHED_AUDIT_DOCUMENT_MODIFY) {
            return false;
        }

        if (entry["key"].get<std::string>() != doc.info.id) {
            return false;
        }

        EXPECT_NE("Jane", entry["real_userid"]["user"].get<std::string>())
                << "Jane should not be audited";

        // The entry should be from Luke
        EXPECT_EQ("Luke", entry["real_userid"]["user"].get<std::string>());
        return true;
    });

    EXPECT_FALSE(found)
            << "Filtering out memcached generated events don't work";
}

TEST_P(AuditTest, MB3750_AuditImpersonatedUser) {
    auto& conn = getAdminConnection();
    conn.selectBucket(bucketName);

    // We should be allowed to fetch a document (should return enoent)
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
                    ret.emplace_back(
                            std::make_unique<cb::mcbp::request::
                                                     ImpersonateUserFrameInfo>(
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
    EXPECT_EQ(format_as(cb::rbac::Domain::Local), domain);
}

TEST_P(AuditTest, MB41183_UnifiedConnectionDescription) {
    BinprotSaslAuthCommand cmd;
    cmd.setChallenge({"\0MB41183\0nopassword", 18});
    cmd.setMechanism("PLAIN");

    auto rsp = getConnection().execute(cmd);
    EXPECT_EQ(cb::mcbp::ClientOpcode::SaslAuth, rsp.getOp());
    EXPECT_EQ(cb::mcbp::Status::AuthError, rsp.getStatus());

    iterate([](const nlohmann::json& entry) {
        if (entry.find("peername") != entry.cend() ||
            entry.find("sockname") != entry.cend()) {
            throw std::runtime_error(
                    "FAIL: peername or sockname should not be present: " +
                    entry.dump());
        }

        if (entry["id"].get<int>() != MEMCACHED_AUDIT_AUTHENTICATION_FAILED) {
            return false;
        }

        // THe following piece of code will throw exceptions if they don't
        // exists or is of wrong type
        entry["remote"]["ip"].get<std::string>();
        entry["remote"]["port"].get<int>();
        entry["local"]["ip"].get<std::string>();
        entry["local"]["port"].get<int>();
        return true;
    });
}

/// Verify that the audit of the document key don't include the collection
/// identifiers
TEST_P(AuditTest, MB51863) {
    auto& conn = getConnection();

    conn.authenticate("Luke");
    conn.selectBucket(bucketName);
    std::vector<cb::mcbp::Feature> features = {
            {cb::mcbp::Feature::MUTATION_SEQNO,
             cb::mcbp::Feature::XATTR,
             cb::mcbp::Feature::XERROR,
             cb::mcbp::Feature::SELECT_BUCKET,
             cb::mcbp::Feature::Collections,
             cb::mcbp::Feature::SubdocReplaceBodyWithXattr}};
    conn.setFeatures(features);
    std::string key;
    key.push_back('\0');
    key.append("MB51863");
    conn.store(key, Vbid(0), "foo");

    nlohmann::json document;
    iterate([&document](const nlohmann::json& entry) -> bool {
        if (entry["id"].get<int>() == MEMCACHED_AUDIT_DOCUMENT_MODIFY &&
            entry["key"].get<std::string>().find("MB51863") !=
                    std::string::npos) {
            document = entry;
            return true;
        }
        return false;
    });

    ASSERT_EQ("MB51863", document["key"].get<std::string>());
    ASSERT_EQ("0x0", document["collection_id"].get<std::string>());
}

#ifdef WIN32
#define AuditDroppedTest DISABLED_AuditDroppedTest
#endif
TEST_P(AuditTest, AuditDroppedTest) {
    auto orgLogDir = mcd_env->getAuditLogDir();
    setEnabled(true);

    auto stats = getAdminConnection().stats("audit");
    // Get the current count for dropped events:
    const auto org_dropped = stats["dropped_events"].get<size_t>();

    auto& json = mcd_env->getAuditConfig();
    // Set the audit log to a path which cannot be created
    // due to access permissions (not just the file but
    // missing path elements in the path which needs to be
    // created which we won't have access to create).
    json["log_path"] = "/AuditTest/auditlog/myaudit";
    try {
        mcd_env->rewriteAuditConfig();
    } catch (std::exception& e) {
        FAIL() << "Failed to toggle audit state: " << e.what();
    }

    getAdminConnection().reloadAuditConfiguration();

    stats = getAdminConnection().stats("audit");
    while (!stats["enabled"].get<bool>()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        stats = getAdminConnection().stats("audit");
    }

    EXPECT_LT(org_dropped, stats["dropped_events"].get<size_t>());

    // Rewrite the config back to the original one
    json["log_path"] = orgLogDir;
    setEnabled(true);
}

/// Verify that memcached don't generate audit events for disabled
/// audit events
TEST_P(AuditTest, MB54426) {
    auto& json = mcd_env->getAuditConfig();
    json["event_states"][std::to_string(MEMCACHED_AUDIT_SESSION_TERMINATED)] =
            "disabled";
    reconfigureAudit();

    // Recreate the user connection; this should generate a Session terminated
    // event if the event is enabled (which is it not)
    rebuildUserConnection(false);

    // Enable the event
    json["event_states"][std::to_string(MEMCACHED_AUDIT_SESSION_TERMINATED)] =
            "enabled";
    reconfigureAudit();

    // Rebuild admin connection which should generate another session terminated
    // event (ut we may differentiate the disconnect with the real userid)
    rebuildAdminConnection();

    iterate([](const nlohmann::json& entry) -> bool {
        if (entry.value("id", 0) == MEMCACHED_AUDIT_SESSION_TERMINATED) {
            EXPECT_EQ("@admin", entry["real_userid"].value("user", "foo"));
            return true;
        }

        return false;
    });
}
