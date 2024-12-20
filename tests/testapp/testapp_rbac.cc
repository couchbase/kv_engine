/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * This file contains tests related to RBAC
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <algorithm>

class RbacTest : public TestappClientTest {};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         RbacTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

/**
 * Memcached supports authentication through external systems (like LDAP),
 * but these users may not be defined as a user within Couchbase. Such
 * users should fail with an Access Denied error instead of Authentication
 * Success even if the username password combination is correct.
 */
TEST_P(RbacTest, DontAllowUnknownUsers) {
    auto& conn = getConnection();
    try {
        conn.authenticate("sharon", "sharonpw");
        FAIL() << "Users without an RBAC profile should not be allowed access";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAuthError()) << error.what();
    }
}

TEST_P(RbacTest, ReloadRbacData_HaveAccess) {
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::RbacRefresh});
    EXPECT_TRUE(rsp.isSuccess());
}

TEST_P(RbacTest, ReloadRbacData_NoAccess) {
    auto& conn = getConnection();
    conn.reconnect();
    conn.setXerrorSupport(true);
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::RbacRefresh, {}, {});
    EXPECT_EQ(cb::mcbp::Status::Eaccess, conn.execute(cmd).getStatus());
}

TEST_P(RbacTest, ReloadSasl_HaveAccess) {
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IsaslRefresh});
    EXPECT_TRUE(rsp.isSuccess());
}

TEST_P(RbacTest, ReloadSasl_NoAccess) {
    auto& conn = getConnection();
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::IsaslRefresh);
    EXPECT_EQ(cb::mcbp::Status::Eaccess, conn.execute(cmd).getStatus());
}

TEST_P(RbacTest, DropPrivilege) {
    auto& c = getAdminConnection();
    c.selectBucket(bucketName);
    c.stats("");
    c.dropPrivilege(cb::rbac::Privilege::SimpleStats);
    try {
        c.stats("");
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(RbacTest, MB23909_ErrorIncudingErrorInfo) {
    const auto resp = userConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::RbacRefresh});
    ASSERT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());
    auto json = resp.getDataJson();

    ASSERT_TRUE(json["error"].is_object());
    ASSERT_TRUE(json["error"]["context"].is_string());
    auto ref = json["error"]["ref"].get<std::string>();

    // @todo I could parse the UUID to see that it is actually an UUID,
    //       but for now I just trust it to be a UUID and not something
    //       else (just add a check for the length of an UUID)
    EXPECT_EQ(36, ref.size());

    const std::string expected{
            "Authorization failure: can't execute RBAC_REFRESH operation "
            "without the NodeSupervisor privilege"};
    EXPECT_EQ(expected, json["error"]["context"].get<std::string>());
}

TEST_P(RbacTest, RangeScanCreate) {
    auto& admin = getAdminConnection();
    admin.setAutoRetryTmpfail(true);
    admin.dropPrivilege(cb::rbac::Privilege::RangeScan);
    admin.setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                       cb::mcbp::Feature::XATTR,
                       cb::mcbp::Feature::XERROR,
                       cb::mcbp::Feature::SELECT_BUCKET,
                       cb::mcbp::Feature::SNAPPY,
                       cb::mcbp::Feature::JSON});
    admin.selectBucket("default");

    // minimal config, we only need a valid JSON object to then reach priv
    // checks
    nlohmann::json config = {
            {"range", {{"start", "dGVzdA=="}, {"end", "dGVzdA=="}}},
            {"collection", "0"}};

    BinprotRangeScanCreate create(Vbid(0), config);
    auto resp = admin.execute(create);
    ASSERT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());
}

class RbacRoleTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        mcd_env->getTestBucket().createBucket(
                "rbac_test", {}, *adminConnection);
        smith_holder = getConnection().clone();
        prepare_auth_connection(*smith_holder, "smith");
        jones_holder = getConnection().clone();
        prepare_auth_connection(*jones_holder, "jones");
        larry_holder = getConnection().clone();
        prepare_auth_connection(*larry_holder, "larry");
    }

    void TearDown() override {
        smith_holder.reset();
        jones_holder.reset();
        larry_holder.reset();
        adminConnection->deleteBucket("rbac_test");
        TestappClientTest::TearDown();
    }

    MemcachedConnection& getROConnection() {
        return *smith_holder;
    }

    MemcachedConnection& getWOConnection() {
        return *jones_holder;
    }

    MemcachedConnection& getRWConnection() {
        return *larry_holder;
    }

protected:
    MutationInfo store(MemcachedConnection& conn, MutationType type) {
        Document document;
        document.info.cas = cb::mcbp::cas::Wildcard;
        document.info.datatype = cb::mcbp::Datatype::JSON;
        document.info.flags = 0xcaffee;
        document.info.id = name;
        document.value = memcached_cfg.dump();
        return conn.mutate(document, Vbid(0), type);
    }

    BinprotResponse createXattr(MemcachedConnection& conn,
                                const std::string& key,
                                const std::string& value) {
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictUpsert);
        cmd.setKey(name);
        cmd.setPath(key);
        cmd.setValue(value);
        cmd.addPathFlags(cb::mcbp::subdoc::PathFlag::XattrPath |
                         cb::mcbp::subdoc::PathFlag::Mkdir_p);

        return conn.execute(cmd);
    }

    BinprotResponse getXattr(MemcachedConnection& conn,
                             const std::string& key) {
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
        cmd.setKey(name);
        cmd.setPath(key);
        cmd.addPathFlags(cb::mcbp::subdoc::PathFlag::XattrPath);
        return conn.execute(cmd);
    }

    static void prepare_auth_connection(MemcachedConnection& c,
                                        const std::string& username) {
        c.authenticate(username);
        c.setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                       cb::mcbp::Feature::XATTR,
                       cb::mcbp::Feature::XERROR,
                       cb::mcbp::Feature::SELECT_BUCKET,
                       cb::mcbp::Feature::SNAPPY,
                       cb::mcbp::Feature::JSON});
        c.selectBucket("rbac_test");
    }

    std::unique_ptr<MemcachedConnection> smith_holder;
    std::unique_ptr<MemcachedConnection> jones_holder;
    std::unique_ptr<MemcachedConnection> larry_holder;

};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         RbacRoleTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

/**
 * An arithmetic operation requires read and write privilege as it returns
 * the value
 */
TEST_P(RbacRoleTest, Arithmetic) {
    auto& ro = getROConnection();
    auto& wo = getWOConnection();
    auto& rw = getRWConnection();

    // Try to increment the key (it doesn't exists, so it should be created
    // with value 0)
    try {
        ro.arithmetic(name, 1, 0);
        FAIL() << "The read-only user should not be allowed to create keys";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    try {
        wo.arithmetic(name, 1, 0);
        FAIL() << "The write-only user should not be allowed to create keys";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    // The rw user should not be able to perform arithmetic if we drop
    // the upsert privilege
    rw.dropPrivilege(cb::rbac::Privilege::Upsert);
    try {
        wo.arithmetic(name, 1, 0);
        FAIL() << "The rw user user should not be allowed to create keys"
               << " the privilege set don't include upsert";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    // reset the connection to get back the privilege
    rw.reconnect();
    prepare_auth_connection(rw, "larry");
    // With upsert it should be allowed to create the key
    rw.arithmetic(name, 0, 0);

    // The key exists, verify that we can't increment it if it exists
    try {
        ro.arithmetic(name, 1);
        FAIL() << "The read-only user should not be allowed to perform "
               << "arithmetic operations";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    try {
        wo.arithmetic(name, 1);
        FAIL() << "The write-only user should not be allowed to perform "
               << "arithmetic operations";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    // The rw user should not be able to perform arithmetic if we drop
    // the upsert privilege
    rw.dropPrivilege(cb::rbac::Privilege::Upsert);
    try {
        wo.arithmetic(name, 1, 0);
        FAIL() << "The rw user user should not be allowed to create keys"
               << " the privilege set don't include upsert";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(RbacRoleTest, MutationTest_ReadOnly) {
    auto& ro = getROConnection();

    try {
        store(ro, MutationType::Add);
        FAIL() << "The read-only user should not be able to add documents";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    auto& rw = getRWConnection();
    store(rw, MutationType::Add);

    for (const auto& type : {MutationType::Append,
                             MutationType::Prepend,
                             MutationType::Set,
                             MutationType::Replace}) {
        try {
            store(ro, type);
            FAIL() << "The read-only user should not be able modify document with operation: "
                   << to_string(type);
        } catch (const ConnectionError& error) {
            EXPECT_TRUE(error.isAccessDenied());
        }
    }
}

TEST_P(RbacRoleTest, MutationTest_WriteOnly) {
    auto& wo = getWOConnection();

    // The Write Only user should be allowed to do all of these ops
    for (int ii = 0; ii < 2; ++ii) {
        for (const auto& type : {MutationType::Add,
                                 MutationType::Append,
                                 MutationType::Prepend,
                                 MutationType::Set,
                                 MutationType::Replace}) {
            store(wo, type);
        }

        wo.remove(name, Vbid(0), 0);

        // If we drop the Insert privilege the upsert privilege should
        // allow us to do all that we want..
        if (ii == 0) {
            wo.dropPrivilege(cb::rbac::Privilege::Insert);
        }
    }

    // Reset privilege set
    wo.reconnect();
    prepare_auth_connection(wo, "jones");

    // If we drop the Upsert privilege we should only be allowed to do add
    wo.dropPrivilege(cb::rbac::Privilege::Upsert);

    store(wo, MutationType::Add);
    for (const auto& type : {MutationType::Append,
                             MutationType::Prepend,
                             MutationType::Set,
                             MutationType::Replace}) {
        try {
            store(wo, type);
            FAIL() << "The write-only user should not be able to modify "
                   << "the document by using " << to_string(type)
                   << " without Upsert privilege";
        } catch (const ConnectionError& error) {
            EXPECT_TRUE(error.isAccessDenied());
        }
    }
}

TEST_P(RbacRoleTest, Remove_ReadOnly) {
    auto& rw = getRWConnection();
    store(rw, MutationType::Add);

    try {
        auto& ro = getROConnection();
        ro.remove(name, Vbid(0), 0);
        FAIL() << "The read-only user should not be able to remove documents";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(RbacRoleTest, Remove_WriteOnly) {
    auto& wo = getWOConnection();
    store(wo, MutationType::Add);
    wo.remove(name, Vbid(0), 0);

    store(wo, MutationType::Add);

    // And if we drop the delete privilege we shouldn't be able to delete
    // the document
    wo.dropPrivilege(cb::rbac::Privilege::Delete);
    try {
        wo.remove(name, Vbid(0), 0);
        FAIL() << "The write-only user should not be able to delete document"
               << " without delete privilege";

    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(RbacRoleTest, NoAccessToSystemXattrs) {
    auto& rw = getRWConnection();
    store(rw, MutationType::Add);

    // The read only user should not have access to create a system xattr
    auto resp = createXattr(getROConnection(), "_meta.author", "\"larry\"");
    ASSERT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());

    // The write only user should not have access to create a system xattr
    resp = createXattr(getROConnection(), "_meta.author", "\"larry\"");
    ASSERT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());

    // The read-write user should have access to create a system xattr
    resp = createXattr(getRWConnection(), "_meta.author", "\"larry\"");
    ASSERT_TRUE(resp.isSuccess());

    // The read only user should not be able to read it
    resp = getXattr(getROConnection(), "_meta.author");
    ASSERT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());

    // The write only user should not be able to read it
    resp = getXattr(getWOConnection(), "_meta.author");
    ASSERT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());

    // The read write user should be able to read it
    resp = getXattr(getRWConnection(), "_meta.author");
    ASSERT_TRUE(resp.isSuccess());
}

// Authenticate as jones; he has access to the bucket named jones and
// make sure we don't automatically select that buket. Then select the
// bucket jones and reauthenticate as Larry. Larry on the other hand
// doesn't have access to the bucket named jones and should be
// moved out of the bucket
TEST_P(RbacRoleTest, DontAutoselectBucketNoAccess) {
    mcd_env->getTestBucket().createBucket("jones", {}, *adminConnection);

    auto& conn = getConnection();
    conn.authenticate("jones");

    nlohmann::json json;
    conn.stats([&json](const auto& k,
                       const auto& v) { json = nlohmann::json::parse(v); },
               "connections self");

    ASSERT_FALSE(json.empty()) << "connections self did not return JSON";
    ASSERT_EQ(0, json["bucket_index"]) << "Should not be put in another bucket";
    conn.selectBucket("jones");
    json = {};
    conn.stats([&json](const auto& k,
                       const auto& v) { json = nlohmann::json::parse(v); },
               "connections self");

    ASSERT_FALSE(json.empty()) << "connections self did not return JSON";
    ASSERT_NE(0, json["bucket_index"]) << "Should not be in no-bucket";

    conn.authenticate("larry");
    json = {};
    conn.stats([&json](const auto& k,
                       const auto& v) { json = nlohmann::json::parse(v); },
               "connections self");

    ASSERT_FALSE(json.empty()) << "connections self did not return JSON";
    ASSERT_EQ(0, json["bucket_index"])
            << "larry does not have access to the bucket named jones and "
               "should be in no-bucket";

    adminConnection->deleteBucket("jones");
}
