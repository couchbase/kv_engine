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
#include <platform/compress.h>

class RbacTest : public TestappClientTest {
public:
};

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
        conn.authenticate("sharon", "sharonpw", "PLAIN");
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
    conn.sendCommand(cmd);

    BinprotResponse resp;
    conn.recvResponse(resp);
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());
}

TEST_P(RbacTest, ReloadSasl_HaveAccess) {
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IsaslRefresh});
    EXPECT_TRUE(rsp.isSuccess());
}

TEST_P(RbacTest, ReloadSasl_NoAccess) {
    auto& conn = getConnection();
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::IsaslRefresh);

    conn.sendCommand(cmd);
    BinprotResponse resp;
    conn.recvResponse(resp);
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());
}

TEST_P(RbacTest, ScrubNoAccess) {
    auto& c = getConnection();
    c.authenticate("larry", "larrypassword", "PLAIN");

    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Scrub);
    BinprotResponse response;

    c.sendCommand(command);
    c.recvResponse(response);
    EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
}

TEST_P(RbacTest, Scrub) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Scrub);

    adminConnection->executeInBucket(bucketName, [](auto& c) {
        BinprotGenericCommand command(cb::mcbp::ClientOpcode::Scrub);
        BinprotResponse response;
        do {
            // Retry if scrubber is already running.
            response = c.execute(command);
        } while (response.getStatus() == cb::mcbp::Status::Ebusy);

        EXPECT_TRUE(response.isSuccess());
    });
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
    auto json = nlohmann::json::parse(resp.getDataString());

    ASSERT_TRUE(json["error"].is_object());
    ASSERT_TRUE(json["error"]["context"].is_string());
    auto ref = json["error"]["ref"].get<std::string>();

    // @todo I could parse the UUID to see that it is actually an UUID,
    //       but for now I just trust it to be a UUID and not something
    //       else (just add a check for the length of an UUID)
    EXPECT_EQ(36, ref.size());

    const std::string expected{
            "Authorization failure: can't execute RBAC_REFRESH operation "
            "without the SecurityManagement privilege"};
    EXPECT_EQ(expected, json["error"]["context"]);
}

class RbacRoleTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        adminConnection->createBucket("rbac_test", "", BucketType::Memcached);

        smith_holder = getConnection().clone();
        jones_holder = getConnection().clone();
        larry_holder = getConnection().clone();
    }

    void TearDown() override {
        smith_holder.reset();
        jones_holder.reset();
        larry_holder.reset();
        adminConnection->deleteBucket("rbac_test");
        TestappClientTest::TearDown();
    }

    MemcachedConnection& getROConnection() {
        auto& smith = *smith_holder.get();
        smith.authenticate("smith", "smithpassword", "PLAIN");
        return prepare_auth_connection(smith);
    }

    MemcachedConnection& getWOConnection() {
        auto& jones = *jones_holder.get();
        jones.authenticate("jones", "jonespassword", "PLAIN");
        return prepare_auth_connection(jones);
    }

    MemcachedConnection& getRWConnection() {
        auto& larry = *larry_holder.get();
        larry.authenticate("larry", "larrypassword", "PLAIN");
        return prepare_auth_connection(larry);
    }

protected:
    MutationInfo store(MemcachedConnection& conn, MutationType type) {
        Document document;
        document.info.cas = mcbp::cas::Wildcard;
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
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);

        conn.sendCommand(cmd);

        BinprotResponse resp;
        conn.recvResponse(resp);
        return resp;
    }

    BinprotResponse getXattr(MemcachedConnection& conn,
                             const std::string& key) {
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
        cmd.setKey(name);
        cmd.setPath(key);
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
        conn.sendCommand(cmd);

        BinprotResponse resp;
        conn.recvResponse(resp);
        return resp;
    }

    MemcachedConnection& prepare_auth_connection(MemcachedConnection& c) {
        c.setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                       cb::mcbp::Feature::XATTR,
                       cb::mcbp::Feature::XERROR,
                       cb::mcbp::Feature::SELECT_BUCKET,
                       cb::mcbp::Feature::SNAPPY,
                       cb::mcbp::Feature::JSON});
        c.selectBucket("rbac_test");
        return c;
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
    rw.authenticate("larry", "larrypassword", "PLAIN");
    prepare_auth_connection(rw);
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
    wo.authenticate("jones", "jonespassword", "PLAIN");
    prepare_auth_connection(wo);

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

TEST_P(RbacRoleTest, NoAccessToUserXattrs) {
    auto& rw = getRWConnection();
    store(rw, MutationType::Add);

    // The read only user should not have access to create a user xattr
    auto resp = createXattr(getROConnection(), "meta.author", "\"larry\"");
    ASSERT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());

    // The write only user should have access to create a user xattr
    resp = createXattr(getWOConnection(), "meta.author", "\"larry\"");
    ASSERT_TRUE(resp.isSuccess());

    // The read only user should be able to read it
    resp = getXattr(getROConnection(), "meta.author");
    ASSERT_TRUE(resp.isSuccess());

    // The write only user should NOT be able to read it
    resp = getXattr(getWOConnection(), "meta.author");
    ASSERT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());

    // The rw user only have access to the system xattrs. Read and write
    // user xattrs should fail!
    resp = createXattr(getRWConnection(), "meta.author", "\"larry\"");
    ASSERT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());

    resp = getXattr(getRWConnection(), "meta.author");
    ASSERT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getStatus());
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

TEST_P(RbacRoleTest, DontAutoselectBucket) {
    adminConnection->createBucket("larry", "", BucketType::Memcached);

    // Authenticate as larry
    auto& conn = getConnection();
    conn.authenticate("larry", "larrypassword", "PLAIN");

    // If we try to run a get request it should return no bucket
    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
    cmd.setKey("foo");
    cmd.setPath("doc.meta");
    const auto resp = conn.execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::NoBucket, resp.getStatus());

    adminConnection->deleteBucket("larry");
}
