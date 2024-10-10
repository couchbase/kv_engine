/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <platform/compress.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <tests/testapp/testapp_subdoc_common.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <string>

using cb::mcbp::ClientOpcode;
using cb::mcbp::Status;
using cb::mcbp::subdoc::DocFlag;
using cb::mcbp::subdoc::PathFlag;
using namespace cb::mcbp::datatype;

static const std::string bucket_name = "test_bucket";
static const std::string value_only_key = "doc_with_value_only";
static const std::string value_user_key = "doc_with_value_and_user_xattrs";
static const std::string value_system_key = "doc_with_value_and_system_xattrs";
static const std::string value_user_system_key =
        "doc_with_value_user_and_system_xattrs";

const std::string value(fmt::format(R"({{ "foo" : "{:x^4096}" }})", 'x'));
const std::string user_xattr_value(R"({"user": {"foo":true}})"_json.dump());
const std::string system_xattr_value(
        R"({"system": {"foo":false}})"_json.dump());

/**
 * Test the implementation of the GetEx command (and GetExReplica to read
 * from a replica). The test creates 4 documents on the server and depending
 * on the access rights it may have to inflate and strip off the system xattr.
 * If there is no system xattrs or the connection holds the privilege to read
 * the system xattrs the document should be returned as stored in memory
 * (and should *not* be inflated)
 */
class GetExTest : public cb::test::ClusterTest {
public:
    static void SetUpTestCase();

protected:
    static auto getBucket() {
        return cluster->getBucket(bucket_name);
    }

    static auto getConnection(ClientOpcode opcode) {
        if (opcode == ClientOpcode::GetEx) {
            return getBucket()->getConnection(Vbid{0});
        }
        return getBucket()->getConnection(Vbid{0}, vbucket_state_replica);
    }

    static auto getAuthedConnection(ClientOpcode opcode) {
        auto ret = getConnection(opcode);
        ret->authenticate("@admin");
        ret->selectBucket(bucket_name);
        return ret;
    }

    static void upsert(MemcachedConnection& conn,
                       const std::string& id,
                       std::string value,
                       std::string xattr_path = {},
                       std::string xattr_value = {},
                       std::string second_xattr_path = {},
                       std::string second_xattr_value = {});

    void verify(MemcachedConnection& conn,
                ClientOpcode opcode,
                const std::string& id,
                bool wait_for_compressed,
                std::string value,
                std::string xattr_path = {},
                std::string xattr_value = {},
                std::string second_xattr_path = {},
                std::string second_xattr_value = {});
};

void GetExTest::SetUpTestCase() {
    auto bucket = cluster->createBucket(bucket_name,
                                        {{"replicas", 2},
                                         {"max_vbuckets", 8},
                                         {"compression_mode", "active"}});

    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin");
    conn->selectBucket(bucket_name);
    upsert(*conn, value_only_key, value);
    upsert(*conn, value_user_key, value, "user", user_xattr_value);
    upsert(*conn, value_system_key, value, "_system", system_xattr_value);
    upsert(*conn,
           value_user_system_key,
           value,
           "user",
           user_xattr_value,
           "_system",
           system_xattr_value);
    waitForReplicationToAllNodes(*bucket, value_user_system_key);
}

void GetExTest::upsert(MemcachedConnection& conn,
                       const std::string& id,
                       std::string value,
                       std::string xattr_path,
                       std::string xattr_value,
                       std::string second_xattr_path,
                       std::string second_xattr_value) {
    if (xattr_path.empty()) {
        Document doc;
        doc.info.id = std::string{id};
        doc.value = std::move(value);
        conn.mutate(doc, Vbid{0}, MutationType::Set);
    } else {
        for (int ii = 0; ii < 2; ++ii) {
            BinprotSubdocMultiMutationCommand cmd;
            cmd.setKey(std::string{id});
            cmd.setVBucket(Vbid{0});
            if (ii == 0) {
                cmd.addMutation(ClientOpcode::SubdocDictUpsert,
                                PathFlag::XattrPath,
                                xattr_path,
                                xattr_value);
                cmd.addMutation(ClientOpcode::Set, PathFlag::None, "", value);
                cmd.addDocFlag(DocFlag::Mkdoc);
            } else {
                cmd.addMutation(ClientOpcode::SubdocDictUpsert,
                                PathFlag::XattrPath,
                                second_xattr_path,
                                second_xattr_value);
            }
            auto rsp = conn.execute(cmd);
            if (!rsp.isSuccess()) {
                throw ConnectionError("Subdoc failed", rsp);
            }
            if (second_xattr_path.empty()) {
                return;
            }
        }
    }
}

void GetExTest::verify(MemcachedConnection& conn,
                       ClientOpcode opcode,
                       const std::string& id,
                       bool wait_for_compressed,
                       std::string value,
                       std::string xattr_path,
                       std::string xattr_value,
                       std::string second_xattr_path,
                       std::string second_xattr_value) {
    auto now = std::chrono::steady_clock::now();
    const auto timeout = now + std::chrono::seconds(30);
    BinprotResponse rsp;
    do {
        rsp = conn.execute(BinprotGenericCommand{opcode, id});
        ASSERT_EQ(Status::Success, rsp.getStatus());
        if (!wait_for_compressed || is_snappy(rsp.getDatatype())) {
            break;
        }
        if (std::chrono::steady_clock::now() > timeout) {
            FAIL() << "Timed out waiting for value to return compressed";
        }
    } while (true);
    // The body is JSON
    EXPECT_TRUE(is_json(rsp.getDatatype()));

    auto returned_value = std::string{rsp.getDataView()};

    if (wait_for_compressed) {
        EXPECT_TRUE(is_snappy(rsp.getDatatype()));
        cb::compression::Buffer buffer;
        if (!inflateSnappy(returned_value, buffer)) {
            throw std::runtime_error("Failed to inflate value");
        }
        returned_value.assign(buffer);
    }

    std::string_view value_view = returned_value;

    if (xattr_path.empty()) {
        ASSERT_FALSE(is_xattr(rsp.getDatatype()));
    } else {
        if (!is_xattr(rsp.getDatatype())) {
            _Exit(1);
        }
        ASSERT_TRUE(is_xattr(rsp.getDatatype())) << id;
        value_view = cb::xattr::get_body(returned_value);
        cb::xattr::Blob blob;
        blob.assign({returned_value.data(),
                     cb::xattr::get_body_offset(returned_value)},
                    false);
        EXPECT_EQ(xattr_value, blob.get(xattr_path));
        if (!second_xattr_path.empty()) {
            EXPECT_EQ(second_xattr_value, blob.get(second_xattr_path));
        }

        // verify that we don't have any unexpected keys
        for (auto [k, v] : blob) {
            if (k != xattr_path && k != second_xattr_path) {
                FAIL() << "Unexpected key found: " << k;
            }
        }
    }
    EXPECT_EQ(value, value_view);
}

TEST_F(GetExTest, GetValueOnlyActive) {
    verify(*getAuthedConnection(ClientOpcode::GetEx),
           ClientOpcode::GetEx,
           value_only_key,
           true,
           value);
}

TEST_F(GetExTest, GetValueOnlyReplica) {
    verify(*getAuthedConnection(ClientOpcode::GetExReplica),
           ClientOpcode::GetExReplica,
           value_only_key,
           true,
           value);
}

TEST_F(GetExTest, GetValueAndUserXattrActive) {
    verify(*getAuthedConnection(ClientOpcode::GetEx),
           ClientOpcode::GetEx,
           value_user_key,
           true,
           value,
           "user",
           user_xattr_value);
}

TEST_F(GetExTest, GetValueAndUserXattrReplica) {
    verify(*getAuthedConnection(ClientOpcode::GetExReplica),
           ClientOpcode::GetExReplica,
           value_user_key,
           true,
           value,
           "user",
           user_xattr_value);
}

TEST_F(GetExTest, GetValueAndSystemXattrActive) {
    verify(*getAuthedConnection(ClientOpcode::GetEx),
           ClientOpcode::GetEx,
           value_system_key,
           true,
           value,
           "_system",
           system_xattr_value);
}

TEST_F(GetExTest, GetValueAndSystemXattrReplica) {
    verify(*getAuthedConnection(ClientOpcode::GetExReplica),
           ClientOpcode::GetExReplica,
           value_system_key,
           true,
           value,
           "_system",
           system_xattr_value);
}

TEST_F(GetExTest, GetValueAndSystemXattrNoSysPrivilegeActive) {
    // Document needs to be inflated and the system xattrs will be stripped off
    auto conn = getAuthedConnection(ClientOpcode::GetEx);
    conn->dropPrivilege(cb::rbac::Privilege::SystemXattrRead);
    verify(*conn, ClientOpcode::GetEx, value_system_key, false, value);
}

TEST_F(GetExTest, GetValueAndSystemXattrNoSysPrivilegeReplica) {
    // Document needs to be inflated and the system xattrs will be stripped off
    auto conn = getAuthedConnection(ClientOpcode::GetExReplica);
    conn->dropPrivilege(cb::rbac::Privilege::SystemXattrRead);
    verify(*conn, ClientOpcode::GetExReplica, value_system_key, false, value);
}

TEST_F(GetExTest, GetValueAndUserAndSystemXattrActive) {
    verify(*getAuthedConnection(ClientOpcode::GetEx),
           ClientOpcode::GetEx,
           value_user_system_key,
           true,
           value,
           "user",
           user_xattr_value,
           "_system",
           system_xattr_value);
}

TEST_F(GetExTest, GetValueAndUserAndSystemXattrReplica) {
    verify(*getAuthedConnection(ClientOpcode::GetExReplica),
           ClientOpcode::GetExReplica,
           value_user_system_key,
           true,
           value,
           "user",
           user_xattr_value,
           "_system",
           system_xattr_value);
}

TEST_F(GetExTest, GetValueAndUserAndSystemXattrNoSysPrivilegeActive) {
    // Document needs to be inflated and the system xattrs will be stripped off
    auto conn = getAuthedConnection(ClientOpcode::GetEx);
    conn->dropPrivilege(cb::rbac::Privilege::SystemXattrRead);
    verify(*conn,
           ClientOpcode::GetEx,
           value_user_system_key,
           false,
           value,
           "user",
           user_xattr_value);
}

TEST_F(GetExTest, GetValueAndUserAndSystemXattrNoSysPrivilegeReplica) {
    // Document needs to be inflated and the system xattrs will be stripped off
    auto conn = getAuthedConnection(ClientOpcode::GetExReplica);
    conn->dropPrivilege(cb::rbac::Privilege::SystemXattrRead);
    verify(*conn,
           ClientOpcode::GetExReplica,
           value_user_system_key,
           false,
           value,
           "user",
           user_xattr_value);
}
