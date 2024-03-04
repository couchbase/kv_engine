/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"
#include "utilities/test_manifest.h"

#include <boost/stacktrace/detail/frame_decl.hpp>
#include <mcbp/codec/frameinfo.h>
#include <nlohmann/json.hpp>

using namespace std::string_view_literals;
using namespace cb::mcbp;
using namespace cb::mcbp::request;

class CollectionsTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappClientTest::SetUp();
        // Default engine does not support changing the collection configuration
        if (!mcd_env->getTestBucket().supportsCollections() ||
            mcd_env->getTestBucket().getName() == "default_engine") {
            return;
        }

        collections.add(CollectionEntry::fruit).add(CollectionEntry::vegetable);
        collections.add(ScopeEntry::customer);
        collections.add(CollectionEntry::customer1, ScopeEntry::customer);
        collections.add(ScopeEntry::maxScope);
        collections.add(CollectionEntry::maxCollection, ScopeEntry::maxScope);
        collections.add(ScopeEntry::systemScope);
        collections.add(CollectionEntry::systemCollection,
                        ScopeEntry::systemScope);

        adminConnection->executeInBucket(bucketName, [&](auto& conn) {
            auto response = conn.execute(
                    BinprotGenericCommand{ClientOpcode::CollectionsSetManifest,
                                          {},
                                          collections.getJson().dump()});

            // The manifest sticks, so if we set it again with the same uid,
            // that is invalid, just assert erange and carry on
            if (!response.isSuccess()) {
                ASSERT_EQ(Status::Erange, response.getStatus());
            }
        });

        key = DocKey::makeWireEncodedString(CollectionEntry::systemCollection,
                                            name);

        if (!user) {
            user = userConnection->clone();
            user->authenticate("Luke");
            user->setFeature(Feature::Collections, true);
            user->selectBucket(bucketName);
        }

        if (!admin) {
            admin = adminConnection->clone();
            admin->authenticate("@admin");
            admin->setFeature(Feature::Collections, true);
            admin->selectBucket(bucketName);
        }
    }

    void createDocument(std::string value = "{}") {
        Document doc;
        doc.info.id = key;
        doc.value = std::move(value);
        admin->mutate(doc, Vbid{0}, MutationType::Set);
    }

    static BinprotResponse execute(MemcachedConnection& connection,
                                   BinprotCommand& command,
                                   bool impersonate = false,
                                   bool read = false,
                                   bool write = false) {
        if (impersonate) {
            command.addFrameInfo(euid);
        }
        if (read) {
            command.addFrameInfo(SystemCollectionLookup);
        }
        if (write) {
            command.addFrameInfo(SystemCollectionMutation);
        }
        return connection.execute(command);
    }

    static void execute(MemcachedConnection& connection,
                        BinprotCommand& command,
                        bool impersonate,
                        bool read,
                        bool write,
                        Status status) {
        auto rsp = execute(connection, command, impersonate, read, write);
        EXPECT_EQ(status, rsp.getStatus()) << fmt::format(
                "Running {} with{} euid (SystemCollectionLookup: {} "
                "SystemCollectionMutation: {})\n. Error: {}",
                command.getOp(),
                command.hasFrameInfos() ? "" : "out",
                read ? "Yes" : "No",
                write ? "Yes" : "No",
                rsp.getDataString());
    }

    static void executeInSystemCollectionWithoutAccess(
            MemcachedConnection& connection,
            BinprotCommand& command,
            bool impersonate) {
        execute(connection,
                command,
                impersonate,
                false,
                false,
                Status::Eaccess);
    }

    static std::unique_ptr<MemcachedConnection> user;
    static std::unique_ptr<MemcachedConnection> admin;
    static const ImpersonateUserFrameInfo euid;
    static const ImpersonateUserExtraPrivilegeFrameInfo SystemCollectionLookup;
    static const ImpersonateUserExtraPrivilegeFrameInfo
            SystemCollectionMutation;

    CollectionsManifest collections;
    std::string key;
};

std::unique_ptr<MemcachedConnection> CollectionsTest::user;
std::unique_ptr<MemcachedConnection> CollectionsTest::admin;
const ImpersonateUserFrameInfo CollectionsTest::euid{"Luke"};
const ImpersonateUserExtraPrivilegeFrameInfo
        CollectionsTest::SystemCollectionLookup{"SystemCollectionLookup"};
const ImpersonateUserExtraPrivilegeFrameInfo
        CollectionsTest::SystemCollectionMutation{"SystemCollectionMutation"};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         CollectionsTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

// Check that an unknown scope/collection error returns the expected JSON
TEST_P(CollectionsTest, ManifestUidInResponse) {
    std::string expectedUid = collections.getUidString();
    if (!mcd_env->getTestBucket().supportsCollections()) {
        return;
    }
    if (mcd_env->getTestBucket().getName() == "default_engine") {
        expectedUid = "0";
    }

    // Force the unknown collection error and check the JSON
    auto response = userConnection->execute(BinprotGenericCommand{
            ClientOpcode::CollectionsGetID, "_default.error"});
    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(Status::UnknownCollection, response.getStatus());
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(response.getDataString());
    } catch (const nlohmann::json::exception& e) {
        FAIL() << "Cannot parse json response:" << response.getDataString()
               << " e:" << e.what();
    }

    auto itr = parsed.find("manifest_uid");
    EXPECT_NE(parsed.end(), itr);
    EXPECT_EQ(expectedUid, itr->get<std::string>());
}

TEST_P(CollectionsTest, ClientOpcode_Get_no_access) {
    BinprotGenericCommand command{ClientOpcode::Get, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Get_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::Get, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Get_access) {
    BinprotGenericCommand command{ClientOpcode::Get, key};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Get_with_euid_with_access) {
    BinprotGenericCommand command{ClientOpcode::Get, key};
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Getq_no_access) {
    BinprotGenericCommand command{ClientOpcode::Getq, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Getq_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::Getq, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Getq_access) {
    createDocument();
    BinprotGenericCommand command{ClientOpcode::Getq, key};
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Getq_with_euid_with_access) {
    createDocument();
    BinprotGenericCommand command{ClientOpcode::Getq, key};
    execute(*admin, command, true, true, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Getk_no_access) {
    BinprotGenericCommand command{ClientOpcode::Getk, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Getk_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::Getk, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Getk_access) {
    BinprotGenericCommand command{ClientOpcode::Getk, key};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Getk_with_euid_with_access) {
    BinprotGenericCommand command{ClientOpcode::Getk, key};
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Getkq_no_access) {
    BinprotGenericCommand command{ClientOpcode::Getkq, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Getkq_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::Getkq, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Getkq_access) {
    createDocument();
    BinprotGenericCommand command{ClientOpcode::Getkq, key};
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Getkq_with_euid_with_access) {
    createDocument();
    BinprotGenericCommand command{ClientOpcode::Getkq, key};
    execute(*admin, command, true, true, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Set_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Set);
    command.setOp(ClientOpcode::Set);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Set_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Set);
    command.setOp(ClientOpcode::Set);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Set_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Set);
    command.setOp(ClientOpcode::Set);
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Set_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Set);
    command.setOp(ClientOpcode::Set);
    execute(*admin, command, true, false, true, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Setq_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Set);
    command.setOp(ClientOpcode::Setq);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Setq_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Set);
    command.setOp(ClientOpcode::Setq);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Setq_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Set);
    command.setOp(ClientOpcode::Setq);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Setq_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Set);
    command.setOp(ClientOpcode::Setq);
    command.addFrameInfo(euid);
    command.addFrameInfo(SystemCollectionMutation);

    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Add_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Add);
    command.setOp(ClientOpcode::Add);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Add_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Add);
    command.setOp(ClientOpcode::Add);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Add_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Add);
    command.setOp(ClientOpcode::Add);
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Add_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Add);
    command.setOp(ClientOpcode::Add);
    execute(*admin, command, true, false, true, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Addq_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Add);
    command.setOp(ClientOpcode::Addq);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Addq_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Add);
    command.setOp(ClientOpcode::Addq);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Addq_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Add);
    command.setOp(ClientOpcode::Addq);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Addq_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Add);
    command.setOp(ClientOpcode::Addq);
    command.addFrameInfo(euid);
    command.addFrameInfo(SystemCollectionMutation);

    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Replace_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Replace);
    command.setOp(ClientOpcode::Replace);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Replace_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Replace);
    command.setOp(ClientOpcode::Replace);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Replace_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Replace);
    command.setOp(ClientOpcode::Replace);
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Replace_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Replace);
    command.setOp(ClientOpcode::Replace);
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Replaceq_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Replace);
    command.setOp(ClientOpcode::Replaceq);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Replaceq_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Replace);
    command.setOp(ClientOpcode::Replaceq);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Replaceq_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Replace);
    command.setOp(ClientOpcode::Replaceq);
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Replaceq_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Replace);
    command.setOp(ClientOpcode::Replaceq);
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Delete_no_access) {
    BinprotGenericCommand command{ClientOpcode::Delete, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Delete_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::Delete, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Delete_access) {
    BinprotGenericCommand command{ClientOpcode::Delete, key};
    execute(*admin, command, false, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Delete_with_euid_with_access) {
    BinprotGenericCommand command{ClientOpcode::Delete, key};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Deleteq_no_access) {
    BinprotGenericCommand command{ClientOpcode::Deleteq, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Deleteq_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::Deleteq, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Deleteq_access) {
    BinprotGenericCommand command{ClientOpcode::Delete, key};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Deleteq_with_euid_with_access) {
    BinprotGenericCommand command{ClientOpcode::Delete, key};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Append_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Append);
    command.setOp(ClientOpcode::Append);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Append_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Append);
    command.setOp(ClientOpcode::Append);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Append_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Append);
    command.setOp(ClientOpcode::Append);
    execute(*admin, command, false, false, false, Status::NotStored);
}
TEST_P(CollectionsTest, ClientOpcode_Append_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Append);
    command.setOp(ClientOpcode::Append);
    execute(*admin, command, true, false, true, Status::NotStored);
}
TEST_P(CollectionsTest, ClientOpcode_Appendq_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Append);
    command.setOp(ClientOpcode::Appendq);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Appendq_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Append);
    command.setOp(ClientOpcode::Appendq);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Appendq_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Append);
    command.setOp(ClientOpcode::Appendq);
    execute(*admin, command, false, false, false, Status::NotStored);
}
TEST_P(CollectionsTest, ClientOpcode_Appendq_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Append);
    command.setOp(ClientOpcode::Appendq);
    execute(*admin, command, true, false, true, Status::NotStored);
}
TEST_P(CollectionsTest, ClientOpcode_Prepend_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Prepend);
    command.setOp(ClientOpcode::Prepend);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Prepend_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Prepend);
    command.setOp(ClientOpcode::Prepend);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Prepend_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Prepend);
    command.setOp(ClientOpcode::Prepend);
    execute(*admin, command, false, false, false, Status::NotStored);
}
TEST_P(CollectionsTest, ClientOpcode_Prepend_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Prepend);
    command.setOp(ClientOpcode::Prepend);
    execute(*admin, command, true, false, true, Status::NotStored);
}
TEST_P(CollectionsTest, ClientOpcode_Prependq_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Prepend);
    command.setOp(ClientOpcode::Prependq);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Prependq_with_euid_no_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Prepend);
    command.setOp(ClientOpcode::Prepend);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Prependq_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Prepend);
    command.setOp(ClientOpcode::Prependq);
    execute(*admin, command, false, false, false, Status::NotStored);
}
TEST_P(CollectionsTest, ClientOpcode_Prependq_with_euid_with_access) {
    DocumentInfo info;
    info.id = key;
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer("foo"sv);
    command.setMutationType(MutationType::Prepend);
    command.setOp(ClientOpcode::Prependq);
    execute(*admin, command, true, false, true, Status::NotStored);
}
TEST_P(CollectionsTest, ClientOpcode_Increment_no_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Increment, key, Vbid{0}, 0, 0, 0);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Increment_with_euid_no_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Increment, key, Vbid{0}, 0, 0, 0);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Increment_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Increment, key, Vbid{0}, 0, 0, 0);
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Increment_with_euid_with_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Increment, key, Vbid{0}, 0, 0, 0);
    execute(*admin, command, true, true, true, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Incrementq_no_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Incrementq, key, Vbid{0}, 0, 0, 0);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Incrementq_with_euid_no_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Incrementq, key, Vbid{0}, 0, 0, 0);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Incrementq_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Incrementq, key, Vbid{0}, 0, 0, 0);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Incrementq_with_euid_with_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Incrementq, key, Vbid{0}, 0, 0, 0);
    command.addFrameInfo(euid);
    command.addFrameInfo(SystemCollectionLookup);
    command.addFrameInfo(SystemCollectionMutation);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Decrement_no_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Decrement, key, Vbid{0}, 0, 0, 0);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Decrement_with_euid_no_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Decrement, key, Vbid{0}, 0, 0, 0);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Decrement_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Decrement, key, Vbid{0}, 0, 0, 0);
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Decrement_with_euid_with_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Decrement, key, Vbid{0}, 0, 0, 0);
    execute(*admin, command, true, true, true, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Decrementq_no_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Decrementq, key, Vbid{0}, 0, 0, 0);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Decrementq_with_euid_no_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Decrementq, key, Vbid{0}, 0, 0, 0);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Decrementq_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Decrementq, key, Vbid{0}, 0, 0, 0);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Decrementq_with_euid_with_access) {
    BinprotIncrDecrCommand command(
            ClientOpcode::Decrementq, key, Vbid{0}, 0, 0, 0);
    command.addFrameInfo(euid);
    command.addFrameInfo(SystemCollectionLookup);
    command.addFrameInfo(SystemCollectionMutation);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Touch_no_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Touch);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Touch_with_euid_no_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Touch);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Touch_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Touch);
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Touch_with_euid_with_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Touch);
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Gat_no_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Gat);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Gat_with_euid_no_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Gat);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Gat_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Gat);
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Gat_with_euid_with_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Gat);
    execute(*admin, command, true, true, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_Gatq_no_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Gatq);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Gatq_with_euid_no_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Gatq);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Gatq_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Gatq);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_Gatq_with_euid_with_access) {
    BinprotGetAndTouchCommand command{key, Vbid{0}, 0};
    command.setOp(ClientOpcode::Gatq);
    command.addFrameInfo(euid);
    command.addFrameInfo(SystemCollectionLookup);
    command.addFrameInfo(SystemCollectionMutation);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_GetReplica_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetReplica, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_GetReplica_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetReplica, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_GetReplica_access) {
    BinprotGenericCommand command{ClientOpcode::GetReplica, key};
    execute(*admin, command, false, false, false, Status::NotSupported);
}
TEST_P(CollectionsTest, ClientOpcode_GetReplica_with_euid_with_access) {
    BinprotGenericCommand command{ClientOpcode::GetReplica, key};
    execute(*admin, command, true, true, false, Status::NotSupported);
}
TEST_P(CollectionsTest, ClientOpcode_Observe_no_access) {
    BinprotObserveCommand command{
            std::vector<std::pair<Vbid, std::string>>{{Vbid{0}, key}}};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_Observe_with_euid_no_access) {
    BinprotObserveCommand command{
            std::vector<std::pair<Vbid, std::string>>{{Vbid{0}, key}}};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_Observe_access) {
    BinprotObserveCommand command{
            std::vector<std::pair<Vbid, std::string>>{{Vbid{0}, key}}};
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_Observe_with_euid_with_access) {
    BinprotObserveCommand command{
            std::vector<std::pair<Vbid, std::string>>{{Vbid{0}, key}}};
    execute(*admin, command, true, true, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_GetLocked_no_access) {
    BinprotGetAndLockCommand command(key);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_GetLocked_with_euid_no_access) {
    BinprotGetAndLockCommand command(key);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_GetLocked_access) {
    BinprotGetAndLockCommand command(key);
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_GetLocked_with_euid_with_access) {
    BinprotGetAndLockCommand command(key);
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_UnlockKey_no_access) {
    BinprotUnlockCommand command(key, Vbid{0}, 0xdeadbeef);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_UnlockKey_with_euid_no_access) {
    BinprotUnlockCommand command(key, Vbid{0}, 0xdeadbeef);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_UnlockKey_access) {
    BinprotUnlockCommand command(key, Vbid{0}, 0xdeadbeef);
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_UnlockKey_with_euid_with_access) {
    BinprotUnlockCommand command(key, Vbid{0}, 0xdeadbeef);
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_GetMeta_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetMeta, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_GetMeta_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetMeta, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_GetMeta_access) {
    BinprotGenericCommand command{ClientOpcode::GetMeta, key};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_GetMeta_with_euid_with_access) {
    BinprotGenericCommand command{ClientOpcode::GetMeta, key};
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_GetqMeta_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetqMeta, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_GetqMeta_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetqMeta, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_GetqMeta_access) {
    BinprotGenericCommand command{ClientOpcode::GetMeta, key};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_GetqMeta_with_euid_with_access) {
    BinprotGenericCommand command{ClientOpcode::GetMeta, key};
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SetWithMeta_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::SetWithMeta);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SetWithMeta_with_euid_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::SetWithMeta);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SetWithMeta_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::SetWithMeta);
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_SetWithMeta_with_euid_with_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::SetWithMeta);
    execute(*admin, command, true, false, true, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_SetqWithMeta_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::SetqWithMeta);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SetqWithMeta_with_euid_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::SetqWithMeta);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SetqWithMeta_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::SetqWithMeta);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_SetqWithMeta_with_euid_with_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::SetqWithMeta);
    command.addFrameInfo(euid);
    command.addFrameInfo(SystemCollectionLookup);
    command.addFrameInfo(SystemCollectionMutation);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_AddWithMeta_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::AddWithMeta);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_AddWithMeta_with_euid_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::AddWithMeta);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_AddWithMeta_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};
    command.setOp(ClientOpcode::AddWithMeta);
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_AddWithMeta_with_euid_with_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};
    command.setOp(ClientOpcode::AddWithMeta);
    execute(*admin, command, true, false, true, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_AddqWithMeta_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::AddqWithMeta);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_AddqWithMeta_with_euid_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::AddqWithMeta);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_AddqWithMeta_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::AddqWithMeta);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_AddqWithMeta_with_euid_with_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::AddqWithMeta);
    command.addFrameInfo(euid);
    command.addFrameInfo(SystemCollectionLookup);
    command.addFrameInfo(SystemCollectionMutation);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_DelWithMeta_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::DelWithMeta);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_DelWithMeta_with_euid_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::DelWithMeta);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_DelWithMeta_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::DelWithMeta);
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_DelWithMeta_with_euid_with_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::DelWithMeta);
    execute(*admin, command, true, false, true, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_DelqWithMeta_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::DelWithMeta);
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_DelqWithMeta_with_euid_no_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::DelqWithMeta);
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_DelqWithMeta_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::DelqWithMeta);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_DelqWithMeta_with_euid_with_access) {
    Document doc;
    doc.info.id = key;
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = "foo";

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand command{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};

    command.setOp(ClientOpcode::DelqWithMeta);
    command.addFrameInfo(euid);
    command.addFrameInfo(SystemCollectionLookup);
    command.addFrameInfo(SystemCollectionMutation);
    admin->sendCommand(command);

    BinprotGenericCommand noop{ClientOpcode::Noop};
    admin->sendCommand(noop);

    BinprotResponse rsp;
    admin->recvResponse(rsp);
    if (rsp.getOp() != ClientOpcode::Noop) {
        admin.reset();
    }
    ASSERT_EQ(ClientOpcode::Noop, rsp.getOp());
}
TEST_P(CollectionsTest, ClientOpcode_ReturnMeta_no_access) {
    Document document;
    document.info.id = key;
    document.value = "foo";

    for (const auto type : std::vector<ReturnMetaType>{{ReturnMetaType::Add,
                                                        ReturnMetaType::Set,
                                                        ReturnMetaType::Del}}) {
        BinprotReturnMetaCommand command{type, document};
        executeInSystemCollectionWithoutAccess(*user, command, false);
    }
}
TEST_P(CollectionsTest, ClientOpcode_ReturnMeta_with_euid_no_access) {
    Document document;
    document.info.id = key;
    document.value = "foo";

    for (const auto type : std::vector<ReturnMetaType>{{ReturnMetaType::Add,
                                                        ReturnMetaType::Set,
                                                        ReturnMetaType::Del}}) {
        BinprotReturnMetaCommand command{type, document};
        executeInSystemCollectionWithoutAccess(*admin, command, true);
    }
}
TEST_P(CollectionsTest, ClientOpcode_ReturnMeta_access) {
    Document document;
    document.info.id = key;
    document.value = "foo";

    for (const auto type : std::vector<ReturnMetaType>{{ReturnMetaType::Add,
                                                        ReturnMetaType::Set,
                                                        ReturnMetaType::Del}}) {
        BinprotReturnMetaCommand command{type, document};
        execute(*admin, command, false, false, false, Status::Success);
    }
}
TEST_P(CollectionsTest, ClientOpcode_ReturnMeta_with_euid_with_access) {
    Document document;
    document.info.id = key;
    document.value = "foo";

    for (const auto type : std::vector<ReturnMetaType>{{ReturnMetaType::Add,
                                                        ReturnMetaType::Set,
                                                        ReturnMetaType::Del}}) {
        BinprotReturnMetaCommand command{type, document};
        execute(*admin, command, true, false, true, Status::Success);
    }
}
TEST_P(CollectionsTest, ClientOpcode_GetRandomKey_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetRandomKey};
    GetRandomKeyPayload payload(
            static_cast<uint32_t>(CollectionUid::systemCollection));
    command.setExtras(payload.getBuffer());
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_GetRandomKey_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetRandomKey};
    GetRandomKeyPayload payload(
            static_cast<uint32_t>(CollectionUid::systemCollection));
    command.setExtras(payload.getBuffer());
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_GetRandomKey_access) {
    createDocument();
    BinprotGenericCommand command{ClientOpcode::GetRandomKey};
    GetRandomKeyPayload payload(
            static_cast<uint32_t>(CollectionUid::systemCollection));
    command.setExtras(payload.getBuffer());
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_GetRandomKey_with_euid_with_access) {
    createDocument();
    BinprotGenericCommand command{ClientOpcode::GetRandomKey};
    GetRandomKeyPayload payload(
            static_cast<uint32_t>(CollectionUid::systemCollection));
    command.setExtras(payload.getBuffer());
    execute(*admin, command, true, true, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_GetKeys_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetKeys, key};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_GetKeys_with_euid_no_access) {
    BinprotGenericCommand command{ClientOpcode::GetKeys, key};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_GetKeys_access) {
    BinprotGenericCommand command{ClientOpcode::GetKeys, key};
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_GetKeys_with_euid_with_access) {
    BinprotGenericCommand command{ClientOpcode::GetKeys, key};
    execute(*admin, command, true, true, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocGet_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocGet, key, "hello"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocGet_with_euid_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocGet, key, "hello"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocGet_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocGet, key, "hello"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocGet_with_euid_with_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocGet, key, "hello"};
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocExists_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocExists, key, "hello"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocExists_with_euid_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocExists, key, "hello"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocExists_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocExists, key, "hello"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocExists_with_euid_with_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocExists, key, "hello"};
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDictAdd_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocDictAdd, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDictAdd_with_euid_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocDictAdd, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDictAdd_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocDictAdd, key, "v1", "true"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDictAdd_with_euid_with_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocDictAdd, key, "v1", "true"};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDictUpsert_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocDictUpsert, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDictUpsert_with_euid_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocDictUpsert, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDictUpsert_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocDictUpsert, key, "v1", "true"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDictUpsert_with_euid_with_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocDictUpsert, key, "v1", "true"};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDelete_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocDelete, key, "hello"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDelete_with_euid_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocDelete, key, "hello"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDelete_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocDelete, key, "hello"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocDelete_with_euid_with_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocDelete, key, "hello"};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocReplace_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocReplace, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocReplace_with_euid_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocReplace, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocReplace_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocReplace, key, "v1", "true"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocReplace_with_euid_with_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocReplace, key, "v1", "true"};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayPushLast_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayPushLast, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayPushLast_with_euid_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayPushLast, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayPushLast_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayPushLast, key, "v1", "true"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest,
       ClientOpcode_SubdocArrayPushLast_with_euid_with_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayPushLast, key, "v1", "true"};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayPushFirst_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayPushFirst, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayPushFirst_with_euid_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayPushFirst, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayPushFirst_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayPushFirst, key, "v1", "true"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest,
       ClientOpcode_SubdocArrayPushFirst_with_euid_with_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayPushFirst, key, "v1", "true"};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayInsert_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayInsert, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayInsert_with_euid_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayInsert, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayInsert_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayInsert, key, "v1", "true"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayInsert_with_euid_with_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayInsert, key, "v1", "true"};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayAddUnique_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayAddUnique, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayAddUnique_with_euid_no_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayAddUnique, key, "v1", "true"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocArrayAddUnique_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayAddUnique, key, "v1", "true"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest,
       ClientOpcode_SubdocArrayAddUnique_with_euid_with_access) {
    BinprotSubdocCommand command{
            ClientOpcode::SubdocArrayAddUnique, key, "v1", "true"};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocGetCount_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocGetCount, key, "hello"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocGetCount_with_euid_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocGetCount, key, "hello"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocGetCount_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocGetCount, key, "hello"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocGetCount_with_euid_with_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocGetCount, key, "hello"};
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocCounter_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocCounter, key, "v1", "1"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocCounter_with_euid_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocCounter, key, "v1", "1"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocCounter_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocCounter, key, "v1", "1"};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocCounter_with_euid_with_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocCounter, key, "v1", "1"};
    execute(*admin, command, true, true, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocMultiLookup_no_access) {
    BinprotSubdocMultiLookupCommand command{
            key,
            {{ClientOpcode::SubdocGet, {}, "missing1"},
             {ClientOpcode::SubdocGet, {}, "missing2"}},
            cb::mcbp::subdoc::DocFlag::None};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocMultiLookup_with_euid_no_access) {
    BinprotSubdocMultiLookupCommand command{
            key,
            {{ClientOpcode::SubdocGet, {}, "missing1"},
             {ClientOpcode::SubdocGet, {}, "missing2"}},
            cb::mcbp::subdoc::DocFlag::None};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocMultiLookup_access) {
    BinprotSubdocMultiLookupCommand command{
            key,
            {{ClientOpcode::SubdocGet, {}, "missing1"},
             {ClientOpcode::SubdocGet, {}, "missing2"}},
            cb::mcbp::subdoc::DocFlag::None};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocMultiLookup_with_euid_with_access) {
    BinprotSubdocMultiLookupCommand command{
            key,
            {{ClientOpcode::SubdocGet, {}, "missing1"},
             {ClientOpcode::SubdocGet, {}, "missing2"}},
            cb::mcbp::subdoc::DocFlag::None};
    execute(*admin, command, true, true, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocMultiMutation_no_access) {
    BinprotSubdocMultiMutationCommand command{
            key,
            {{ClientOpcode::SubdocDictUpsert, {}, "foo.missing.bar", "true"},
             {ClientOpcode::SubdocDictUpsert, {}, "foo.missing.foo", "true"}},
            cb::mcbp::subdoc::DocFlag::None};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocMultiMutation_with_euid_no_access) {
    BinprotSubdocMultiMutationCommand command{
            key,
            {{ClientOpcode::SubdocDictUpsert, {}, "foo.missing.bar", "true"},
             {ClientOpcode::SubdocDictUpsert, {}, "foo.missing.foo", "true"}},
            cb::mcbp::subdoc::DocFlag::None};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocMultiMutation_access) {
    BinprotSubdocMultiMutationCommand command{
            key,
            {{ClientOpcode::SubdocDictUpsert, {}, "foo.missing.bar", "true"},
             {ClientOpcode::SubdocDictUpsert, {}, "foo.missing.foo", "true"}},
            cb::mcbp::subdoc::DocFlag::None};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest,
       ClientOpcode_SubdocMultiMutation_with_euid_with_access) {
    BinprotSubdocMultiMutationCommand command{
            key,
            {{ClientOpcode::SubdocDictUpsert, {}, "foo.missing.bar", "true"},
             {ClientOpcode::SubdocDictUpsert, {}, "foo.missing.foo", "true"}},
            cb::mcbp::subdoc::DocFlag::None};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocReplaceBodyWithXattr_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocReplaceBodyWithXattr,
                                 key,
                                 "v1",
                                 "",
                                 cb::mcbp::subdoc::PathFlag::XattrPath};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest,
       ClientOpcode_SubdocReplaceBodyWithXattr_with_euid_no_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocReplaceBodyWithXattr,
                                 key,
                                 "v1",
                                 "",
                                 cb::mcbp::subdoc::PathFlag::XattrPath};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_SubdocReplaceBodyWithXattr_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocReplaceBodyWithXattr,
                                 key,
                                 "v1",
                                 "",
                                 cb::mcbp::subdoc::PathFlag::XattrPath};
    execute(*admin, command, false, false, false, Status::KeyEnoent);
}
TEST_P(CollectionsTest,
       ClientOpcode_SubdocReplaceBodyWithXattr_with_euid_with_access) {
    BinprotSubdocCommand command{ClientOpcode::SubdocReplaceBodyWithXattr,
                                 key,
                                 "v1",
                                 "",
                                 cb::mcbp::subdoc::PathFlag::XattrPath};
    execute(*admin, command, true, false, true, Status::KeyEnoent);
}
TEST_P(CollectionsTest, ClientOpcode_CollectionsGetManifest_no_access) {
    BinprotGenericCommand command(ClientOpcode::CollectionsGetManifest);
    auto rsp = execute(*user, command);
    ASSERT_EQ(Status::Success, rsp.getStatus()) << rsp.getDataString();
    const auto manifest = rsp.getDataString();
    EXPECT_EQ(std::string::npos, manifest.find("_system")) << manifest;
}
TEST_P(CollectionsTest,
       ClientOpcode_CollectionsGetManifest_with_euid_no_access) {
    BinprotGenericCommand command(ClientOpcode::CollectionsGetManifest);
    auto rsp = execute(*admin, command, true);
    ASSERT_EQ(Status::Success, rsp.getStatus()) << rsp.getDataString();
    const auto manifest = rsp.getDataString();
    EXPECT_EQ(std::string::npos, manifest.find("_system")) << manifest;
}
TEST_P(CollectionsTest, ClientOpcode_CollectionsGetManifest_access) {
    BinprotGenericCommand command(ClientOpcode::CollectionsGetManifest);
    auto rsp = execute(*admin, command);
    ASSERT_EQ(Status::Success, rsp.getStatus()) << rsp.getDataString();
    const auto manifest = rsp.getDataString();
    EXPECT_NE(std::string::npos, manifest.find("_system")) << manifest;
}
TEST_P(CollectionsTest,
       ClientOpcode_CollectionsGetManifest_with_euid_with_access) {
    BinprotGenericCommand command(ClientOpcode::CollectionsGetManifest);
    auto rsp = execute(*admin, command, true, true);
    ASSERT_EQ(Status::Success, rsp.getStatus()) << rsp.getDataString();
    const auto manifest = rsp.getDataString();
    EXPECT_NE(std::string::npos, manifest.find("_system")) << manifest;
}
TEST_P(CollectionsTest, ClientOpcode_CollectionsGetID_no_access) {
    BinprotGenericCommand command{
            ClientOpcode::CollectionsGetID, {}, "_system._system"};
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest, ClientOpcode_CollectionsGetID_with_euid_no_access) {
    BinprotGenericCommand command{
            ClientOpcode::CollectionsGetID, {}, "_system._system"};
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_CollectionsGetID_access) {
    BinprotGenericCommand command{
            ClientOpcode::CollectionsGetID, {}, "_system._system"};
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_CollectionsGetID_with_euid_with_access) {
    BinprotGenericCommand command{
            ClientOpcode::CollectionsGetID, {}, "_system._system"};
    execute(*admin, command, true, true, false, Status::Success);
}
TEST_P(CollectionsTest, ClientOpcode_CollectionsGetScopeID_no_access) {
    BinprotGenericCommand command(
            ClientOpcode::CollectionsGetScopeID, {}, "_system");
    executeInSystemCollectionWithoutAccess(*user, command, false);
}
TEST_P(CollectionsTest,
       ClientOpcode_CollectionsGetScopeID_with_euid_no_access) {
    BinprotGenericCommand command(
            ClientOpcode::CollectionsGetScopeID, {}, "_system");
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}
TEST_P(CollectionsTest, ClientOpcode_CollectionsGetScopeID_access) {
    BinprotGenericCommand command(
            ClientOpcode::CollectionsGetScopeID, {}, "_system");
    execute(*admin, command, false, false, false, Status::Success);
}
TEST_P(CollectionsTest,
       ClientOpcode_CollectionsGetScopeID_with_euid_with_access) {
    BinprotGenericCommand command(
            ClientOpcode::CollectionsGetScopeID, {}, "_system");
    execute(*admin, command, true, true, false, Status::Success);
}

TEST_P(CollectionsTest, ClientOpcode_CollectionByIDStats_no_access) {
    BinprotGenericCommand command(
            ClientOpcode::Stat,
            fmt::format("collections-byid {}", CollectionUid::systemCollection),
            {});
    executeInSystemCollectionWithoutAccess(*user, command, false);
}

TEST_P(CollectionsTest, ClientOpcode_CollectionByIDStats_with_euid_no_access) {
    BinprotGenericCommand command(
            ClientOpcode::Stat,
            fmt::format("collections-byid {}", CollectionUid::systemCollection),
            {});
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}

TEST_P(CollectionsTest, ClientOpcode_CollectionByIDStats_access) {
    // throw for failure
    admin->stats(fmt::format("collections-byid {}",
                             CollectionUid::systemCollection));
}

TEST_P(CollectionsTest,
       ClientOpcode_CollectionByIDStats_with_euid_with_access) {
    auto getFrameInfos = [this]() -> FrameInfoVector {
        FrameInfoVector ret;
        ret.emplace_back(
                std::make_unique<cb::mcbp::request::ImpersonateUserFrameInfo>(
                        euid));
        ret.emplace_back(std::make_unique<
                         cb::mcbp::request::
                                 ImpersonateUserExtraPrivilegeFrameInfo>(
                SystemCollectionLookup));
        return ret;
    };
    // throw for failure
    admin->stats(
            fmt::format("collections-byid {}", CollectionUid::systemCollection),
            getFrameInfos);
}

TEST_P(CollectionsTest, ClientOpcode_CollectionStats_no_access) {
    BinprotGenericCommand command(
            ClientOpcode::Stat, "collections _system._system", {});
    executeInSystemCollectionWithoutAccess(*user, command, false);
}

TEST_P(CollectionsTest, ClientOpcode_CollectionStats_with_euid_no_access) {
    BinprotGenericCommand command(
            ClientOpcode::Stat, "collections _system._system", {});
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}

TEST_P(CollectionsTest, ClientOpcode_CollectionStats_access) {
    // throw for failure
    admin->stats("collections _system._system");
}

TEST_P(CollectionsTest, ClientOpcode_CollectionStats_with_euid_with_access) {
    auto getFrameInfos = [this]() -> FrameInfoVector {
        FrameInfoVector ret;
        ret.emplace_back(
                std::make_unique<cb::mcbp::request::ImpersonateUserFrameInfo>(
                        euid));
        ret.emplace_back(std::make_unique<
                         cb::mcbp::request::
                                 ImpersonateUserExtraPrivilegeFrameInfo>(
                SystemCollectionLookup));
        return ret;
    };
    // throw for failure
    admin->stats("collections _system._system", getFrameInfos);
}

TEST_P(CollectionsTest, ClientOpcode_ScopeByIDStats_no_access) {
    BinprotGenericCommand command(
            ClientOpcode::Stat,
            fmt::format("scopes-byid {}", ScopeUid::systemScope),
            {});
    executeInSystemCollectionWithoutAccess(*user, command, false);
}

TEST_P(CollectionsTest, ClientOpcode_ScopeByIDStats_with_euid_no_access) {
    BinprotGenericCommand command(
            ClientOpcode::Stat,
            fmt::format("scopes-byid {}", ScopeUid::systemScope),
            {});
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}

TEST_P(CollectionsTest, ClientOpcode_ScopeByIDStats_access) {
    // throw for failure
    admin->stats(fmt::format("scopes-byid {}", ScopeUid::systemScope));
}

TEST_P(CollectionsTest, ClientOpcode_ScopeByIDStats_with_euid_with_access) {
    auto getFrameInfos = [this]() -> FrameInfoVector {
        FrameInfoVector ret;
        ret.emplace_back(
                std::make_unique<cb::mcbp::request::ImpersonateUserFrameInfo>(
                        euid));
        ret.emplace_back(std::make_unique<
                         cb::mcbp::request::
                                 ImpersonateUserExtraPrivilegeFrameInfo>(
                SystemCollectionLookup));
        return ret;
    };
    // throw for failure
    admin->stats(fmt::format("scopes-byid {}", ScopeUid::systemScope),
                 getFrameInfos);
}

TEST_P(CollectionsTest, ClientOpcode_ScopeStats_no_access) {
    BinprotGenericCommand command(ClientOpcode::Stat, "scopes _system", {});
    executeInSystemCollectionWithoutAccess(*user, command, false);
}

TEST_P(CollectionsTest, ClientOpcode_ScopeStats_with_euid_no_access) {
    BinprotGenericCommand command(ClientOpcode::Stat, "scopes _system", {});
    executeInSystemCollectionWithoutAccess(*admin, command, true);
}

TEST_P(CollectionsTest, ClientOpcode_ScopeStats_access) {
    // throw for failure
    admin->stats("scopes _system");
}

TEST_P(CollectionsTest, ClientOpcode_ScopeStats_with_euid_with_access) {
    auto getFrameInfos = [this]() -> FrameInfoVector {
        FrameInfoVector ret;
        ret.emplace_back(
                std::make_unique<cb::mcbp::request::ImpersonateUserFrameInfo>(
                        euid));
        ret.emplace_back(std::make_unique<
                         cb::mcbp::request::
                                 ImpersonateUserExtraPrivilegeFrameInfo>(
                SystemCollectionLookup));
        return ret;
    };
    // throw for failure
    admin->stats("scopes _system", getFrameInfos);
}

// Run the top-level "collections" and test the visibility of returned data
TEST_P(CollectionsTest, ClientOpcode_CollectionStats_check_visibility) {
    // Build a stat key for system and normal collection
    auto systemKey = fmt::format("{}:{}:name",
                                 ScopeUid::systemScope,
                                 CollectionUid::systemCollection);
    auto userKey = fmt::format(
            "{}:{}:name", ScopeUid::customer, CollectionUid::customer1);
    auto stats = userConnection->stats("collections");
    EXPECT_EQ(stats.find(systemKey), stats.end()) << stats.dump(2);
    EXPECT_NE(stats.find(userKey), stats.end()) << stats.dump(2);

    stats = admin->stats("collections");
    EXPECT_NE(stats.find(systemKey), stats.end()) << stats.dump(2);
    EXPECT_NE(stats.find(userKey), stats.end()) << stats.dump(2);
}

// Run the top-level "scopes" and test the visibility of returned data
TEST_P(CollectionsTest, ClientOpcode_ScopeStats_check_visibility) {
    // Build a stat key for system and normal collection
    auto systemKey = fmt::format("{}:name", ScopeUid::systemScope);
    auto userKey = fmt::format("{}:name", ScopeUid::customer);

    auto stats = userConnection->stats("scopes");
    EXPECT_EQ(stats.find(systemKey), stats.end()) << stats.dump(2);
    EXPECT_NE(stats.find(userKey), stats.end()) << stats.dump(2);

    stats = admin->stats("scopes");
    EXPECT_NE(stats.find(systemKey), stats.end()) << stats.dump(2);
    EXPECT_NE(stats.find(userKey), stats.end()) << stats.dump(2);
}

// Run the top-level "collections-details" and test the visibility of returned
// data
TEST_P(CollectionsTest, ClientOpcode_CollectionDetailsStats_check_visibility) {
    // Build a stat key for system and normal collection
    auto systemKey =
            fmt::format("vb_0:{}:name", CollectionUid::systemCollection);
    auto userKey = fmt::format("vb_0:{}:name", CollectionUid::customer1);
    auto stats = userConnection->stats("collections-details 0"); // vb:0
    EXPECT_EQ(stats.find(systemKey), stats.end()) << stats.dump(2);
    EXPECT_NE(stats.find(userKey), stats.end()) << stats.dump(2) << "\n\n"
                                                << userKey;

    stats = admin->stats("collections-details 0"); // vb:0

    EXPECT_NE(stats.find(systemKey), stats.end()) << stats.dump(2);
    EXPECT_NE(stats.find(userKey), stats.end()) << stats.dump(2);
}

TEST_P(CollectionsTest, ClientOpcode_ScopeDetailsStats_check_visibility) {
    // Build a stat key for system and normal scope
    auto systemKey = fmt::format("vb_0:{}:name:", ScopeUid::systemScope);
    auto userKey = fmt::format("vb_0:{}:name:", ScopeUid::customer);

    auto stats = userConnection->stats("scopes-details 0"); // vb:0
    EXPECT_EQ(stats.find(systemKey), stats.end()) << stats.dump(2);
    EXPECT_NE(stats.find(userKey), stats.end()) << stats.dump(2);

    stats = admin->stats("scopes-details 0"); // vb:0

    EXPECT_NE(stats.find(systemKey), stats.end()) << stats.dump(2);
    EXPECT_NE(stats.find(userKey), stats.end()) << stats.dump(2);
}