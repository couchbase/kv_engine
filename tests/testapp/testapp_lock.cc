/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <algorithm>
#include <array>

class LockTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();

        document.info.cas = mcbp::cas::Wildcard;
        document.info.flags = 0xcaffee;
        document.info.id = name;
        document.value = memcached_cfg.dump();
    }

protected:
    Document document;
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         LockTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(LockTest, LockNonexistingDocument) {
    try {
        userConnection->get_and_lock(name, Vbid(0), 0);
        FAIL() << "It should not be possible to lock a non-existing document";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotFound());
    }
}

TEST_P(LockTest, LockIncorrectVBucket) {
    try {
        userConnection->get_and_lock(name, Vbid(1), 0);
        FAIL() << "vbucket 1 should not exist";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotMyVbucket());
    }
}

TEST_P(LockTest, LockWithDefaultValue) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    userConnection->get_and_lock(name, Vbid(0), 0);
}

TEST_P(LockTest, LockWithTimeValue) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    userConnection->get_and_lock(name, Vbid(0), 5);
}


TEST_P(LockTest, LockLockedDocument) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    userConnection->get_and_lock(name, Vbid(0), 0);

    try {
        userConnection->get_and_lock(name, Vbid(0), 0);
        FAIL() << "it is not possible to lock a locked document";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isLocked());
    }
}

/**
 * Verify that we return the correct error code when we try to lock
 * a locked item without XERROR enabled
 */
TEST_P(LockTest, MB_22459_LockLockedDocument_WithoutXerror) {
    userConnection->setXerrorSupport(false);

    userConnection->mutate(document, Vbid(0), MutationType::Add);
    userConnection->get_and_lock(name, Vbid(0), 0);

    try {
        userConnection->get_and_lock(name, Vbid(0), 0);
        FAIL() << "it is not possible to lock a locked document";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isTemporaryFailure()) << ex.what();
    }
}

TEST_P(LockTest, MutateLockedDocument) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);

    for (const auto op : {MutationType::Set,
                          MutationType::Replace,
                          MutationType::Append,
                          MutationType::Prepend}) {
        const auto locked = userConnection->get_and_lock(name, Vbid(0), 0);
        EXPECT_NE(uint64_t(-1), locked.info.cas);
        try {
            userConnection->mutate(document, Vbid(0), op);
            FAIL() << "It should not be possible to mutate a locked document";
        } catch (const ConnectionError& ex) {
            EXPECT_TRUE(ex.isLocked());
        }

        // But using the locked cas should work!
        document.info.cas = locked.info.cas;
        userConnection->mutate(document, Vbid(0), op);
    }
}

TEST_P(LockTest, ArithmeticLockedDocument) {
    userConnection->arithmetic(name, 1);
    userConnection->get_and_lock(name, Vbid(0), 0);

    try {
        userConnection->arithmetic(name, 1);
        FAIL() << "incr/decr a locked document should not be possible";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isLocked());
    }

    // You can't unlock the data with incr
}

TEST_P(LockTest, DeleteLockedDocument) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    const auto locked = userConnection->get_and_lock(name, Vbid(0), 0);

    try {
        userConnection->remove(name, Vbid(0), 0);
        FAIL() << "Remove a locked document should not be possible";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isLocked());
    }

    userConnection->remove(name, Vbid(0), locked.info.cas);
}

TEST_P(LockTest, UnlockNoSuchDocument) {
    try {
        userConnection->unlock(name, Vbid(0), 0xdeadbeef);
        FAIL() << "The document should not exist";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotFound());
    }
}

TEST_P(LockTest, UnlockInvalidVBucket) {
    try {
        userConnection->unlock(name, Vbid(1), 0xdeadbeef);
        FAIL() << "The vbucket should not exist";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotMyVbucket());
    }
}

TEST_P(LockTest, UnlockWrongCas) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    const auto locked = userConnection->get_and_lock(name, Vbid(0), 0);

    try {
        userConnection->unlock(name, Vbid(0), locked.info.cas + 1);
        FAIL() << "The cas value should not match";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isLocked());
    }
}

TEST_P(LockTest, UnlockThereIsNoCasWildcard) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    const auto locked = userConnection->get_and_lock(name, Vbid(0), 0);

    try {
        userConnection->unlock(name, Vbid(0), 0);
        FAIL() << "The cas value should not match";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isInvalidArguments());
    }
}

TEST_P(LockTest, UnlockSuccess) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    const auto locked = userConnection->get_and_lock(name, Vbid(0), 0);
    userConnection->unlock(name, Vbid(0), locked.info.cas);

    // The document should no longer be locked
    userConnection->mutate(document, Vbid(0), MutationType::Set);
}

/**
 * This test stores a document and then tries to unlock the same document
 * without specifying a lock timeout (use the default value). The bug we
 * had in the server was that it did not calculate the correct offset
 * for the key in the packet
 */
TEST_P(LockTest, MB_22778) {
    // I've modified the packet dump added in the bug report by changing
    // vbucket to 0 and not 0x4d (the offset for the vbucket id is 6 bytes)
    std::array<uint8_t, 110> store = {{0x80, 0x01, 0x00, 0x03, 0x08, 0x00,
                                         0x00, 0x00, 0x00, 0x00, 0x00, 0x56,
                                         0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
                                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                         0x02, 0x00, 0x00, 0x01, 0x00, 0x00,
                                         0x00, 0x00, 0x4e, 0x45, 0x54, 0x7b,
                                         0x22, 0x69, 0x64, 0x22, 0x3a, 0x22,
                                         0x4e, 0x45, 0x54, 0x22, 0x2c, 0x22,
                                         0x63, 0x61, 0x73, 0x22, 0x3a, 0x30,
                                         0x2c, 0x22, 0x65, 0x78, 0x70, 0x69,
                                         0x72, 0x79, 0x22, 0x3a, 0x30, 0x2c,
                                         0x22, 0x63, 0x6f, 0x6e, 0x74, 0x65,
                                         0x6e, 0x74, 0x22, 0x3a, 0x7b, 0x22,
                                         0x6e, 0x61, 0x6d, 0x65, 0x22, 0x3a,
                                         0x22, 0x43, 0x6f, 0x75, 0x63, 0x68,
                                         0x62, 0x61, 0x73, 0x65, 0x22, 0x7d,
                                         0x2c, 0x22, 0x74, 0x6f, 0x6b, 0x65,
                                         0x6e, 0x22, 0x3a, 0x6e, 0x75, 0x6c,
                                         0x6c, 0x7d}};

    Frame command;
    std::copy(store.begin(), store.end(), std::back_inserter(command.payload));

    userConnection->sendFrame(command);

    BinprotResponse response;
    userConnection->recvResponse(response);
    EXPECT_EQ(cb::mcbp::Status::Success, response.getStatus())
            << to_string(response.getStatus());

    std::array<uint8_t, 27> lock = {{0x80, // magic
                                         0x94, // opcode
                                         0x00, 0x03, // keylen
                                         0x00, // extlen
                                         0x00, // datatype
                                         0x00, 0x00, // vbucket
                                         0x00, 0x00, 0x00, 0x03, // bodylen
                                         0x00, 0x00, 0x00, 0x06, // opaque
                                         0x00, 0x00, 0x00, 0x00, //
                                         0x00, 0x00, 0x00, 0x00, //  - cas
                                         0x4e, 0x45, 0x54}}; // key

    command.reset();
    std::copy(lock.begin(), lock.end(), std::back_inserter(command.payload));
    userConnection->sendFrame(command);
    userConnection->recvResponse(response);
    EXPECT_EQ(cb::mcbp::Status::Success, response.getStatus())
            << to_string(response.getStatus());

    userConnection->remove("NET", Vbid(0), response.getCas());
}
