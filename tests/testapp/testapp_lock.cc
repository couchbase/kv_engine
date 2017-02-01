/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "testapp.h"
#include "testapp_client_test.h"
#include <protocol/connection/client_mcbp_connection.h>

#include <algorithm>
#include <platform/compress.h>

class LockTest : public TestappClientTest {
public:
    void SetUp() override {
        document.info.cas = Greenstack::CAS::Wildcard;
        document.info.compression = Greenstack::Compression::None;
        document.info.datatype = Greenstack::Datatype::Json;
        document.info.flags = 0xcaffee;
        document.info.id = name;
        const std::string content = to_string(memcached_cfg, false);
        std::copy(content.begin(), content.end(),
                  std::back_inserter(document.value));
    }

protected:
    MemcachedBinprotConnection& getMcbpConnection() {
        return dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    }

    Document document;
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        LockTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

TEST_P(LockTest, LockNonexistingDocument) {
    auto& conn = getMcbpConnection();

    try {
        conn.get_and_lock(name, 0, 0);
        FAIL() << "It should not be possible to lock a non-existing document";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotFound());
    }
}

TEST_P(LockTest, LockIncorrectVBucket) {
    auto& conn = getMcbpConnection();

    try {
        conn.get_and_lock(name, 1, 0);
        FAIL() << "vbucket 1 should not exist";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotMyVbucket());
    }
}

TEST_P(LockTest, LockWithDefaultValue) {
    auto& conn = getMcbpConnection();

    conn.mutate(document, 0, Greenstack::MutationType::Add);
    conn.get_and_lock(name, 0, 0);
}

TEST_P(LockTest, LockWithTimeValue) {
    auto& conn = getMcbpConnection();

    conn.mutate(document, 0, Greenstack::MutationType::Add);
    conn.get_and_lock(name, 0, 5);
}


TEST_P(LockTest, LockLockedDocument) {
    auto& conn = getMcbpConnection();

    conn.mutate(document, 0, Greenstack::MutationType::Add);
    conn.get_and_lock(name, 0, 0);

    try {
        conn.get_and_lock(name, 0, 0);
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
    auto& conn = getMcbpConnection();
    conn.setXerrorSupport(false);

    conn.mutate(document, 0, Greenstack::MutationType::Add);
    conn.get_and_lock(name, 0, 0);

    try {
        conn.get_and_lock(name, 0, 0);
        FAIL() << "it is not possible to lock a locked document";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isTemporaryFailure()) << ex.what();
    }
}

TEST_P(LockTest, MutateLockedDocument) {
    auto& conn = getMcbpConnection();

    conn.mutate(document, 0, Greenstack::MutationType::Add);

    for (const auto op : {Greenstack::MutationType::Set,
                          Greenstack::MutationType::Replace,
                          Greenstack::MutationType::Append,
                          Greenstack::MutationType::Prepend}) {
        const auto locked = conn.get_and_lock(name, 0, 0);
        EXPECT_NE(uint64_t(-1), locked.info.cas);
        try {
            conn.mutate(document, 0, op);
            FAIL() << "It should not be possible to mutate a locked document";
        } catch (const ConnectionError& ex) {
            EXPECT_TRUE(ex.isLocked());
        }

        // But using the locked cas should work!
        document.info.cas = locked.info.cas;
        conn.mutate(document, 0, op);
    }
}

TEST_P(LockTest, ArithmeticLockedDocument) {
    auto& conn = getMcbpConnection();

    conn.arithmetic(name, 1);
    conn.get_and_lock(name, 0, 0);

    try {
        conn.arithmetic(name, 1);
        FAIL() << "incr/decr a locked document should not be possible";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isLocked());
    }

    // You can't unlock the data with incr
}

TEST_P(LockTest, DeleteLockedDocument) {
    auto& conn = getMcbpConnection();

    conn.mutate(document, 0, Greenstack::MutationType::Add);
    const auto locked = conn.get_and_lock(name, 0, 0);

    try {
        conn.remove(name, 0, 0);
        FAIL() << "Remove a locked document should not be possible";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isLocked());
    }

    conn.remove(name, 0, locked.info.cas);
}

TEST_P(LockTest, UnlockNoSuchDocument) {
    auto& conn = getMcbpConnection();
    try {
        conn.unlock(name, 0, 0xdeadbeef);
        FAIL() << "The document should not exist";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotFound());
    }
}

TEST_P(LockTest, UnlockInvalidVBucket) {
    auto& conn = getMcbpConnection();
    try {
        conn.unlock(name, 1, 0xdeadbeef);
        FAIL() << "The vbucket should not exist";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isNotMyVbucket());
    }
}

TEST_P(LockTest, UnlockWrongCas) {
    auto& conn = getMcbpConnection();
    conn.mutate(document, 0, Greenstack::MutationType::Add);
    const auto locked = conn.get_and_lock(name, 0, 0);

    try {
        conn.unlock(name, 0, locked.info.cas + 1);
        FAIL() << "The cas value should not match";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isAlreadyExists());
    }
}

TEST_P(LockTest, UnlockThereIsNoCasWildcard) {
    auto& conn = getMcbpConnection();
    conn.mutate(document, 0, Greenstack::MutationType::Add);
    const auto locked = conn.get_and_lock(name, 0, 0);

    try {
        conn.unlock(name, 0, 0);
        FAIL() << "The cas value should not match";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isInvalidArguments());
    }
}

TEST_P(LockTest, UnlockSuccess) {
    auto& conn = getMcbpConnection();
    conn.mutate(document, 0, Greenstack::MutationType::Add);
    const auto locked = conn.get_and_lock(name, 0, 0);
    conn.unlock(name, 0, locked.info.cas);

    // The document should no longer be locked
    conn.mutate(document, 0, Greenstack::MutationType::Set);
}
