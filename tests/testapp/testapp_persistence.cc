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
#include <string.h>
#include <cerrno>
#include <csignal>

#include "testapp_shutdown.h"

/// The different types of shutdown we can trigger.
enum class ShutdownMode { Clean, Unclean };

std::ostream& operator<<(std::ostream& os, const ShutdownMode& mode) {
    os << (mode == ShutdownMode::Clean ? "Clean" : "Unclean");
    return os;
}

/**
 * Tests Persist_To functionality for bucket types which support it (i.e. EP
 * Bucket).
 *
 * Reuses the functionality of ShutdownTest to start / stop memcached for each
 * test instance.
 */
class PersistToTest : public ShutdownTest,
                      public ::testing::WithParamInterface<ShutdownMode> {
protected:
    void SetUp() override {
        if (!mcd_env->getTestBucket().supportsPersistence()) {
            std::cout << "Note: skipping test '"
                      << ::testing::UnitTest::GetInstance()
                                 ->current_test_info()
                                 ->name()
                      << "' as persistence isn't supported.\n";
            skipTest = true;
            return;
        }
        ShutdownTest::SetUp();
    }

    void TearDown() override {
        if (skipTest) {
            return;
        }
        ShutdownTest::TearDown();
    }

    // Helper functions for tests /////////////////////////////////////////////

    Document storeAndPersistItem(std::string key) {
        MemcachedConnection& conn = getConnection();
        conn.setMutationSeqnoSupport(true);
        Document doc;
        doc.info.id = key;
        doc.value = "persist me";
        auto mutation = conn.mutate(doc, vbid, MutationType::Set);
        EXPECT_NE(0, mutation.seqno);
        EXPECT_NE(0, mutation.vbucketuuid);
        doc.info.cas = mutation.cas;

        waitForAtLeastSeqno(mutation.vbucketuuid, mutation.seqno);

        return doc;
    }

    void waitForAtLeastSeqno(uint64_t uuid, uint64_t seqno) {
        // Poll for that sequence number to be persisted.
        ObserveInfo observe;
        MemcachedConnection& conn = getConnection();
        do {
            observe = conn.observeSeqno(vbid, uuid);
            EXPECT_EQ(0, observe.formatType);
            EXPECT_EQ(vbid, observe.vbId);
            EXPECT_EQ(uuid, observe.uuid);
        } while (observe.lastPersistedSeqno < seqno);
    }

    void shutdownMemcached(ShutdownMode mode) {
        switch (mode) {
        case ShutdownMode::Unclean:
#ifdef WIN32
            // There's no direct equivalent of SIGKILL for Windows;
            // TerminateProcess() behaves like SIGTERM - it allows pending IO
            // to complete; however it's the best we have...
            TerminateProcess(server_pid, 0);
#else
            kill(server_pid, SIGKILL);
#endif
            break;
        case ShutdownMode::Clean: {
            auto& admin = getAdminConnection();
            BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_SHUTDOWN);
            cmd.setCas(token);
            admin.sendCommand(cmd);

            BinprotResponse rsp;
            admin.recvResponse(rsp);
            EXPECT_TRUE(rsp.isSuccess());
            break;
        }
        }
        waitForShutdown(mode == ShutdownMode::Unclean);
    }

    Vbid vbid = Vbid(0);
    bool skipTest = false;
};

/**
 * Verify that items are successfully persisted (and can be read) after a
 * clean / unclean shutdown and restart.
 */
TEST_P(PersistToTest, PersistedAfterShutdown) {
    if (skipTest) {
        return;
    }

    // Store 1 item, noting it's sequence number
    Vbid vbid = Vbid(0);
    auto doc = storeAndPersistItem("1");

    // Shutdown of memcached.
    shutdownMemcached(GetParam());

    // Restart memcached, and attempt to read the item we persisted.
    SetUp();

    MemcachedConnection& conn = getConnection();
    try {
        auto doc2 = conn.get(doc.info.id, vbid);
        EXPECT_EQ(doc, doc2);
    } catch (const ConnectionError& e) {
        FAIL() << e.what();
    }
}

/**
 * Verify that the vBucket is in a consistent state after a shutdown;
 * even if there are missing items.
 *
 * By "consistent", what we mean here is that for a sequence of items
 * {key, seqno} : ({1,a}, {2,b}, {3,c}, ...) written to a vBucket,
 * that if seqno N was persisted then all seqnos <N were also persisted,
 * and nonee of seqnos >N were persisted.
 *
 * To test this, create a sequence of keys, additionally writing
 * the current highest key to a additional "high" doc between each key:
 *
 *     {1, high=1, 2, high=2, 3, high=3, 4, high=4, ...
 *
 * Then shutdown memcached when it's in the processes of persisting these keys
 * (making sure "high" has been persisted at least once).
 *
 * On restart, we read what value "high" has (i.e. how far through the sequence
 * persistence got). We then verify that the vBucket is in one of two valid
 * states:
 *
 * a) "high" was the very last document persisted - which means that the key
 *    matching the value of "high", and all proceeding keys should exist.
 *
 * b) "high" was the last but one document persisted - which means that there
 *    is one additional key in existence (named high+1)
 *
 * Any other state is invalid and hence a failure.
 */
TEST_P(PersistToTest, ConsistentStateAfterShutdown) {
    if (skipTest) {
        return;
    }

    // Start off with persistence disabled.
    {
        auto& admin = getAdminConnection();
        admin.selectBucket("default");
        admin.disablePersistence();
    }

    MemcachedConnection& conn = getConnection();
    conn.setMutationSeqnoSupport(true);

    // Store our series of documents:1, high=1, 2, high=2, 3, ...
    Document high;
    high.info.id = "high";
    uint64_t uuid;
    const size_t docCount = 100;

    for (size_t i = 0; i < docCount; i++) {
        Document doc;
        doc.info.id = std::to_string(i);
        doc.value = doc.info.id;
        auto mutation = conn.mutate(doc, vbid, MutationType::Set);
        uuid = mutation.vbucketuuid;

        high.value = doc.info.id;
        conn.mutate(high, vbid, MutationType::Set);
    }

    // Re-enable persistence, and check we've stored to at least seqno 2 -
    // i.e. one iteration of high being written.
    {
        auto& admin = getAdminConnection();
        admin.selectBucket("default");
        admin.enablePersistence();
    }

    waitForAtLeastSeqno(uuid, 2);

    // Perform a shutdown of memcached.
    shutdownMemcached(GetParam());

    // Restart memcached.
    SetUp();

    // Read "high" to determine how far we got, and then validate that (1)
    // all previous documents exist and (2) no more than 1 extra document exists
    // after high.
    {
        MemcachedConnection& conn = getConnection();
        high = conn.get("high", vbid);

        // Check that all keys up to "high" exist:
        size_t highNumber = std::stoi(high.value);
        for (size_t i = 0; i < highNumber; i++) {
            conn.get(std::to_string(i), vbid);
        }

        // We permit the key one above "high" to not exist - see state (b)
        // above.

        // Check that all keys above high+1 do not exist.
        for (size_t i = highNumber + 1; i < docCount; i++) {
            auto key = std::to_string(i);
            try {
                conn.get(key, vbid);
            } catch (ConnectionError&) {
                // expect the get to fail.
                continue;
            }
            FAIL() << "Found key '" << key << "'"
                   << " which should not exist";
        }
    }
}

// MB-27539: ThreadSanitizer detects false positives on 'Clean' shutdown
// tests run after 'Unclean' shutdown tests
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define SKIP_UNCLEAN
#endif
#endif

#if defined(SKIP_UNCLEAN)
INSTANTIATE_TEST_CASE_P(Clean,
                        PersistToTest,
                        ::testing::Values(ShutdownMode::Clean),
                        ::testing::PrintToStringParamName());
#else
INSTANTIATE_TEST_CASE_P(CleanOrUnclean,
                        PersistToTest,
                        ::testing::Values(ShutdownMode::Clean,
                                          ShutdownMode::Unclean),
                        ::testing::PrintToStringParamName());
#endif
