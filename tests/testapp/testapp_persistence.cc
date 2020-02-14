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
#include <platform/dirutils.h>
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
        try {
            cb::io::rmrf(mcd_env->getDbPath());
        } catch (...) { /* nothing exists */
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
        return TestappTest::storeAndPersistItem(vbid, key);
    }

    void waitForAtLeastSeqno(uint64_t uuid, uint64_t seqno) {
        TestappTest::waitForAtLeastSeqno(vbid, uuid, seqno);
    }

    void shutdownMemcached(ShutdownMode mode) {
        switch (mode) {
        case ShutdownMode::Unclean:
#ifdef WIN32
            // There's no direct equivalent of SIGKILL for Windows;
            // TerminateProcess() behaves like SIGTERM - it allows pending IO
            // to complete; however it's the best we have...
            TerminateProcess(pidTToHandle(server_pid), 0);
#else
            kill(server_pid, SIGKILL);
#endif
            break;
        case ShutdownMode::Clean: {
            auto& admin = getAdminConnection();
            BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Shutdown);
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
    ShutdownTest::SetUp();

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

        // It was observed that this test originally did not clear up the old
        // files before running so all of the seqnos were wrong and could not be
        // relied on. The following seqno expectation ensures that this test is
        // run in the correct environment.
        EXPECT_EQ(i * 2 + 1, mutation.seqno);

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
    ShutdownTest::SetUp();

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

        // Check that all keys ABOVE high+1 do not exist.
        for (size_t i = highNumber + 2; i < docCount; i++) {
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
INSTANTIATE_TEST_SUITE_P(Clean,
                         PersistToTest,
                         ::testing::Values(ShutdownMode::Clean),
                         ::testing::PrintToStringParamName());
#else
INSTANTIATE_TEST_SUITE_P(CleanOrUnclean,
                         PersistToTest,
                         ::testing::Values(ShutdownMode::Clean,
                                           ShutdownMode::Unclean),
                         ::testing::PrintToStringParamName());
#endif
