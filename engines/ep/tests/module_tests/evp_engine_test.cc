/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
 * Unit tests for the EventuallyPersistentEngine class.
 */

#include "evp_engine_test.h"

#include "ep_engine.h"
#include "item.h"
#include "kv_bucket.h"
#include "objectregistry.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/module_tests/test_helpers.h"
#include "vb_visitors.h"
#include <executor/cb3_taskqueue.h>

#include <boost/algorithm/string/join.hpp>
#include <configuration_impl.h>
#include <folly/synchronization/Baton.h>
#include <platform/dirutils.h>
#include <chrono>
#include <filesystem>
#include <thread>

EventuallyPersistentEngineTest::EventuallyPersistentEngineTest()
    : test_dbname(dbnameFromCurrentGTestInfo()) {
}

void EventuallyPersistentEngineTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    try {
        cb::io::rmrf(test_dbname);
    } catch (std::system_error& e) {
        if (e.code() != std::error_code(ENOENT, std::system_category())) {
            throw e;
        }
    }

    // Create an ExecutorPool unless its already created by SetUp in a subclass
    if (!ExecutorPool::exists()) {
        ExecutorPool::create();
    }

    initializeEngine();
}

void EventuallyPersistentEngineTest::initializeEngine() {
    // Setup an engine with a single active vBucket.
    EXPECT_EQ(cb::engine_errc::success,
              create_ep_engine_instance(get_mock_server_api, &handle))
            << "Failed to create ep engine instance";
    engine = reinterpret_cast<EventuallyPersistentEngine*>(handle);
    ObjectRegistry::onSwitchThread(engine);

    // Add dbname to config string.
    std::string config = config_string;
    if (!config.empty()) {
        config += ";";
    }
    config += "dbname=" + test_dbname;

    // Set the bucketType
    if (!bucketType.empty()) {
        config += ";";
        config += generateBucketTypeConfig(bucketType);
    }

    // Setup vBucket and Shard count
    config += ";max_vbuckets=" + std::to_string(numVbuckets) +
              ";max_num_shards=" + std::to_string(numShards);

    if (bucketType == "persistent_magma") {
        config += ";" + magmaConfig;
    }

    EXPECT_EQ(cb::engine_errc::success, engine->initialize(config))
            << "Failed to initialize engine.";

    // Wait for warmup to complete.
    while (engine->getKVBucket()->isWarmupLoadingData()) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    // Once warmup is complete, set VB to active.
    engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);

    cookie = create_mock_cookie(engine);
}

void EventuallyPersistentEngineTest::TearDown() {
    shutdownEngine();
    ExecutorPool::shutdown();
    // Cleanup any files we created - ignore if they don't exist
    try {
        cb::io::rmrf(test_dbname);
    } catch (std::system_error& e) {
        if (e.code() != std::error_code(ENOENT, std::system_category())) {
            throw e;
        }
    }
}

void EventuallyPersistentEngineTest::shutdownEngine() {
    if (cookie) {
        destroy_mock_cookie(cookie);
        cookie = nullptr;
    }
    if (engine) {
        // Need to force the destroy (i.e. pass true) because
        // NonIO threads may have been disabled (see DCPTest subclass).
        engine->destroy(true);
        engine = nullptr;
    }
}

queued_item EventuallyPersistentEngineTest::store_item(
        Vbid vbid, const std::string& key, const std::string& value) {
    auto item = makeCommittedItem(makeStoredDocKey(key), value);
    item->setVBucketId(vbid);
    uint64_t cas;
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->storeInner(cookie, *item, cas, StoreSemantics::Set, false));
    return item;
}

queued_item EventuallyPersistentEngineTest::store_pending_item(
        Vbid vbid,
        const std::string& key,
        const std::string& value,
        cb::durability::Requirements reqs) {
    auto item = makePendingItem(makeStoredDocKey(key), value, reqs);
    uint64_t cas;
    EXPECT_EQ(
            cb::engine_errc::would_block,
            engine->storeInner(cookie, *item, cas, StoreSemantics::Set, false))
            << "pending SyncWrite should initially block (until durability "
               "met).";
    return item;
}

queued_item EventuallyPersistentEngineTest::store_pending_delete(
        Vbid vbid, const std::string& key, cb::durability::Requirements reqs) {
    auto item = makePendingItem(makeStoredDocKey(key), {}, reqs);
    item->setDeleted(DeleteSource::Explicit);
    uint64_t cas;
    EXPECT_EQ(
            cb::engine_errc::would_block,
            engine->storeInner(cookie, *item, cas, StoreSemantics::Set, false))
            << "pending SyncDelete should initially block (until durability "
               "met).";
    return item;
}

void EventuallyPersistentEngineTest::store_committed_item(
        Vbid vbid, const std::string& key, const std::string& value) {
    auto item = makeCommittedviaPrepareItem(makeStoredDocKey(key), value);
    uint64_t cas;
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->storeInner(cookie, *item, cas, StoreSemantics::Set, false));
}

TEST_P(EPEngineParamTest, requirements_bucket_type) {
    std::string bucketType = engine->getConfiguration().getBucketType();

    struct value_t {
        std::string param;
        std::string value;
        std::string bucketType;
    };

    std::vector<value_t> values{
            // Parameter, Example value, applicable bucket
            {"access_scanner_enabled", "true", "persistent"},
            {"alog_sleep_time", "1441", "persistent"},
            {"alog_task_time", "3", "persistent"},
            {"ephemeral_full_policy", "auto_delete", "ephemeral"},
    };

    std::string msg;

    for (auto v : values) {
        auto ret = engine->setFlushParam(v.param.c_str(), v.value.c_str(), msg);
        if (bucketType == v.bucketType) {
            EXPECT_EQ(cb::engine_errc::success, ret)
                    << "Parameter " << v.param
                    << "could not be set on bucket type \"" << bucketType
                    << "\"";
        } else {
            EXPECT_EQ(cb::engine_errc::invalid_arguments, ret)
                    << "Setting parameter " << v.param
                    << "should be invalid for bucket type \"" << bucketType
                    << "\"";
        }
    }
}

/**
 * Test to verify if the compression mode in the configuration
 * is updated then the compression mode in the engine is
 * also updated correctly
 */
TEST_P(EPEngineParamTest, compressionModeConfigTest) {
    Configuration& config = engine->getConfiguration();

    config.setCompressionMode("off");
    EXPECT_EQ(BucketCompressionMode::Off, engine->getCompressionMode());

    config.setCompressionMode("passive");
    EXPECT_EQ(BucketCompressionMode::Passive, engine->getCompressionMode());

    config.setCompressionMode("active");
    EXPECT_EQ(BucketCompressionMode::Active, engine->getCompressionMode());

    EXPECT_THROW(config.setCompressionMode("invalid"), std::range_error);
    EXPECT_EQ(BucketCompressionMode::Active, engine->getCompressionMode());

    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam("compression_mode", "off", msg));
    EXPECT_EQ(BucketCompressionMode::Off, engine->getCompressionMode());

    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam("compression_mode", "passive", msg));
    EXPECT_EQ(BucketCompressionMode::Passive, engine->getCompressionMode());

    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam("compression_mode", "active", msg));
    EXPECT_EQ(BucketCompressionMode::Active, engine->getCompressionMode());

    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              engine->setFlushParam("compression_mode", "invalid", msg));
}

/**
 * Test to verify if the min compression ratio in the configuration
 * is updated then the min compression ratio in the engine is
 * also updated correctly
 */
TEST_P(EPEngineParamTest, minCompressionRatioConfigTest) {
    Configuration& config = engine->getConfiguration();

    config.setMinCompressionRatio(1.8f);
    EXPECT_FLOAT_EQ(1.8f, engine->getMinCompressionRatio());

    // The compressed length can be greater than the uncompressed length
    config.setMinCompressionRatio(0.5f);
    EXPECT_FLOAT_EQ(0.5f, engine->getMinCompressionRatio());

    // Set a negative value, that should result in an error
    EXPECT_THROW(config.setMinCompressionRatio(-1), std::range_error);

    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam("min_compression_ratio", "1.8", msg));
    EXPECT_FLOAT_EQ(1.8f, engine->getMinCompressionRatio());

    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam("min_compression_ratio", "0.5", msg));
    EXPECT_FLOAT_EQ(0.5f, engine->getMinCompressionRatio());

    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              engine->setFlushParam("min_compression_ratio", "-1", msg));
}

TEST_P(EPEngineParamTest, DynamicConfigValuesModifiable) {
    Configuration& config = engine->getConfiguration();

    // For each dynamic config variable (should be possible to change at
    // runtime), attempt to set (to the same as it's current value).
    config.visit([this](const std::string& key,
                        bool dynamic,
                        std::string value) {
        std::vector<std::string> handled;
        if (dynamic) {
            std::string msg;
            using namespace cb::mcbp;
            if (engine->setFlushParam(key, value, msg) ==
                cb::engine_errc::success) {
                handled.push_back("setFlushParam");
            }
            if (engine->setReplicationParam(key, value, msg) ==
                cb::engine_errc::success) {
                handled.push_back("setReplicationParam");
            }
            if (engine->setCheckpointParam(key, value, msg) ==
                cb::engine_errc::success) {
                handled.push_back("setCheckpointParam");
            }
            if (engine->setDcpParam(key, value, msg) ==
                cb::engine_errc::success) {
                handled.push_back("setDcpParam");
                return;
            }
            if (engine->setVbucketParam(Vbid(0), key, value, msg) ==
                cb::engine_errc::success) {
                handled.push_back("setVBucketParam");
            }
            if (handled.empty()) {
                ADD_FAILURE() << "Dynamic config key \"" << key
                              << "\" cannot be set via any of the set...Param "
                                 "methods.";
            } else if (handled.size() > 1) {
                ADD_FAILURE()
                        << "Dynamic config key \"" << key
                        << "\" should only be settable by a single "
                           "set...Param() method - actually settable via: ["
                        << boost::algorithm::join(handled, ", ") << "]";
            }
        }
    });
}

TEST_P(EPEngineParamTest, VBucketSanityChecking) {
    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setFlushParam(
                      "vbucket_mapping_sanity_checking", "true", msg));
    ASSERT_EQ(
            cb::engine_errc::success,
            engine->setFlushParam("vbucket_mapping_sanity_checking_error_mode",
                                  "throw",
                                  msg));

    EXPECT_THROW(store_item(Vbid(0), "key", "value"), std::logic_error);

    engine->getKVBucket()->setVBucketState(Vbid(1), vbucket_state_active);
    EXPECT_NO_THROW(store_item(Vbid(1), "key", "value"));
}

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         EPEngineParamTest,
                         EPEngineParamTest::allConfigValues(),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

TEST_P(DurabilityTest, DurabilityStateStats) {
    // test that vbucket-durability-state stats group includes (only) the
    // expected stats
    std::map<std::string, std::string> stats{};

    auto expectStatsForVB = [&stats](int vb) {
        EXPECT_NE(stats.end(),
                  stats.find("vb_" + std::to_string(vb) + ":high_seqno"));
        EXPECT_NE(stats.end(),
                  stats.find("vb_" + std::to_string(vb) + ":topology"));
        EXPECT_NE(stats.end(),
                  stats.find("vb_" + std::to_string(vb) +
                             ":high_prepared_seqno"));
        EXPECT_NE(stats.end(), stats.find("vb_" + std::to_string(vb)));
    };

    auto dummyAddStats = [&stats](std::string_view key,
                                  std::string_view value,
                                  const auto&) {
        stats[std::string(key.data(), key.size())] =
                std::string(value.data(), value.size());
    };

    engine->getKVBucket()->setVBucketState(Vbid(1), vbucket_state_active);
    engine->getKVBucket()->setVBucketState(Vbid(2), vbucket_state_active);

    // get stats for all vbs
    EXPECT_EQ(cb::engine_errc::success,
              engine->get_stats(
                      *cookie, "vbucket-durability-state", {}, dummyAddStats));

    // 4 stats, 3 vbs, 12 total
    EXPECT_EQ(12, stats.size());

    for (int vb = 0; vb < 3; vb++) {
        expectStatsForVB(vb);
    }

    stats.clear();

    int vb = 1;

    // get stats for vb 1
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->get_stats(*cookie,
                              "vbucket-durability-state " + std::to_string(vb),
                              {},
                              dummyAddStats));

    // 4 stats for one specified vb
    EXPECT_EQ(4, stats.size());
    expectStatsForVB(vb);
}

namespace {
struct ESTestDestructor {
    static int instances;
    ESTestDestructor() {
        ++instances;
    }
    ESTestDestructor(const ESTestDestructor&) {
        ++instances;
    }
    ESTestDestructor(ESTestDestructor&&) {
        ++instances;
    }
    ~ESTestDestructor() {
        --instances;
    }
};
int ESTestDestructor::instances{0};
} // namespace

TEST_P(EPEnginePersistentTest, EngineSpecificStorageGetsReleased) {
    {
        ESTestDestructor test;
        EXPECT_EQ(1, ESTestDestructor::instances);
        MockCookie cookie(engine);
        engine->storeEngineSpecific(&cookie, test);
        EXPECT_EQ(2, ESTestDestructor::instances);
    }
    EXPECT_EQ(0, ESTestDestructor::instances);
}

TEST_P(EPEnginePersistentTest, EngineSpecificStorageCanBeReadBack) {
    MockCookie cookie(engine);
    engine->storeEngineSpecific(&cookie, 1);
    EXPECT_EQ(1, *engine->getEngineSpecific<int>(&cookie));
}

TEST_P(EPEnginePersistentTest, EngineSpecificStorageCanBeCleared) {
    MockCookie cookie(engine);
    engine->storeEngineSpecific(&cookie, 1);
    ASSERT_TRUE(engine->getEngineSpecific<int>(&cookie).has_value());
    engine->clearEngineSpecific(&cookie);
    EXPECT_FALSE(engine->getEngineSpecific<int>(&cookie).has_value());
}

TEST_P(EPEnginePersistentTest, EngineSpecificStorageThrowsBadCast) {
    MockCookie cookie(engine);
    engine->storeEngineSpecific(&cookie, 1);
    ASSERT_TRUE(engine->getEngineSpecific<int>(&cookie).has_value());
    EXPECT_THROW(engine->getEngineSpecific<char>(&cookie), std::bad_cast);
}

TEST_P(EPEnginePersistentTest, ShardCountsOnSecondBucketInit) {
    auto originalShardCount = engine->getWorkLoadPolicy().getNumShards();
    auto newShardCount = originalShardCount + 1;

    // We populate the config with shards from this value in the initialize fn
    numShards = newShardCount;
    shutdownEngine();
    initializeEngine();

    if (bucketType == "persistent_magma") {
        EXPECT_EQ(originalShardCount,
                  engine->getWorkLoadPolicy().getNumShards());
    } else {
        EXPECT_EQ(newShardCount, engine->getWorkLoadPolicy().getNumShards());
    }
}

TEST_P(EPEnginePersistentTest, EngineInitReadOnlyDataDir) {
    store_item(vbid, "key", "value");

    shutdownEngine();
    using namespace std::filesystem;
    // As we're modifying the directory we still need execute permissions or we
    // can't even look in it.
    permissions(test_dbname,
                perms::others_read | perms::owner_read | perms::group_read |
                        perms::others_exec | perms::owner_exec |
                        perms::group_exec);

    std::string config = config_string;
    config += "dbname=" + test_dbname;
    config += ";max_vbuckets=" + std::to_string(numVbuckets) +
              ";max_num_shards=" + std::to_string(numShards) + ";";

    // Set the bucketType
    config += generateBucketTypeConfig(bucketType);

    EXPECT_EQ(cb::engine_errc::success,
              create_ep_engine_instance(get_mock_server_api, &handle))
            << "Failed to create ep engine instance";
    engine = reinterpret_cast<EventuallyPersistentEngine*>(handle);
    ObjectRegistry::onSwitchThread(engine);

    // Should come up fine, but in some sort of read only mode
    EXPECT_EQ(cb::engine_errc::success, engine->initialize(config));

    // Set the filesystem permissions back for the next test
    permissions(test_dbname, perms::all);

    // Reset our cookie to have a ptr to the new engine which is required when
    // we destroy it in TearDown()
    cookie = create_mock_cookie(engine);
}

// Tests that engine initializes fine even if the data dir doesn't exist
TEST_P(EPEnginePersistentTest, EngineInitNoDataDir) {
    shutdownEngine();

    cb::io::rmrf(test_dbname);

    std::string config = config_string;
    config += "dbname=" + test_dbname + ";";

    // Set the bucketType
    config += generateBucketTypeConfig(bucketType);

    EXPECT_EQ(cb::engine_errc::success,
              create_ep_engine_instance(get_mock_server_api, &handle))
            << "Failed to create ep engine instance";
    engine = reinterpret_cast<EventuallyPersistentEngine*>(handle);
    ObjectRegistry::onSwitchThread(engine);

    EXPECT_EQ(cb::engine_errc::success, engine->initialize(config));
    cookie = create_mock_cookie(engine);
}

INSTANTIATE_TEST_SUITE_P(Persistent,
                         EPEnginePersistentTest,
                         EPEngineParamTest::persistentConfigValues(),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         DurabilityTest,
                         EPEngineParamTest::allConfigValues(),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

/**
 * Regression test for MB-48925 - if a Task is scheduled against a Taskable
 * (Bucket) which has already been unregistered, then the ExecutorPool throws
 * and crashes the process.
 * Note: This test as it stands will *not* crash kv-engine if the fix for the
 * issue (see rest of this commit) is reverted. This is because the fix is
 * to change the currentVb Task member variable from an (owning)
 * shared_ptr<VBucket> to a (non-owning) VBucket* - the same thing TestVisior
 * below does. However it is included here for reference as to the original
 * problematic scenario.
 */
TEST_F(EventuallyPersistentEngineTest, MB48925_ScheduleTaskAfterUnregistered) {
    class TestVisitor : public InterruptableVBucketVisitor {
    public:
        TestVisitor(int& visitCount,
                    folly::Baton<>& waitForVisit,
                    folly::Baton<>& waitForDeinitialise)
            : visitCount(visitCount),
              waitForVisit(waitForVisit),
              waitForDeinitialise(waitForDeinitialise) {
        }

        void visitBucket(VBucket& vb) override {
            if (visitCount++ == 0) {
                currentVb = &vb;
                // On first call to visitBucket() perform the necessary
                // interleaved baton wait / sleeping.
                // Suspend execution of this thread; and allow main thread to
                // continue, delete Bucket and unregisterTaskable.
                waitForVisit.post();

                // Keep task running until unregisterTaskable() has been called
                // and starts to cancel tasks - this ensures that the Task
                // object is still alive (ExecutorPool has a reference to it)
                // and hence is passed out from unregisterTaskable(), hence kept
                // alive past when KVBucket is deleted.
                waitForDeinitialise.wait();
            }
        }
        InterruptableVBucketVisitor::ExecutionState shouldInterrupt() override {
            return ExecutionState::Continue;
        }

        int& visitCount;
        folly::Baton<>& waitForVisit;
        folly::Baton<>& waitForDeinitialise;

        // Model the behaviour of PagingVisitor prior to the bugfix. Note that
        // _if_ this is changed to a shared_ptr<VBucket> then we crash.
        VBucket* currentVb;
    };

    int visitCount{0};
    folly::Baton waitForVisit;
    folly::Baton waitForUnregister;
    engine->getKVBucket()->visitAsync(
            std::make_unique<TestVisitor>(
                    visitCount, waitForVisit, waitForUnregister),
            "MB48925_ScheduleTaskAfterUnregistered",
            TaskId::ExpiredItemPagerVisitor,
            std::chrono::seconds{1});
    waitForVisit.wait();

    // Setup testing hook so we allow our TestVisitor's Task above to
    // continue once we are inside unregisterTaskable.
    ExecutorPool::get()->unregisterTaskablePostCancelHook =
            [&waitForUnregister]() { waitForUnregister.post(); };

    // Delete the vbucket; so the file deletion will be performed by
    // VBucket::DeferredDeleter when the last reference goes out of scope
    // (expected to be the ExpiryPager.
    engine->getKVBucket()->deleteVBucket(vbid);

    // Destroy the engine. This does happen implicitly in TearDown, but call
    // it earlier because we need to call destroy() before our various Baton
    // local variables etc go out of scope.
    shutdownEngine();
}
