/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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
#include "taskqueue.h"
#include "tests/module_tests/test_helpers.h"

#include <boost/algorithm/string/join.hpp>
#include <configuration_impl.h>
#include <platform/dirutils.h>
#include <chrono>
#include <thread>

EventuallyPersistentEngineTest::EventuallyPersistentEngineTest()
    : test_dbname(dbnameFromCurrentGTestInfo()), bucketType("persistent") {
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

    // Setup an engine with a single active vBucket.
    EXPECT_EQ(ENGINE_SUCCESS,
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
    config += ";bucket_type=" + bucketType;

    // Setup vBucket and Shard count
    config += ";max_vbuckets=" + std::to_string(numVbuckets) +
              ";max_num_shards=" + std::to_string(numShards);

    EXPECT_EQ(ENGINE_SUCCESS, engine->initialize(config.c_str()))
        << "Failed to initialize engine.";

    // Wait for warmup to complete.
    while (engine->getKVBucket()->isWarmingUp()) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    // Once warmup is complete, set VB to active.
    engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);

    cookie = create_mock_cookie(engine);
}

void EventuallyPersistentEngineTest::TearDown() {
    destroy_mock_cookie(cookie);
    // Need to force the destroy (i.e. pass true) because
    // NonIO threads may have been disabled (see DCPTest subclass).
    engine->destroy(true);
    ExecutorPool::shutdown();
    // Cleanup any files we created.
    cb::io::rmrf(test_dbname);
}

queued_item EventuallyPersistentEngineTest::store_item(
        Vbid vbid, const std::string& key, const std::string& value) {
    auto item = makeCommittedItem(makeStoredDocKey(key), value);
    uint64_t cas;
    EXPECT_EQ(
            ENGINE_SUCCESS,
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
            ENGINE_EWOULDBLOCK,
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
            ENGINE_EWOULDBLOCK,
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
            ENGINE_SUCCESS,
            engine->storeInner(cookie, *item, cas, StoreSemantics::Set, false));
}

TEST_P(SetParamTest, requirements_bucket_type) {
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
            EXPECT_EQ(cb::mcbp::Status::Success, ret)
                    << "Parameter " << v.param
                    << "could not be set on bucket type \"" << bucketType
                    << "\"";
        } else {
            EXPECT_EQ(cb::mcbp::Status::Einval, ret)
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
TEST_P(SetParamTest, compressionModeConfigTest) {
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
    ASSERT_EQ(cb::mcbp::Status::Success,
              engine->setFlushParam("compression_mode", "off", msg));
    EXPECT_EQ(BucketCompressionMode::Off, engine->getCompressionMode());

    ASSERT_EQ(cb::mcbp::Status::Success,
              engine->setFlushParam("compression_mode", "passive", msg));
    EXPECT_EQ(BucketCompressionMode::Passive, engine->getCompressionMode());

    ASSERT_EQ(cb::mcbp::Status::Success,
              engine->setFlushParam("compression_mode", "active", msg));
    EXPECT_EQ(BucketCompressionMode::Active, engine->getCompressionMode());

    EXPECT_EQ(cb::mcbp::Status::Einval,
              engine->setFlushParam("compression_mode", "invalid", msg));
}

/**
 * Test to verify if the min compression ratio in the configuration
 * is updated then the min compression ratio in the engine is
 * also updated correctly
 */
TEST_P(SetParamTest, minCompressionRatioConfigTest) {
    Configuration& config = engine->getConfiguration();

    config.setMinCompressionRatio(1.8f);
    EXPECT_FLOAT_EQ(1.8f, engine->getMinCompressionRatio());

    // The compressed length can be greater than the uncompressed length
    config.setMinCompressionRatio(0.5f);
    EXPECT_FLOAT_EQ(0.5f, engine->getMinCompressionRatio());

    // Set a negative value, that should result in an error
    EXPECT_THROW(config.setMinCompressionRatio(-1), std::range_error);

    std::string msg;
    ASSERT_EQ(cb::mcbp::Status::Success,
              engine->setFlushParam("min_compression_ratio", "1.8", msg));
    EXPECT_FLOAT_EQ(1.8f, engine->getMinCompressionRatio());

    ASSERT_EQ(cb::mcbp::Status::Success,
              engine->setFlushParam("min_compression_ratio", "0.5", msg));
    EXPECT_FLOAT_EQ(0.5f, engine->getMinCompressionRatio());

    EXPECT_EQ(cb::mcbp::Status::Einval,
              engine->setFlushParam("min_compression_ratio", "-1", msg));
}

TEST_P(SetParamTest, DynamicConfigValuesModifiable) {
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
            if (engine->setFlushParam(key, value, msg) == Status::Success) {
                handled.push_back("setFlushParam");
            }
            if (engine->setReplicationParam(key, value, msg) ==
                Status::Success) {
                handled.push_back("setReplicationParam");
            }
            if (engine->setCheckpointParam(key, value, msg) ==
                Status::Success) {
                handled.push_back("setCheckpointParam");
            }
            if (engine->setDcpParam(key, value, msg) == Status::Success) {
                handled.push_back("setDcpParam");
                return;
            }
            if (engine->setVbucketParam(Vbid(0), key, value, msg) ==
                Status::Success) {
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

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         SetParamTest,
                         ::testing::Values("persistent", "ephemeral"),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

TEST_P(DurabilityTest, TimeoutTaskScheduled) {
    auto* executor = dynamic_cast<MockExecutorPool*>(ExecutorPool::get());
    ASSERT_TRUE(executor);
    EXPECT_TRUE(
            executor->isTaskScheduled(NONIO_TASK_IDX, "DurabilityTimeoutTask"));
}

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
                                  gsl::not_null<const void*> cookie) {
        stats[std::string(key.data(), key.size())] =
                std::string(value.data(), value.size());
    };

    engine->getKVBucket()->setVBucketState(Vbid(1), vbucket_state_active);
    engine->getKVBucket()->setVBucketState(Vbid(2), vbucket_state_active);

    // get stats for all vbs
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->get_stats(
                      cookie, "vbucket-durability-state", {}, dummyAddStats));

    // 4 stats, 3 vbs, 12 total
    EXPECT_EQ(12, stats.size());

    for (int vb = 0; vb < 3; vb++) {
        expectStatsForVB(vb);
    }

    stats.clear();

    int vb = 1;

    // get stats for vb 1
    EXPECT_EQ(
            ENGINE_SUCCESS,
            engine->get_stats(cookie,
                              "vbucket-durability-state " + std::to_string(vb),
                              {},
                              dummyAddStats));

    // 4 stats for one specified vb
    EXPECT_EQ(4, stats.size());
    expectStatsForVB(vb);
}

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         DurabilityTest,
                         ::testing::Values("persistent", "ephemeral"),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });
