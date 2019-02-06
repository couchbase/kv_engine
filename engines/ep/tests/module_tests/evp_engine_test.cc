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
#include "kv_bucket.h"
#include "taskqueue.h"

#include "programs/engine_testapp/mock_server.h"
#include "tests/module_tests/test_helpers.h"

#include <configuration_impl.h>
#include <platform/dirutils.h>

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
    EXPECT_EQ(ENGINE_SUCCESS, create_instance(get_mock_server_api, &handle))
            << "Failed to create ep engine instance";
    engine_v1 = reinterpret_cast<EngineIface*>(handle);

    engine = reinterpret_cast<EventuallyPersistentEngine*>(handle);
    ObjectRegistry::onSwitchThread(engine);

    // Add dbname to config string.
    std::string config = config_string;
    if (config.size() > 0) {
        config += ";";
    }
    config += "dbname=" + std::string(test_dbname);

    // Set the bucketType
    config += ";bucket_type=" + bucketType;

    EXPECT_EQ(ENGINE_SUCCESS, engine->initialize(config.c_str()))
        << "Failed to initialize engine.";

    // Wait for warmup to complete.
    while (engine->getKVBucket()->isWarmingUp()) {
        usleep(10);
    }

    // Once warmup is complete, set VB to active.
    engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);

    cookie = create_mock_cookie();
}

void EventuallyPersistentEngineTest::TearDown() {
    // Need to force the destroy (i.e. pass true) because
    // NonIO threads may have been disabled (see DCPTest subclass).
    engine_v1->destroy(true);
    destroy_mock_event_callbacks();
    destroy_mock_cookie(cookie);
    destroy_engine();
    // Cleanup any files we created.
    cb::io::rmrf(test_dbname);
}

void EventuallyPersistentEngineTest::store_item(Vbid vbid,
                                                const std::string& key,
                                                const std::string& value) {
    Item item(makeStoredDocKey(key),
              /*flags*/ 0,
              /*exp*/ 0,
              value.c_str(),
              value.size(),
              PROTOCOL_BINARY_RAW_BYTES,
              0 /*cas*/,
              -1 /*seqno*/,
              vbid);
    uint64_t cas;
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->storeInner(cookie, &item, cas, OPERATION_SET));
}

void EventuallyPersistentEngineTest::store_pending_item(
        Vbid vbid, const std::string& key, const std::string& value) {
    auto item = makePendingItem(makeStoredDocKey(key), value);
    uint64_t cas;
    EXPECT_EQ(ENGINE_EWOULDBLOCK,
              engine->storeInner(cookie, item.get(), cas, OPERATION_SET))
            << "pending SyncWrite should initially block (until durability "
               "met).";
}

const char EventuallyPersistentEngineTest::test_dbname[] = "ep_engine_ep_unit_tests_db";


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

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_CASE_P(EphemeralOrPersistent,
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

INSTANTIATE_TEST_CASE_P(EphemeralOrPersistent,
                        DurabilityTest,
                        ::testing::Values("persistent", "ephemeral"),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
