/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"
#include <string>
#include <vector>

class ItemCompressionTaskTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string += "compression_mode=active";
        SingleThreadedKVBucketTest::SetUp();
        store->enableItemCompressor();
    }

    void setCompressionMode(std::string mode) {
        engine->getConfiguration().setCompressionMode(mode);
        engine->setCompressionMode(mode);
    }

    void runNextTask() {
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        SingleThreadedKVBucketTest::runNextTask(lpNonioQ);
    }
};

TEST_F(ItemCompressionTaskTest, snooze_test) {
    std::vector<std::string> modes = {"off", "passive"};
    for (auto& mode : modes) {
        setCompressionMode(mode);
        runNextTask();
        ASSERT_TRUE(KVBucketTest::itemCompressorTaskIsSleepingForever());
    }

    setCompressionMode("active");
    runNextTask();
    ASSERT_FALSE(KVBucketTest::itemCompressorTaskIsSleepingForever());
}