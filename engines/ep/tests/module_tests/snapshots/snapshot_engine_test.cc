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

#include "ep_bucket.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"

#include <gtest/gtest.h>

class SnapshotEngineTest : public SingleThreadedEPBucketTest,
                           public ::testing::WithParamInterface<std::string> {
public:
    void SetUp() override {
        SingleThreadedEPBucketTest::SetUp();
    }
};

TEST_P(SnapshotEngineTest, nmvb) {
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
    setVBucketState(vbid, vbucket_state_replica);
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot_no_disk_state) {
    // Active VB with nothing on disk
    setVBucketState(vbid, vbucket_state_active);

    // this is returning failed, but should not be something to fail rebalance.
    EXPECT_EQ(cb::engine_errc::failed,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json manifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, [&manifest](auto& m) { manifest = m; }));
    EXPECT_TRUE(manifest.contains("uuid"));

    if (isMagma()) {
        // Not sure what we can assume?
        FAIL() << "No magma testing\n";
    }
    EXPECT_EQ(1, manifest["files"].size());
    EXPECT_EQ(1, manifest["files"][0]["id"]);
    EXPECT_EQ("0.couch.1", manifest["files"][0]["path"]);
    EXPECT_GT(manifest["files"][0]["size"], 0);
}

static std::string PrintToStringParamName(
        const testing::TestParamInfo<std::string>& info) {
    return info.param;
}

// todo: add magma (and maybe nexus)
INSTANTIATE_TEST_SUITE_P(SnapshotEngineTests,
                         SnapshotEngineTest,
                         ::testing::Values("persistent_couchdb"),
                         PrintToStringParamName);