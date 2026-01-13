/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustermap_utils.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <string_view>

using namespace cb;

TEST(ParseActiveVBucketsInFastForwardMap, ValidConfigWithActiveVBuckets) {
    // Config with 3 nodes, thisNode is at index 1
    // vbucket 0: [0, 1, 2] - node 1 is replica
    // vbucket 1: [1, 2, 0] - node 1 is active
    // vbucket 2: [2, 0, 1] - node 1 is replica
    // vbucket 3: [1, 0, 2] - node 1 is active
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1, 2],
                [1, 2, 0],
                [2, 0, 1],
                [1, 0, 2]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(2, result->active);
    EXPECT_EQ(2, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, ValidConfigWithNoActiveVBuckets) {
    // Config where thisNode (index 2) has no active vbuckets but has replicas
    // vbucket 0: [0, 1, 2] - node 2 is replica
    // vbucket 1: [1, 2, 0] - node 2 is replica
    // vbucket 2: [0, 2, 1] - node 2 is replica
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1, 2],
                [1, 2, 0],
                [0, 2, 1]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0, result->active);
    EXPECT_EQ(3, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, ValidConfigAllActiveVBuckets) {
    // Config where thisNode (index 0) has all active vbuckets and no replicas
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1],
                [0, 1],
                [0, 1],
                [0, 1]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(4, result->active);
    EXPECT_EQ(0, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, MissingNodesExt) {
    constexpr std::string_view config = R"({
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1, 2]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, NodesExtNotArray) {
    constexpr std::string_view config = R"({
        "nodesExt": "not an array",
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1, 2]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, EmptyNodesExt) {
    constexpr std::string_view config = R"({
        "nodesExt": [],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1, 2]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, NoThisNode) {
    // thisNode field is omitted from all nodes
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, ThisNodeNotBoolean) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": "true"}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, MissingVBucketMap) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ]
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, VBucketMapNotObject) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": []
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, MissingVBucketMapForward) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {}
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, VBucketMapForwardNotArray) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": "not an array"
        }
    })";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, EmptyVBucketMapForward) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": []
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0, result->active);
    EXPECT_EQ(0, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, InvalidJSON) {
    const char* config = "{ invalid json }";

    auto result = getFutureVbucketCounts(config);
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, EmptyString) {
    auto result = getFutureVbucketCounts("");
    EXPECT_FALSE(result.has_value());
}

TEST(ParseActiveVBucketsInFastForwardMap, VBucketArrayWithNonIntegerValues) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                ["string", 1, 2],
                [0, 1, 2]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    // First vbucket is skipped (not an integer at index 0), second is counted
    // as active
    EXPECT_EQ(1, result->active);
    EXPECT_EQ(0, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, VBucketMapForwardWithEmptyArrays) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [],
                [0, 1],
                []
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    // Only the second vbucket is valid and matches node 0 as active
    EXPECT_EQ(1, result->active);
    EXPECT_EQ(0, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap,
     VBucketMapForwardWithNonArrayElements) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                "not an array",
                [0, 1],
                {"key": "value"}
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    // Only the second element is a valid array matching node 0 as active
    EXPECT_EQ(1, result->active);
    EXPECT_EQ(0, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, MultipleNodesWithThisNodeTrue) {
    // Edge case: multiple nodes marked as thisNode (should use first match)
    // vbucket 0: [1, 0, 2] - node 1 is active
    // vbucket 1: [2, 1, 0] - node 1 is replica
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [1, 0, 2],
                [2, 1, 0]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    // Should use first thisNode found (index 1)
    EXPECT_EQ(1, result->active);
    EXPECT_EQ(1, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, LargeNumberOfVBuckets) {
    // Test with realistic number of vbuckets (1024)
    // Pattern: [i%3, (i+1)%3, (i+2)%3]
    // i=0: [0, 1, 2] - node 0 is active
    // i=1: [1, 2, 0] - node 0 is replica at index 2
    // i=2: [2, 0, 1] - node 0 is replica at index 1
    // i=3: [0, 1, 2] - node 0 is active
    nlohmann::json config = {
            {"nodesExt",
             {{{"services", {{"kv", 11210}}}, {"thisNode", true}},
              {{"services", {{"kv", 11210}}}},
              {{"services", {{"kv", 11210}}}}}},
            {"vBucketServerMap",
             {{"vBucketMapForward", nlohmann::json::array()}}}};

    // Add 1024 vbuckets with pattern [i%3, (i+1)%3, (i+2)%3]
    auto& vbucketMapForward = config["vBucketServerMap"]["vBucketMapForward"];
    for (int i = 0; i < 1024; i++) {
        vbucketMapForward.push_back({i % 3, (i + 1) % 3, (i + 2) % 3});
    }

    auto result = getFutureVbucketCounts(config.dump());
    ASSERT_TRUE(result.has_value());
    // Node 0 is active when i % 3 == 0: vbuckets 0, 3, 6, ..., 1023 = 342
    // Node 0 is replica when i % 3 == 1 (at index 2): 341 vbuckets
    // Node 0 is replica when i % 3 == 2 (at index 1): 341 vbuckets
    // Total replicas: 682
    EXPECT_EQ(342, result->active);
    EXPECT_EQ(682, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, NegativeNodeIndex) {
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [-1, 0, 1],
                [0, 1, 2]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    // First vbucket has -1 as active, node 0 is replica
    // Second vbucket has node 0 as active
    EXPECT_EQ(1, result->active);
    EXPECT_EQ(1, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, OnlyReplicaVBuckets) {
    // Config where thisNode (index 1) only has replica vbuckets
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1, 2],
                [2, 1, 0],
                [0, 1, 2]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0, result->active);
    EXPECT_EQ(3, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, MixedActiveAndReplica) {
    // Config where thisNode (index 1) has both active and replica vbuckets
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [1, 2, 0],
                [0, 1, 2],
                [1, 0, 2],
                [2, 0, 1],
                [1, 2, 0]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(3, result->active); // vbuckets 0, 2, 4
    EXPECT_EQ(2, result->replica); // vbuckets 1, 3
}

TEST(ParseActiveVBucketsInFastForwardMap, NodeNotInAnyVBucket) {
    // Config where thisNode (index 3) doesn't appear in any vbucket
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1, 2],
                [1, 2, 0],
                [2, 0, 1]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0, result->active);
    EXPECT_EQ(0, result->replica);
}

TEST(ParseActiveVBucketsInFastForwardMap, SingleReplicaPerVBucket) {
    // Config with only one replica per vbucket
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [1],
                [0],
                [1]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(1, result->active); // vbucket 1
    EXPECT_EQ(0, result->replica); // No replicas
}

TEST(ParseActiveVBucketsInFastForwardMap, ReplicaSetToMinusOne) {
    // Config where replica positions are set to -1 (no replica configured)
    // vbucket 0: [0, -1, -1] - node 0 is active, no replicas configured
    // vbucket 1: [1, 0, -1] - node 0 is replica at index 1
    // vbucket 2: [0, 1, -1] - node 0 is active, third replica not configured
    constexpr std::string_view config = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, -1, -1],
                [1, 0, -1],
                [0, 1, -1]
            ]
        }
    })";

    auto result = getFutureVbucketCounts(config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(2, result->active); // vbuckets 0 and 2
    EXPECT_EQ(1, result->replica); // vbucket 1 (at index 1)
}

// Tests for signature

TEST(FFMapSignature, IdenticalMapsProduceSameSignature) {
    // Two configs with identical vBucketMapForward should produce same
    // signature
    constexpr std::string_view config1 = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1],
                [1, 0],
                [0, 1]
            ]
        }
    })";

    constexpr std::string_view config2 = R"({
        "nodesExt": [
            {"services": {"kv": 11210}},
            {"services": {"kv": 11210}, "thisNode": true}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1],
                [1, 0],
                [0, 1]
            ]
        }
    })";

    EXPECT_NE(config1, config2);

    auto result1 = getFutureVbucketCounts(config1);
    auto result2 = getFutureVbucketCounts(config2);

    ASSERT_TRUE(result1.has_value());
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(result1->signature, result2->signature);
}

TEST(FFMapSignature, DifferentVBucketMapProducesDifferentSignature) {
    // Two configs with different vBucketMapForward should produce different
    // signatures.
    constexpr std::string_view config1 = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1],
                [1, 0]
            ]
        }
    })";

    constexpr std::string_view config2 = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [1, 0],
                [0, 1]
            ]
        }
    })";

    EXPECT_NE(config1, config2);

    auto result1 = getFutureVbucketCounts(config1);
    auto result2 = getFutureVbucketCounts(config2);

    ASSERT_TRUE(result1.has_value());
    ASSERT_TRUE(result2.has_value());
    // Counts are the same, but this isn't the same map, so signature should
    // differ
    EXPECT_EQ(result1->active, result2->active);
    EXPECT_EQ(result1->replica, result2->replica);
    EXPECT_NE(result1->signature, result2->signature);
}

TEST(FFMapSignature, AddingVBucketChangesSignature) {
    constexpr std::string_view config1 = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1],
                [1, 0]
            ]
        }
    })";

    constexpr std::string_view config2 = R"({
        "nodesExt": [
            {"services": {"kv": 11210}, "thisNode": true},
            {"services": {"kv": 11210}}
        ],
        "vBucketServerMap": {
            "vBucketMapForward": [
                [0, 1],
                [1, 0],
                [0, 1]
            ]
        }
    })";

    auto result1 = getFutureVbucketCounts(config1);
    auto result2 = getFutureVbucketCounts(config2);

    ASSERT_TRUE(result1.has_value());
    ASSERT_TRUE(result2.has_value());
    EXPECT_NE(result1->signature, result2->signature);
}

TEST(FFMapSignature, LargeMap) {
    // Test that large maps produce consistent signatures
    nlohmann::json config = {
            {"nodesExt",
             {{{"services", {{"kv", 11210}}}, {"thisNode", true}},
              {{"services", {{"kv", 11210}}}},
              {{"services", {{"kv", 11210}}}}}},
            {"vBucketServerMap",
             {{"vBucketMapForward", nlohmann::json::array()}}}};

    auto& vbucketMapForward = config["vBucketServerMap"]["vBucketMapForward"];
    for (int i = 0; i < 1024; i++) {
        vbucketMapForward.push_back({i % 3, (i + 1) % 3, (i + 2) % 3});
    }

    auto configStr = config.dump();
    auto result1 = getFutureVbucketCounts(configStr);
    auto result2 = getFutureVbucketCounts(configStr);

    ASSERT_TRUE(result1.has_value());
    ASSERT_TRUE(result2.has_value());
    // Same input should always produce same signature
    EXPECT_EQ(result1->signature, result2->signature);
}

TEST(FFMapSignature, ModifyingLargeMap) {
    // Test that modifying just one vbucket in a large map changes the signature
    nlohmann::json config1 = {
            {"nodesExt",
             {{{"services", {{"kv", 11210}}}, {"thisNode", true}},
              {{"services", {{"kv", 11210}}}},
              {{"services", {{"kv", 11210}}}}}},
            {"vBucketServerMap",
             {{"vBucketMapForward", nlohmann::json::array()}}}};

    auto& vbucketMapForward1 = config1["vBucketServerMap"]["vBucketMapForward"];
    for (int i = 0; i < 100; i++) {
        vbucketMapForward1.push_back({i % 3, (i + 1) % 3, (i + 2) % 3});
    }

    // Create a second config identical to the first
    nlohmann::json config2 = config1;
    // Modify a vbucket
    config2["vBucketServerMap"]["vBucketMapForward"][1] =
            nlohmann::json::array({2, 0, 1});

    auto result1 = getFutureVbucketCounts(config1.dump());
    auto result2 = getFutureVbucketCounts(config2.dump());

    ASSERT_TRUE(result1.has_value());
    ASSERT_TRUE(result2.has_value());
    // Modifying a single vbucket should change the signature
    EXPECT_NE(result1->signature, result2->signature);
    EXPECT_NE(config1, config2);
}
