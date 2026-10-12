/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "timings.h"

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

using namespace std::chrono_literals;

class TimingsTest : public ::testing::Test {
protected:
    Timings timings;
};

// Test behaviour of values which exceed the max tracked value of the underlying
// hdr_histogram are still counted in the number of values.
TEST_F(TimingsTest, Overflow) {
    // Setup - collect an initial value to initialise the histogram, so we
    // can determine its max tracked value.
    timings.collect(cb::mcbp::ClientOpcode::Get, 1ms);
    auto* histo =
            timings.get_timing_histogram(uint8_t(cb::mcbp::ClientOpcode::Get));
    ASSERT_TRUE(histo);
    ASSERT_EQ(1, histo->getValueCount());

    // Add a second value, which exceeds the highest tracked value of the
    // histogram.
    auto maxTracked = std::chrono::microseconds{histo->getMaxTrackableValue()};
    timings.collect(cb::mcbp::ClientOpcode::Get, maxTracked * 2);

    // Test - out of range values _should_ be included in the JSON output as
    // 'overflowed' and 'overflowed_sum' fields.
    auto json = histo->to_json();
    auto overflowed = json.find("overflowed");
    ASSERT_NE(json.end(), overflowed);
    ASSERT_TRUE(overflowed->is_number());
    EXPECT_EQ(1, overflowed->get<int64_t>());

    auto overflowedSum = json.find("overflowed_sum");
    ASSERT_NE(json.end(), overflowedSum);
    ASSERT_TRUE(overflowedSum->is_number());
    EXPECT_EQ(maxTracked.count() * 2, overflowedSum->get<int64_t>());

    // Also expect 'a max_trackable' to determine what constitutes 'overflowed'
    // values.
    auto maxTrackable = json.find("max_trackable");
    ASSERT_NE(json.end(), maxTrackable);
    ASSERT_TRUE(maxTrackable->is_number());
    EXPECT_EQ(maxTracked.count(), maxTrackable->get<int64_t>());
}

// Test that each mutation opcode is only counted once by the aggregated
// mutation stats used for cmd_mutation (SubdocArrayAddUnique was previously
// listed twice, so each op was counted twice).
TEST_F(TimingsTest, SubdocArrayAddUniqueCountedOnce) {
    timings.collect(cb::mcbp::ClientOpcode::SubdocArrayAddUnique, 1ms);
    EXPECT_EQ(1, timings.get_aggregated_mutation_stats());
}

// Test that the *WithMeta mutation opcodes are counted as mutations (and not
// as retrievals) by the aggregated stats used for cmd_mutation / cmd_lookup.
TEST_F(TimingsTest, WithMetaOpsCountedAsMutations) {
    const std::array withMetaOpcodes{cb::mcbp::ClientOpcode::AddWithMeta,
                                     cb::mcbp::ClientOpcode::AddqWithMeta,
                                     cb::mcbp::ClientOpcode::DelWithMeta,
                                     cb::mcbp::ClientOpcode::DelqWithMeta,
                                     cb::mcbp::ClientOpcode::MutateWithMeta,
                                     cb::mcbp::ClientOpcode::SetWithMeta,
                                     cb::mcbp::ClientOpcode::SetqWithMeta};
    for (const auto opcode : withMetaOpcodes) {
        timings.collect(opcode, 1ms);
    }

    EXPECT_EQ(withMetaOpcodes.size(), timings.get_aggregated_mutation_stats());
    EXPECT_EQ(0, timings.get_aggregated_retrieval_stats());
}
