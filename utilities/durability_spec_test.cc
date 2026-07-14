/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <memcached/durability_spec.h>
#include <nlohmann/json.hpp>

using namespace cb::durability;

// =====================================================================
// to_string(Level) / to_level()
// =====================================================================

TEST(DurabilityLevel, ToString) {
    EXPECT_EQ("none", to_string(Level::None));
    EXPECT_EQ("majority", to_string(Level::Majority));
    EXPECT_EQ("majority_and_persist_on_master",
              to_string(Level::MajorityAndPersistOnMaster));
    EXPECT_EQ("persist_to_majority", to_string(Level::PersistToMajority));
}

TEST(DurabilityLevel, ToLevel) {
    EXPECT_EQ(Level::None, to_level("none"));
    EXPECT_EQ(Level::Majority, to_level("majority"));
    EXPECT_EQ(Level::MajorityAndPersistOnMaster,
              to_level("majority_and_persist_on_master"));
    EXPECT_EQ(Level::PersistToMajority, to_level("persist_to_majority"));
}

TEST(DurabilityLevel, ToLevelUnknownThrows) {
    try {
        to_level("bogus");
        FAIL() << "Expected to_level to throw for an unknown level";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("cb::durability::to_level: unknown level bogus", e.what());
    }
}

TEST(DurabilityLevel, ComparisonOperators) {
    EXPECT_LT(Level::None, Level::Majority);
    EXPECT_LT(Level::Majority, Level::MajorityAndPersistOnMaster);
    EXPECT_LT(Level::MajorityAndPersistOnMaster, Level::PersistToMajority);
    EXPECT_GT(Level::PersistToMajority, Level::None);
}

// =====================================================================
// to_string(Timeout)
// =====================================================================

TEST(DurabilityTimeout, ToStringDefault) {
    EXPECT_EQ("default", to_string(Timeout()));
}

TEST(DurabilityTimeout, ToStringInfinite) {
    EXPECT_EQ("infinite", to_string(Timeout::Infinity()));
}

TEST(DurabilityTimeout, ToStringNumeric) {
    EXPECT_EQ("1234", to_string(Timeout(1234)));
}

// =====================================================================
// Requirements: construction, accessors, isValid(), to_string()
// =====================================================================

TEST(DurabilityRequirements, DefaultConstructor) {
    Requirements req;
    EXPECT_EQ(Level::Majority, req.getLevel());
    EXPECT_TRUE(req.getTimeout().isDefault());
}

TEST(DurabilityRequirements, SettersAndGetters) {
    Requirements req;
    req.setLevel(Level::PersistToMajority);
    req.setTimeout(Timeout(42));
    EXPECT_EQ(Level::PersistToMajority, req.getLevel());
    EXPECT_EQ(42, req.getTimeout().get());
}

TEST(DurabilityRequirements, IsValid) {
    EXPECT_FALSE(Requirements(Level::None, Timeout()).isValid());
    EXPECT_TRUE(Requirements(Level::Majority, Timeout()).isValid());
    EXPECT_TRUE(Requirements(Level::MajorityAndPersistOnMaster, Timeout())
                        .isValid());
    EXPECT_TRUE(Requirements(Level::PersistToMajority, Timeout()).isValid());
}

TEST(DurabilityRequirements, NoRequirementsConstant) {
    EXPECT_EQ(Level::None, NoRequirements.getLevel());
    EXPECT_TRUE(NoRequirements.getTimeout().isDefault());
    EXPECT_FALSE(NoRequirements.isValid());
}

TEST(DurabilityRequirements, ToStringDefaultTimeout) {
    EXPECT_EQ("{majority, timeout=default}",
              to_string(Requirements(Level::Majority, Timeout())));
}

TEST(DurabilityRequirements, ToStringNumericTimeout) {
    EXPECT_EQ("{persist_to_majority, timeout=500}",
              to_string(Requirements(Level::PersistToMajority, Timeout(500))));
}

TEST(DurabilityRequirements, ToStringInfiniteTimeout) {
    EXPECT_EQ("{majority, timeout=infinite}",
              to_string(Requirements(Level::Majority, Timeout::Infinity())));
}

// =====================================================================
// to_json(Requirements)
// =====================================================================

TEST(DurabilityRequirements, ToJsonLevels) {
    nlohmann::json obj;
    to_json(obj, Requirements(Level::None, Timeout()));
    EXPECT_EQ("None", obj["level"]);

    to_json(obj, Requirements(Level::Majority, Timeout()));
    EXPECT_EQ("Majority", obj["level"]);

    to_json(obj, Requirements(Level::MajorityAndPersistOnMaster, Timeout()));
    EXPECT_EQ("MajorityAndPersistOnMaster", obj["level"]);

    to_json(obj, Requirements(Level::PersistToMajority, Timeout()));
    EXPECT_EQ("PersistToMajority", obj["level"]);
}

TEST(DurabilityRequirements, ToJsonTimeoutDefault) {
    nlohmann::json obj;
    to_json(obj, Requirements(Level::Majority, Timeout()));
    EXPECT_EQ("Default", obj["timeout"]);
}

TEST(DurabilityRequirements, ToJsonTimeoutInfinite) {
    nlohmann::json obj;
    to_json(obj, Requirements(Level::Majority, Timeout::Infinity()));
    EXPECT_EQ("Infinite", obj["timeout"]);
}

TEST(DurabilityRequirements, ToJsonTimeoutNumeric) {
    nlohmann::json obj;
    to_json(obj, Requirements(Level::Majority, Timeout(777)));
    EXPECT_EQ(777, obj["timeout"]);
}

TEST(DurabilityRequirements, ToJsonClearsPreviousContent) {
    // to_json() must clear() the object first so stale keys from a
    // previously-populated json object don't leak through.
    nlohmann::json obj;
    obj["stale"] = "value";
    to_json(obj, Requirements(Level::Majority, Timeout()));
    EXPECT_FALSE(obj.contains("stale"));
}

// =====================================================================
// Requirements(cb::const_byte_buffer): on-the-wire parsing
// =====================================================================

TEST(DurabilityRequirementsFromBuffer, OneByteLevelOnly) {
    for (auto level : {Level::Majority,
                       Level::MajorityAndPersistOnMaster,
                       Level::PersistToMajority}) {
        std::vector<uint8_t> buffer{static_cast<uint8_t>(level)};
        Requirements req(buffer);
        EXPECT_EQ(level, req.getLevel());
        EXPECT_TRUE(req.getTimeout().isDefault());
    }
}

TEST(DurabilityRequirementsFromBuffer, ThreeBytesWithTimeout) {
    // Timeout is encoded in network (big-endian) byte order: 0x1234.
    std::vector<uint8_t> buffer{
            static_cast<uint8_t>(Level::PersistToMajority), 0x12, 0x34};
    Requirements req(buffer);
    EXPECT_EQ(Level::PersistToMajority, req.getLevel());
    EXPECT_EQ(0x1234, req.getTimeout().get());
}

TEST(DurabilityRequirementsFromBuffer, RejectsBucketDefaultTimeoutValue) {
    std::vector<uint8_t> buffer{
            static_cast<uint8_t>(Level::Majority), 0x00, 0x00};
    EXPECT_THROW(Requirements{buffer}, std::invalid_argument);
}

TEST(DurabilityRequirementsFromBuffer, RejectsInfiniteTimeoutValue) {
    std::vector<uint8_t> buffer{
            static_cast<uint8_t>(Level::Majority), 0xff, 0xff};
    EXPECT_THROW(Requirements{buffer}, std::invalid_argument);
}

TEST(DurabilityRequirementsFromBuffer, InvalidBufferSizeThrows) {
    for (std::size_t size : {0u, 2u, 4u, 5u}) {
        std::vector<uint8_t> buffer(size, 0x01);
        bool detected = false;
        try {
            Requirements req(buffer);
        } catch (const std::invalid_argument& e) {
            EXPECT_EQ("Requirements(): Invalid sized buffer provided: " +
                              std::to_string(size),
                      std::string(e.what()));
            detected = true;
        }
        EXPECT_TRUE(detected) << "Did not detect invalid buffer size " << size;
    }
}

TEST(DurabilityRequirementsFromBuffer, InvalidLevelNoneThrows) {
    std::vector<uint8_t> buffer{static_cast<uint8_t>(Level::None)};
    EXPECT_THROW(Requirements{buffer}, std::runtime_error);
}

TEST(DurabilityRequirementsFromBuffer, InvalidLevelOutOfRangeThrows) {
    std::vector<uint8_t> buffer{4};
    EXPECT_THROW(Requirements{buffer}, std::runtime_error);
}
