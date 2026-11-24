/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "crc32.h"

#include <folly/portability/GTest.h>
#include <cstdint>
#include <random>
#include <string_view>
#include <vector>

namespace {
/**
 * Bit-at-a-time reference implementation of the (reflected) CRC-32 used by
 * crc32buf. It is intentionally simple and independent of any lookup tables
 * or hardware acceleration.
 */
uint32_t referenceCrc32(const uint8_t* buf, size_t len) {
    uint32_t crc = 0xffffffff;
    for (size_t idx = 0; idx < len; ++idx) {
        crc ^= buf[idx];
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
        }
    }
    return ~crc;
}

uint32_t checksum(std::string_view data) {
    return crc32buf(reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

/**
 * Map a key to a vBucket the same way as the clients (and
 * validateKeyMapping()) do.
 */
uint32_t keyToVbucket(std::string_view key, uint32_t numVbuckets) {
    return ((checksum(key) >> 16) & 0x7fff) % numVbuckets;
}
} // namespace

/** Verify the well known CRC-32 check values (and that this isn't CRC-32C) */
TEST(Crc32Test, KnownValues) {
    EXPECT_EQ(0x00000000U, checksum(""));
    EXPECT_EQ(0xe8b7be43U, checksum("a"));
    // The standard CRC-32 check value (CRC-32C would be 0xe3069283)
    EXPECT_EQ(0xcbf43926U, checksum("123456789"));
    EXPECT_EQ(0x414fa339U,
              checksum("The quick brown fox jumps over the lazy dog"));
}

/** The CRC is used to map keys to vBuckets so the mapping must never change */
TEST(Crc32Test, KeyToVbucketMapping) {
    EXPECT_EQ(115U, keyToVbucket("foo", 1024));
    EXPECT_EQ(182U, keyToVbucket("user::0000001", 1024));
    EXPECT_EQ(449U, keyToVbucket("pymc0", 1024));
    EXPECT_EQ(115U, keyToVbucket("foo", 128));
    EXPECT_EQ(54U, keyToVbucket("user::0000001", 128));
    EXPECT_EQ(65U, keyToVbucket("pymc0", 128));
}

/**
 * Compare against the reference implementation for all lengths up to a few
 * KiB and with different alignments, to cover all of the code paths used by
 * the accelerated implementations (short tails, blocks, unaligned starts).
 */
TEST(Crc32Test, MatchesReferenceImplementation) {
    std::mt19937 generator(0xc0ffee);
    std::uniform_int_distribution<int> distribution(0, 255);
    std::vector<uint8_t> data(4096 + 16);
    for (auto& byte : data) {
        byte = static_cast<uint8_t>(distribution(generator));
    }

    for (size_t offset = 0; offset < 16; ++offset) {
        for (size_t len = 0; len <= 4096; ++len) {
            const auto* ptr = data.data() + offset;
            ASSERT_EQ(referenceCrc32(ptr, len), crc32buf(ptr, len))
                    << "offset: " << offset << " len: " << len;
        }
    }
}
