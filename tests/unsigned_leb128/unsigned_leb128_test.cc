/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <gtest/gtest.h>
#include <mcbp/protocol/unsigned_leb128.h>

#include <limits>
#include <random>

template <class T>
class UnsignedLeb128 : public ::testing::Test {};

using MyTypes = ::testing::Types<uint8_t, uint16_t, uint32_t, uint64_t>;
TYPED_TEST_CASE(UnsignedLeb128, MyTypes);

TEST(UnsignedLeb128, MaxSize) {
    EXPECT_EQ(2, cb::mcbp::unsigned_leb128<uint8_t>::getMaxSize());
    EXPECT_EQ(3, cb::mcbp::unsigned_leb128<uint16_t>::getMaxSize());
    EXPECT_EQ(5, cb::mcbp::unsigned_leb128<uint32_t>::getMaxSize());
    EXPECT_EQ(10, cb::mcbp::unsigned_leb128<uint64_t>::getMaxSize());
}

TYPED_TEST(UnsignedLeb128, EncodeDecode0) {
    cb::mcbp::unsigned_leb128<TypeParam> zero(0);
    EXPECT_EQ(1, zero.get().size());
    EXPECT_EQ(0, zero.get().data()[0]);
    auto rv = cb::mcbp::decode_unsigned_leb128<TypeParam>(zero.get());
    EXPECT_EQ(0, rv.first);
    EXPECT_EQ(0, rv.second.size()); // All input consumed
    EXPECT_EQ(0, *cb::mcbp::unsigned_leb128_get_stop_byte_index(zero.get()));
}

TYPED_TEST(UnsignedLeb128, EncodeDecodeMax) {
    cb::mcbp::unsigned_leb128<TypeParam> max(
            std::numeric_limits<TypeParam>::max());
    auto rv = cb::mcbp::decode_unsigned_leb128<TypeParam>(max.get());
    EXPECT_EQ(std::numeric_limits<TypeParam>::max(), rv.first);
    EXPECT_EQ(0, rv.second.size());
}

// Input has the MSbit set for every byte
TYPED_TEST(UnsignedLeb128, EncodeDecode0x80) {
    TypeParam value = 0;
    for (size_t i = 0; i < sizeof(TypeParam); i++) {
        value |= 0x80ull << (i * 8);
    }
    cb::mcbp::unsigned_leb128<TypeParam> leb(value);
    auto rv = cb::mcbp::decode_unsigned_leb128<TypeParam>(leb.get());
    EXPECT_EQ(value, rv.first);
    EXPECT_EQ(0, rv.second.size());
    EXPECT_EQ(leb.get().size() - 1,
              *cb::mcbp::unsigned_leb128_get_stop_byte_index(leb.get()));
}

TYPED_TEST(UnsignedLeb128, EncodeDecodeRandomValue) {
    std::mt19937_64 twister(sizeof(TypeParam));
    auto value = gsl::narrow_cast<TypeParam>(twister());
    cb::mcbp::unsigned_leb128<TypeParam> leb(value);
    auto rv = cb::mcbp::decode_unsigned_leb128<TypeParam>(leb.get());
    EXPECT_EQ(value, rv.first);
    EXPECT_EQ(0, rv.second.size());
    EXPECT_EQ(leb.get().size() - 1,
              *cb::mcbp::unsigned_leb128_get_stop_byte_index(leb.get()));
}

TYPED_TEST(UnsignedLeb128, EncodeDecodeValues) {
    std::vector<uint64_t> values = {1,
                                    10,
                                    100,
                                    255,
                                    256,
                                    1000,
                                    10000,
                                    65535,
                                    65536,
                                    100000,
                                    1000000,
                                    100000000,
                                    4294967295,
                                    4294967296,
                                    1000000000000};

    for (auto v : values) {
        if (v <= std::numeric_limits<TypeParam>::max()) {
            cb::mcbp::unsigned_leb128<TypeParam> leb(
                    gsl::narrow_cast<TypeParam>(v));
            auto rv = cb::mcbp::decode_unsigned_leb128<TypeParam>(leb.get());
            EXPECT_EQ(v, rv.first);
            EXPECT_EQ(0, rv.second.size());
            EXPECT_EQ(
                    leb.get().size() - 1,
                    *cb::mcbp::unsigned_leb128_get_stop_byte_index(leb.get()));
        }
    }
}

TYPED_TEST(UnsignedLeb128, EncodeDecodeMultipleValues) {
    std::mt19937_64 twister(sizeof(TypeParam));
    std::vector<uint8_t> data;
    std::vector<TypeParam> values;
    const int iterations = 10;

    // Encode
    for (int n = 0; n < iterations; n++) {
        values.push_back(gsl::narrow_cast<TypeParam>(twister()));
        cb::mcbp::unsigned_leb128<TypeParam> leb(values.back());
        for (auto c : leb.get()) {
            data.push_back(c);
        }
    }

    std::pair<TypeParam, cb::const_byte_buffer> decoded = {0, {data}};
    int index = 0;

    // Decode
    do {
        decoded = cb::mcbp::decode_unsigned_leb128<TypeParam>(decoded.second);
        EXPECT_EQ(values[index], decoded.first);
        index++;
    } while (decoded.second.size() != 0);
    EXPECT_EQ(iterations, index);
}

TYPED_TEST(UnsignedLeb128, DecodeInvalidInput) {
    // Encode a value and then break the value by removing the stop-byte
    std::mt19937_64 twister(sizeof(TypeParam));
    auto value = gsl::narrow_cast<TypeParam>(twister());
    cb::mcbp::unsigned_leb128<TypeParam> leb(value);

    // Take a copy of the const encoded value for modification
    std::vector<uint8_t> data;
    for (auto c : leb.get()) {
        data.push_back(c);
    }

    // Set the MSbit of the MSB so it's no longer a stop-byte
    data.back() |= 0x80ull;

    EXPECT_FALSE(cb::mcbp::unsigned_leb128_get_stop_byte_index({data}));
    try {
        cb::mcbp::decode_unsigned_leb128<TypeParam>({data});
        FAIL() << "Decode didn't throw";
    } catch (const std::invalid_argument&) {
    }
}

// Encode a value and expect the iterators to iterate the encoded bytes
TYPED_TEST(UnsignedLeb128, iterators) {
    TypeParam value = 1; // Upto 127 and it's 1 byte
    cb::mcbp::unsigned_leb128<TypeParam> leb(value);
    int loopCounter = 0;
    for (const auto c : leb) {
        (void)c;
        loopCounter++;
    }
    EXPECT_EQ(1, loopCounter);
    loopCounter = 0;

    for (auto itr = leb.begin(); itr != leb.end(); itr++) {
        loopCounter++;
    }
    EXPECT_EQ(1, loopCounter);
}

// Set some expectations around the get/data/size API
TYPED_TEST(UnsignedLeb128, basic_api_checks) {
    TypeParam value = gsl::narrow_cast<TypeParam>(5555);
    cb::mcbp::unsigned_leb128<TypeParam> leb(value);
    EXPECT_EQ(leb.get().size(), leb.size());
    EXPECT_EQ(leb.get().data(), leb.data());
}

// Test a few non-canonical encodings decode as expected
TYPED_TEST(UnsignedLeb128, non_canonical) {
    std::vector<std::pair<TypeParam, std::vector<std::vector<uint8_t>>>>
            testData = {
                    {0, {{0}, {0x80, 0}, {0x80, 0x80, 0}}},
                    {1, {{1}, {0x81, 0}}},
            };

    for (const auto& test : testData) {
        for (const auto& data : test.second) {
            auto value = cb::mcbp::decode_unsigned_leb128<TypeParam>({data});
            EXPECT_EQ(test.first, value.first);
        }
    }
}
