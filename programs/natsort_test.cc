/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "natsort.h"

#include <folly/portability/GTest.h>
#include <platform/random.h>
#include <string>

class NatSortTest : public ::testing::Test {
public:
    std::string randomString() {
        std::string str(random.next() % 3, 'a');
        for (auto& ch : str) {
            ch = random.next() % 3 + 'A';
        }
        return str;
    }

protected:
    cb::RandomGenerator random;
};

TEST_F(NatSortTest, ParseNumber) {
    std::string_view str = "1234";
    auto it = str.begin();
    auto n = cb::natsort::detail::parseNumber(it, str.end());
    EXPECT_EQ(1234, n);
    EXPECT_EQ(str.end(), it);
}

TEST_F(NatSortTest, ParseNumberOverflow) {
    // Maximum 64-bit signed integer.
    std::string_view str = "9223372036854775807";
    auto it = str.begin();
    auto n = cb::natsort::detail::parseNumber<std::int64_t>(it, str.end());
    // Should only parse up to (excluding) the last digit, since if that
    // happened to be 8 or 9, we couldn't possibly store the result in int64_t.
    EXPECT_EQ(922337203685477580, n);
    EXPECT_EQ('7', *it);
}

TEST_F(NatSortTest, StringFuzzTest) {
    for (int i = 0; i < 10000; i++) {
        auto a = randomString();
        auto b = randomString();

        ASSERT_EQ(a <=> b, cb::natsort::compare_three_way{}(a, b))
                << "A=" << a << "\nB=" << b << "\nPassed: " << i;
    }
}

TEST_F(NatSortTest, NumberFuzzTest) {
    for (int i = 0; i < 10000; i++) {
        auto a = random.next() % 100;
        auto b = random.next() % 100;

        ASSERT_EQ(a <=> b,
                  cb::natsort::compare_three_way{}(std::to_string(a),
                                                   std::to_string(b)))
                << "A=" << a << "\nB=" << b << "\nPassed: " << i;
    }
}

TEST_F(NatSortTest, StringThenNumberFuzzTest) {
    for (int i = 0; i < 10000; i++) {
        std::pair<std::string, int> a{randomString(), random.next() % 100};
        std::pair<std::string, int> b{randomString(), random.next() % 100};

        auto strA = a.first + '-' + std::to_string(a.second);
        auto strB = b.first + '-' + std::to_string(b.second);

        ASSERT_EQ(a <=> b, cb::natsort::compare_three_way{}(strA, strB))
                << "A=" << strA << "\nB=" << strB << "\nPassed: " << i;
    }
}

TEST_F(NatSortTest, NumberThenStringFuzzTest) {
    for (int i = 0; i < 10000; i++) {
        std::pair<int, std::string> a{random.next() % 100, randomString()};
        std::pair<int, std::string> b{random.next() % 100, randomString()};

        auto strA = std::to_string(a.first) + '-' + a.second;
        auto strB = std::to_string(b.first) + '-' + b.second;

        ASSERT_EQ(a <=> b, cb::natsort::compare_three_way{}(strA, strB))
                << "A=" << strA << "\nB=" << strB << "\nPassed: " << i;
    }
}
