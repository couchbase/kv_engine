/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <cbsasl/scram-sha/stringutils.h>
#include <folly/portability/GTest.h>

TEST(SASLPrep, SASLPrepPlainAscii) {
    // We should allow all "printable" ascii characters, and no
    // remapping should happen
    for (int ii = 0x01; ii < 0x80; ++ii) {
        if (isprint(ii)) {
            std::string string = {static_cast<char>(ii)};
            EXPECT_NO_THROW(SASLPrep(string)) << "A: " << ii;
            EXPECT_EQ(string, SASLPrep(string));
        }
    }
}

TEST(SASLPrep, SASLPrepControlAscii) {
    // We should allow all "printable" ascii characters, and no
    // remapping should happen
    for (int ii = 0x01; ii < 0x80; ++ii) {
        if (iscntrl(ii)) {
            std::string string = {static_cast<char>(ii)};
            EXPECT_THROW(SASLPrep(string), std::runtime_error) << "A: " << ii;
        }
    }
}

TEST(SASLPrep, SASLPrepDetecMultibyteUTF8) {
    // Bare continuation byte (0x80) is not a valid lead byte
    std::string str;
    str.push_back(static_cast<char>(0x80));
    EXPECT_THROW(SASLPrep(str), std::runtime_error);
}

TEST(SASLPrep, ValidTwoByteUTF8) {
    // U+00A1 INVERTED EXCLAMATION MARK: 0xC2 0xA1
    std::string str;
    str.push_back(static_cast<char>(0xC2));
    str.push_back(static_cast<char>(0xA1));
    EXPECT_NO_THROW(SASLPrep(str));
}

TEST(SASLPrep, ValidThreeByteUTF8) {
    // U+20AC EURO SIGN: 0xE2 0x82 0xAC
    std::string str;
    str.push_back(static_cast<char>(0xE2));
    str.push_back(static_cast<char>(0x82));
    str.push_back(static_cast<char>(0xAC));
    EXPECT_NO_THROW(SASLPrep(str));
}

TEST(SASLPrep, ValidFourByteUTF8) {
    // U+1F600: 0xF0 0x9F 0x98 0x80
    std::string str;
    str.push_back(static_cast<char>(0xF0));
    str.push_back(static_cast<char>(0x9F));
    str.push_back(static_cast<char>(0x98));
    str.push_back(static_cast<char>(0x80));
    EXPECT_NO_THROW(SASLPrep(str));
}

TEST(SASLPrep, OverlongTwoByteRejected) {
    // 0xC0/0xC1 lead bytes always produce overlong encodings
    for (std::byte lead : {std::byte{0xC0}, std::byte{0xC1}}) {
        std::string str;
        str.push_back(static_cast<char>(lead));
        str.push_back(static_cast<char>(0x80));
        EXPECT_THROW(SASLPrep(str), std::runtime_error)
                << "lead=0x" << std::hex << static_cast<int>(lead);
    }
}

TEST(SASLPrep, OutOfRangeFourByteRejected) {
    // 0xF5-0xF7 encode code points above U+10FFFF
    for (std::byte lead : {std::byte{0xF5}, std::byte{0xF6}, std::byte{0xF7}}) {
        std::string str;
        str.push_back(static_cast<char>(lead));
        str.push_back(static_cast<char>(0x80));
        str.push_back(static_cast<char>(0x80));
        str.push_back(static_cast<char>(0x80));
        EXPECT_THROW(SASLPrep(str), std::runtime_error)
                << "lead=0x" << std::hex << static_cast<int>(lead);
    }
}

TEST(SASLPrep, IncompleteUTF8Rejected) {
    // Lead byte with no continuation bytes
    std::string str;
    str.push_back(static_cast<char>(0xC2));
    EXPECT_THROW(SASLPrep(str), std::runtime_error);
}

TEST(SASLPrep, InvalidContinuationByteRejected) {
    // Lead byte followed by a non-continuation byte
    std::string str;
    str.push_back(static_cast<char>(0xC2));
    str.push_back('A');
    EXPECT_THROW(SASLPrep(str), std::runtime_error);
}
