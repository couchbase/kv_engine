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
            char string[2] = {(char)(ii)};
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
            char string[2] = {(char)(ii)};
            EXPECT_THROW(SASLPrep(string), std::runtime_error) << "A: " << ii;
        }
    }
}

TEST(SASLPrep, SASLPrepDetecMultibyteUTF8) {
    std::string str;
    str.push_back(static_cast<char>(0x80));
    try {
        SASLPrep(str);
        FAIL() << "Expected exception not thrown";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ("SASLPrep: Multibyte UTF-8 is not implemented yet",
                     e.what());
    }
}
