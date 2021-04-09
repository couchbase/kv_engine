/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

TEST(Authname, SingleComma) {
    EXPECT_EQ(",", decodeUsername("=2C"));
}

TEST(Authname, SingleEqual) {
    EXPECT_EQ("=", decodeUsername("=3D"));
}

TEST(Authname, DoubleEqual) {
    EXPECT_EQ("==", decodeUsername("=3D=3D"));
}

TEST(Authname, InvalidEqualSequence) {
    EXPECT_THROW(decodeUsername("=3F"), std::runtime_error);
}

TEST(Authname, InvalidEqualSequenceStringTooShort) {
    EXPECT_THROW(decodeUsername("=3"), std::runtime_error);
}

TEST(Authname, StringContainingAMix) {
    EXPECT_EQ("This,=Where I=use,",
              decodeUsername("This=2C=3DWhere I=3Duse=2C"));
}

TEST(Authname, EncodeUsername) {
    EXPECT_EQ("Hi=2CI'm=3D", encodeUsername("Hi,I'm="));
}

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
    char string[2] = {(char)(0x80)};
    EXPECT_THROW(SASLPrep(string), std::runtime_error);
}
