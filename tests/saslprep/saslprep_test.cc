/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "config.h"

#include <gtest/gtest.h>
#include <cbsasl/scram-sha/stringutils.h>

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
