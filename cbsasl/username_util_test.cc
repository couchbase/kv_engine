/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "username_util.h"
#include <folly/portability/GTest.h>

TEST(Authname, SingleComma) {
    EXPECT_EQ(",", cb::sasl::username::decode("=2C"));
}

TEST(Authname, SingleEqual) {
    EXPECT_EQ("=", cb::sasl::username::decode("=3D"));
}

TEST(Authname, DoubleEqual) {
    EXPECT_EQ("==", cb::sasl::username::decode("=3D=3D"));
}

TEST(Authname, InvalidEqualSequence) {
    EXPECT_THROW(cb::sasl::username::decode("=3F"), std::runtime_error);
}

TEST(Authname, InvalidEqualSequenceStringTooShort) {
    EXPECT_THROW(cb::sasl::username::decode("=3"), std::runtime_error);
}

TEST(Authname, StringContainingAMix) {
    EXPECT_EQ("This,=Where I=use,",
              cb::sasl::username::decode("This=2C=3DWhere I=3Duse=2C"));
}

TEST(Authname, EncodeUsername) {
    EXPECT_EQ("Hi=2CI'm=3D", cb::sasl::username::encode("Hi,I'm="));
}
