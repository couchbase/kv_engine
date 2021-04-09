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

#include "string_utils.h"

#include <folly/portability/GTest.h>

TEST(cb_stobTest, ValidWorks) {
    EXPECT_TRUE(cb_stob("true"));
    EXPECT_FALSE(cb_stob("false"));
}

TEST(cb_stobTest, ThrowsInvalid) {
    EXPECT_THROW(cb_stob("TRUE"), invalid_argument_bool);
    EXPECT_THROW(cb_stob("True"), invalid_argument_bool);
    EXPECT_THROW(cb_stob("completelyinvalid"), invalid_argument_bool);
    EXPECT_THROW(cb_stob("FALSE"), invalid_argument_bool);
    EXPECT_THROW(cb_stob("FLASE"), invalid_argument_bool);
    EXPECT_THROW(cb_stob("farce"), invalid_argument_bool);
}
