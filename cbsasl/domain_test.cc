/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/domain.h>

#include <folly/portability/GTest.h>
#include <stdexcept>
#include <string>

using namespace cb::sasl;
using namespace std::string_literals;

TEST(Domain, to_domain) {
    EXPECT_EQ(Domain::Local, to_domain("local"s));
    EXPECT_EQ(Domain::Local, to_domain("builtin"s));
    EXPECT_EQ(Domain::Local, to_domain("couchbase"s));
    EXPECT_EQ(Domain::External, to_domain("external"s));
    EXPECT_EQ(Domain::Unknown, to_domain("unknown"s));
    EXPECT_THROW((void)to_domain("blahblah"s), std::invalid_argument);
}
