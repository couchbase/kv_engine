/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <fmt/format.h>
#include <folly/portability/GTest.h>
#include <mcbp/protocol/json_utilities.h>
#include <memcached/protocol_binary.h>

using namespace cb::mcbp::subdoc;
using namespace std::string_literals;

TEST(PathFlags, format_as) {
    PathFlag flag = PathFlag::None;
    EXPECT_EQ(R"("None")", fmt::format("{}", flag));
    flag |= PathFlag::Mkdir_p;
    EXPECT_EQ(R"("Mkdir_p")", fmt::format("{}", flag));
    flag |= PathFlag::XattrPath;
    EXPECT_EQ(R"(["Mkdir_p","XattrPath"])", fmt::format("{}", flag));
    flag = static_cast<PathFlag>(0xff);
    EXPECT_EQ(
            R"(["Mkdir_p","unknown:0x2","XattrPath","unknown:0x8","ExpandMacros","unknown:0x20","unknown:0x40","unknown:0x80"])",
            fmt::format("{}", flag));
}
