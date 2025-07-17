/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <mcbp/protocol/json_utilities.h>

TEST(DcpOpenFlag, to_and_from_json) {
    auto flags = static_cast<cb::mcbp::DcpOpenFlag>(
            std::numeric_limits<uint32_t>::max());
    nlohmann::json json = flags;

    EXPECT_EQ(
            "[\"Producer\",\"unknown:0x2\",\"IncludeXattrs\",\"NoValue\","
            "\"unknown:0x10\",\"IncludeDeleteTimes\","
            "\"NoValueWithUnderlyingDatatype\",\"PiTR\","
            "\"IncludeDeletedUserXattrs\",\"unknown:0x200\",\"unknown:0x400\","
            "\"unknown:0x800\",\"unknown:0x1000\",\"unknown:0x2000\",\"unknown:"
            "0x4000\",\"unknown:0x8000\",\"unknown:0x10000\",\"unknown:"
            "0x20000\",\"unknown:0x40000\",\"unknown:0x80000\",\"unknown:"
            "0x100000\",\"unknown:0x200000\",\"unknown:0x400000\",\"unknown:"
            "0x800000\",\"unknown:0x1000000\",\"unknown:0x2000000\",\"unknown:"
            "0x4000000\",\"unknown:0x8000000\",\"unknown:0x10000000\","
            "\"unknown:0x20000000\",\"unknown:0x40000000\",\"unknown:"
            "0x80000000\"]",
            json.dump());

    EXPECT_EQ(flags, json.get<cb::mcbp::DcpOpenFlag>()) << json.dump();
}
