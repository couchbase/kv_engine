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

using namespace cb::mcbp;
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

TEST(DocFlag, format_as) {
    DocFlag flag = DocFlag::None;
    EXPECT_EQ(R"("None")", fmt::format("{}", flag));
    flag |= DocFlag::Mkdoc;
    EXPECT_EQ(R"("Mkdoc")", fmt::format("{}", flag));
    flag |= DocFlag::Add;
    EXPECT_EQ(R"(["Mkdoc","Add"])", fmt::format("{}", flag));
    flag = static_cast<DocFlag>(0xff);
    EXPECT_EQ(
            R"(["Mkdoc","Add","AccessDeleted","CreateAsDeleted","ReviveDocument","ReplicaRead","unknown:0x40","unknown:0x80"])",
            fmt::format("{}", flag));
}

TEST(DcpOpenFlag, format_as) {
    DcpOpenFlag flag = DcpOpenFlag::None;
    EXPECT_EQ(R"("None")", fmt::format("{}", flag));
    flag |= DcpOpenFlag::Producer;
    EXPECT_EQ(R"("Producer")", fmt::format("{}", flag));
    flag |= DcpOpenFlag::IncludeXattrs;
    EXPECT_EQ(R"(["Producer","IncludeXattrs"])", fmt::format("{}", flag));
    flag = static_cast<DcpOpenFlag>(0xfffffffff);
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
            fmt::format("{}", flag));
}

TEST(DcpAddStreamFlag, format_as) {
    DcpAddStreamFlag flag = DcpAddStreamFlag::None;
    EXPECT_EQ(R"("None")", fmt::format("{}", flag));
    flag |= DcpAddStreamFlag::TakeOver;
    EXPECT_EQ(R"("Takeover")", fmt::format("{}", flag));
    flag |= DcpAddStreamFlag::DiskOnly;
    EXPECT_EQ(R"(["Takeover","DiskOnly"])", fmt::format("{}", flag));
    flag = static_cast<DcpAddStreamFlag>(0xfffffffff);
    EXPECT_EQ(
            "[\"Takeover\",\"DiskOnly\",\"ToLatest\",\"NoValue\","
            "\"ActiveVbOnly\",\"StrictVbUuid\",\"FromLatest\","
            "\"IgnorePurgedTombstones\",\"unknown:0x100\",\"unknown:0x200\","
            "\"unknown:0x400\",\"unknown:0x800\",\"unknown:0x1000\",\"unknown:"
            "0x2000\",\"unknown:0x4000\",\"unknown:0x8000\",\"unknown:"
            "0x10000\",\"unknown:0x20000\",\"unknown:0x40000\",\"unknown:"
            "0x80000\",\"unknown:0x100000\",\"unknown:0x200000\",\"unknown:"
            "0x400000\",\"unknown:0x800000\",\"unknown:0x1000000\",\"unknown:"
            "0x2000000\",\"unknown:0x4000000\",\"unknown:0x8000000\",\"unknown:"
            "0x10000000\",\"unknown:0x20000000\",\"unknown:0x40000000\","
            "\"unknown:0x80000000\"]",
            fmt::format("{}", flag));
}

TEST(DcpSnapshotMarkerFlag, format_as) {
    using request::DcpSnapshotMarkerFlag;
    DcpSnapshotMarkerFlag flag = DcpSnapshotMarkerFlag::None;
    EXPECT_EQ(R"("None")", fmt::format("{}", flag));
    EXPECT_EQ(R"("None")", fmt::format("{}", flag));
    flag |= DcpSnapshotMarkerFlag::Memory;
    EXPECT_EQ(R"("Memory")", fmt::format("{}", flag));
    flag |= DcpSnapshotMarkerFlag::Acknowledge;
    EXPECT_EQ(R"(["Memory","Acknowledge"])", fmt::format("{}", flag));
    flag = static_cast<DcpSnapshotMarkerFlag>(0xfffffffff);
    EXPECT_EQ(
            "[\"Memory\",\"Disk\",\"Checkpoint\",\"Acknowledge\",\"History\","
            "\"MayContainDuplicates\",\"unknown:0x40\",\"unknown:0x80\","
            "\"unknown:0x100\",\"unknown:0x200\",\"unknown:0x400\",\"unknown:"
            "0x800\",\"unknown:0x1000\",\"unknown:0x2000\",\"unknown:0x4000\","
            "\"unknown:0x8000\",\"unknown:0x10000\",\"unknown:0x20000\","
            "\"unknown:0x40000\",\"unknown:0x80000\",\"unknown:0x100000\","
            "\"unknown:0x200000\",\"unknown:0x400000\",\"unknown:0x800000\","
            "\"unknown:0x1000000\",\"unknown:0x2000000\",\"unknown:0x4000000\","
            "\"unknown:0x8000000\",\"unknown:0x10000000\",\"unknown:"
            "0x20000000\",\"unknown:0x40000000\",\"unknown:0x80000000\"]",
            fmt::format("{}", flag));
}