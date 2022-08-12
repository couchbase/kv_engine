/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "datatype_filter.h"
#include <folly/portability/GTest.h>

/*
 * An empty set would only have RAW enabled (which is basically no datatype)
 */
TEST(DatatypeFilterTest, Empty) {
    DatatypeFilter datatype;
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_RAW_BYTES));

    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_SNAPPY));
    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_XATTR));
    EXPECT_FALSE(datatype.isSnappyEnabled());
    EXPECT_FALSE(datatype.isJsonEnabled());
    EXPECT_FALSE(datatype.isXattrEnabled());
}

TEST(DatatypeFilterTest, All) {
    DatatypeFilter datatype;
    datatype.enableAll();
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_RAW_BYTES));

    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_SNAPPY));
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_XATTR));
    EXPECT_TRUE(datatype.isSnappyEnabled());
    EXPECT_TRUE(datatype.isJsonEnabled());
    EXPECT_TRUE(datatype.isXattrEnabled());
}

TEST(DatatypeFilterTest, EnableDisable) {
    DatatypeFilter datatype;
    datatype.enableAll();
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_RAW_BYTES));

    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_SNAPPY));
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_XATTR));
    EXPECT_TRUE(datatype.isSnappyEnabled());
    EXPECT_TRUE(datatype.isJsonEnabled());
    EXPECT_TRUE(datatype.isXattrEnabled());

    datatype.disableAll();
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_RAW_BYTES));

    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_SNAPPY));
    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_XATTR));
    EXPECT_FALSE(datatype.isSnappyEnabled());
    EXPECT_FALSE(datatype.isJsonEnabled());
    EXPECT_FALSE(datatype.isXattrEnabled());
}

TEST(DatatypeFilterTest, EnableFeatures) {
    DatatypeFilter datatype;
    EXPECT_NO_THROW(datatype.enable(cb::mcbp::Feature::XATTR));
    EXPECT_NO_THROW(datatype.enable(cb::mcbp::Feature::JSON));
    EXPECT_NO_THROW(datatype.enable(cb::mcbp::Feature::SNAPPY));

    EXPECT_THROW(datatype.enable(cb::mcbp::Feature::TLS),
                 std::invalid_argument);
    EXPECT_THROW(datatype.enable(cb::mcbp::Feature::MUTATION_SEQNO),
                 std::invalid_argument);
    EXPECT_THROW(datatype.enable(cb::mcbp::Feature::TCPDELAY),
                 std::invalid_argument);
    EXPECT_THROW(datatype.enable(cb::mcbp::Feature::SELECT_BUCKET),
                 std::invalid_argument);
    EXPECT_THROW(datatype.enable(cb::mcbp::Feature::Collections),
                 std::invalid_argument);
    EXPECT_THROW(datatype.enable(cb::mcbp::Feature::XERROR),
                 std::invalid_argument);
}

TEST(DatatypeFilterTest, Intersect0) {
    DatatypeFilter datatype;

    protocol_binary_datatype_t d1 = cb::mcbp::datatype::highest;
    auto d2 = datatype.getIntersection(d1);
    EXPECT_FALSE(cb::mcbp::datatype::is_json(d2));
    EXPECT_FALSE(cb::mcbp::datatype::is_xattr(d2));
    EXPECT_FALSE(cb::mcbp::datatype::is_snappy(d2));
    EXPECT_TRUE(cb::mcbp::datatype::is_raw(d2));
}

TEST(DatatypeFilterTest, Intersect1) {
    DatatypeFilter datatype;
    datatype.enable(cb::mcbp::Feature::JSON);

    protocol_binary_datatype_t d1 = cb::mcbp::datatype::highest;
    auto d2 = datatype.getIntersection(d1);
    EXPECT_TRUE(cb::mcbp::datatype::is_json(d2));
    EXPECT_FALSE(cb::mcbp::datatype::is_xattr(d2));
    EXPECT_FALSE(cb::mcbp::datatype::is_snappy(d2));
    EXPECT_FALSE(cb::mcbp::datatype::is_raw(d2));
}

TEST(DatatypeFilterTest, Intersect2) {
    DatatypeFilter datatype;
    datatype.enable(cb::mcbp::Feature::JSON);
    datatype.enable(cb::mcbp::Feature::XATTR);

    protocol_binary_datatype_t d1 = cb::mcbp::datatype::highest;
    auto d2 = datatype.getIntersection(d1);
    EXPECT_TRUE(cb::mcbp::datatype::is_json(d2));
    EXPECT_TRUE(cb::mcbp::datatype::is_xattr(d2));
    EXPECT_FALSE(cb::mcbp::datatype::is_snappy(d2));
    EXPECT_FALSE(cb::mcbp::datatype::is_raw(d2));
}

TEST(DatatypeFilterTest, IntersectAll) {
    DatatypeFilter datatype;
    datatype.enableAll();

    protocol_binary_datatype_t d1 = cb::mcbp::datatype::highest;
    auto d2 = datatype.getIntersection(d1);
    EXPECT_TRUE(cb::mcbp::datatype::is_json(d2));
    EXPECT_TRUE(cb::mcbp::datatype::is_xattr(d2));
    EXPECT_TRUE(cb::mcbp::datatype::is_snappy(d2));
    EXPECT_FALSE(cb::mcbp::datatype::is_raw(d2));
}

TEST(DatatypeFilterTest, Raw0) {
    DatatypeFilter datatype;
    EXPECT_EQ(0, datatype.getRaw());
}

TEST(DatatypeFilterTest, Raw1) {
    DatatypeFilter datatype;
    datatype.enable(cb::mcbp::Feature::JSON);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, datatype.getRaw());
}

TEST(DatatypeFilterTest, RawAll) {
    DatatypeFilter datatype;
    datatype.enableAll();
    protocol_binary_datatype_t d1 = cb::mcbp::datatype::highest;
    EXPECT_EQ(d1, datatype.getRaw());
}
