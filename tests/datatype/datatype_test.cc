/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <daemon/datatype.h>
#include <gtest/gtest.h>

/*
 * An empty set would only have RAW enabled (which is basically no datatype)
 */
TEST(DatatypeTest, Empty) {
    Datatype datatype;
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_RAW_BYTES));

    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_SNAPPY));
    EXPECT_FALSE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_XATTR));
    EXPECT_FALSE(datatype.isSnappyEnabled());
    EXPECT_FALSE(datatype.isJsonEnabled());
    EXPECT_FALSE(datatype.isXattrEnabled());
}

TEST(DatatypeTest, All) {
    Datatype datatype;
    datatype.enableAll();
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_RAW_BYTES));

    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_SNAPPY));
    EXPECT_TRUE(datatype.isEnabled(PROTOCOL_BINARY_DATATYPE_XATTR));
    EXPECT_TRUE(datatype.isSnappyEnabled());
    EXPECT_TRUE(datatype.isJsonEnabled());
    EXPECT_TRUE(datatype.isXattrEnabled());
}

TEST(DatatypeTest, EnableDisable) {
    Datatype datatype;
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

TEST(DatatypeTest, EnableFeatures) {
    Datatype datatype;
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

TEST(DatatypeTest, Intersect0) {
    Datatype datatype;

    protocol_binary_datatype_t d1 = mcbp::datatype::highest;
    auto d2 = datatype.getIntersection(d1);
    EXPECT_FALSE(mcbp::datatype::is_json(d2));
    EXPECT_FALSE(mcbp::datatype::is_xattr(d2));
    EXPECT_FALSE(mcbp::datatype::is_snappy(d2));
    EXPECT_TRUE(mcbp::datatype::is_raw(d2));
}

TEST(DatatypeTest, Intersect1) {
    Datatype datatype;
    datatype.enable(cb::mcbp::Feature::JSON);

    protocol_binary_datatype_t d1 = mcbp::datatype::highest;
    auto d2 = datatype.getIntersection(d1);
    EXPECT_TRUE(mcbp::datatype::is_json(d2));
    EXPECT_FALSE(mcbp::datatype::is_xattr(d2));
    EXPECT_FALSE(mcbp::datatype::is_snappy(d2));
    EXPECT_FALSE(mcbp::datatype::is_raw(d2));
}

TEST(DatatypeTest, Intersect2) {
    Datatype datatype;
    datatype.enable(cb::mcbp::Feature::JSON);
    datatype.enable(cb::mcbp::Feature::XATTR);

    protocol_binary_datatype_t d1 = mcbp::datatype::highest;
    auto d2 = datatype.getIntersection(d1);
    EXPECT_TRUE(mcbp::datatype::is_json(d2));
    EXPECT_TRUE(mcbp::datatype::is_xattr(d2));
    EXPECT_FALSE(mcbp::datatype::is_snappy(d2));
    EXPECT_FALSE(mcbp::datatype::is_raw(d2));
}

TEST(DatatypeTest, IntersectAll) {
    Datatype datatype;
    datatype.enableAll();

    protocol_binary_datatype_t d1 = mcbp::datatype::highest;
    auto d2 = datatype.getIntersection(d1);
    EXPECT_TRUE(mcbp::datatype::is_json(d2));
    EXPECT_TRUE(mcbp::datatype::is_xattr(d2));
    EXPECT_TRUE(mcbp::datatype::is_snappy(d2));
    EXPECT_FALSE(mcbp::datatype::is_raw(d2));
}

TEST(DatatypeTest, Raw0) {
    Datatype datatype;
    EXPECT_EQ(0, datatype.getRaw());
}

TEST(DatatypeTest, Raw1) {
    Datatype datatype;
    datatype.enable(cb::mcbp::Feature::JSON);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, datatype.getRaw());
}

TEST(DatatypeTest, RawAll) {
    Datatype datatype;
    datatype.enableAll();
    protocol_binary_datatype_t d1 = mcbp::datatype::highest;
    EXPECT_EQ(d1, datatype.getRaw());
}
