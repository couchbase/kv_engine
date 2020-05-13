/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/*
 * Testsuite for Item class in ep-engine.
 */

#include "item.h"
#include "test_helpers.h"

#include <folly/portability/GTest.h>
#include <memcached/protocol_binary.h>
#include <memory>

class ItemNoValuePruneTest : public ::testing::TestWithParam<
                             std::tuple<IncludeValue, IncludeXattrs>> {
public:

    SingleThreadedRCPtr<Item> item;
    void SetUp() {
        item = std::make_unique<Item>(makeStoredDocKey("key"),
                                      Vbid(0),
                                      queue_op::empty,
                                      /*revSeq*/ 0,
                                      /*bySeq*/ 0);
        }
};

TEST_P(ItemNoValuePruneTest, testPrune) {
    IncludeValue includeValue = std::get<0>(GetParam());
    IncludeXattrs includeXattrs = std::get<1>(GetParam());
    item->removeBodyAndOrXattrs(
            includeValue, includeXattrs, IncludeDeletedUserXattrs::No);

    auto datatype = item->getDataType();
    EXPECT_FALSE(mcbp::datatype::is_json(datatype));
    EXPECT_FALSE(mcbp::datatype::is_xattr(datatype));
    EXPECT_FALSE(mcbp::datatype::is_snappy(datatype));
    EXPECT_TRUE(mcbp::datatype::is_raw(datatype));
    // should not have value
    EXPECT_EQ(0, item->getNBytes());
}

INSTANTIATE_TEST_CASE_P(
        PruneTestWithParameters,
        ItemNoValuePruneTest,
        ::testing::Combine(
                testing::Values(IncludeValue::Yes,
                                IncludeValue::No,
                                IncludeValue::NoWithUnderlyingDatatype),
                testing::Values(IncludeXattrs::Yes, IncludeXattrs::No)), );

class ItemTest : public ::testing::Test {
public:

    SingleThreadedRCPtr<Item> item;
};

class ItemPruneTest : public ItemTest {
public:

    void SetUp() {
        std::string valueData = R"({"json":"yes"})";
        std::string data = createXattrValue(valueData);
        protocol_binary_datatype_t datatype = (PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_XATTR);

         item = std::make_unique<Item>(
                makeStoredDocKey("key"),
                0,
                0,
                data.data(),
                data.size(),
                datatype);
        }
};

TEST_F(ItemTest, getAndSetCachedDataType) {
    std::string valueData = R"(raw data)";
    item = std::make_unique<Item>(
            makeStoredDocKey("key"),
            0,
            0,
            valueData.c_str(),
            valueData.size());

    // Item was created with no extended meta data so datatype should be
    // the default PROTOCOL_BINARY_RAW_BYTES
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, item->getDataType());

    // We can set still set the cached datatype
    item->setDataType(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    // Check that the datatype equals what we set it to
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_SNAPPY, item->getDataType());

    std::string jsonValueData = R"({"json":"yes"})";

    auto blob = Blob::New(jsonValueData.c_str(), jsonValueData.size());

    // Replace the item's blob with one that contains extended meta data
    item->replaceValue(blob);
    item->setDataType(PROTOCOL_BINARY_DATATYPE_JSON);

    // Expect the cached datatype to be equal to the one in the new blob
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON,
              (PROTOCOL_BINARY_DATATYPE_JSON & item->getDataType()));
}

TEST_F(ItemTest, checkNRUandFreqCounterValueSetCorrectly) {
    std::string valueData = R"(raw data)";
    item = std::make_unique<Item>(makeStoredDocKey("key"),
                                  0 /* flags */,
                                  0 /* exptime */,
                                  valueData.c_str(),
                                  valueData.size(),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  0 /* cas */,
                                  -1 /* bySeqno */,
                                  Vbid(0),
                                  1 /* revSeqno */,
                                  1 /* nru */,
                                  128 /* freqCount */);
    EXPECT_EQ(1, int(item->getNRUValue()));
    EXPECT_EQ(128, item->getFreqCounterValue());
}

TEST_F(ItemTest, retainInfoUponItemCopy) {
    // Setup the item using non-default parameters
    std::string valueData = R"(oranges)";
    auto key = makeStoredDocKey("apples");
    Item item1 = Item(key,
                      0xdeadbeef /* flags */,
                      3600 /* exptime */,
                      valueData.c_str(),
                      valueData.size(),
                      PROTOCOL_BINARY_DATATYPE_JSON,
                      42 /* cas */,
                      7 /* bySeqno */,
                      Vbid(99),
                      13 /* revSeqno */,
                      2 /* nru */,
                      128 /* freqCount */);
    // Delete the item via TTL
    item1.setDeleted(DeleteSource::TTL);

    // Set non-default committed state.
    using namespace cb::durability;
    item1.setPendingSyncWrite(
            Requirements{Level::MajorityAndPersistOnMaster, Timeout(3)});
    item1.setPreparedMaybeVisible();

    // Copy item using constructor
    Item item2 = Item(item1);

    EXPECT_EQ(item1, item2) << "Item values not retained on copy";
}

TEST_F(ItemPruneTest, testPruneNothing) {
    item->removeBodyAndOrXattrs(IncludeValue::Yes,
                                IncludeXattrs::Yes,
                                IncludeDeletedUserXattrs::No);

    auto datatype = item->getDataType();
    EXPECT_TRUE(mcbp::datatype::is_json(datatype));
    EXPECT_TRUE(mcbp::datatype::is_xattr(datatype));
    EXPECT_FALSE(mcbp::datatype::is_snappy(datatype));
    EXPECT_FALSE(mcbp::datatype::is_raw(datatype));

    // data should include the value and the xattrs
    std::string valueData = R"({"json":"yes"})";
    auto data = createXattrValue(valueData);
    EXPECT_EQ(data.size(), item->getNBytes());
    EXPECT_EQ(0, memcmp(item->getData(), data.data(), item->getNBytes()));
}

TEST_F(ItemPruneTest, testPruneXattrs) {
    item->removeBodyAndOrXattrs(
            IncludeValue::Yes, IncludeXattrs::No, IncludeDeletedUserXattrs::No);

    auto datatype = item->getDataType();
    EXPECT_TRUE(mcbp::datatype::is_json(datatype));
    EXPECT_FALSE(mcbp::datatype::is_xattr(datatype));
    EXPECT_FALSE(mcbp::datatype::is_snappy(datatype));
    EXPECT_FALSE(mcbp::datatype::is_raw(datatype));

    // data should include the value but not the xattrs
    std::string valueData = R"({"json":"yes"})";
    EXPECT_EQ(valueData.size(), item->getNBytes());
    EXPECT_EQ(0, memcmp(item->getData(), valueData.c_str(),
                         item->getNBytes()));
}

TEST_F(ItemPruneTest, testPruneValue) {
    item->removeBodyAndOrXattrs(
            IncludeValue::No, IncludeXattrs::Yes, IncludeDeletedUserXattrs::No);

    auto datatype = item->getDataType();
    EXPECT_FALSE(mcbp::datatype::is_json(datatype));
    EXPECT_TRUE(mcbp::datatype::is_xattr(datatype));
    EXPECT_FALSE(mcbp::datatype::is_snappy(datatype));
    EXPECT_FALSE(mcbp::datatype::is_raw(datatype));

    // data should include the xattrs but not the value
    auto data = createXattrValue("");
    EXPECT_EQ(data.size(), item->getNBytes());
    EXPECT_EQ(0, memcmp(item->getData(), data.data(), item->getNBytes()));
}

TEST_F(ItemPruneTest, testPruneValueUnderlyingDatatype) {
    item->removeBodyAndOrXattrs(IncludeValue::NoWithUnderlyingDatatype,
                                IncludeXattrs::Yes,
                                IncludeDeletedUserXattrs::No);

    auto datatype = item->getDataType();
    EXPECT_TRUE(mcbp::datatype::is_json(datatype))
            << "Datatype should be preserved with NoWithUnderlyingDatatype";
    EXPECT_TRUE(mcbp::datatype::is_xattr(datatype));
    EXPECT_FALSE(mcbp::datatype::is_snappy(datatype));
    EXPECT_FALSE(mcbp::datatype::is_raw(datatype));

    // data should include the xattrs but not the value
    auto data = createXattrValue("");
    EXPECT_EQ(data.size(), item->getNBytes());
    EXPECT_EQ(0, memcmp(item->getData(), data.data(), item->getNBytes()));
}

TEST_F(ItemPruneTest, testPruneValueAndXattrs) {
    item->removeBodyAndOrXattrs(
            IncludeValue::No, IncludeXattrs::No, IncludeDeletedUserXattrs::No);

    auto datatype = item->getDataType();
    EXPECT_FALSE(mcbp::datatype::is_json(datatype));
    EXPECT_FALSE(mcbp::datatype::is_xattr(datatype));
    EXPECT_FALSE(mcbp::datatype::is_snappy(datatype));
    EXPECT_TRUE(mcbp::datatype::is_raw(datatype));

    // should not have value or xattrs
    EXPECT_EQ(0, item->getNBytes());
}

TEST_F(ItemPruneTest, testPruneValueAndXattrsUnderlyingDatatype) {
    item->removeBodyAndOrXattrs(IncludeValue::NoWithUnderlyingDatatype,
                                IncludeXattrs::No,
                                IncludeDeletedUserXattrs::No);

    auto datatype = item->getDataType();
    EXPECT_TRUE(mcbp::datatype::is_json(datatype))
            << "Datatype should be preserved with NoWithUnderlyingDatatype";
    EXPECT_TRUE(mcbp::datatype::is_xattr(datatype))
            << "Datatype should be preserved with NoWithUnderlyingDatatype";
    EXPECT_FALSE(mcbp::datatype::is_snappy(datatype));

    // should not have value or xattrs
    EXPECT_EQ(0, item->getNBytes());
}

TEST_F(ItemPruneTest, testPruneValueWithNoXattrs) {
    std::string valueData = R"({"json":"yes"})";
    protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON;

    item = std::make_unique<Item>(
            makeStoredDocKey("key"),
            0,
            0,
            const_cast<char*>(valueData.data()),
            valueData.size(),
            datatype);

    item->removeBodyAndOrXattrs(
            IncludeValue::No, IncludeXattrs::Yes, IncludeDeletedUserXattrs::No);

    auto dtype = item->getDataType();
    EXPECT_FALSE(mcbp::datatype::is_json(dtype));
    EXPECT_FALSE(mcbp::datatype::is_xattr(dtype));
    EXPECT_FALSE(mcbp::datatype::is_snappy(dtype));
    EXPECT_TRUE(mcbp::datatype::is_raw(dtype));

    // should not have value
    EXPECT_EQ(0, item->getNBytes());
}

TEST_F(ItemPruneTest, testPruneValueWithNoXattrsUnderlyingDatatype) {
    std::string valueData = R"({"json":"yes"})";
    protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON;

    item = std::make_unique<Item>(makeStoredDocKey("key"),
                                  0,
                                  0,
                                  const_cast<char*>(valueData.data()),
                                  valueData.size(),
                                  datatype);

    item->removeBodyAndOrXattrs(IncludeValue::NoWithUnderlyingDatatype,
                                IncludeXattrs::Yes,
                                IncludeDeletedUserXattrs::No);

    auto dtype = item->getDataType();
    EXPECT_TRUE(mcbp::datatype::is_json(dtype))
            << "Datatype should be preserved with NoWithUnderlyingDatatype";
    EXPECT_FALSE(mcbp::datatype::is_xattr(dtype));
    EXPECT_FALSE(mcbp::datatype::is_snappy(dtype));

    // should not have value
    EXPECT_EQ(0, item->getNBytes());
}
