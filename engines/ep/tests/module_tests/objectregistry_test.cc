/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "config.h"

#include "objectregistry.h"

#include "item.h"
#include "test_helpers.h"
#include "tests/mock/mock_synchronous_ep_engine.h"

#include <gtest/gtest.h>

class ObjectRegistryTest : public ::testing::Test {
protected:
    void SetUp() override {
        ObjectRegistry::onSwitchThread(&engine);
    }
    void TearDown() override {
        ObjectRegistry::onSwitchThread(nullptr);
    }

    SynchronousEPEngine engine;
};

// Check that constructing & destructing an Item is correctly tracked in
// EpStats::numItem via ObjectRegistry::on{Create,Delete}Item.
TEST_F(ObjectRegistryTest, NumItem) {
    ASSERT_EQ(0, *engine.getEpStats().numItem);

    {
        auto item = make_item(0, makeStoredDocKey("key"), "value");
        EXPECT_EQ(1, *engine.getEpStats().numItem);
    }
    EXPECT_EQ(0, *engine.getEpStats().numItem);
}

// Check that constructing & destructing an Item is correctly tracked in
// EpStats::memOverhead via ObjectRegistry::on{Create,Delete}Item.
TEST_F(ObjectRegistryTest, MemOverhead) {
    ASSERT_EQ(0, *engine.getEpStats().memOverhead);

    {
        auto item = make_item(0, makeStoredDocKey("key"), "value");
        // Currently just checking the overhead is non-zero; could expand
        // to calculate expected size based on the Item's size.
        EXPECT_NE(0, *engine.getEpStats().memOverhead);
    }
    EXPECT_EQ(0, *engine.getEpStats().memOverhead);
}
