/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <libgreenstack/Greenstack.h>
#include <gtest/gtest.h>

namespace Greenstack {
    class ListBucketsTest : public ::testing::Test {
    };

    TEST_F(ListBucketsTest, ListBucketsRequest) {
        ListBucketsRequest request;
        std::vector<uint8_t> data;
        Frame::encode(request, data, 0);
        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<ListBucketsRequest*>(msg.get());
        ASSERT_NE(nullptr, decoded);
    }

    TEST_F(ListBucketsTest, ListBucketsResponse) {
        ListBucketsResponse response(Greenstack::Status::Success);
        EXPECT_EQ(Greenstack::Status::Success, response.getStatus());

        std::vector<uint8_t> data;
        Frame::encode(response, data, 0);
        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<ListBucketsResponse*>(msg.get());
        ASSERT_NE(nullptr, decoded);
    }

    TEST_F(ListBucketsTest, ListBucketsResponseWithBuckets) {
        std::vector<std::string> names = { "foo", "bar" };

        ListBucketsResponse response(names);
        EXPECT_EQ(Greenstack::Status::Success, response.getStatus());

        std::vector<uint8_t> data;
        Frame::encode(response, data, 0);
        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<ListBucketsResponse*>(msg.get());
        ASSERT_NE(nullptr, decoded);

        EXPECT_EQ(names, decoded->getBuckets());
    }
}
