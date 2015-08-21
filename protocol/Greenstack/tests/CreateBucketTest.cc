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
    class CreateBucketTest : public ::testing::Test {
    };

    TEST_F(CreateBucketTest, CreateBucketRequest) {
        CreateBucketRequest request("mybucket", "myconfig", Greenstack::BucketType::Memcached);
        EXPECT_STREQ("mybucket", request.getName().c_str());
        EXPECT_STREQ("myconfig", request.getConfig().c_str());
        EXPECT_EQ(Greenstack::BucketType::Memcached, request.getType());
        std::vector<uint8_t> data;
        Frame::encode(request, data, 0);
        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<CreateBucketRequest*>(msg.get());
        ASSERT_NE(nullptr, decoded);
        EXPECT_STREQ(request.getName().c_str(), decoded->getName().c_str());
        EXPECT_STREQ(request.getConfig().c_str(), decoded->getConfig().c_str());
        EXPECT_EQ(request.getType(), decoded->getType());
    }

    TEST_F(CreateBucketTest, CreateBucketRequestMissingName) {
        EXPECT_ANY_THROW(new CreateBucketRequest("", "a", Greenstack::BucketType::Memcached));
    }

    TEST_F(CreateBucketTest, CreateBucketRequestMissingConfig) {
        CreateBucketRequest *req = 0;
        EXPECT_NO_THROW(req = new CreateBucketRequest("name", "", Greenstack::BucketType::Memcached));
        delete req;
    }

    TEST_F(CreateBucketTest, CreateBucketResponse) {
        CreateBucketResponse response;
        EXPECT_EQ(Greenstack::Status::Success, response.getStatus());

        std::vector<uint8_t> data;
        Frame::encode(response, data, 0);
        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<CreateBucketResponse*>(msg.get());
        ASSERT_NE(nullptr, decoded);
    }
}
