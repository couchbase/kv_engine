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
    class HelloTest : public ::testing::Test {
    };

    TEST_F(HelloTest, HelloRequest) {
        HelloRequest request("libgreenstack", "1.0.0", "testing");
        EXPECT_STREQ("libgreenstack", request.getUserAgent().c_str());
        EXPECT_STREQ("1.0.0", request.getUserAgentVersion().c_str());
        EXPECT_STREQ("testing", request.getComment().c_str());
        std::vector<uint8_t> data;
        Frame::encode(request, data, 0);
        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<HelloRequest*>(msg.get());
        ASSERT_NE(nullptr, decoded);
        EXPECT_STREQ(request.getUserAgent().c_str(), decoded->getUserAgent().c_str());
        EXPECT_STREQ(request.getUserAgentVersion().c_str(), decoded->getUserAgentVersion().c_str());
        EXPECT_STREQ(request.getComment().c_str(), decoded->getComment().c_str());
    }

    TEST_F(HelloTest, HelloRequestMissingAgentName) {
        EXPECT_ANY_THROW(new HelloRequest("", ""));
    }

    TEST_F(HelloTest, HelloResponse) {
        HelloResponse response("libgreenstack", "1.0.0", "PLAIN");
        EXPECT_STREQ("libgreenstack", response.getUserAgent().c_str());
        EXPECT_STREQ("1.0.0", response.getUserAgentVersion().c_str());
        EXPECT_STREQ("PLAIN", response.getSaslMechanisms().c_str());

        std::vector<uint8_t> data;
        Frame::encode(response, data, 0);
        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<HelloResponse*>(msg.get());
        ASSERT_NE(nullptr, decoded);
        EXPECT_STREQ(response.getUserAgent().c_str(), decoded->getUserAgent().c_str());
        EXPECT_STREQ(response.getUserAgentVersion().c_str(), decoded->getUserAgentVersion().c_str());
        EXPECT_STREQ(response.getSaslMechanisms().c_str(), decoded->getSaslMechanisms().c_str());
    }

    TEST_F(HelloTest, HelloResponseMissingAgentName) {
        EXPECT_ANY_THROW(new HelloResponse("", ""));
    }
}
