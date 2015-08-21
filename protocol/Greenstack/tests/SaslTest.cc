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
    class SaslTest : public ::testing::Test {
    };

    TEST_F(SaslTest, SaslAuthRequest) {
        SaslAuthRequest request("PLAIN", "xxxx");
        EXPECT_STREQ("xxxx", request.getChallenge().c_str());
        EXPECT_STREQ("PLAIN", request.getMechanism().c_str());

        std::vector<uint8_t> data;
        Frame::encode(request, data, 0);

        ByteArrayReader reader(data);
        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<SaslAuthRequest*>(msg.get());
        ASSERT_NE(nullptr, decoded);

        EXPECT_STREQ(request.getChallenge().c_str(), decoded->getChallenge().c_str());
        EXPECT_STREQ(request.getMechanism().c_str(), decoded->getMechanism().c_str());
    }

    TEST_F(SaslTest, SaslAuthResponse) {
        SaslAuthResponse response("xxxx");
        EXPECT_STREQ("xxxx", response.getChallenge().c_str());

        std::vector<uint8_t> data;
        Frame::encode(response, data, 0);
        ByteArrayReader reader(data);
        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<SaslAuthResponse*>(msg.get());
        ASSERT_NE(nullptr, decoded);

        EXPECT_STREQ(response.getChallenge().c_str(), decoded->getChallenge().c_str());
    }
}
