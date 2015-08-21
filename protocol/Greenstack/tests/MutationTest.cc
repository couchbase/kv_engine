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
    class MutationTest : public ::testing::Test {
    };

    TEST_F(MutationTest, MutationRequest) {
        auto info = std::make_shared<Greenstack::DocumentInfo>();
        auto value = std::make_shared<Greenstack::FixedByteArrayBuffer>(1024);
        info->setId("Foo");

        Greenstack::MutationRequest request;
        request.setDocumentInfo(info);
        request.setValue(value);
        request.setMutationType(Greenstack::MutationType::Set);
        request.assemble();
        request.getFlexHeader().setVbucketId(0xabcd);

        std::vector<uint8_t> data;
        Frame::encode(request, data, 0);

        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<MutationRequest *>(msg.get());
        ASSERT_NE(nullptr, decoded);

        decoded->disassemble();
        EXPECT_EQ(Greenstack::MutationType::Set, decoded->getMutationType());

        auto di = decoded->getDocumentInfo();
        EXPECT_STREQ(info->getId().c_str(), di->getId().c_str());

        auto val = decoded->getValue();
        EXPECT_EQ(value->getSize(), val->getSize());
        EXPECT_EQ(0, memcmp(value->getData(), val->getData(), val->getSize()));
        EXPECT_TRUE(decoded->getFlexHeader().haveVbucketId());
        EXPECT_EQ(0xabcd, decoded->getFlexHeader().getVbucketId());
    }

    // @todo add response
}
