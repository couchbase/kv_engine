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
    class GetTest : public ::testing::Test {
    };

    TEST_F(GetTest, RequestMissingId) {
        EXPECT_ANY_THROW(new Greenstack::GetRequest(""));
    }


    TEST_F(GetTest, RequestGetId) {
        Greenstack::GetRequest request("mydoc");
        EXPECT_STREQ("mydoc", request.getId().c_str());
    }

    TEST_F(GetTest, EncodeDecodeGetRequest) {
        Greenstack::GetRequest request("mydoc");
        request.getFlexHeader().setVbucketId(0xabcd);
        std::vector<uint8_t> data;
        Frame::encode(request, data, 0);

        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<GetRequest *>(msg.get());
        ASSERT_NE(nullptr, decoded);
        EXPECT_TRUE(decoded->getFlexHeader().haveVbucketId());
        EXPECT_EQ(0xabcd, decoded->getFlexHeader().getVbucketId());

        EXPECT_STREQ(request.getId().c_str(), decoded->getId().c_str());
    }

    TEST_F(GetTest, Response) {
        GetResponse response;
        EXPECT_EQ(Status::Success, response.getStatus());
        EXPECT_ANY_THROW(response.assemble());
    }

    TEST_F(GetTest, ResponseWithStatus) {
        GetResponse response(Status::InternalError);
        EXPECT_EQ(Status::InternalError, response.getStatus());
    }

    TEST_F(GetTest, ResponseWithPayload) {
        GetResponse response;

        auto info = std::make_shared<Greenstack::DocumentInfo>();
        auto value = std::make_shared<Greenstack::FixedByteArrayBuffer>(1024);
        info->setId("Foo");
        info->setCompression(Compression::None);
        info->setDatatype(Datatype::Json);
        info->setFlags(0x0abcdef0);
        info->setCas(0xff);

        response.setDocumentInfo(info);
        response.setValue(value);
        response.assemble();

        std::vector<uint8_t> data;
        Frame::encode(response, data, 0);

        ByteArrayReader reader(data);

        auto msg = Frame::createUnique(reader);
        ASSERT_NE(nullptr, msg.get());

        auto decoded = dynamic_cast<GetResponse *>(msg.get());
        ASSERT_NE(nullptr, decoded);

        decoded->disassemble();

        auto di = decoded->getDocumentInfo();
        EXPECT_STREQ(info->getId().c_str(), di->getId().c_str());
        EXPECT_EQ(info->getCas(), di->getCas());
        EXPECT_EQ(info->getCompression(), di->getCompression());
        EXPECT_EQ(info->getFlags(), di->getFlags());
        EXPECT_EQ(info->getDatatype(), di->getDatatype());

        auto val = decoded->getValue();
        EXPECT_EQ(value->getSize(), val->getSize());
        EXPECT_EQ(0, memcmp(value->getData(), val->getData(), val->getSize()));
    }
}
