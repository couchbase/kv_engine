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
 *   Unless resuired by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <cstdlib>
#include <map>
#include <string>
#include <iostream>
#include <libgreenstack/Response.h>

#include <gtest/gtest.h>


namespace Greenstack {
    class ResponseTest : public ::testing::Test {
    };

    TEST_F(ResponseTest, Constructor) {
        Response response;
        EXPECT_EQ(std::numeric_limits<uint32_t>::max(), response.getOpaque());
        EXPECT_EQ(Opcode::InvalidOpcode, response.getOpcode());
        EXPECT_TRUE(response.getFlexHeader().isEmpty());
        EXPECT_FALSE(response.isFenceBitSet());
        EXPECT_FALSE(response.isMoreBitSet());
        EXPECT_FALSE(response.isQuietBitSet());
    }

    TEST_F(ResponseTest, EmptyEncode) {
        Response response;
        std::vector<uint8_t> data;
        response.encode(data);
        EXPECT_EQ(9, data.size());

        // opaque 4 byes == 0xffffffff
        EXPECT_EQ(0xff, data[0]);
        EXPECT_EQ(0xff, data[1]);
        EXPECT_EQ(0xff, data[2]);
        EXPECT_EQ(0xff, data[3]);

        // opcode 2 bytes 0xffff
        EXPECT_EQ(0xff, data[4]);
        EXPECT_EQ(0xff, data[5]);

        // flags 1 byte, 1
        EXPECT_EQ(0x1, data[6]);

        // Status code 2 bytes, 0xffff
        EXPECT_EQ(0xff, data[7]);
        EXPECT_EQ(0xff, data[8]);
    }

    TEST_F(ResponseTest, Opaque) {
        Response res;
        std::vector<uint8_t> data;

        res.setOpaque(0xaabbccdd);
        EXPECT_EQ(0xaabbccdd, res.getOpaque());
        res.encode(data);
        EXPECT_EQ(0xaa, data[0]);
        EXPECT_EQ(0xbb, data[1]);
        EXPECT_EQ(0xcc, data[2]);
        EXPECT_EQ(0xdd, data[3]);
    }

    TEST_F(ResponseTest, Opcode) {
        Response res;
        std::vector<uint8_t> data;

        res.setOpcode(Opcode::Mutation);
        EXPECT_EQ(Opcode::Mutation, res.getOpcode());
        res.encode(data);
        EXPECT_EQ(0x04, data[4]);
        EXPECT_EQ(0x05, data[5]);
    }

    TEST_F(ResponseTest, FenceBit) {
        Response res;
        std::vector<uint8_t> data;

        res.setFenceBit(true);
        EXPECT_TRUE(res.isFenceBitSet());
        res.encode(data);
        EXPECT_EQ(0x05, data[6]);
        res.setFenceBit(false);
        EXPECT_FALSE(res.isFenceBitSet());
        res.encode(data);
        EXPECT_EQ(0x01, data[6]);
    }

    TEST_F(ResponseTest, MoreBit) {
        Response res;
        std::vector<uint8_t> data;

        res.setMoreBit(true);
        EXPECT_TRUE(res.isMoreBitSet());
        res.encode(data);
        EXPECT_EQ(0x09, data[6]);
        res.setMoreBit(false);
        EXPECT_FALSE(res.isMoreBitSet());
        res.encode(data);
        EXPECT_EQ(0x01, data[6]);
    }

    TEST_F(ResponseTest, QuietBit) {
        Response res;
        std::vector<uint8_t> data;

        res.setQuietBit(true);
        EXPECT_TRUE(res.isQuietBitSet());
        res.encode(data);
        EXPECT_EQ(0x11, data[6]);
        res.setQuietBit(false);
        EXPECT_FALSE(res.isQuietBitSet());
        res.encode(data);
        EXPECT_EQ(0x01, data[6]);
    }

    TEST_F(ResponseTest, MultipleBits) {
        Response res;
        std::vector<uint8_t> data;

        res.setFenceBit(true);
        res.setMoreBit(true);
        res.setQuietBit(true);

        EXPECT_TRUE(res.isFenceBitSet());
        EXPECT_TRUE(res.isQuietBitSet());
        EXPECT_TRUE(res.isMoreBitSet());
        res.encode(data);
        EXPECT_EQ(0x1d, data[6]);
    }

    TEST_F(ResponseTest, FlexHeaderBit) {
        Response res;
        std::vector<uint8_t> data;
        res.getFlexHeader().setHash(32);
        res.encode(data);
        EXPECT_EQ(0x3, data[6]); // is causing the flex header bit to be added
    }

    TEST_F(ResponseTest, Status) {
        Response res;
        res.setStatus(Status::InvalidArguments);
        std::vector<uint8_t> data;
        res.encode(data);
        EXPECT_EQ(0x00, data[7]);
        EXPECT_EQ(0x01, data[8]);
    }

    TEST_F(ResponseTest, Payload) {
        Response res;
        std::vector<uint8_t> data;

        std::vector<uint8_t> payload;
        payload.resize(20);
        res.setPayload(payload);
        res.encode(data);

        EXPECT_EQ(29, data.size());
        auto iterator = data.begin();
        iterator += 9;
        EXPECT_TRUE(equal(iterator, data.end(), payload.begin()));
    }

    TEST_F(ResponseTest, Decode) {
        // We've already tested the encode functionality, so
        // we can just create a new one and encode it and see
        // that it doesn't differ...
        Response res;
        res.setOpcode(Opcode::Noop);
        res.setStatus(Status::Success);
        res.setOpaque(0xaabbccdd);
        res.setFenceBit(true);
        res.setMoreBit(true);
        res.setQuietBit(true);
        res.getFlexHeader().setHash(32);
        std::vector<uint8_t> data;
        res.encode(data);

        ByteArrayReader reader(data);

        Message *created = Message::create(reader, data.size());
        EXPECT_FALSE(dynamic_cast<Response*>(created) == 0);
        std::vector<uint8_t> data2;
        created->encode(data2);

        EXPECT_TRUE(data == data2);
        delete created;
    }
}
