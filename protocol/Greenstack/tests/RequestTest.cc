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

#include <cstdlib>
#include <map>
#include <string>
#include <cstring>
#include <iostream>
#include <libgreenstack/Request.h>

#include <gtest/gtest.h>


namespace Greenstack {
    class RequestTest : public ::testing::Test {
    };

    TEST_F(RequestTest, Constructor) {
        Request request;
        EXPECT_EQ(std::numeric_limits<uint32_t>::max(), request.getOpaque());
        EXPECT_EQ(Opcode::InvalidOpcode, request.getOpcode());
        EXPECT_TRUE(request.getFlexHeader().isEmpty());
        EXPECT_FALSE(request.isFenceBitSet());
        EXPECT_FALSE(request.isMoreBitSet());
        EXPECT_FALSE(request.isQuietBitSet());
    }

    TEST_F(RequestTest, EmptyEncode) {
        Request request;
        std::vector<uint8_t> data;
        request.encode(data);
        EXPECT_EQ(7, data.size());

        // opaque 4 byes == 0xffffffff
        EXPECT_EQ(0xff, data[0]);
        EXPECT_EQ(0xff, data[1]);
        EXPECT_EQ(0xff, data[2]);
        EXPECT_EQ(0xff, data[3]);

        // opcode 2 bytes 0xffff
        EXPECT_EQ(0xff, data[4]);
        EXPECT_EQ(0xff, data[5]);

        // flags 1 byte 0
        EXPECT_EQ(0x0, data[6]);
    }

    TEST_F(RequestTest, Opaque) {
        Request req;
        std::vector<uint8_t> data;

        req.setOpaque(0xaabbccdd);
        EXPECT_EQ(0xaabbccdd, req.getOpaque());
        req.encode(data);
        EXPECT_EQ(0xaa, data[0]);
        EXPECT_EQ(0xbb, data[1]);
        EXPECT_EQ(0xcc, data[2]);
        EXPECT_EQ(0xdd, data[3]);
    }

    TEST_F(RequestTest, Opcode) {
        Request req;
        std::vector<uint8_t> data;

        req.setOpcode(Opcode::Mutation);
        EXPECT_EQ(Opcode::Mutation, req.getOpcode());
        req.encode(data);
        EXPECT_EQ(0x04, data[4]);
        EXPECT_EQ(0x05, data[5]);
    }

    TEST_F(RequestTest, FenceBit) {
        Request req;
        std::vector<uint8_t> data;

        req.setFenceBit(true);
        EXPECT_TRUE(req.isFenceBitSet());
        req.encode(data);
        EXPECT_EQ(0x04, data[6]);
        req.setFenceBit(false);
        EXPECT_FALSE(req.isFenceBitSet());
        req.encode(data);
        EXPECT_EQ(0x00, data[6]);
    }

    TEST_F(RequestTest, MoreBit) {
        Request req;
        std::vector<uint8_t> data;

        req.setMoreBit(true);
        EXPECT_TRUE(req.isMoreBitSet());
        req.encode(data);
        EXPECT_EQ(0x08, data[6]);
        req.setMoreBit(false);
        EXPECT_FALSE(req.isMoreBitSet());
        req.encode(data);
        EXPECT_EQ(0x00, data[6]);
    }

    TEST_F(RequestTest, QuietBit) {
        Request req;
        std::vector<uint8_t> data;

        req.setQuietBit(true);
        EXPECT_TRUE(req.isQuietBitSet());
        req.encode(data);
        EXPECT_EQ(0x10, data[6]);
        req.setQuietBit(false);
        EXPECT_FALSE(req.isQuietBitSet());
        req.encode(data);
        EXPECT_EQ(0x00, data[6]);
    }

    TEST_F(RequestTest, MultipleBits) {
        Request req;
        std::vector<uint8_t> data;

        req.setFenceBit(true);
        req.setMoreBit(true);
        req.setQuietBit(true);

        EXPECT_TRUE(req.isFenceBitSet());
        EXPECT_TRUE(req.isQuietBitSet());
        EXPECT_TRUE(req.isMoreBitSet());
        req.encode(data);
        EXPECT_EQ(0x1c, data[6]);
    }

    TEST_F(RequestTest, FlexHeaderBit) {
        Request req;
        std::vector<uint8_t> data;
        req.getFlexHeader().setHash(32);
        req.encode(data);
        EXPECT_EQ(0x2, data[6]); // is causing the flex header bit to be added
    }

    TEST_F(RequestTest, Payload) {
        Request req;
        std::vector<uint8_t> data;

        std::vector<uint8_t> payload;
        payload.resize(20);
        req.setPayload(payload);
        req.encode(data);

        EXPECT_EQ(27, data.size());
        auto iterator = data.begin();
        iterator += 7;
        EXPECT_TRUE(equal(iterator, data.end(), payload.begin()));
    }

    TEST_F(RequestTest, Decode) {
        // We've already tested the encode functionality, so
        // we can just create a new one and encode it and see
        // that it doesn't differ...
        Request req;
        req.setOpaque(0xaabbccdd);
        req.setOpcode(Opcode::ListBuckets);
        req.setFenceBit(true);
        req.setMoreBit(true);
        req.setQuietBit(true);
        req.getFlexHeader().setHash(32);
        std::vector<uint8_t> data;
        req.encode(data);

        ByteArrayReader reader(data);

        Message *created = Message::create(reader, data.size());
        EXPECT_FALSE(dynamic_cast<Request*>(created) == 0);
        std::vector<uint8_t> data2;
        created->encode(data2);

        EXPECT_TRUE(data == data2);
        delete created;
    }
}
