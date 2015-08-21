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
#include <libgreenstack/FlexHeader.h>
#include <libgreenstack/Writer.h>
#include <libgreenstack/Reader.h>
#include <iostream>
#include <gtest/gtest.h>

// I need to subclass the FlexHeader class in order
// to get access to the encode function..
class MyFlexHeader : public Greenstack::FlexHeader {
public:
    void encode(Greenstack::VectorWriter&writer) {
        Greenstack::FlexHeader::encode(writer);
    }
};

namespace Greenstack {
    class FlexHeaderTest : public ::testing::Test {
    };

    TEST_F(FlexHeaderTest, LaneId) {
        MyFlexHeader header;
        EXPECT_FALSE(header.haveLaneId());
        EXPECT_ANY_THROW(header.getLaneId());
        header.setLaneId("foo");
        EXPECT_ANY_THROW(header.setLaneId("foo"));
        EXPECT_TRUE(header.haveLaneId());

        try {
            auto laneid = header.getLaneId();
            EXPECT_STRCASEEQ("FOO", laneid.c_str());
        } catch (...) {
            FAIL();
        }

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);
        EXPECT_EQ(7, data.size());
        std::vector<uint8_t> expected = {0x00, 0x00, 0x00, 0x03, 'f', 'o', 'o'};
        EXPECT_TRUE(data == expected);
    }

    TEST_F(FlexHeaderTest, Txid) {
        MyFlexHeader header;
        EXPECT_FALSE(header.haveTXID());
        EXPECT_ANY_THROW(header.getTXID());
        header.setTXID("txid");
        EXPECT_ANY_THROW(header.setTXID("blob"));
        EXPECT_TRUE(header.haveTXID());

        try {
            auto txid = header.getTXID();
            EXPECT_STRCASEEQ("txid", txid.c_str());
        } catch (...) {
            FAIL();
        }

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);
        EXPECT_EQ(8, data.size());

        std::vector<uint8_t> expected = {0x00, 0x04, 0x00, 0x04, 't', 'x', 'i', 'd'};
        EXPECT_TRUE(data == expected);
    }

    TEST_F(FlexHeaderTest, DcpId) {
        MyFlexHeader header;
        EXPECT_FALSE(header.haveDcpId());
        EXPECT_ANY_THROW(header.getDcpId());
        header.setDcpId("DCP");
        EXPECT_ANY_THROW(header.setDcpId("blob"));
        EXPECT_TRUE(header.haveDcpId());

        try {
            auto dcpid = header.getDcpId();
            EXPECT_STRCASEEQ("DCP", dcpid.c_str());
        } catch (...) {
            FAIL();
        }

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);
        EXPECT_EQ(7, data.size());

        std::vector<uint8_t> expected = {0x00, 0x06, 0x00, 0x03, 'D', 'C', 'P'};
        EXPECT_TRUE(data == expected);
    }

    TEST_F(FlexHeaderTest, CommandTimings) {
        MyFlexHeader header;
        EXPECT_FALSE(header.haveCommandTimings());
        EXPECT_ANY_THROW(header.getCommandTimings());
        header.setCommandTimings("foo");
        EXPECT_ANY_THROW(header.setCommandTimings("blob"));
        EXPECT_TRUE(header.haveCommandTimings());

        try {
            auto timings = header.getCommandTimings();
            EXPECT_STRCASEEQ("foo", timings.c_str());
        } catch (...) {
            FAIL();
        }

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);
        EXPECT_EQ(7, data.size());

        std::vector<uint8_t> expected = {0x00, 0x0a, 0x00, 0x03, 'f', 'o', 'o'};
        EXPECT_TRUE(data == expected);
    }

    TEST_F(FlexHeaderTest, Priority) {
        MyFlexHeader header;
        EXPECT_FALSE(header.havePriority());
        EXPECT_ANY_THROW(header.getPriority());
        header.setPriority(0);
        EXPECT_ANY_THROW(header.setPriority(1));
        EXPECT_TRUE(header.havePriority());

        try {
            auto priority = header.getPriority();
            EXPECT_EQ(0, priority);
        } catch (...) {
            FAIL();
        }

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);
        EXPECT_EQ(5, data.size());

        std::vector<uint8_t> expected = {0x00, 0x05, 0x00, 0x01, 0x00};
        EXPECT_TRUE(data == expected);
    }

    TEST_F(FlexHeaderTest, VbucketId) {
        MyFlexHeader header;
        EXPECT_FALSE(header.haveVbucketId());
        EXPECT_ANY_THROW(header.getVbucketId());

        header.setVbucketId(0xaa);
        EXPECT_ANY_THROW(header.setVbucketId(0xbb));
        EXPECT_TRUE(header.haveVbucketId());

        try {
            EXPECT_EQ(0xaa, header.getVbucketId());
        } catch (...) {
            FAIL();
        }

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);
        EXPECT_EQ(6, data.size());

        std::vector<uint8_t> expected = {0x00, 0x07, 0x00, 0x02, 0x00, 0xaa};
        EXPECT_TRUE(data == expected);
    }

    TEST_F(FlexHeaderTest, Hash) {
        MyFlexHeader header;
        EXPECT_FALSE(header.haveHash());
        EXPECT_ANY_THROW(header.getHash());

        header.setHash(0xaa);
        EXPECT_ANY_THROW(header.setHash(0xbb));
        EXPECT_TRUE(header.haveHash());

        try {
            EXPECT_EQ(0xaa, header.getHash());
        } catch (...) {
            FAIL();
        }

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);
        EXPECT_EQ(8, data.size());

        std::vector<uint8_t> expected = {0x00, 0x08, 0x00, 0x04, 0x00, 0x00, 0x00, 0xaa};
        EXPECT_TRUE(data == expected);
    }

    TEST_F(FlexHeaderTest, CommandTimeout) {
        MyFlexHeader header;
        EXPECT_FALSE(header.haveTimeout());
        EXPECT_ANY_THROW(header.getTimeout());

        header.setTimeout(0xaa);
        EXPECT_ANY_THROW(header.setTimeout(0xbb));
        EXPECT_TRUE(header.haveTimeout());

        try {
            EXPECT_EQ(0xaa, header.getTimeout());
        } catch (...) {
            FAIL();
        }

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);
        EXPECT_EQ(8, data.size());

        std::vector<uint8_t> expected = {0x00, 0x09, 0x00, 0x04, 0x00, 0x00, 0x00, 0xaa};
        EXPECT_TRUE(data == expected);
    }

    TEST_F(FlexHeaderTest, Mix) {
        MyFlexHeader header;

        header.setLaneId("foo");
        header.setTXID("txid");
        header.setDcpId("dcp channel");
        header.setPriority(1);
        header.setVbucketId(32);
        header.setHash(0xdeadbeef);
        header.setTimeout(100);
        header.setCommandTimings("get 32ms");

        std::vector<uint8_t> data;
        VectorWriter writer(data);

        header.encode(writer);\
        EXPECT_EQ(69, data.size());

        ByteArrayReader reader(data);
        try {
            FlexHeader decoded = FlexHeader::create(reader);
            EXPECT_STREQ("foo", decoded.getLaneId().c_str());
            EXPECT_STREQ("txid", decoded.getTXID().c_str());
            EXPECT_STREQ("dcp channel", decoded.getDcpId().c_str());
            EXPECT_EQ(1, decoded.getPriority());
            EXPECT_EQ(32, decoded.getVbucketId());
            EXPECT_EQ(0xdeadbeef, decoded.getHash());
            EXPECT_EQ(100, decoded.getTimeout());
            EXPECT_STREQ("get 32ms", decoded.getCommandTimings().c_str());
        } catch (...) {
            FAIL();
        }
    }
}
