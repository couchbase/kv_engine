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
#include <iostream>
#include <vector>
#include <libgreenstack/Reader.h>
#include <gtest/gtest.h>

namespace Greenstack {
    class ReaderTest : public ::testing::Test {
    protected:
        ReaderTest() {
            // write in 0xdeadfeefcafeed
            data.push_back(0xde);
            data.push_back(0xad);
            data.push_back(0xbe);
            data.push_back(0xef);
            data.push_back(0xca);
            data.push_back(0xfe);
            data.push_back(0xfe);
            data.push_back(0xed);
        }

        std::vector<uint8_t> data;
    };

    TEST_F(ReaderTest, Remainder) {
        Greenstack::ByteArrayReader reader(data);
        EXPECT_EQ(data.size(), reader.getRemainder());
    }

    TEST_F(ReaderTest, SkipSingleByte) {
        Greenstack::ByteArrayReader reader(data);
        // We should be able to skip items
        for (auto ii = data.size(); ii > 0; --ii) {
            EXPECT_EQ(ii, reader.getRemainder());
            reader.skip();
        }
    }

    TEST_F(ReaderTest, SkipMultipleByte) {
        Greenstack::ByteArrayReader reader(data);
        // We should be able to skip items
        for (auto ii = data.size(); ii > 0; ii -= 2) {
            EXPECT_EQ(ii, reader.getRemainder());
            reader.skip(2);
        }
    }

    TEST_F(ReaderTest, Reset) {
        Greenstack::ByteArrayReader reader(data);
        EXPECT_EQ(data.size(), reader.getRemainder());
        reader.skip(data.size());
        EXPECT_EQ(0, reader.getRemainder());
        reader.reset();
        EXPECT_EQ(data.size(), reader.getRemainder());
    }

    TEST_F(ReaderTest, ReadSingleByte) {
        Greenstack::ByteArrayReader reader(data);
        for (auto ii = data.size(); ii > 0; --ii) {
            uint8_t u8;
            reader.read(u8);
            EXPECT_EQ(data[data.size() - ii], u8);
        }
    }

    TEST_F(ReaderTest, ReadUnderflow) {
        Greenstack::ByteArrayReader reader(data);
        reader.skip(reader.getRemainder());
        uint8_t u8;
        EXPECT_ANY_THROW(reader.read(u8));
    }

    TEST_F(ReaderTest, ReadUint16) {
        Greenstack::ByteArrayReader reader(data);
        uint16_t u16[4];
        for (int ii = 0; ii < 4; ++ii) {
            reader.read(u16[ii]);
        }
        EXPECT_EQ(0xdead, u16[0]);
        EXPECT_EQ(0xbeef, u16[1]);
        EXPECT_EQ(0xcafe, u16[2]);
        EXPECT_EQ(0xfeed, u16[3]);
    }

    TEST_F(ReaderTest, ReadUint32) {
        Greenstack::ByteArrayReader reader(data);
        uint32_t u32[2];
        for (int ii = 0; ii < 2; ++ii) {
            reader.read(u32[ii]);
        }
        EXPECT_EQ(0xdeadbeef, u32[0]);
        EXPECT_EQ(0xcafefeed, u32[1]);
    }

    TEST_F(ReaderTest, ReadUint64) {
        Greenstack::ByteArrayReader reader(data);
        uint64_t u64;
        reader.read(u64);
        EXPECT_EQ(0xdeadbeefcafefeed, u64);
    }

    TEST_F(ReaderTest, ByteArrayReader) {
        Greenstack::ByteArrayReader reader(data.data(), 1, 3);
        uint16_t u16;
        reader.read(u16);
        EXPECT_EQ(0xadbe, u16);
    }
}
