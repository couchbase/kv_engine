/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <iostream>
#include <libgreenstack/Writer.h>
#include <iomanip>
#include <gtest/gtest.h>

namespace Greenstack {
    class ByteArrayWriterTest : public ::testing::Test {
    protected:
        ByteArrayWriterTest() : writer(new VectorWriter(data)) {
        }

        ~ByteArrayWriterTest() {
            delete writer;
        }

        Greenstack::Writer *writer;
        std::vector<uint8_t> data;
    };

    TEST_F(ByteArrayWriterTest, WriteUint8_t) {
        uint8_t val = 0xde;
        writer->write(val);
        EXPECT_EQ(1, data.size());
        EXPECT_EQ(0xde, data[0]);
    }

    TEST_F(ByteArrayWriterTest, WriteUint16_t) {
        uint16_t val = 0xdead;
        writer->write(val);
        EXPECT_EQ(2, data.size());
        EXPECT_EQ(0xde, data[0]);
        EXPECT_EQ(0xad, data[1]);
    }

    TEST_F(ByteArrayWriterTest, WriteUint32_t) {
        uint32_t val = 0xdeadbeef;
        writer->write(val);
        EXPECT_EQ(4, data.size());
        EXPECT_EQ(0xde, data[0]);
        EXPECT_EQ(0xad, data[1]);
        EXPECT_EQ(0xbe, data[2]);
        EXPECT_EQ(0xef, data[3]);
    }

    TEST_F(ByteArrayWriterTest, WriteUint64_t) {
        uint64_t val = 0xdeadbeef;
        val <<= 32;
        val |= 0xbeefdead;
        writer->write(val);
        EXPECT_EQ(8, data.size());
        EXPECT_EQ(0xde, data[0]);
        EXPECT_EQ(0xad, data[1]);
        EXPECT_EQ(0xbe, data[2]);
        EXPECT_EQ(0xef, data[3]);
        EXPECT_EQ(0xbe, data[4]);
        EXPECT_EQ(0xef, data[5]);
        EXPECT_EQ(0xde, data[6]);
        EXPECT_EQ(0xad, data[7]);
    }

    TEST_F(ByteArrayWriterTest, PWriteUint8_t) {
        uint8_t val = 0xde;
        writer->pwrite(val, 10);
        EXPECT_EQ(11, data.size());
        EXPECT_EQ(0xde, data[10]);
    }

    TEST_F(ByteArrayWriterTest, PWriteUint16_t) {
        uint16_t val = 0xdead;
        writer->pwrite(val, 10);
        EXPECT_EQ(12, data.size());
        EXPECT_EQ(0xde, data[10]);
        EXPECT_EQ(0xad, data[11]);
    }

    TEST_F(ByteArrayWriterTest, PWriteUint32_t) {
        uint32_t val = 0xdeadbeef;
        writer->pwrite(val, 10);
        EXPECT_EQ(14, data.size());
        EXPECT_EQ(0xde, data[10]);
        EXPECT_EQ(0xad, data[11]);
        EXPECT_EQ(0xbe, data[12]);
        EXPECT_EQ(0xef, data[13]);
    }

    TEST_F(ByteArrayWriterTest, PWriteUint64_t) {
        uint64_t val = 0xdeadbeef;
        val <<= 32;
        val |= 0xbeefdead;
        writer->pwrite(val, 10);
        EXPECT_EQ(18, data.size());
        EXPECT_EQ(0xde, data[10]);
        EXPECT_EQ(0xad, data[11]);
        EXPECT_EQ(0xbe, data[12]);
        EXPECT_EQ(0xef, data[13]);
        EXPECT_EQ(0xbe, data[14]);
        EXPECT_EQ(0xef, data[15]);
        EXPECT_EQ(0xde, data[16]);
        EXPECT_EQ(0xad, data[17]);
    }

    TEST_F(ByteArrayWriterTest, GetOffset) {
        EXPECT_EQ(0, writer->getOffset());
        writer->write((uint8_t)0xde);
        EXPECT_EQ(1, writer->getOffset());
        writer->write((uint16_t)0xde);
        EXPECT_EQ(3, writer->getOffset());
    }


    class ByteArrayWriterOffsetTest : public ::testing::Test {
    protected:
        ByteArrayWriterOffsetTest() : writer(new VectorWriter(data, 100)) {
        }

        ~ByteArrayWriterOffsetTest() {
            delete writer;
        }

        Greenstack::Writer *writer;
        std::vector<uint8_t> data;
    };

    TEST_F(ByteArrayWriterOffsetTest, WriteUint8_t) {
        uint8_t val = 0xde;
        writer->write(val);
        EXPECT_EQ(101, data.size());
        EXPECT_EQ(0xde, data[100]);
    }

    TEST_F(ByteArrayWriterOffsetTest, WriteUint16_t) {
        uint16_t val = 0xdead;
        writer->write(val);
        EXPECT_EQ(102, data.size());
        EXPECT_EQ(0xde, data[100]);
        EXPECT_EQ(0xad, data[101]);
    }

    TEST_F(ByteArrayWriterOffsetTest, WriteUint32_t) {
        uint32_t val = 0xdeadbeef;
        writer->write(val);
        EXPECT_EQ(104, data.size());
        EXPECT_EQ(0xde, data[100]);
        EXPECT_EQ(0xad, data[101]);
        EXPECT_EQ(0xbe, data[102]);
        EXPECT_EQ(0xef, data[103]);
    }

    TEST_F(ByteArrayWriterOffsetTest, WriteUint64_t) {
        uint64_t val = 0xdeadbeef;
        val <<= 32;
        val |= 0xbeefdead;
        writer->write(val);
        EXPECT_EQ(108, data.size());
        EXPECT_EQ(0xde, data[100]);
        EXPECT_EQ(0xad, data[101]);
        EXPECT_EQ(0xbe, data[102]);
        EXPECT_EQ(0xef, data[103]);
        EXPECT_EQ(0xbe, data[104]);
        EXPECT_EQ(0xef, data[105]);
        EXPECT_EQ(0xde, data[106]);
        EXPECT_EQ(0xad, data[107]);
    }

    TEST_F(ByteArrayWriterOffsetTest, PWriteUint8_t) {
        uint8_t val = 0xde;
        writer->pwrite(val, 10);
        EXPECT_EQ(111, data.size());
        EXPECT_EQ(0xde, data[110]);
    }

    TEST_F(ByteArrayWriterOffsetTest, PWriteUint16_t) {
        uint16_t val = 0xdead;
        writer->pwrite(val, 10);
        EXPECT_EQ(112, data.size());
        EXPECT_EQ(0xde, data[110]);
        EXPECT_EQ(0xad, data[111]);
    }

    TEST_F(ByteArrayWriterOffsetTest, PWriteUint32_t) {
        uint32_t val = 0xdeadbeef;
        writer->pwrite(val, 10);
        EXPECT_EQ(114, data.size());
        EXPECT_EQ(0xde, data[110]);
        EXPECT_EQ(0xad, data[111]);
        EXPECT_EQ(0xbe, data[112]);
        EXPECT_EQ(0xef, data[113]);
    }

    TEST_F(ByteArrayWriterOffsetTest, PWriteUint64_t) {
        uint64_t val = 0xdeadbeef;
        val <<= 32;
        val |= 0xbeefdead;
        writer->pwrite(val, 10);
        EXPECT_EQ(118, data.size());
        EXPECT_EQ(0xde, data[110]);
        EXPECT_EQ(0xad, data[111]);
        EXPECT_EQ(0xbe, data[112]);
        EXPECT_EQ(0xef, data[113]);
        EXPECT_EQ(0xbe, data[114]);
        EXPECT_EQ(0xef, data[115]);
        EXPECT_EQ(0xde, data[116]);
        EXPECT_EQ(0xad, data[117]);
    }

    TEST_F(ByteArrayWriterOffsetTest, GetOffset) {
        EXPECT_EQ(0, writer->getOffset());
        writer->write((uint8_t)0xde);
        EXPECT_EQ(1, writer->getOffset());
        writer->write((uint16_t)0xde);
        EXPECT_EQ(3, writer->getOffset());
    }


}
