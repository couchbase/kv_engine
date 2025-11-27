/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <mcbp/codec/crc_sink.h>
#include <platform/crc32c.h>

#ifdef WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

// Test params
// 1. number of chunks
// 2. request chunk size (the chunk which is checksummed)
// 3. true add a tail chunk which is smaller than the chunk to cover cases when
// file doesn't divide evenly into the chunk size
class CRCSinkTest
    : public ::testing::TestWithParam<std::tuple<size_t, size_t, bool> > {
public:
    void SetUp() override {
        buildData(std::get<0>(GetParam()),
                  std::get<1>(GetParam()),
                  std::get<2>(GetParam()));
    }
    void buildData(size_t chunk, size_t chunkSize, bool addTail);
    std::string data;
};

class TestSink : public cb::io::Sink {
public:
    void sink(std::string_view data) override {
        bytes_written += data.size();
    }
    std::size_t fsync() override {
        return 0;
    }
    std::size_t close() override {
        return 0;
    }
    std::size_t getBytesWritten() const override {
        return bytes_written;
    }
    size_t bytes_written{0};
};

// Build "GetFileFragment" with checksum data.
void CRCSinkTest::buildData(size_t chunks, size_t chunkSize, bool addTail) {
    const size_t tailSize = addTail ? chunkSize - 1 : 0;
    ASSERT_NE(0, chunks);
    ASSERT_NE(0, chunkSize);
    ASSERT_LT(tailSize, chunkSize);
    auto appendChecksummedChunk = [&](size_t size) {
        if (size == 0) {
            return;
        }
        // Generate the "file data" in chunkSize chunks.
        for (size_t fileByte = 0; fileByte < size; ++fileByte) {
            data.push_back('a' + static_cast<char>(fileByte & 0xffull));
        }
        std::string_view fileData{&data.back() - (size - 1), size};
        // And append the checksum as per GetFileFragment (network order)
        auto crc = htonl(crc32c(fileData, 0));
        data.push_back(crc & 0xff);
        data.push_back((crc >> 8) & 0xff);
        data.push_back((crc >> 16) & 0xff);
        data.push_back((crc >> 24) & 0xff);
    };

    // This loop generates the "GetFileFragment" data when checksumming is
    // enabled.
    for (size_t chunk = 0; chunk < chunks; ++chunk) {
        appendChecksummedChunk(chunkSize);
    }

    // Finally add any tail data (chunk smaller than chunkSize)
    appendChecksummedChunk(tailSize);
}

TEST_P(CRCSinkTest, TestCrcData) {
    const size_t chunks = std::get<0>(GetParam());
    const size_t chunkSize = std::get<1>(GetParam());
    const size_t tailSize = std::get<2>(GetParam()) ? chunkSize - 1 : 0;
    // Create a TestSink which has a lifetime longer than the CRCSink. This
    // is to test that the bytes passed through are correct. In the real
    // usage of the CRCSink the real_sink will be a FileSink which may have
    // a lifetime longer than the CRCSink because it is appending to the
    // real file.
    auto real_sink = std::make_unique<TestSink>();

    // Run tests using different "spans", this is mimicking the network
    // reading layer reading the stream in smaller spans.
    for (size_t span = 1; span <= data.size(); ++span) {
        // Create the CRC sink and attach to the test sink
        auto sink = std::make_unique<cb::snapshot::CRCSink>(
                chunkSize, data.size(), *real_sink);

        // Split the data into subsets to mimic the network reading layer
        // reading the stream in smaller spans.
        std::string_view view{data};
        while (view.size()) {
            // Create a view that is the span size or the remaining data
            // size, whichever is smaller.
            auto chunk = view.substr(0, std::min(view.size(), span));
            view.remove_prefix(chunk.size());
            ASSERT_NO_THROW(sink->sink(chunk))
                    << "span: " << span << " chunk: " << chunk.size()
                    << " data size: " << data.size() << " chunks: " << chunks
                    << " chunkSize: " << chunkSize << " tailSize: " << tailSize;
        }
        // Bytes written vs bytes passed through differs as the lifetime of
        // the real_sink is longer than the CRCSink.
        EXPECT_EQ(sink->getBytesWritten(),
                  span * (chunks * chunkSize + tailSize));
        EXPECT_EQ(sink->getBytesPassedThrough(), chunks * chunkSize + tailSize);
    }
}

TEST_P(CRCSinkTest, TestCrcDataCorrupt) {
    auto real_sink = std::make_unique<TestSink>();

    const size_t chunks = std::get<0>(GetParam());
    const size_t chunkSize = std::get<1>(GetParam());

    // Flip a bit
    size_t offset = (chunks * (chunkSize + sizeof(uint32_t))) - 1;
    data[offset] = data[offset] ^ 0x1;

    for (size_t span = 1; span <= data.size(); ++span) {
        // Create the CRC sink and attach to the test sink
        auto sink = std::make_unique<cb::snapshot::CRCSink>(
                chunkSize, data.size(), *real_sink);

        // Split the data into subsets to mimic the network reading layer
        // reading the stream in smaller spans.
        std::string_view view{data};
        try {
            while (view.size()) {
                // Create a view that is the span size or the remaining data
                // size, whichever is smaller.
                auto chunk = view.substr(0, std::min(view.size(), span));
                view.remove_prefix(chunk.size());
                sink->sink(chunk);
            }
        } catch (const std::exception& e) {
            continue;
        }
        FAIL();
    }
}

// Parameters are #chunks, chunkSize, true add a tail (smaller thank
// chunkSize)
INSTANTIATE_TEST_SUITE_P(
        CRCSinkTest,
        CRCSinkTest,
        ::testing::Combine(::testing::Values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                           ::testing::Values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                           ::testing::Values(true, false)));