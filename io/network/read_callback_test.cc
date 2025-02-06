/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "read_callback.h"
#include <folly/portability/GTest.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>
#include <memcached/vbucket.h>

enum class AllocationMode { Ask, Movable };
std::ostream& operator<<(std::ostream& os, const AllocationMode& mode) {
    switch (mode) {
    case AllocationMode::Ask:
        os << "ask";
        break;
    case AllocationMode::Movable:
        os << "movable";
        break;
    }
    return os;
}

class AsyncReaderUnitTests
    : public testing::Test,
      public ::testing::WithParamInterface<AllocationMode> {
protected:
    /**
     * Add a complete packet to the stream with the given frame size
     */
    void addFrameToStream(size_t bodylen) {
        std::vector<uint8_t> backing(bodylen + sizeof(cb::mcbp::Header));
        auto* request = reinterpret_cast<cb::mcbp::Request*>(backing.data());
        request->setMagic(cb::mcbp::Magic::ClientRequest);
        request->setVBucket(Vbid{666});
        request->setOpcode(cb::mcbp::ClientOpcode::Noop);
        request->setCas(0xdeadbeef);
        request->setBodylen(bodylen);
        addDataToStream(request->getFrame());
    }

    void addDataToStream(cb::const_byte_buffer data) {
        switch (GetParam()) {
        case AllocationMode::Ask:
            addDataToStream_ask(data);
            return;
        case AllocationMode::Movable:
            addDataToStream_movable(data);
            return;
        }
    }
    void addDataToStream_ask(cb::const_byte_buffer data);
    void addDataToStream_movable(cb::const_byte_buffer data);

    void SetUp() override;

    std::optional<bool> frame_available;
    std::optional<std::string> invalid_packet;
    std::optional<bool> eof;
    std::optional<std::pair<std::string, bool>> error;

    cb::io::network::AsyncReadCallback readCallback;
};

void AsyncReaderUnitTests::addDataToStream_ask(cb::const_byte_buffer data) {
    while (!data.empty()) {
        void* ptr = nullptr;
        size_t nbytes = 0;

        readCallback.getReadBuffer(&ptr, &nbytes);
        ASSERT_NE(nullptr, ptr);
        ASSERT_NE(0, nbytes);

        if (nbytes > data.size()) {
            nbytes = data.size();
        }

        memcpy(ptr, data.data(), nbytes);
        readCallback.readDataAvailable(nbytes);
        data = {data.data() + nbytes, data.size() - nbytes};
    }
}

void AsyncReaderUnitTests::addDataToStream_movable(cb::const_byte_buffer data) {
    auto buf = folly::IOBuf::create(data.size());
    std::ranges::copy(data, buf->writableTail());
    buf->append(data.size());
    readCallback.readBufferAvailable(std::move(buf));
}

void AsyncReaderUnitTests::SetUp() {
    auto& listener = readCallback.getInputStreamListener();
    listener.frame_available = [this]() { frame_available = true; };
    listener.invalid_packet = [this](auto message) {
        invalid_packet = std::string{message};
    };
    listener.eof = [this]() { eof = true; };
    listener.error = [this](auto message, auto reset) {
        error = {std::string{message}, reset};
    };
}

INSTANTIATE_TEST_SUITE_P(AllocationMode,
                         AsyncReaderUnitTests,
                         ::testing::Values(AllocationMode::Ask,
                                           AllocationMode::Movable));

// The code should support the movable mode
TEST_P(AsyncReaderUnitTests, isBufferMovable) {
    EXPECT_TRUE(readCallback.isBufferMovable());
}

TEST_P(AsyncReaderUnitTests, maxBufferSize) {
    EXPECT_EQ(21 * 1024 * 1024, readCallback.maxBufferSize());
}

TEST_P(AsyncReaderUnitTests, eof) {
    readCallback.readEOF();
    EXPECT_TRUE(eof.has_value());
    EXPECT_TRUE(eof.value());
}

TEST_P(AsyncReaderUnitTests, error) {
    readCallback.readErr(folly::AsyncSocketException{
            folly::AsyncSocketException::AsyncSocketExceptionType::UNKNOWN,
            "This is the message",
            int(std::errc::connection_refused)});
    EXPECT_TRUE(error.has_value());
    EXPECT_FALSE(error.value().second);
}

TEST_P(AsyncReaderUnitTests, errorReset) {
    readCallback.readErr(folly::AsyncSocketException{
            folly::AsyncSocketException::AsyncSocketExceptionType::UNKNOWN,
            "This is the message",
            int(std::errc::connection_reset)});
    EXPECT_TRUE(error.has_value());
    auto [message, reset] = error.value();
    EXPECT_EQ("Connection reset by peer", message);
    EXPECT_TRUE(reset);
}

TEST_P(AsyncReaderUnitTests, MessageReceived) {
    cb::mcbp::Request request = {};
    request.setMagic(cb::mcbp::Magic::ClientRequest);
    request.setOpcode(cb::mcbp::ClientOpcode::Noop);
    request.setCas(0xdeadbeef);
    request.setDatatype(cb::mcbp::Datatype::Raw);
    addDataToStream(request.getFrame());
    EXPECT_TRUE(frame_available.has_value());
    frame_available.reset();
    EXPECT_TRUE(readCallback.isPacketAvailable());
    const auto& packet = readCallback.getPacket();
    EXPECT_EQ(request.getFrame(), packet.getFrame());
    readCallback.nextPacket();
    EXPECT_FALSE(readCallback.isPacketAvailable());
}

TEST_P(AsyncReaderUnitTests, ProtocolErrorDetected) {
    size_t ii = 0;
    while (!frame_available.has_value()) {
        ++ii;
        addDataToStream({reinterpret_cast<const uint8_t*>(&ii), 1});
        ASSERT_FALSE(invalid_packet.has_value())
                << "Header should not have been parsed yet";
    }
    EXPECT_TRUE(frame_available.value());
    ASSERT_FALSE(invalid_packet.has_value())
            << "Header should not have been parsed yet";
    try {
        readCallback.isPacketAvailable();
        FAIL() << "Protocol error should throw an exception";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(nullptr,
                  std::strstr(e.what(),
                              "AsyncReadCallback::isPacketAvailable(): Invalid "
                              "packet header detected"));
    }
    EXPECT_TRUE(invalid_packet.has_value());
}

TEST_P(AsyncReaderUnitTests, MessageStreamReceived) {
    for (int ii = 0; ii < 100; ++ii) {
        addFrameToStream(ii * 1024);
    }
    for (int ii = 0; ii < 100; ++ii) {
        EXPECT_TRUE(readCallback.isPacketAvailable());
        readCallback.nextPacket();
    }
    EXPECT_FALSE(readCallback.isPacketAvailable());
}
