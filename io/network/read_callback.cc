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
#include <fmt/format.h>
#include <fmt/ostream.h>

namespace cb::io::network {

static constexpr size_t MaxFrameSize = 21 * 1024 * 1024;
static constexpr size_t DefaultBufferSize = 8192;
static constexpr size_t MinTailroomSize = 256;

AsyncReadCallback::AsyncReadCallback() = default;

AsyncReadCallback::AsyncReadCallback(InputStreamListener listener)
    : listener(std::move(listener)) {
}

AsyncReadCallback::~AsyncReadCallback() = default;

void AsyncReadCallback::getReadBuffer(void** bufReturn, size_t* lenReturn) {
    try {
        auto [p, s] = input.preallocate(MinTailroomSize, DefaultBufferSize);
        *bufReturn = p;
        *lenReturn = s;
    } catch (const std::bad_alloc&) {
        *lenReturn = 0;
        *bufReturn = nullptr;
        return;
    }
}

void AsyncReadCallback::readDataAvailable(size_t len) noexcept {
    input.postallocate(len);
    maybeTriggerFrameAvailable();
}

bool AsyncReadCallback::isBufferMovable() noexcept {
    return true;
}

size_t AsyncReadCallback::maxBufferSize() const {
    return MaxFrameSize;
}

void AsyncReadCallback::readBufferAvailable(
        std::unique_ptr<folly::IOBuf> ptr) noexcept {
    input.append(std::move(ptr));
    maybeTriggerFrameAvailable();
}

void AsyncReadCallback::readEOF() noexcept {
    listener.eof();
}

void AsyncReadCallback::readErr(
        const folly::AsyncSocketException& ex) noexcept {
    int error = ex.getErrno();
#ifdef WIN32
    // Remap the WSAECONNRESET so we don't need to deal with that error
    // code.
    if (error == WSAECONNRESET) {
        error = ECONNRESET;
    }
#endif
    if (error == ECONNRESET) {
        listener.error("Connection reset by peer", true);
    } else {
        listener.error(fmt::format("Read error: {}", ex.what()), false);
    }
}

cb::const_byte_buffer AsyncReadCallback::getAvailableBytes(size_t max) const {
    return {input.front()->data(),
            std::min(std::size_t(max), input.front()->length())};
}

void AsyncReadCallback::nextPacket() {
    input.trimStart(getPacket().getFrame().size());
}

bool AsyncReadCallback::isPacketAvailable() {
    return isPacketAvailable(true);
}

const cb::mcbp::Header& AsyncReadCallback::getPacket() const {
    return *reinterpret_cast<const cb::mcbp::Header*>(input.front()->data());
}

void AsyncReadCallback::maybeTriggerFrameAvailable() {
    if (isPacketAvailable(false)) {
        listener.frame_available();
    }
}

/**
 * Check if we've got a packet available. When called from the reader
 * callback from folly we're not allowed to throw any exceptions and we
 * need to deal with them elsewhere
 *
 * @param throwOnError throw an exception for protocol errors
 * @return true if a packet is available or there was a protocol error
 */
bool AsyncReadCallback::isPacketAvailable(bool throwOnError) {
    if (input.empty()) {
        return false;
    }

    try {
        // gather may throw an exception if:
        // it fails to allocate memory to a continuous segment
        // there isn't enough bytes of data available
        input.gather(sizeof(cb::mcbp::Header));
    } catch (const std::exception&) {
        return false;
    }
    if (input.front()->length() < sizeof(cb::mcbp::Header)) {
        return false;
    }

    auto& packet = getPacket();
    if (!packet.isValid()) {
        if (throwOnError) {
            listener.invalid_packet(
                    {reinterpret_cast<const char*>(input.front()->data()),
                     std::min(std::size_t(1024), input.front()->length())});
            throw std::runtime_error(
                    fmt::format("AsyncReadCallback::isPacketAvailable(): "
                                "Invalid packet header detected: ({})",
                                packet));
        } else {
            return true;
        }
    }

    const auto framesize = packet.getFrame().size();
    if (framesize > MaxFrameSize) {
        if (throwOnError) {
            throw std::runtime_error(fmt::format(
                    "AsyncReadCallback::isPacketAvailable(): The packet size "
                    "{} exceeds the max allowed packet size {}",
                    std::to_string(framesize),
                    std::to_string(MaxFrameSize)));
        } else {
            return true;
        }
    }

    try {
        input.gather(framesize);
    } catch (const std::overflow_error&) {
        // not enough bytes available in the chain of buffers
        return false;
    } catch (const std::bad_alloc&) {
        // Failed to allocate continuous space..
        return false;
    }
    return (input.front()->length() >= framesize);
}

} // namespace cb::io::network
