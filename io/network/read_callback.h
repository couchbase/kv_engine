/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocket.h>
#include <mcbp/protocol/header.h>
#include <functional>

namespace cb::io::network {

/**
 * The InputStreamListener is the glue between the AsyncReadCallback class
 * and the "application" and is just a set of function pointers which will
 * get called when certain things happen. The primary motivation for doing
 * this is to make it easy to unit test (or benchmarks when changing methods)
 * Note: None of the functions should throw any exception.
 */
struct InputStreamListener {
    /// Called as part of the callback when new data was put into the input
    /// stream _if_ there is a full frame available in the input pipe.
    std::function<void()> frame_available;
    /// The end of the stream was reached
    std::function<void()> eof;
    /// An error occurred. The string_view contains an error message, the
    /// bool is set to true if this is connection reset by peer.
    std::function<void(std::string_view, bool)> error;
    /// An invalid packet was detected on the stream. The string view contains
    /// parts of the data received.
    std::function<void(std::string_view)> invalid_packet;
};

/**
 * The AsyncReadCallback class is the class registered for a socket in
 * folly. Folly use the instance to get the buffers it should use for
 * reading data off the socket.
 *
 * In the simplest mode it may look like the implementation in folly
 * looks something like:
 *
 *     while (true) {
 *        getReadBuffer();
 *        nr = recv(buffer, size)
 *        if (nr > 0) {
 *           readDataAvailable(nr);
 *        } else (nr == 0) {
 *           readEOF();
 *        } else {
 *           readErr()
 *        }
 *     }
 *
 * "unfortunately" we have no idea how much data we may read out when
 * folly ask for the read buffer, so we'll just start off with an 8k buffer.
 * Once folly tells us that there is data available in the buffer we'll try
 * to check if we've now got a full mcbp frame and if there is we'll call the
 * frame_available callback to let the consumer start traversing the stream
 * by something like:
 *
 *     while (isPacketAvailable()) {
 *        auto& packet = getPacket();
 *        ... do something ...
 *        nextPacket();
 *     }
 *
 * The next time folly ask for a network buffer we'll start off by checking
 * if we have data in the existing buffer, and if we do have a partial header
 * we'll try to grow the buffer to fit the entire message. If we don't have
 * any data, but the current buffer is bigger than twice the default size
 * we replace the buffer with a smaller buffer (to avoid the case where all of
 * the connections keep a 21MB buffer because they _once_ operated on a 20MB
 * document.
 *
 * Note that we've not tested the "isBufferMovable" version of folly as
 * I've not yet seen it in use (it could be when used together with
 * "fizz" and TLS1.3) We'll investigate that further on.
 */
class AsyncReadCallback : public folly::AsyncReader::ReadCallback {
public:
    /**
     * Create a new instance of the AsyncReadCallback without setting any
     * of the listeners. This is solely intended for unit tests!!
     */
    AsyncReadCallback();

    /**
     * Create a new instance of the AsyncReadCallback with the provided
     * set of listeners.
     *
     * @param listener where to notify when events occurs
     */
    AsyncReadCallback(InputStreamListener listener);

    ~AsyncReadCallback() override;

    // Override folly's read callback interface
    void getReadBuffer(void** bufReturn, size_t* lenReturn) override;
    void readDataAvailable(size_t len) noexcept override;
    bool isBufferMovable() noexcept override;
    size_t maxBufferSize() const override;
    void readBufferAvailable(
            std::unique_ptr<folly::IOBuf> ptr) noexcept override;
    void readEOF() noexcept override;
    void readErr(const folly::AsyncSocketException& ex) noexcept override;
    // End folly::AsyncReader::ReadCallback interface

    /**
     * Get a view of the data available in the input stream
     * up to a provided max number. Note that the view
     * is only valid right after the callback and until the
     * control is given back to folly (or drain was called)
     *
     * @param max The maximum number of bytes to put in the view
     *            Note that the higher this number is, the higher
     *            is the likelihood of having to reallocate to
     *            concatenate the underlying buffers
     * @return The view of the data
     */
    cb::const_byte_buffer getAvailableBytes(size_t max) const;

    /// Move to the next packet in the stream
    void nextPacket();

    /**
     * Check to see if there is a packet available in the input
     * stream
     *
     * @return true if there is a packet available
     */
    bool isPacketAvailable();

    /**
     * Get the current packet at the head of the stream (only
     * available if isPacketAvailable() returns true)
     */
    const cb::mcbp::Header& getPacket() const;

    /**
     * Get a reference to the listener in use.
     *
     * This is intended for unit tests only!
     */
    InputStreamListener& getInputStreamListener() {
        return listener;
    }

protected:
    /// Check to see if there is a full frame available, and if so
    /// trigger the frame_available callback
    void maybeTriggerFrameAvailable();

    /**
     * Check if we've got a packet available. When called from the reader
     * callback from folly we're not allowed to throw any exceptions and we
     * need to deal with them elsewhere
     *
     * @param throwOnError throw an exception for protocol errors
     * @return true if a packet is available or there was a protocol error
     */
    bool isPacketAvailable(bool throwOnError);

    /// The owner which wants notifications when things happens
    InputStreamListener listener;

    /// The queue of buffers containing data from the stream
    folly::IOBufQueue input;
};

} // namespace cb::io::network
