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

#include <daemon/sendbuffer.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/header.h>
#include <functional>
#include <memory>

namespace cb::io::network {

/**
 * The OutputStreamListener is the glue between the AsyncWriteCallback class
 * and the "application" and is just a set of function pointers which will
 * get called when certain things happen. The primary motivation for doing
 * this is to make it easy to unit test (or benchmarks when changing methods)
 * Note: None of the functions should throw any exception.
 */
struct OutputStreamListener {
    /// Called after successfully send the provided number of bytes
    std::function<void(size_t)> transferred;
    /// Called when an error occurs while sending data
    std::function<void(std::string_view)> error;
};

/**
 * The write callback gets called from folly when it is done writing
 * the provided data.
 *
 * Our backend use a single WriteCallback object for all data transfer, and
 * internally we keep a "list" of our requests so that we may release
 * references to the objects when they're no longer needed.
 */
class AsyncWriteCallback : public folly::AsyncWriter::WriteCallback {
public:
    /**
     * Create a new instance of the AsyncWriteCallback
     *
     * @param listener Where to notify when things happens from folly
     * @param sink Where to forward the data
     */
    AsyncWriteCallback(OutputStreamListener listener,
                       std::function<void(AsyncWriteCallback&,
                                          std::unique_ptr<folly::IOBuf>)> sink);
    ~AsyncWriteCallback() override;

    // Override folly's write callback interface
    void writeSuccess() noexcept override;
    void writeErr(size_t bytesWritten,
                  const folly::AsyncSocketException& ex) noexcept override;
    // End folly::AsyncWriter::WriteCallback interface

    /// Get the total number of bytes currently queued for sending
    std::size_t getSendQueueSize();

    /// Take ownership of the iobuf and transfer it
    void send(std::unique_ptr<folly::IOBuf> iob);

    /**
     * Copy the content of a span of views of data to the socket
     *
     * @param data The data to send
     */
    void send(gsl::span<std::string_view> data);

    /**
     * Take the ownership of a send buffer and tell folly to send the data
     * it contains of without making a local copy)
     *
     * @param buffer The data to send
     */
    void send(std::unique_ptr<SendBuffer> buffer);

    /**
     * Get a reference to the listener in use.
     *
     * This is intended for unit tests only!
     */
    OutputStreamListener& getOutputStreamListener() {
        return listener;
    }

protected:
    OutputStreamListener listener;
    struct SendRequest {
        SendRequest(std::size_t nb) : nbytes(nb) {
        }
        SendRequest(std::unique_ptr<SendBuffer> buffer)
            : nbytes(buffer->getPayload().size()), buffer(std::move(buffer)) {
        }
        std::size_t nbytes;
        std::unique_ptr<SendBuffer> buffer;
    };

    std::deque<SendRequest> sendq;
    std::function<void(AsyncWriteCallback&, std::unique_ptr<folly::IOBuf>)>
            sink;
};
} // namespace cb::io::network
