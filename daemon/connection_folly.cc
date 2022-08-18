/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "connection_folly.h"
#include "buckets.h"
#include "connections.h"
#include "cookie.h"
#include "front_end_thread.h"
#include "mcaudit.h"
#include "settings.h"
#include "tracing.h"

#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <phosphor/phosphor.h>

namespace cb::daemon {

/**
 * The AsyncReadCallback class is the class registed for a socket in
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
 * It is not allowed to throw any exceptions from within the callback,
 * so we try to schedule a invocation of our object in order to act
 * on the data received.
 *
 * Given that we don't know up front how mych data we've got available we try
 * to allocate a "chunk" of data (2k by default) and start parsing the packets
 * in there, but once we know how much data we actually need for (you read
 * out the first 2k of a 20MB MCBP frame) the next allocation tries to
 * allocate enough space to get the rest to avoid too many reallocations
 * happening at a later time. Ideally we should allocate a buffer big
 * enough so that we could move over the trailing piece of the current
 * buffer and avoid a reallocation and memory move later on). (and if the
 * client pipeline multiple 8k sets and they all arrive at the socket buffer
 * on the server before we start to drain it, it is only the _first_ command
 * which gets the bigger allocation on the following buffers as we (currently)
 * only inspect the first packet.
 *
 * There is a second functionality in folly where it will do the allocation
 * of the io buffers for you and provide them to you, but I haven't seen
 * that in use on any of my attempts. It could be that it is used when using
 * TLS.
 */
class AsyncReadCallback : public folly::AsyncReader::ReadCallback {
public:
    AsyncReadCallback(FollyConnection& c) : connection(c) {
    }

    ~AsyncReadCallback() override = default;

    void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
        if (inputQueue.empty() || inputQueue.back()->tailroom() < 256) {
            std::size_t allocsize =
                    std::max(current_frame_bytes_left, std::size_t(2048));
            inputQueue.emplace_back(folly::IOBuf::create(allocsize));
        }
        *bufReturn = inputQueue.back()->writableTail();
        *lenReturn = inputQueue.back()->tailroom();
    }

    void readDataAvailable(size_t len) noexcept override {
        inputQueue.back()->append(len);
        maybeScheduleExecution();
    }

    bool isBufferMovable() noexcept override {
        return true;
    }

    size_t maxBufferSize() const override {
        return Settings::instance().getMaxPacketSize() + 1024;
    }

    void readBufferAvailable(
            std::unique_ptr<folly::IOBuf> ptr) noexcept override {
        inputQueue.emplace_back(std::move(ptr));
        maybeScheduleExecution();
    }

    void readEOF() noexcept override {
        LOG_DEBUG("{}: Socket EOF", connection.getId());
        connection.setTerminationReason("Client closed connection");
        connection.sendQueueInfo.term = true;
        if (connection.state == Connection::State::running) {
            connection.shutdown();
        }
        connection.scheduleExecution();
    }

    void readErr(const folly::AsyncSocketException& ex) noexcept override {
        int error = ex.getErrno();
#ifdef WIN32
        // Remap the WSAECONNRESET so we don't need to deal with that error
        // code.
        if (error == WSAECONNRESET) {
            error = ECONNRESET;
        }
#endif
        if (error == ECONNRESET) {
            LOG_DEBUG("{} Client closed socket", connection.getId());
            connection.setTerminationReason("Client closed connection");
        } else {
            LOG_WARNING("{} Read error: {}", connection.getId(), ex.what());
            connection.setTerminationReason(ex.what());
        }
        connection.sendQueueInfo.term = true;
        if (connection.state == Connection::State::running) {
            connection.shutdown();
        }
        connection.scheduleExecution();
    }

    cb::const_byte_buffer getAvailableBytes(size_t max) const {
        if (inputQueue.empty()) {
            return {};
        }
        auto& buf = inputQueue.front();
        return {buf->data(), std::min(std::size_t(1024), buf->length())};
    }

    void drained(size_t nb) {
        next = nullptr;
        inputQueue.front()->trimStart(nb);
    }

    bool isPacketAvailable() {
        return isPacketAvailable(true);
    }

    const cb::mcbp::Header& getPacket() const {
        return *next;
    }

protected:
    /// The number of bytes left for the current "current" packet.
    std::size_t current_frame_bytes_left = 0;

    void maybeScheduleExecution() {
        if (isPacketAvailable(false)) {
            connection.scheduleExecution();
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
    bool isPacketAvailable(bool throwOnError) {
        if (inputQueue.empty()) {
            return false;
        }

        do {
            // The input queue contains a "list" of IOBufs provided by
            // the read callbacks. If there isn't enough data in the current
            // iobuf for the entire MCBP frame we try to append the next entry
            // in the inputQueue to the current buffer. This might not be
            // optimal, but we currently require the entire input frame
            // to be a single continous segment. It would probably be better
            // if we required the header, extras and key to be a single buffer,
            // and the value could be an io-vector. _or_ we could just create
            // a new buffer containing _just_ the data we need from "this"
            // and the next buffer (to avoid having huge memcpy if we just
            // want 2 bytes from a 20MB buffer).
            auto& buf = inputQueue.front();
            if (buf->isChained()) {
                buf->coalesce();
            }

            if (buf->length() >= sizeof(cb::mcbp::Header)) {
                // We have the header; do we have the body?
                const auto* header =
                        reinterpret_cast<const cb::mcbp::Header*>(buf->data());
                if (!header->isValid()) {
                    if (throwOnError) {
                        audit_invalid_packet(
                                connection,
                                {buf->data(),
                                 std::min(std::size_t(1024), buf->length())});
                        throw std::runtime_error(
                                "Connection::isPacketAvailable(): Invalid "
                                "packet header detected");
                    } else {
                        current_frame_bytes_left = 0;
                        return true;
                    }
                }

                const auto framesize = sizeof(*header) + header->getBodylen();
                if (buf->length() >= framesize) {
                    // Header and body present
                    next = header;
                    current_frame_bytes_left = 0;
                    return true;
                }

                // We don't have the entire frame available. Are we receiving
                // an incredible big packet so that we want to disconnect the
                // client?
                const auto max = Settings::instance().getMaxPacketSize();
                if (framesize > max) {
                    if (throwOnError) {
                        throw std::runtime_error(fmt::format(
                                "The packet size {} exceeds the max allowed "
                                "packet size {}",
                                std::to_string(framesize),
                                std::to_string(max)));
                    } else {
                        current_frame_bytes_left = 0;
                        return true;
                    }
                }
                current_frame_bytes_left = framesize - buf->length();
            } else {
                current_frame_bytes_left =
                        sizeof(cb::mcbp::Header) - buf->length();
            }

            if (inputQueue.size() == 1) {
                return false;
            }

            // Append the next buffer to this buffer
            auto iter = inputQueue.begin();
            ++iter;
            if (iter == inputQueue.end()) {
                // This should never occur? or
                return false;
            }

            buf->appendChain(std::move(*iter));
            inputQueue.erase(iter);
        } while (true);
        // not reached
    }

    std::vector<uint8_t> bytes;
    FollyConnection& connection;
    std::deque<std::unique_ptr<folly::IOBuf>> inputQueue;
    const cb::mcbp::Header* next;
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
    AsyncWriteCallback(FollyConnection& c) : connection(c) {
    }
    ~AsyncWriteCallback() override = default;
    void writeSuccess() noexcept override {
        connection.updateSendBytes(sendq.front().nbytes);
        sendq.pop_front();
    }
    void writeErr(size_t bytesWritten,
                  const folly::AsyncSocketException& ex) noexcept override {
        LOG_WARNING("{} writeErr {}", connection.getId(), ex.what());
        connection.setTerminationReason(ex.what());
        connection.sendQueueInfo.term = true;
        if (connection.state == Connection::State::running) {
            connection.shutdown();
        }
        connection.scheduleExecution();
    }
    std::size_t getSendQueueSize() {
        size_t ret = 0;
        for (const auto& v : sendq) {
            ret += v.nbytes;
        }
        return ret;
    }

    /**
     * Copy the content of a span of views of data to the socket
     *
     * @param sock The socket object to send the data on
     * @param data The data to send
     */
    void send(folly::AsyncSocket& sock, gsl::span<std::string_view> data) {
        std::size_t total = 0;

        for (const auto& d : data) {
            total += d.size();
        }

        if (!total) {
            return;
        }

        auto iob = folly::IOBuf::createCombined(total);
        for (const auto& d : data) {
            if (!d.empty()) {
                std::memcpy(iob->writableTail(), d.data(), d.size());
                iob->append(d.size());
            }
        }

        sendq.emplace_back(SendRequest{total});
        sock.writeChain(this, std::move(iob));
    }

    /**
     * Take the ownership of a send buffer and tell folly to send the data
     * it contains of without making a local copy)
     *
     * @param sock The socket object to send the data on
     * @param buffer The data to send
     */
    void send(folly::AsyncSocket& sock, std::unique_ptr<SendBuffer> buffer) {
        auto payload = buffer->getPayload();
        auto iob = folly::IOBuf::wrapBuffer(payload.data(), payload.size());
        sendq.emplace_back(SendRequest{std::move(buffer)});
        sock.writeChain(this, std::move(iob));
    }

protected:
    FollyConnection& connection;
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
};

} // namespace cb::daemon

FollyConnection::FollyConnection(SOCKET sfd,
                                 FrontEndThread& thr,
                                 std::shared_ptr<ListeningPort> descr,
                                 uniqueSslPtr sslStructure)
    : Connection(sfd, thr, std::move(descr), sslStructure ? true : false) {
    using cb::daemon::AsyncReadCallback;
    using cb::daemon::AsyncWriteCallback;
    asyncReadCallback = std::make_unique<AsyncReadCallback>(*this);
    asyncWriteCallback = std::make_unique<AsyncWriteCallback>(*this);

    asyncSocket = folly::AsyncSocket::newSocket(&thread.eventBase,
                                                folly::NetworkSocket(sfd));
    if (!asyncSocket->setZeroCopy(true)) {
        LOG_DEBUG("{} Failed to set zero copy mode", getId());
    }
    asyncSocket->setReadCB(asyncReadCallback.get());
}

FollyConnection::~FollyConnection() = default;

void FollyConnection::scheduleExecution() {
    if (!executionScheduled) {
        executionScheduled = true;
        incrementRefcount();
        thread.eventBase.runInEventBaseThreadAlwaysEnqueue([this]() {
            decrementRefcount();
            executionScheduled = false;
            TRACE_LOCKGUARD_TIMED(thread.mutex,
                                  "mutex",
                                  "Connection::scheduleExecution::threadLock",
                                  SlowMutexThreshold);
            if (!executeCommandsCallback()) {
                conn_destroy(this);
            }
        });
    }
}

void FollyConnection::copyToOutputStream(std::string_view data) {
    std::array<std::string_view, 1> d{{data}};
    asyncWriteCallback->send(*asyncSocket, d);
}

void FollyConnection::copyToOutputStream(gsl::span<std::string_view> data) {
    asyncWriteCallback->send(*asyncSocket, data);
}

void FollyConnection::chainDataToOutputStream(
        std::unique_ptr<SendBuffer> buffer) {
    if (!buffer || buffer->getPayload().empty()) {
        throw std::logic_error(
                "Connection::chainDataToOutputStream: buffer must be set");
    }

    asyncWriteCallback->send(*asyncSocket, std::move(buffer));
}

bool FollyConnection::isPacketAvailable() const {
    return asyncReadCallback->isPacketAvailable();
}

const cb::mcbp::Header& FollyConnection::getPacket() const {
    return asyncReadCallback->getPacket();
}

cb::const_byte_buffer FollyConnection::getAvailableBytes(size_t max) const {
    return asyncReadCallback->getAvailableBytes(max);
}

size_t FollyConnection::getSendQueueSize() const {
    return asyncWriteCallback->getSendQueueSize();
}

void FollyConnection::triggerCallback() {
    scheduleExecution();
}

void FollyConnection::disableReadEvent() {
    asyncSocket->setReadCB(nullptr);
}

void FollyConnection::enableReadEvent() {
    asyncSocket->setReadCB(asyncReadCallback.get());
}

void FollyConnection::drainInputPipe(size_t bytes) {
    asyncReadCallback->drained(bytes);
}
