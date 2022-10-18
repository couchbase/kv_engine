/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "async_client_connection.h"
#include "client_mcbp_commands.h"
#include <cbsasl/client.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <mcbp/protocol/header.h>
#include <iostream>
#include <vector>

class AsyncClientConnectionImpl : public AsyncClientConnection,
                                  public folly::AsyncReader::ReadCallback,
                                  public folly::AsyncWriter::WriteCallback,
                                  public folly::AsyncSocket::ConnectCallback {
public:
    AsyncClientConnectionImpl(folly::EventBase& base)
        : asyncSocket(folly::AsyncSocket::newSocket(&base)) {
    }

    void connect(std::string_view host, std::string_view port) override {
        asyncSocket->connect(this,
                             std::string{host.data(), host.size()},
                             std::stoi(std::string{port.data(), port.size()}));
    }

    ~AsyncClientConnectionImpl() override = default;

    /// Folly ReadCallback interface
    void getReadBuffer(void** bufReturn, size_t* lenReturn) override;
    void readDataAvailable(size_t len) noexcept override;
    bool isBufferMovable() noexcept override;
    size_t maxBufferSize() const override;
    void readBufferAvailable(
            std::unique_ptr<folly::IOBuf> ptr) noexcept override;
    void readEOF() noexcept override;
    void readErr(const folly::AsyncSocketException& ex) noexcept override;
    /// Folly ReadCallback interface end

    /// Folly WriteCallback interface
    void writeSuccess() noexcept override;
    void writeErr(size_t bytesWritten,
                  const folly::AsyncSocketException& ex) noexcept override;
    /// Folly WriteCallback interface end

    void connectSuccess() noexcept override;
    void connectErr(const folly::AsyncSocketException& ex) noexcept override;

    /// Listener functions
    void setConnectListener(std::function<void()> listener) override {
        connect_listener = std::move(listener);
    }
    void setEofListener(std::function<void(Direction)> listener) override {
        eof_listener = std::move(listener);
    }
    void setIoErrorListener(
            std::function<void(Direction, const folly::AsyncSocketException&)>
                    listener) override {
        ioerror_listener = std::move(listener);
    }
    void setProtocolErrorListener(std::function<void()> listener) override {
        protocol_error_listener = std::move(listener);
    }
    void setFrameReceivedListener(
            std::function<void(const cb::mcbp::Header&)> listener) override {
        frame_listener = std::move(listener);
    }

    void send(std::unique_ptr<folly::IOBuf> frame) override;
    void send(const BinprotCommand& cmd) override;
    BinprotResponse execute(const BinprotCommand& cmd) override;

protected:
    /// Check to see if we have an input packet available in the input stream
    bool isPacketAvailable();

    /// Process as much as possible of the input data
    void tryProcessInputData();
    std::unique_ptr<folly::AsyncSocket, folly::DelayedDestruction::Destructor>
            asyncSocket;

    /// The number of bytes left for the current "current" packet (in the case
    /// we received a partial frame we want to be able to fit the rest of the
    /// packet inside a single allocation instead of a bunch of smaller
    /// allocations which needs to be reallocated later on)
    std::size_t current_frame_bytes_left = 0;
    /// A list of all of the IOBufs we've received so far
    std::deque<std::unique_ptr<folly::IOBuf>> input_queue;
    /// Did we schedule a callback for processing all of the input data
    /// received or not (we might get multiple smaller read callbacks
    /// in a sequence before we try to consume the input data and we don't
    /// want to schedule multiple callbacks as we consume the entire input
    /// stream once we try to consume it)
    bool scheduled_callback{false};

    std::function<void()> connect_listener;
    std::function<void(Direction)> eof_listener;
    std::function<void(Direction, const folly::AsyncSocketException&)>
            ioerror_listener;
    std::function<void()> protocol_error_listener = []() {
        std::cout << "Protocol error" << std::endl;
        std::_Exit(EXIT_FAILURE);
    };
    std::function<void(const cb::mcbp::Header&)> frame_listener;
};

bool AsyncClientConnectionImpl::isPacketAvailable() {
    if (input_queue.empty()) {
        return false;
    }

    do {
        // The input queue contains a "list" of IOBufs provided by
        // the read callbacks. If there isn't enough data in the current
        // iobuf for the entire MCBP frame we try to append the next entry
        // in the input_queue to the current buffer. This might not be
        // optimal, but we currently require the entire input frame
        // to be a single continuous segment.
        auto& buf = input_queue.front();
        if (buf->isChained()) {
            buf->coalesce();
        }

        if (buf->length() >= sizeof(cb::mcbp::Header)) {
            // We have the header; do we have the body?
            const auto* header =
                    reinterpret_cast<const cb::mcbp::Header*>(buf->data());
            if (!header->isValid()) {
                protocol_error_listener();
                return false;
            }

            const auto framesize = sizeof(*header) + header->getBodylen();
            if (buf->length() >= framesize) {
                // Header and body present
                current_frame_bytes_left = 0;
                return true;
            }
            current_frame_bytes_left = framesize - buf->length();
        } else {
            current_frame_bytes_left = sizeof(cb::mcbp::Header) - buf->length();
        }

        if (input_queue.size() == 1) {
            return false;
        }

        // Append the next buffer to this buffer
        auto iter = input_queue.begin();
        ++iter;
        if (iter == input_queue.end()) {
            // This should never occur? or
            return false;
        }

        buf->appendChain(std::move(*iter));
        input_queue.erase(iter);
    } while (true);
    // not reached
}

void AsyncClientConnectionImpl::getReadBuffer(void** bufReturn,
                                              size_t* lenReturn) {
    if (input_queue.empty() || input_queue.back()->tailroom() < 256) {
        std::size_t allocsize =
                std::max(current_frame_bytes_left, std::size_t(2048));
        input_queue.emplace_back(folly::IOBuf::create(allocsize));
    }
    *bufReturn = input_queue.back()->writableTail();
    *lenReturn = input_queue.back()->tailroom();
}

void AsyncClientConnectionImpl::readDataAvailable(size_t len) noexcept {
    // `len` bytes have been written into the backing buffer, advance the
    // tail ptr by this amount.
    input_queue.back()->append(len);
    tryProcessInputData();
}

bool AsyncClientConnectionImpl::isBufferMovable() noexcept {
    return true;
}
size_t AsyncClientConnectionImpl::maxBufferSize() const {
    return ReadCallback::maxBufferSize();
}
void AsyncClientConnectionImpl::readBufferAvailable(
        std::unique_ptr<folly::IOBuf> ptr) noexcept {
    input_queue.emplace_back(std::move(ptr));
    tryProcessInputData();
}

void AsyncClientConnectionImpl::readEOF() noexcept {
    if (eof_listener) {
        eof_listener(Direction::In);
    }
}

void AsyncClientConnectionImpl::readErr(
        const folly::AsyncSocketException& ex) noexcept {
    if (ioerror_listener) {
        ioerror_listener(Direction::In, ex);
    }
}

void AsyncClientConnectionImpl::tryProcessInputData() {
    if (isPacketAvailable() && !scheduled_callback) {
        scheduled_callback = true;
        asyncSocket->getEventBase()->runInEventBaseThreadAlwaysEnqueue(
                [this]() {
                    scheduled_callback = false;
                    while (isPacketAvailable()) {
                        auto& buf = input_queue.front();
                        const auto* header =
                                reinterpret_cast<const cb::mcbp::Header*>(
                                        buf->data());

                        size_t consumed =
                                sizeof(*header) + header->getBodylen();
                        if (frame_listener) {
                            frame_listener(*header);
                        }
                        buf->trimStart(consumed);
                    }
                });
    }
}

void AsyncClientConnectionImpl::writeSuccess() noexcept {
}

void AsyncClientConnectionImpl::writeErr(
        size_t bytesWritten, const folly::AsyncSocketException& ex) noexcept {
    if (ioerror_listener) {
        ioerror_listener(Direction::Out, ex);
    }
}

void AsyncClientConnectionImpl::send(std::unique_ptr<folly::IOBuf> frame) {
    asyncSocket->writeChain(this, std::move(frame));
}

void AsyncClientConnectionImpl::send(const BinprotCommand& cmd) {
    std::vector<uint8_t> vector;
    cmd.encode(vector);
    asyncSocket->write(this, vector.data(), vector.size());
}

void AsyncClientConnectionImpl::connectSuccess() noexcept {
    if (connect_listener) {
        connect_listener();
    }
    asyncSocket->setReadCB(this);
}

void AsyncClientConnectionImpl::connectErr(
        const folly::AsyncSocketException& ex) noexcept {
    if (ioerror_listener) {
        ioerror_listener(Direction::Connect, ex);
    }
}

BinprotResponse AsyncClientConnectionImpl::execute(const BinprotCommand& cmd) {
    send(cmd);

    /// set the read callback
    auto old_listener = std::move(frame_listener);
    auto old_ioerr_listener = std::move(ioerror_listener);

    auto scopeGuard =
            folly::makeGuard([this, &old_listener, &old_ioerr_listener] {
                frame_listener = std::move(old_listener);
                ioerror_listener = std::move(old_ioerr_listener);
            });

    BinprotResponse response;
    // @todo I need ot enqueue the old commands!
    frame_listener = [this, &response](const cb::mcbp::Header& header) {
        std::vector<uint8_t> payload(sizeof(cb::mcbp::Header) +
                                     header.getBodylen());
        std::memcpy(payload.data(), &header, payload.size());
        response.assign(std::move(payload));
        asyncSocket->getEventBase()->terminateLoopSoon();
    };

    std::optional<folly::AsyncSocketException> exception;
    ioerror_listener = [this, &exception](
                               AsyncClientConnection::Direction dir,
                               const folly::AsyncSocketException& ex) {
        exception = ex;
        asyncSocket->getEventBase()->terminateLoopSoon();
    };

    asyncSocket->getEventBase()->loopForever();
    if (exception) {
        throw std::runtime_error(
                std::string{"AsyncClientConnectionImpl::execute: "} +
                exception->what());
    }

    return response;
}

void AsyncClientConnection::authenticate(std::string_view user,
                                         std::string_view password,
                                         std::string_view mech) {
    std::string mechanism = std::string{mech.data(), mech.size()};
    if (mech.empty()) {
        auto response = execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::SaslListMechs});
        if (response.isSuccess()) {
            mechanism = response.getDataString();
            mech = mechanism;
        } else {
            throw std::runtime_error(
                    "AsyncClientConnectionImpl::authenticate (): Failed to get "
                    "server SASL mechanisms: " +
                    to_string(response.getStatus()));
        }
    }

    cb::sasl::client::ClientContext client(
            [user]() -> std::string {
                return std::string{user.data(), user.size()};
            },
            [password]() -> std::string {
                return std::string{password.data(), password.size()};
            },
            std::string{mech.data(), mech.size()});
    auto client_data = client.start();

    if (client_data.first != cb::sasl::Error::OK) {
        throw std::runtime_error(
                std::string("AsyncClientConnectionImpl::authenticate (") +
                std::string(client.getName()) + std::string("): ") +
                ::to_string(client_data.first));
    }

    auto response = execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SaslAuth,
            client.getName(),
            std::string{client_data.second.data(), client_data.second.size()}});

    while (response.getStatus() == cb::mcbp::Status::AuthContinue) {
        auto respdata = response.getData();
        client_data =
                client.step({reinterpret_cast<const char*>(respdata.data()),
                             respdata.size()});
        if (client_data.first != cb::sasl::Error::OK &&
            client_data.first != cb::sasl::Error::CONTINUE) {
            throw std::runtime_error(
                    std::string("AsyncClientConnectionImpl::authenticate: ") +
                    ::to_string(client_data.first));
        }

        BinprotSaslStepCommand stepCommand;
        stepCommand.setMechanism(client.getName());
        stepCommand.setChallenge(client_data.second);
        response = execute(stepCommand);
    }

    if (response.isSuccess()) {
        const auto challenge = response.getDataView();
        if (!challenge.empty()) {
            auto [e, c] = client.step(challenge);
            if (e != cb::sasl::Error::OK) {
                throw std::runtime_error(
                        fmt::format("Authentication failed as part of final "
                                    "verification: Error:{} Reason:{}",
                                    ::to_string(e),
                                    c));
            }
        }
    }

    if (!response.isSuccess()) {
        throw ConnectionError("Authentication failed", response);
    }
}

std::vector<cb::mcbp::Feature> AsyncClientConnection::hello(
        std::string_view agent,
        std::string_view cid,
        const std::vector<cb::mcbp::Feature>& features) {
    BinprotHelloCommand cmd(nlohmann::json{{"a", agent}, {"i", cid}}.dump());
    for (const auto& feature : features) {
        cmd.enableFeature(feature);
    }
    auto rsp = BinprotHelloResponse{execute(cmd)};
    return rsp.getFeatures();
}

std::unique_ptr<AsyncClientConnection> AsyncClientConnection::create(
        folly::EventBase& base) {
    return std::make_unique<AsyncClientConnectionImpl>(base);
}
