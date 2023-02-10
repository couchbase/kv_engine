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
#include <folly/io/async/AsyncTransport.h>
#include <io/network/read_callback.h>
#include <io/network/write_callback.h>
#include <mcbp/protocol/header.h>
#include <iostream>
#include <vector>

class AsyncClientConnectionImpl : public AsyncClientConnection,
                                  public folly::AsyncSocket::ConnectCallback {
public:
    AsyncClientConnectionImpl(folly::EventBase& base)
        : asyncSocket(folly::AsyncSocket::newSocket(&base)) {
        cb::io::network::OutputStreamListener osl;
        osl.transferred = [](size_t) {};
        osl.error = [this](auto message) {
            if (ioerror_listener) {
                ioerror_listener(Direction::Out, message);
            }
        };

        writeCallback = std::make_unique<cb::io::network::AsyncWriteCallback>(
                std::move(osl), [this](auto& cb, auto buf) {
                    asyncSocket->writeChain(&cb, std::move(buf));
                });

        cb::io::network::InputStreamListener isl;
        isl.eof = [this]() {
            if (eof_listener) {
                eof_listener(Direction::In);
            }
            asyncSocket->setReadCB(nullptr);
        };
        isl.error = [this](auto message, bool) {
            if (ioerror_listener) {
                ioerror_listener(Direction::In, message);
            }
            asyncSocket->setReadCB(nullptr);
        };
        isl.invalid_packet = [this](auto message) {
            if (protocol_error_listener) {
                protocol_error_listener();
            }
        };
        isl.frame_available = [this]() { scheduleExecution(); };
        readCallback = std::make_unique<cb::io::network::AsyncReadCallback>(
                std::move(isl));
    }

    void connect(std::string_view host, std::string_view port) override {
        asyncSocket->connect(this,
                             std::string{host.data(), host.size()},
                             std::stoi(std::string{port.data(), port.size()}));
    }

    ~AsyncClientConnectionImpl() override {
        asyncSocket->setReadCB(nullptr);
        readCallback.reset();
        writeCallback.reset();
    }

    void connectSuccess() noexcept override;
    void connectErr(const folly::AsyncSocketException& ex) noexcept override;

    /// Listener functions
    void setConnectListener(std::function<void()> listener) override {
        connect_listener = std::move(listener);
    }
    void setEofListener(std::function<void(Direction)> listener) override {
        eof_listener = std::move(listener);
    }
    void setIoErrorListener(std::function<void(Direction, std::string_view)>
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

    void send(const BinprotCommand& cmd) override;
    void send(std::string_view data) override;
    BinprotResponse execute(const BinprotCommand& cmd) override;

protected:
    /// Try to schedule the connection for execution. It should _ONLY_
    /// be called within the execution thread context as it use the
    /// "executionScheduled" member to avoid scheduling the connection
    /// multiple times if it gets called multiple times before the
    /// connection get the chance to execute.
    void scheduleExecution() {
        if (!executionScheduled) {
            executionScheduled = true;
            asyncSocket->getEventBase()->runInEventBaseThreadAlwaysEnqueue(
                    [this]() {
                        executionScheduled = false;

                        while (readCallback->isPacketAvailable()) {
                            if (frame_listener) {
                                frame_listener(readCallback->getPacket());
                            }
                            readCallback->nextPacket();
                        }
                    });
        }
    }
    bool executionScheduled = false;

    std::unique_ptr<folly::AsyncSocket, folly::DelayedDestruction::Destructor>
            asyncSocket;

    std::function<void()> connect_listener;
    std::function<void(Direction)> eof_listener;
    std::function<void(Direction, std::string_view)> ioerror_listener;
    std::function<void()> protocol_error_listener = []() {
        std::cout << "Protocol error" << std::endl;
        std::_Exit(EXIT_FAILURE);
    };
    std::function<void(const cb::mcbp::Header&)> frame_listener;

    std::unique_ptr<cb::io::network::AsyncReadCallback> readCallback;
    std::unique_ptr<cb::io::network::AsyncWriteCallback> writeCallback;
};

void AsyncClientConnectionImpl::send(const BinprotCommand& cmd) {
    std::vector<uint8_t> vector;
    cmd.encode(vector);
    std::array<std::string_view, 1> array{
            {{reinterpret_cast<const char*>(vector.data()), vector.size()}}};
    writeCallback->send(array);
}

void AsyncClientConnectionImpl::send(std::string_view data) {
    std::array<std::string_view, 1> array{{data}};
    writeCallback->send(array);
}

void AsyncClientConnectionImpl::connectSuccess() noexcept {
    if (connect_listener) {
        connect_listener();
    }
    asyncSocket->setReadCB(readCallback.get());
}

void AsyncClientConnectionImpl::connectErr(
        const folly::AsyncSocketException& ex) noexcept {
    if (ioerror_listener) {
        ioerror_listener(Direction::Connect, ex.what());
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

    std::optional<std::string> exception;
    ioerror_listener = [this, &exception](AsyncClientConnection::Direction dir,
                                          std::string_view message) {
        exception = message;
        asyncSocket->getEventBase()->terminateLoopSoon();
    };

    asyncSocket->getEventBase()->loopForever();
    if (exception) {
        throw std::runtime_error(
                std::string{"AsyncClientConnectionImpl::execute: "} +
                exception.value());
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
