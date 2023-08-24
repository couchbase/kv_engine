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
#include "cookie.h"
#include "front_end_thread.h"
#include "mcaudit.h"
#include "network_interface_manager.h"
#include "settings.h"
#include "tracing.h"
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/SSLContext.h>
#include <io/network/read_callback.h>
#include <io/network/write_callback.h>
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <phosphor/phosphor.h>

FollyConnection::FollyConnection(SOCKET sfd,
                                 FrontEndThread& thr,
                                 std::shared_ptr<ListeningPort> descr,
                                 std::shared_ptr<folly::SSLContext> ssl_context)
    : Connection(sfd, thr, std::move(descr)) {
    cb::io::network::InputStreamListener inputDataListener;
    inputDataListener.frame_available = [this]() {
        if (state == State::running) {
            TRACE_LOCKGUARD_TIMED(
                    thread.mutex,
                    "mutex",
                    "FollyConnection::scheduleExecution::threadLock",
                    SlowMutexThreshold);
            try {
                executeCommandsCallback();
            } catch (const std::exception& e) {
                LOG_WARNING("{} Got exception: {}", getId(), e.what());
            }
        }
        if (state != State::running) {
            scheduleExecution();
        }
    };
    inputDataListener.eof = [this]() {
        setTerminationReason("Client closed connection");
        sendQueueInfo.term = true;
        if (state == Connection::State::running) {
            shutdown();
        }
        scheduleExecution();
    };
    inputDataListener.error = [this](std::string_view message, bool reset) {
        setTerminationReason(std::string{message});
        if (reset) {
            LOG_DEBUG("{} {}", getId(), message);
        } else {
            LOG_WARNING("{} {}", getId(), message);
        }
        sendQueueInfo.term = true;
        if (state == Connection::State::running) {
            shutdown();
        }
        scheduleExecution();
    };
    inputDataListener.invalid_packet = [this](std::string_view packet) {
        audit_invalid_packet(*this,
                             {reinterpret_cast<const uint8_t*>(packet.data()),
                              packet.size()});
    };
    asyncReadCallback = std::make_unique<cb::io::network::AsyncReadCallback>(
            inputDataListener);

    cb::io::network::OutputStreamListener outputDataListener;
    outputDataListener.transferred = [this](size_t nbytes) {
        updateSendBytes(nbytes);
    };
    outputDataListener.error = [this](std::string_view message) {
        if (state != Connection::State::immediate_close) {
            LOG_WARNING("{} writeErr {}", getId(), message);
        }
        if (getTerminationReason().empty()) {
            setTerminationReason(std::string{message});
        }
        sendQueueInfo.term = true;
        if (state == Connection::State::running) {
            shutdown();
        }
        scheduleExecution();
    };
    asyncWriteCallback = std::make_unique<cb::io::network::AsyncWriteCallback>(
            outputDataListener,
            [this](auto& cb, std::unique_ptr<folly::IOBuf> iob) {
                asyncSocket->writeChain(&cb, std::move(iob));
            });

    if (ssl_context) {
        auto* ss = new folly::AsyncSSLSocket(
                ssl_context, &thread.eventBase, folly::NetworkSocket(sfd));
        asyncSocket.reset(ss);
        ss->sslAccept(this);
    } else {
        asyncSocket = folly::AsyncSocket::newSocket(&thread.eventBase,
                                                    folly::NetworkSocket(sfd));
    }

    if (asyncSocket->setZeroCopy(true)) {
        LOG_DEBUG("{}: Using zero copy mode", getId());
    }
    asyncSocket->setReadCB(asyncReadCallback.get());
}

FollyConnection::~FollyConnection() = default;

bool FollyConnection::handshakeVer(folly::AsyncSSLSocket* socket,
                                   bool preverifyOk,
                                   X509_STORE_CTX* ctx) noexcept {
    return HandshakeCB::handshakeVer(socket, preverifyOk, ctx);
}

void FollyConnection::handshakeSuc(folly::AsyncSSLSocket* sock) noexcept {
    onTlsConnect(sock->getSSL());
}

void FollyConnection::handshakeErr(
        folly::AsyncSSLSocket* sock,
        const folly::AsyncSocketException& ex) noexcept {
    LOG_WARNING("{}: Failed to do TLS handshake: {}", getId(), ex.what());
}

void FollyConnection::scheduleExecution() {
    if (!executionScheduled) {
        executionScheduled = true;
        incrementRefcount();
        thread.eventBase.runInEventBaseThreadAlwaysEnqueue([this]() {
            decrementRefcount();
            executionScheduled = false;
            TRACE_LOCKGUARD_TIMED(
                    thread.mutex,
                    "mutex",
                    "FollyConnection::scheduleExecution::threadLock",
                    SlowMutexThreshold);
            if (state == State::immediate_close) {
                // we might have a stack of events we're waiting for
                // notifications on..
                if (asyncWriteCallback->getSendQueueSize() == 0) {
                    thread.destroy_connection(*this);
                }
                return;
            }

            if (!executeCommandsCallback() &&
                asyncWriteCallback->getSendQueueSize() == 0) {
                thread.destroy_connection(*this);
            }
        });
    }
}

void FollyConnection::copyToOutputStream(std::string_view data) {
    std::array<std::string_view, 1> d{{data}};
    copyToOutputStream(d);
}

void FollyConnection::copyToOutputStream(gsl::span<std::string_view> data) {
    Expects(asyncSocket->getEventBase()->isInEventBaseThread());
    asyncWriteCallback->send(data);
}

void FollyConnection::chainDataToOutputStream(
        std::unique_ptr<SendBuffer> buffer) {
    Expects(asyncSocket->getEventBase()->isInEventBaseThread());
    if (!buffer || buffer->getPayload().empty()) {
        throw std::logic_error(
                "FollyConnection::chainDataToOutputStream: buffer must be set");
    }

    asyncWriteCallback->send(std::move(buffer));
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

void FollyConnection::nextPacket() {
    asyncReadCallback->nextPacket();
}
