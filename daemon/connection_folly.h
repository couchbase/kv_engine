/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "connection.h"
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/DelayedDestruction.h>
#include <memory>

namespace cb::io::network {
class AsyncReadCallback;
class AsyncWriteCallback;
} // namespace cb::io::network

namespace folly {
class AsyncSocket;
class EventBase;
} // namespace folly
using AsyncSocketUniquePtr =
        std::unique_ptr<folly::AsyncSocket,
                        folly::DelayedDestruction::Destructor>;

/// Implementation of the Connection class using Folly for IO.
class FollyConnection : public Connection,
                        public folly::AsyncSSLSocket::HandshakeCB {
public:
    FollyConnection(SOCKET sfd,
                    FrontEndThread& thr,
                    std::shared_ptr<ListeningPort> descr,
                    std::shared_ptr<folly::SSLContext> ssl_context);
    ~FollyConnection() override;
    void copyToOutputStream(std::string_view data) override;
    void copyToOutputStream(gsl::span<std::string_view> data) override;
    void chainDataToOutputStream(std::unique_ptr<SendBuffer> buffer) override;
    bool isPacketAvailable() const override;
    const cb::mcbp::Header& getPacket() const override;
    cb::const_byte_buffer getAvailableBytes(size_t max = 1024) const override;
    size_t getSendQueueSize() const override;
    void nextPacket() override;
    void triggerCallback() override;
    void disableReadEvent() override;
    void enableReadEvent() override;

    ///////// Handshake callback API override
    bool handshakeVer(folly::AsyncSSLSocket* socket,
                      bool preverifyOk,
                      X509_STORE_CTX* ctx) noexcept override;

    void handshakeSuc(folly::AsyncSSLSocket* sock) noexcept override;

    void handshakeErr(folly::AsyncSSLSocket* sock,
                      const folly::AsyncSocketException& ex) noexcept override;
    ///////// End handshake callback API override

protected:
    /// The async read callback in use
    std::unique_ptr<cb::io::network::AsyncReadCallback> asyncReadCallback;
    /// The async write callback in use
    std::unique_ptr<cb::io::network::AsyncWriteCallback> asyncWriteCallback;
    /// The underlying Folly Async Socket object
    AsyncSocketUniquePtr asyncSocket;

    /// Try to schedule the connection for execution. It should _ONLY_
    /// be called within the execution thread context as it use the
    /// "executionScheduled" member to avoid scheduling the connection
    /// multiple times if it gets called multiple times before the
    /// connection get the chance to execute.
    void scheduleExecution();
    bool executionScheduled = false;
};
