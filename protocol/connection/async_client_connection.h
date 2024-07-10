/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "client_mcbp_commands.h"
#include <functional>

namespace folly {
class AsyncSocketException;
class EventBase;
class IOBuf;
class SSLContext;
class SocketAddress;
} // namespace folly

namespace cb::mcbp {
class Header;
class Request;
class Response;
} // namespace cb::mcbp

/**
 * The async client connection allows for being run in an event base and
 * get callbacks for each frame received over the connection.
 *
 * For simplicity, it allows for some synchronous operations to be used
 * in the case where you know you won't suddenly receive an unexpected
 * message (out of order responses, server initiated push messages etc).
 * That is to be fixed in later versions.
 */
class AsyncClientConnection {
public:
    enum class Direction { Connect, In, Out };
    virtual ~AsyncClientConnection() = default;

    /**
     * Create a new instance
     *
     * @param address The IP address to connect to
     * @param ssl_context An SSL context to use if TLS is to be used, empty
     *                    for non-TLS
     * @param base The event base this instance should be bound to (its
     *             lifetime must exceed the lifetime of the created instance)
     * @return The newly created instance
     */
    static std::unique_ptr<AsyncClientConnection> create(
            folly::SocketAddress address,
            std::shared_ptr<folly::SSLContext> ssl_context,
            folly::EventBase& base);

    /// Initiate connect
    virtual void connect() = 0;

    /// Set up the function to be called once we're connected
    virtual void setConnectListener(std::function<void()> listener) = 0;

    /// Set up the EOF listener to be called once we reach end of stream
    virtual void setEofListener(std::function<void(Direction)> listener) = 0;

    /// Set up the Error listener which is called when an IO error occurs
    virtual void setIoErrorListener(
            std::function<void(Direction, std::string_view)> listener) = 0;

    /// Set up the protocol error listener which is to be called if we
    /// encounter an error on the stream
    virtual void setProtocolErrorListener(std::function<void()> listener) = 0;

    /// Set up the listener to be called every time we receive a frame over
    /// the network
    virtual void setFrameReceivedListener(
            std::function<void(const cb::mcbp::Header&)> listener) = 0;

    /// Send a BinprotCommand to the other end
    virtual void send(const BinprotCommand& cmd) = 0;

    /// Send a "blob" to the other end
    virtual void send(std::string_view data) = 0;
    virtual void send(std::unique_ptr<folly::IOBuf> iobuf) = 0;

    // The following API don't belong in a async client,  but it makes
    // it a lot easier to use in a common way. They will mock around with the
    // response listeners and should NOT be called when the event loop is
    // running

    /// Authenticate as the provided username by using the provided password
    /// and the provided list of mechanism to choose from
    void authenticate(std::string_view user,
                      std::string_view password,
                      std::string_view mech = {});

    /// Run hello to enable features on the connection and return the
    /// result
    std::vector<cb::mcbp::Feature> hello(
            std::string_view agent,
            std::string_view cid,
            const std::vector<cb::mcbp::Feature>& features);

    /// Send the provided command to the other end and return the response
    virtual BinprotResponse execute(const BinprotCommand& cmd) = 0;
};
