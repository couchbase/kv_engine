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

#include <folly/io/async/EventBase.h>
#include <protocol/connection/async_client_connection.h>
#include <string>

namespace cb::mcbp {
class Header;
class Response;
class Request;
} // namespace cb::mcbp

namespace cb::test {

/**
 * The DcpDrain class is a class which provides a minimalistic implementation
 * to drain a buckets content via DCP
 */
class DcpDrain {
public:
    /**
     * Create a new instance of the DcpDrain
     *
     * @param host The host to connect to (127.0.0.1)
     * @param port The port number on the host
     * @param username The username to authenticate as
     * @param password The users password
     * @param bucketname The name of the bucket to drain
     */
    DcpDrain(std::string host,
             std::string port,
             std::string username,
             std::string password,
             std::string bucketname);

    /**
     * Connect to the server and drain the entire bucket
     *
     * @throws std::exception if an error occurs
     */
    void drain();

    /// Get the number of mutations received
    size_t getNumMutations() const {
        return num_mutations;
    }

    /// Get the number of read units received
    size_t getRu() const {
        return ru;
    }

protected:
    void onFrameReceived(const cb::mcbp::Header& header);
    void onResponse(const cb::mcbp::Response& res);
    void onRequest(const cb::mcbp::Request& req);
    void connect();
    void setFeatures();
    void selectBucket();
    void openDcp();
    void setControlMessages();
    void sendStreamRequest();
    void handleDcpNoop(const cb::mcbp::Request& header);

    const std::string host;
    const std::string port;
    const std::string username;
    const std::string password;
    const std::string bucketname;
    folly::EventBase base;
    std::unique_ptr<AsyncClientConnection> connection;
    std::optional<std::string> error;
    std::size_t num_mutations = 0;
    std::size_t num_deletions = 0;
    std::size_t num_expirations = 0;
    std::size_t ru = 0;
};
} // namespace cb::test
