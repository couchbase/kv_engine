/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <platform/socket.h>
#include <atomic>
#include <memory>

class ListeningPort;
struct event_base;

/**
 * The ServerSocket represents the socket used to accept new clients.
 */
class ServerSocket {
public:
    ServerSocket(const ServerSocket&) = delete;

    /**
     * Create a new instance
     *
     * @param sfd The socket to operate on
     * @param b The event base to use (the caller owns the event base)
     * @param interf The interface object containing properties to use
     */
    static std::unique_ptr<ServerSocket> create(
            SOCKET sfd, event_base* b, std::shared_ptr<ListeningPort> interf);

    virtual ~ServerSocket() = default;

    virtual const ListeningPort& getInterfaceDescription() const = 0;

    /// Update the interface description to use the provided SSL info
    virtual void updateSSL(const std::string& key, const std::string& cert) = 0;

    /**
     * Get the details for this connection to put in the portnumber
     * file so that the test framework may pick up the port numbers
     */
    virtual nlohmann::json toJson() const = 0;

    virtual const std::string& getUuid() const = 0;

    /// Get the number of instances of ServerSocket currently in use (used
    /// by the stats and we could probably move it elsewhere?)
    static uint64_t getNumInstances() {
        return numInstances.load();
    }

protected:
    ServerSocket() = default;

    /// The current number of instances of ServerSockets
    static std::atomic<uint64_t> numInstances;
};
