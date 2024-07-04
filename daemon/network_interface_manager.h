/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "network_interface.h"
#include "server_socket.h"
#include "ssl_utils.h"
#include "tls_configuration.h"

#include <folly/Synchronized.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/socket.h>
#include <statistics/prometheus.h>
#include <array>
#include <atomic>
#include <queue>

class NetworkInterfaceDescription;

namespace cb::mcbp {
enum class Status : uint16_t;
}

namespace folly {
class EventBase;
}

/**
 * The NetworkInterfaceManager will eventually be responsible for adding /
 * removing network interfaces and keep control of all of the network
 * interfaces memcached currently expose.
 *
 * Right now it is just a copy of what used to be the old dispatcher
 * logic
 */
class NetworkInterfaceManager {
public:
    /**
     * Create a new instance and bind it to a given event base (the same
     * base as all of the listening sockets use)
     */
    NetworkInterfaceManager(folly::EventBase& base,
                            cb::prometheus::AuthCallback authCB);

    /// Create the bootstrap interface for external users to connect to
    void createBootstrapInterface();

    // Check to see if TLS is configured or not
    bool isTlsConfigured();

    std::pair<cb::mcbp::Status, std::string> getTlsConfig();
    std::pair<cb::mcbp::Status, std::string> reconfigureTlsConfig(
            const nlohmann::json& spec);
    std::pair<cb::mcbp::Status, std::string> listInterface();
    std::pair<cb::mcbp::Status, std::string> defineInterface(
            const nlohmann::json& spec);
    std::pair<cb::mcbp::Status, std::string> deleteInterface(
            const std::string& uuid);

    uniqueSslPtr createClientSslHandle();

    std::size_t getNumberOfDaemonConnections() const;

protected:
    std::pair<cb::mcbp::Status, std::string> doListInterface();
    std::pair<cb::mcbp::Status, std::string> doDeleteInterface(
            const std::string& uuid);
    std::pair<cb::mcbp::Status, std::string> doDefineInterface(
            const nlohmann::json& spec);

    /**
     * Create the file containing all of the interfaces we're currently
     * listening to.
     *
     * @param terminate set to true if the program should terminate if an
     *                  error occurs while writing the file (normally this
     *                  is during bootstrap)
     */
    void writeInterfaceFile(bool terminate);

    /**
     * Create a new interface by using the provided attributes
     *
     * @return a JSON description of the define ports and all of the errors
     * @throws std::system_error if an error occurs
     */
    std::pair<nlohmann::json, nlohmann::json> createInterface(
            const NetworkInterfaceDescription& description);

    folly::EventBase& eventBase;
    const cb::prometheus::AuthCallback authCallback;
    /// The listen_conn vector owns all of the server sockets, and all
    /// access to the listen_conn vector _MUST_ be performed by the thread
    /// running the event base (as the objects are registered within libevent
    /// and may be in use in a callback at the same time)
    std::vector<std::unique_ptr<ServerSocket>> listen_conn;
    /// The current TLS configuration. May be accessed from all threads
    folly::Synchronized<std::unique_ptr<TlsConfiguration>> tlsConfiguration;
};

/// The one and only instance of the network interface manager.
extern std::unique_ptr<NetworkInterfaceManager> networkInterfaceManager;
