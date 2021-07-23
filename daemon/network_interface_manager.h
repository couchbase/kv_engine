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

    std::pair<cb::mcbp::Status, std::string> doTlsReconfigure(
            const nlohmann::json& spec);
    std::pair<cb::mcbp::Status, std::string> doDefineInterface(
            const nlohmann::json& spec);
    std::pair<cb::mcbp::Status, std::string> doDeleteInterface(
            const std::string& uuid);
    std::pair<cb::mcbp::Status, std::string> doListInterface();

    uniqueSslPtr createClientSslHandle();

    /// @todo Remove once we don't allow TLS properties in memcached.json
    bool allowTlsSettingsInConfigFile() const {
        return allowMemcachedJsonTlsProps;
    }

    void disallowTlsSettingsInConfigFile() {
        allowMemcachedJsonTlsProps.store(false);
    }

protected:
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
    std::vector<std::unique_ptr<ServerSocket>> listen_conn;
    folly::Synchronized<std::unique_ptr<TlsConfiguration>> tlsConfiguration;

    // @todo remove me once we don't need to automatically update
    //       tls config from settings (people use the TLS command)
    std::atomic_bool allowMemcachedJsonTlsProps = true;
};

/// The one and only instance of the network interface manager.
extern std::unique_ptr<NetworkInterfaceManager> networkInterfaceManager;
