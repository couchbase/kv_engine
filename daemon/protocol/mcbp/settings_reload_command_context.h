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

#include "file_reload_command_context.h"
#include <daemon/network_interface_description.h>

class Settings;

/**
 * Class to deal with the reload of memcached.json and reconfigure
 * the system.
 *
 * Until ns_server implements MB-46863 reconfigure of the system may
 * be a slow process as we need to reconfigure system interfaces
 * and TLS properties as part of the reload.
 */
class SettingsReloadCommandContext : public FileReloadCommandContext {
public:
    explicit SettingsReloadCommandContext(Cookie& cookie);

protected:
    /**
     * Dispatch the task to call doSettingsReload
     *
     * @return cb::engine_errc::would_block always
     */
    cb::engine_errc reload() override;

private:
    /**
     * Get the list of all configured interfaces from the NetworkIterfaceManager
     *
     * @throws std::runtime_error if there is an error
     */
    std::vector<NetworkInterfaceDescription> getInterfaces();

    /**
     * Delete the interface identified by the provided UUID
     *
     * @param uuid The identifier of the interface to delete
     * @throws std::runtime_error if there is an error
     */
    void deleteInterface(const std::string& uuid);

    /**
     * Create a new interface from the provided spec (see
     * docs/NetworkInterface.md for a description of the spec)
     *
     * @param spec Description of the interface to create
     * @throws std::runtime_error if there is an error
     */
    void createInterface(const nlohmann::json& spec);

    /**
     * Check to see if the Prometheus port/family is updated, and if
     * so shut down the old interface and create the new one
     *
     * @param next The new configuration
     * @throws std::runtime_error if there is an error
     */
    void maybeReconfigurePrometheus(Settings& next);

    /**
     * Check to see if the any of the interfaces stored in the new
     * configuration differs from what we're currently using.
     * If they differ first create all of the new interfaces before
     * shutting down the ones no longer in the list
     *
     * @param next The new configuration
     * @throws std::runtime_error if there is an error
     */
    void maybeReconfigureInterfaces(Settings& next);

    /**
     * Do the actual settings reload
     *
     * This is the callback method being executed in the separate thread
     * in the executor pool.
     *
     * For simplicity it use exception handling for error handling to avoid
     * cluttering the code with too many explicit return code paths and to
     * make sure we get the correct error information stored in the response
     * message.
     *
     * It starts off reading the configuration file, then verifies that
     * none of the constant configuration parameters was changed before it
     * tries to change the prometheus port; then the network interfaces;
     * TLS properties and finally the rest of the properties.
     */
    cb::engine_errc doSettingsReload();
};
