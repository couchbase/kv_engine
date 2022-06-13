/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "cardinality.h"

#include <array>
#include <functional>
#include <memory>
#include <string>

#include <memcached/engine_error.h>
#include <platform/socket.h>

namespace prometheus {
// forward declaration
class Exposer;
} // namespace prometheus

class PrometheusStatCollector;

namespace cb::prometheus {
/**
 * Flag whether a given scrape request should generate and include timestamps.
 *
 * Generating timestamps allows KV to minimize the disk space required for
 * Prometheus to store metric samples.
 *
 * However, if ns_server is scraping KV metrics on behalf of an external
 * Prometheus instance, it is preferred that timestamps are _not_ included,
 * as ns_server concatenates all component metrics and other components do not
 * include timestamps.
 */
enum class IncludeTimestamps {
    No,
    Yes,
};

using AuthCallback =
        std::function<bool(const std::string&, const std::string&)>;

using GetStatsCallback =
        std::function<cb::engine_errc(const PrometheusStatCollector&)>;

/**
 * Initialize the prometheus exporter
 *
 * @param config the port number and address family to bind to (specifying
 *               0 as the port number will use an ephemeral port)
 * @param authCB The callback to use for authentication for the requests
 * @return The current configuration
 * @throws std::bad_alloc for memory allocation failures
 * @throws std::runtime_errors if we failed to start the exporter service
 */
nlohmann::json initialize(const std::pair<in_port_t, sa_family_t>& config,
                          AuthCallback authCB);

void addEndpoint(std::string path,
                 IncludeTimestamps timestamps,
                 GetStatsCallback getStatsCB);

void shutdown();

std::pair<in_port_t, sa_family_t> getRunningConfig();

nlohmann::json getRunningConfigAsJson();

/**
 * Global manager for exposing stats for Prometheus.
 *
 * Callbacks may be registered which will be called when the
 * appropriate HTTP endpoint is scraped.
 */
class MetricServer {
public:
    /**
     * Construct a MetricServer instance listening on
     * the interface and port specified as arguments.
     *
     * @param port port to listen on, 0 for random free port
     * @param family AF_INET/AF_INET6
     */
    explicit MetricServer(in_port_t port,
                          sa_family_t family,
                          AuthCallback authCB);
    ~MetricServer();

    MetricServer(const MetricServer&) = delete;
    MetricServer(MetricServer&&) = delete;

    MetricServer& operator=(const MetricServer&) = delete;
    MetricServer& operator=(MetricServer&&) = delete;

    void addEndpoint(std::string path,
                     IncludeTimestamps timestamps,
                     GetStatsCallback getStatsCB);

    /**
     * Check if the HTTP server was created successfully and
     * can server incoming requests.
     *
     * Creating the server (Exposer) may have failed if the port is
     * in use.
     */
    [[nodiscard]] bool isAlive() const;

    /**
     * Get the port the HTTP server is listening on. Useful if the
     * port was specified as 0 and a random free port was allocated.
     *
     * Requires that the exposer was created successfully, so
     * isAlive() should always be checked first.
     */
    [[nodiscard]] in_port_t getListeningPort() const;

    [[nodiscard]] std::pair<in_port_t, sa_family_t> getRunningConfig() const;

    [[nodiscard]] nlohmann::json getRunningConfigAsJson() const;

private:
    class Endpoint;

    // Prometheus exposer takes weak pointers to `Collectable`s
    std::vector<std::shared_ptr<Endpoint>> endpoints;

    // May be empty if the exposer could not be initialised
    // e.g., port already in use
    std::unique_ptr<::prometheus::Exposer> exposer;

    // Realm name sent to unauthed clients in 401 Unauthorized responses.
    static const std::string authRealm;
    const AuthCallback authCB;

    const sa_family_t family;
    const std::string uuid;
};
} // namespace cb::prometheus