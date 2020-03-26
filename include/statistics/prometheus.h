/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include <memory>
#include <string>

#include <platform/socket.h>

namespace prometheus {
// forward declaration
class Exposer;
} // namespace prometheus

namespace cb::prometheus {

/**
 * Indicates which group of stats should be collected for a given
 * request
 *  * low cardinality: per-bucket or global instance stats
 *  * high cardinality: per-collection/per-scope stats
 */
enum class Cardinality { Low, High };

void initialize(const std::pair<in_port_t, sa_family_t>& config);

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
    explicit MetricServer(in_port_t port, sa_family_t family);
    ~MetricServer();

    MetricServer(const MetricServer&) = delete;
    MetricServer(MetricServer&&) = delete;

    MetricServer& operator=(const MetricServer&) = delete;
    MetricServer& operator=(MetricServer&&) = delete;

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

private:
    class KVCollectable;

    // Prometheus exposer takes weak pointers to `Collectable`s
    std::shared_ptr<KVCollectable> stats;
    std::shared_ptr<KVCollectable> statsHC;

    // May be empty if the exposer could not be initialised
    // e.g., port already in use
    std::unique_ptr<::prometheus::Exposer> exposer;
};
} // namespace cb::prometheus