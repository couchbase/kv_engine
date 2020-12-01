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

#include "statistics/prometheus.h"
#include "statistics/prometheus_collector.h"

#include <daemon/log_macros.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <folly/SynchronizedPtr.h>
#include <logger/logger.h>
#include <prometheus/exposer.h>
#include <gsl/gsl>
#include <utility>

namespace cb::prometheus {

const std::string MetricServer::lowCardinalityPath = "/_prometheusMetrics";
const std::string MetricServer::highCardinalityPath = "/_prometheusMetricsHigh";
const std::string MetricServer::authRealm = "KV Prometheus Exporter";

folly::SynchronizedPtr<std::unique_ptr<MetricServer>> instance;

void initialize(const std::pair<in_port_t, sa_family_t>& config,
                GetStatsCallback getStatsCB,
                AuthCallback authCB) {
    auto handle = instance.wlockPointer();
    // May be called at init or on config change.
    // If an instance already exists, destroy it before creating
    // a new one.
    handle->reset();

    auto [port, family] = config;
    *handle = std::make_unique<MetricServer>(
            port, family, std::move(getStatsCB), std::move(authCB));
    if (!(*handle)->isAlive()) {
        FATAL_ERROR(EXIT_FAILURE,
                    fmt::format("Failed to start Prometheus exposer on "
                                "family:{} port:{}",
                                (family == AF_INET) ? "inet" : "inet6",
                                port));
    }

    // if the configured port is set to 0, an available port number will have
    // been selected, log that instead of 0.
    auto listeningPort = (*handle)->getListeningPort();
    LOG_INFO("Prometheus Exporter started, listening on family:{} port:{}",
             (family == AF_INET) ? "inet" : "inet6",
             listeningPort);
}

void shutdown() {
    instance.wlockPointer()->reset();
}

std::pair<in_port_t, sa_family_t> getRunningConfig() {
    auto handle = instance.rlock();
    if (!handle || !handle->isAlive()) {
        // no MetricServer, or it is not listening
        return {};
    }
    return handle->getRunningConfig();
}

class MetricServer::KVCollectable : public ::prometheus::Collectable {
public:
    KVCollectable(Cardinality cardinality, GetStatsCallback getStatsCB)
        : cardinality(cardinality), getStatsCB(std::move(getStatsCB)) {
    }
    /**
     * Gathers high or low cardinality metrics
     * and returns them in the prometheus required structure.
     */
    [[nodiscard]] std::vector<::prometheus::MetricFamily> Collect()
            const override {
        std::unordered_map<std::string, ::prometheus::MetricFamily> statsMap;
        PrometheusStatCollector collector(statsMap);
        getStatsCB(collector, cardinality);

        // KVCollectable interface requires a vector of metric families,
        // but during collection it is necessary to frequently look up
        // families by name, so they are stored in a map.
        // Unpack them into a vector.
        std::vector<::prometheus::MetricFamily> result;

        result.reserve(statsMap.size());

        for (const auto& statEntry : statsMap) {
            result.push_back(statEntry.second /* MetricFamily */);
        }

        return result;
    }

private:
    Cardinality cardinality;

    // function to call on every incoming request to generate stats
    GetStatsCallback getStatsCB;
};

MetricServer::MetricServer(in_port_t port,
                           sa_family_t family,
                           GetStatsCallback getStatsCB,
                           AuthCallback authCB)
    : stats(std::make_shared<KVCollectable>(Cardinality::Low, getStatsCB)),
      statsHC(std::make_shared<KVCollectable>(Cardinality::High, getStatsCB)),
      family(family) {
    try {
        /*
         * The connectionStr should meet the spec for civetweb's
         * "listening_ports" config - see
         * https://github.com/civetweb/civetweb/blob/master/docs/UserManual.md#listening_ports-8080
         *  : e.g.,
         *  - "127.0.0.1:8080" (ipv4)
         *  - "[::1]:8080" (ipv6)
         *  - "127.0.0.1:8080,[::1]:8080" (both)
         *
         * For now, given Prometheus is serving over HTTP and uses
         * Basic Auth, so only ever listen on localhost.
         */
        auto localhost = (family == AF_INET) ? "127.0.0.1" : "[::1]";

        auto connectionStr = fmt::format("{}:{}", localhost, port);

        exposer = std::make_unique<::prometheus::Exposer>(connectionStr);

        exposer->RegisterCollectable(stats, lowCardinalityPath);
        exposer->RegisterCollectable(statsHC, highCardinalityPath);

        exposer->RegisterAuth(authCB, authRealm, lowCardinalityPath);
        exposer->RegisterAuth(authCB, authRealm, highCardinalityPath);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed start Prometheus Exposer: {}", e.what());
    }
}

// defined here as Exposer must be a complete type. Avoids
// polluting the header with prometheus headers.
MetricServer::~MetricServer() = default;

bool MetricServer::isAlive() const {
    // if exposer was successfully created, it is running
    // and can serve Prometheus scrapes.
    return bool(exposer);
}

in_port_t MetricServer::getListeningPort() const {
    // Caller should always check the exposer is alive first.
    Expects(isAlive());
    std::vector<int> listeningPorts = exposer->GetListeningPorts();
    // only one port should have been specified when constructing
    // the exposer.
    Expects(listeningPorts.size() == 1);

    return in_port_t(listeningPorts[0]);
}

std::pair<in_port_t, sa_family_t> MetricServer::getRunningConfig() const {
    return {getListeningPort(), family};
}

} // namespace cb::prometheus
