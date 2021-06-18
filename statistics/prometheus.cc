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

#include "statistics/prometheus.h"
#include "statistics/prometheus_collector.h"

#include <daemon/log_macros.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <folly/SynchronizedPtr.h>
#include <gsl/gsl-lite.hpp>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/uuid.h>
#include <prometheus/exposer.h>
#include <chrono>
#include <utility>

namespace cb::prometheus {

const std::string MetricServer::lowCardinalityPath = "/_prometheusMetrics";
const std::string MetricServer::highCardinalityPath = "/_prometheusMetricsHigh";
const std::string MetricServer::excludeTimestampsSuffix = "NoTS";
const std::string MetricServer::authRealm = "KV Prometheus Exporter";

folly::SynchronizedPtr<std::unique_ptr<MetricServer>> instance;

nlohmann::json initialize(const std::pair<in_port_t, sa_family_t>& config,
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
        handle->reset();
        auto message = fmt::format(
                "Failed to start Prometheus "
                "exposer on family:{} port:{}",
                (family == AF_INET) ? "inet" : "inet6",
                port);
        throw std::runtime_error(message);
    }

    // if the configured port is set to 0, an available port number will have
    // been selected, log that instead of 0.
    auto listeningPort = (*handle)->getListeningPort();
    LOG_INFO("Prometheus Exporter started, listening on family:{} port:{}",
             (family == AF_INET) ? "inet" : "inet6",
             listeningPort);
    return (*handle)->getRunningConfigAsJson();
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

nlohmann::json getRunningConfigAsJson() {
    auto handle = instance.rlock();
    if (!handle || !handle->isAlive()) {
        // no MetricServer, or it is not listening
        return {};
    }
    return handle->getRunningConfigAsJson();
}

class MetricServer::KVCollectable : public ::prometheus::Collectable {
public:
    KVCollectable(Cardinality cardinality,
                  IncludeTimestamps timestamps,
                  GetStatsCallback getStatsCB)
        : cardinality(cardinality),
          timestamps(timestamps),
          getStatsCB(std::move(getStatsCB)) {
    }
    /**
     * Gathers high or low cardinality metrics
     * and returns them in the prometheus required structure.
     */
    [[nodiscard]] std::vector<::prometheus::MetricFamily> Collect()
            const override {
        using namespace std::chrono;
        // get current time in seconds as double
        double timestamp = duration_cast<duration<double>>(
                                   system_clock::now().time_since_epoch())
                                   .count();
        // round to the nearest second. This makes the interval between samples
        // stored in Prometheus more likely to be consistent to the millisecond.
        // As a result of Prometheus' timestamp delta-of-delta encoding,
        // this can reduce the disk space needed to store stats significantly.
        // See MB-46675.
        timestamp = std::round(timestamp);

        // convert to ms for prometheus-cpp
        auto timestampMs = int64_t(timestamp * 1000);

        // collect KV stats
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
            result.push_back(std::move(statEntry.second) /* MetricFamily */);
            // only set timestamps if requested
            if (timestamps == IncludeTimestamps::Yes) {
                for (auto& clientMetric : result.back().metric) {
                    clientMetric.timestamp_ms = timestampMs;
                }
            }
        }

        return result;
    }

private:
    const Cardinality cardinality;
    const IncludeTimestamps timestamps;

    // function to call on every incoming request to generate stats
    GetStatsCallback getStatsCB;
};

MetricServer::MetricServer(in_port_t port,
                           sa_family_t family,
                           GetStatsCallback getStatsCB,
                           AuthCallback authCB)
    : family(family), uuid(::to_string(cb::uuid::random())) {
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

        // used to store the newly created KVCollectable into the array
        auto arrayItr = endpoints.begin();
        for (auto [cardinality, timestamps] : {
                     std::make_pair(Cardinality::Low, IncludeTimestamps::No),
                     std::make_pair(Cardinality::Low, IncludeTimestamps::Yes),
                     std::make_pair(Cardinality::High, IncludeTimestamps::No),
                     std::make_pair(Cardinality::High, IncludeTimestamps::Yes),
             }) {
            auto ptr = std::make_shared<KVCollectable>(
                    cardinality, timestamps, getStatsCB);

            // construct the path this endpoint should listen on
            auto path = cardinality == Cardinality::High ? highCardinalityPath
                                                         : lowCardinalityPath;

            if (timestamps == IncludeTimestamps::No) {
                path += excludeTimestampsSuffix;
            }

            exposer->RegisterAuth(authCB, authRealm, path);
            exposer->RegisterCollectable(ptr, path);
            *arrayItr = std::move(ptr);
            ++arrayItr;
        }
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to start Prometheus Exposer: {}", e.what());
        // Kill a partially initialized object
        exposer.reset();
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

nlohmann::json MetricServer::getRunningConfigAsJson() const {
    nlohmann::json ret;
    if (family == AF_INET) {
        ret["host"] = "127.0.0.1";
        ret["family"] = "inet";
    } else {
        ret["host"] = "::1";
        ret["family"] = "inet6";
    }
    ret["port"] = getListeningPort();
    ret["type"] = "prometheus";
    ret["uuid"] = uuid;
    return ret;
}

} // namespace cb::prometheus
