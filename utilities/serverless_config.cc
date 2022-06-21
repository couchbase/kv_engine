/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <nlohmann/json.hpp>
#include <serverless/config.h>

namespace cb::serverless {
Config& Config::instance() {
    static Config instance;
    return instance;
}

nlohmann::json Config::to_json() const {
    nlohmann::json json;
    json["default_throttle_limit"] =
            defaultThrottleLimit.load(std::memory_order_acquire);
    json["max_connections_per_bucket"] =
            maxConnectionsPerBucket.load(std::memory_order_acquire);
    json["read_compute_unit_size"] =
            readComputeUnitSize.load(std::memory_order_acquire);
    json["write_compute_unit_size"] =
            writeComputeUnitSize.load(std::memory_order_acquire);
    return json;
}

void Config::update_from_json(const nlohmann::json& json) {
    defaultThrottleLimit.store(
            json.value("default_throttle_limit", DefaultThrottleLimit),
            std::memory_order_release);
    maxConnectionsPerBucket.store(
            json.value("max_connections_per_bucket", MaxConnectionsPerBucket),
            std::memory_order_release);
    readComputeUnitSize.store(
            json.value("read_compute_unit_size", ReadComputeUnitSize),
            std::memory_order_release);
    writeComputeUnitSize.store(
            json.value("write_compute_unit_size", WriteComputeUnitSize),
            std::memory_order_release);
}
} // namespace cb::serverless
