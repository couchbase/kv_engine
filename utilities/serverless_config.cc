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
    json["read_unit_size"] = readUnitSize.load(std::memory_order_acquire);
    json["write_unit_size"] = writeUnitSize.load(std::memory_order_acquire);
    return json;
}

void Config::update_from_json(const nlohmann::json& json) {
    defaultThrottleLimit.store(
            json.value("default_throttle_limit", DefaultThrottleLimit),
            std::memory_order_release);
    maxConnectionsPerBucket.store(
            json.value("max_connections_per_bucket", MaxConnectionsPerBucket),
            std::memory_order_release);
    readUnitSize.store(json.value("read_unit_size", ReadUnitSize),
                       std::memory_order_release);
    writeUnitSize.store(json.value("write_unit_size", WriteUnitSize),
                        std::memory_order_release);
}
} // namespace cb::serverless
