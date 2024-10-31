/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "sdk_connection_manager.h"
#include <gsl/gsl-lite.hpp>

SdkConnectionManager::SdkConnectionManager() = default;

SdkConnectionManager& SdkConnectionManager::instance() {
    static SdkConnectionManager inst;
    return inst;
}

void SdkConnectionManager::registerSdk(std::string_view agent) {
    auto space = agent.find(' ');
    if (space != std::string::npos) {
        agent = agent.substr(0, space);
    }

    std::string name(agent);

    agents.withLock([&name](auto& map) {
        auto iter = map.find(name);
        if (iter == map.end()) {
            if (map.size() == MaximumTrackedSdk) {
                // we need to throw out the oldest entry
                auto ts = map.begin()->second.timestamp;
                std::string victim = map.begin()->first;
                for (const auto& [id, entry] : map) {
                    if (entry.timestamp < ts) {
                        victim = id;
                        ts = entry.timestamp;
                    }
                }
                map.erase(victim);
            }
            map[name] = TimestampedCounter{};
        } else {
            iter->second.increment();
        }
    });
}

std::unordered_map<std::string, std::size_t>
SdkConnectionManager::getConnectedSdks() const {
    return agents.withLock([](auto& map) {
        std::unordered_map<std::string, std::size_t> result;
        for (const auto& [name, entry] : map) {
            result[name] = entry.counter;
        }
        return result;
    });
}
