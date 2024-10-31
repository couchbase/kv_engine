/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <chrono>
#include <string>
#include <string_view>
#include <unordered_map>

/**
 * The sdk connection manager keeps track of unique SDK instances
 * up to a maximum of 100. Once the limit is reached we'll evict
 * the least recently used entry.
 */
class SdkConnectionManager {
public:
    /// The maximum number of SDKs we track before we kick them out
    constexpr static std::size_t MaximumTrackedSdk = 100;

    /// Get a handle to the one and only instance of the ConnectionManager (
    /// we're currently using a single list for all buckets)
    static SdkConnectionManager& instance();

    /// Look at the provided agent and try to register the underlying SDK
    void registerSdk(std::string_view agent);

    /// Get an unordered set of the connected SDKs with the number of current
    /// instances.
    std::unordered_map<std::string, std::size_t> getConnectedSdks() const;

protected:
    SdkConnectionManager();
    struct TimestampedCounter {
        void increment() {
            timestamp = std::chrono::steady_clock::now();
            ++counter;
        }
        std::chrono::steady_clock::time_point timestamp =
                std::chrono::steady_clock::now();
        std::uint64_t counter = 1;
    };
    folly::Synchronized<std::unordered_map<std::string, TimestampedCounter>,
                        std::mutex>
            agents;
};
