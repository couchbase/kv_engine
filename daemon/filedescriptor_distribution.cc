/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "filedescriptor_distribution.h"
#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>

namespace cb ::environment::filedescriptor {

void to_json(nlohmann::json& json, const Distribution& distribution) {
    json = {{"filedescriptor_limit", distribution.filedescriptorLimit},
            {"reserved_core", distribution.reservedCore},
            {"reserved_ep_engine", distribution.reservedEpEngine},
            {"reserved_magma", distribution.reservedMagma},
            {"reserved_system_connections",
             distribution.reservedSystemConnections},
            {"reserved_user_connections",
             distribution.reservedUserConnections}};
}

constexpr static size_t ReservedForCore = 2000;
constexpr static size_t ReservedForEpEngine = 30000;
constexpr static size_t ReservedForSystemConnections = 5000;
constexpr static size_t ReservedForUserConnections = 60000;
constexpr static size_t ReservedForMagma = 2000;
constexpr static size_t TotalReserved =
        ReservedForCore + ReservedForEpEngine + ReservedForSystemConnections +
        ReservedForUserConnections + ReservedForMagma;

Distribution getDistribution(size_t max_file_descriptors) {
    Distribution ret;
    ret.filedescriptorLimit = max_file_descriptors;

    // The common case; we have plenty of file descriptors (the startup
    // script claims we should have at least 200k and hopefully users
    // deploying us meet that requirement).
    if (ret.filedescriptorLimit >= TotalReserved) {
        ret.reservedCore = ReservedForCore;
        ret.reservedEpEngine = ReservedForEpEngine;
        ret.reservedSystemConnections = ReservedForSystemConnections;
        ret.reservedUserConnections = ReservedForUserConnections;
        ret.reservedMagma =
                ret.filedescriptorLimit -
                (ret.reservedCore + ret.reservedEpEngine +
                 ret.reservedSystemConnections + ret.reservedUserConnections);
        return ret;
    }

    // We're below the ideal level; scale down the various pools
    // to fit within the available file descriptors
    constexpr static size_t MinimumCore = 500;
    constexpr static size_t MinimumEpEngine = 500;
    constexpr static size_t MinimumMagma = 500;
    constexpr static size_t MinimumSystemConnections = 500;
    constexpr static size_t MinimumUserConnections = 500;
    constexpr static size_t TotalMinimum =
            MinimumCore + MinimumEpEngine + MinimumSystemConnections +
            MinimumUserConnections + MinimumMagma;

    ret.filedescriptorLimit = max_file_descriptors;
    ret.reservedCore = MinimumCore;
    ret.reservedEpEngine = MinimumEpEngine;
    ret.reservedMagma = MinimumMagma;
    ret.reservedSystemConnections = MinimumSystemConnections;
    ret.reservedUserConnections = MinimumUserConnections;

    if (ret.filedescriptorLimit < TotalMinimum) {
        // We're below the minimum required; just return the minimums
        // and the user may experience bad behavior
        return ret;
    }

    // Distribute the remaining file descriptors to the various pools
    // by using the factors below until they they cap at their maximums
    constexpr static size_t CoreFactor = 1;
    constexpr static size_t EpEngineFactor = 2;
    constexpr static size_t MagmaFactor = 5;
    constexpr static size_t SystemFactor = 1;
    constexpr static size_t UserFactor = 3;

    // Lambda to get the factor for a pool (0 if it is full)
    const auto get_factor = [](size_t& pool, size_t max, size_t factor) {
        return pool < max ? factor : 0;
    };

    size_t rest = 0;
    size_t total_factor =
            get_factor(ret.reservedCore, ReservedForCore, CoreFactor) +
            get_factor(
                    ret.reservedEpEngine, ReservedForEpEngine, EpEngineFactor) +
            get_factor(ret.reservedMagma, ReservedForMagma, MagmaFactor) +
            get_factor(ret.reservedSystemConnections,
                       ReservedForSystemConnections,
                       SystemFactor) +
            get_factor(ret.reservedUserConnections,
                       ReservedForUserConnections,
                       UserFactor);
    do {
        size_t free = ret.filedescriptorLimit -
                      (ret.reservedCore + ret.reservedEpEngine +
                       ret.reservedSystemConnections +
                       ret.reservedUserConnections + ret.reservedMagma);

        size_t quota = 0;
        if (total_factor) {
            quota = free / total_factor;
            rest = (free % total_factor);
        } else {
            quota = 0;
            rest = free;
        }

        // Lambda to increment a pool by its factor * quota unless it is
        // full
        const auto increment_pool =
                [&](size_t& pool, size_t max, size_t factor) {
                    const auto increase = factor * quota;
                    if (pool + increase > max) {
                        rest += (pool + increase) - max;
                        pool = max;
                    } else {
                        pool += increase;
                    }
                };

        increment_pool(ret.reservedCore, ReservedForCore, CoreFactor);
        increment_pool(
                ret.reservedEpEngine, ReservedForEpEngine, EpEngineFactor);
        increment_pool(ret.reservedMagma, ReservedForMagma, MagmaFactor);
        increment_pool(ret.reservedSystemConnections,
                       ReservedForSystemConnections,
                       SystemFactor);
        increment_pool(ret.reservedUserConnections,
                       ReservedForUserConnections,
                       UserFactor);
        // Repeat while we still have some left and we were able to
        // allocate something in this round
        total_factor =
                get_factor(ret.reservedCore, ReservedForCore, CoreFactor) +
                get_factor(ret.reservedEpEngine,
                           ReservedForEpEngine,
                           EpEngineFactor) +
                get_factor(ret.reservedMagma, ReservedForMagma, MagmaFactor) +
                get_factor(ret.reservedSystemConnections,
                           ReservedForSystemConnections,
                           SystemFactor) +
                get_factor(ret.reservedUserConnections,
                           ReservedForUserConnections,
                           UserFactor);
    } while (rest > total_factor);

    // Lambda to increment a pool with extra capacity until it reaches its
    // maximum size
    const auto maybe_increment_pool = [&](size_t& pool, size_t max) {
        if (rest == 0) {
            return;
        }

        if (pool < max) {
            const auto need = max - pool;
            const auto give = std::min(need, rest);
            pool += give;
            rest -= give;
        }
    };

    maybe_increment_pool(ret.reservedCore, ReservedForCore);
    maybe_increment_pool(ret.reservedEpEngine, ReservedForEpEngine);
    maybe_increment_pool(ret.reservedMagma, ReservedForMagma);
    maybe_increment_pool(ret.reservedSystemConnections,
                         ReservedForSystemConnections);
    maybe_increment_pool(ret.reservedUserConnections,
                         ReservedForUserConnections);
    return ret;
}

size_t getMinimumRequiredFileDescriptorsForDynamicReconfig() {
    // Unfortunately our aarch CV builder have a low file descriptor limit
    // so for unit tests we lower the requirement to allow the tests to run
    if (getenv("MEMCACHED_UNIT_TESTS")) {
        return 10000;
    }
    return TotalReserved;
}

} // namespace cb::environment::filedescriptor
