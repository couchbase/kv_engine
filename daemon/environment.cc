/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "environment.h"
#include "log_macros.h"
#include "settings.h"

#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <utilities/magma_support.h>

namespace cb {
using namespace environment::filedescriptor;
Environment& Environment::instance() {
    static Environment instance(getDistribution(cb::io::maximizeFileDescriptors(
            std::numeric_limits<uint32_t>::max())));
    return instance;
}

bool Environment::recalculate(size_t desiredMaxConnections, bool check) {
    // Just make sure we don't have multiple threads trying to reconfigure
    // at the same time
    static std::mutex mutex;
    std::lock_guard<std::mutex> lock(mutex);

    if (getMinimumRequiredFileDescriptorsForDynamicReconfig() >
        max_file_descriptors) {
        // Sorry; we can't support that many connections
        return false;
    }

    if (check) {
        return true;
    }

    magma_file_limit = max_file_descriptors - reserved_for_core -
                       reserved_for_epengine - desiredMaxConnections;
    magma::Magma::SetMaxOpenFiles(magma_file_limit, true);

    return true;
}

void Environment::updateSettingsWithInitialSizes() {
    auto& settings = Settings::instance();
    bool warning = false;

    if (getMinimumRequiredFileDescriptorsForDynamicReconfig() >
        max_file_descriptors) {
        auto distribution = getDistribution(max_file_descriptors);
        settings.setMaxConnections(distribution.reservedUserConnections +
                                   distribution.reservedSystemConnections);
        settings.setSystemConnections(distribution.reservedSystemConnections);
        warning = true;
    }
    auto config = to_json();
    config["connection_limits"] = {{"user", settings.getMaxConnections()},
                                   {"system", settings.getSystemConnections()}};
    if (warning) {
        auto severity = spdlog::level::level_enum::warn;
        if (max_file_descriptors < 20000) {
            severity = spdlog::level::level_enum::critical;
        } else if (max_file_descriptors < 30000) {
            severity = spdlog::level::level_enum::err;
        }
        CB_LOG_ENTRY_CTX(severity,
                         "System file descriptor setting too low to support "
                         "desired number of connections; adjusted settings",
                         config);
    } else {
        LOG_INFO_CTX("System file descriptor settings", config);
    }
}

nlohmann::json Environment::to_json() const {
    nlohmann::json json = {{"max_fds", max_file_descriptors},
                           {"reserved",
                            {{"core", reserved_for_core},
                             {"ep_engine", reserved_for_epengine}}}};
    if (isMagmaSupportEnabled()) {
        json["magma"] = {{"current_open", magma::Magma::GetNumOpenFiles()},
                         {"max_limit", magma_file_limit.load()}};
    }
    return json;
}

Environment::Environment(const Distribution& distribution)
    : max_file_descriptors(distribution.filedescriptorLimit),
      reserved_for_core(distribution.reservedCore),
      reserved_for_epengine(distribution.reservedEpEngine),
      magma_file_limit(distribution.reservedMagma) {
    magma::Magma::SetMaxOpenFiles(magma_file_limit, false);
}
} // namespace cb
