/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "cluster_config.h"

#include <subdoc/operations.h>

#include <nlohmann/json.hpp>
#include <cstdlib>
#include <stdexcept>

nlohmann::json ClustermapVersion::to_json() const {
    return {{"epoch", epoch}, {"revno", revno}};
}

std::string to_string(const ClustermapVersion& version) {
    return version.to_json().dump();
}

std::ostream& operator<<(std::ostream& os, const ClustermapVersion& version) {
    os << to_string(version);
    return os;
}

void ClusterConfiguration::reset() {
    config.lock()->reset();
}

void ClusterConfiguration::setConfiguration(
        std::shared_ptr<Configuration> configuration) {
    *config.lock() = std::move(configuration);
}

std::shared_ptr<ClusterConfiguration::Configuration>
ClusterConfiguration::maybeGetConfiguration(const ClustermapVersion& version,
                                            bool dedupe) const {
    return config.withLock(
            [&version, dedupe](std::shared_ptr<Configuration> active)
                    -> std::shared_ptr<Configuration> {
                if (active && (version < active->version || !dedupe)) {
                    return active;
                }

                return {};
            });
}
