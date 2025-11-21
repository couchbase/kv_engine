/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "filedescriptor_distribution.h"
#include <nlohmann/json_fwd.hpp>
#include <atomic>
#include <cstddef>

namespace cb {

/**
 * The Environment class is intended to be a singleton that holds static,
 * process-wide, environment information relevant to the memcached process. This
 * information includes things such as file descriptor limits.
 */
class Environment {
public:
    Environment(const Environment&) = delete;
    Environment& operator=(const Environment&) = delete;
    Environment(Environment&&) = delete;
    Environment& operator=(Environment&&) = delete;
    static Environment& instance();

    /// Recalculate the maximum number of connections and engine file
    /// descriptors based upon the number of file descriptors available
    ///
    /// @param desiredMaxConnections The desired maximum number of connections
    /// @param check If set to true, the method will only check if the desired
    ///              number of connections can be supported, without actually
    ///              applying the new limit.
    /// @return true if the desired Max Connections may be used; false if it
    ///         needs to be reduced
    bool recalculate(size_t desiredMaxConnections, bool check);

    void updateSettingsWithInitialSizes();

    nlohmann::json to_json() const;

protected:
    /// The maximum number of file descriptors we may have. During startup we
    /// try to increase the allowed number of file handles to the limit
    /// specified for the current user.
    const size_t max_file_descriptors;
    const size_t reserved_for_core;
    const size_t reserved_for_epengine;

    std::atomic<size_t> magma_file_limit{0};

    Environment(const environment::filedescriptor::Distribution& distribution);
};
} // namespace cb
