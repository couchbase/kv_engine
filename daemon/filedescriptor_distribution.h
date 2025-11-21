/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <cstddef>

/// This file is not intended to be included directly as it contains
/// functionality used by the Environment class; include environment.h
/// instead. Its sole purpose as a standalone file is to allow for unit
/// testing of the distribution logic.
namespace cb ::environment::filedescriptor {
struct Distribution {
    size_t filedescriptorLimit;
    size_t reservedCore;
    size_t reservedEpEngine;
    size_t reservedMagma;
    size_t reservedSystemConnections;
    size_t reservedUserConnections;
};
void to_json(nlohmann::json& json, const Distribution& distribution);

/// Get the initial distrubution of file descrptors based on the maximum
/// number of file descriptors available to the process (and using
/// the default distribution ns_server use for connections)
Distribution getDistribution(size_t max_file_descriptors);
size_t getMinimumRequiredFileDescriptorsForDynamicReconfig();
} // namespace cb::environment::filedescriptor
