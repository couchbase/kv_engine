/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace cb::config {

/// Nightmare parameters, in file order. Values are kept as strings so they can
/// be handed to Configuration::parseConfiguration.
using NightmareParameters = std::vector<std::pair<std::string, std::string>>;

/**
 * Read the nightmare mode configuration from the given JSON file: a flat map
 * of parameter name to value, where the value may be a string, number or
 * boolean.
 *
 * Does not check that the parameters exist - only a Configuration knows that,
 * so the caller checks when applying them.
 *
 * @throws std::exception if the file cannot be read, is not a JSON object, or
 *         holds a value which is not a string, number or boolean.
 */
NightmareParameters loadNightmareConfiguration(std::string_view path);

/// Join parameters into the "key=value;" form parseConfiguration accepts.
std::string toConfigString(const NightmareParameters& parameters);

} // namespace cb::config
