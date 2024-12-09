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

#include <filesystem>
#include <functional>
#include <optional>
#include <string_view>

namespace cb::dek::util {

/**
 * Iterate over all key files for the given key identifier in the provided
 *
 * @param id The key file to search for
 * @param dir The directory containing the key files
 * @param visitor The visitor to call for each key file found
 */
void iterateKeyFiles(
        std::string_view id,
        const std::filesystem::path& dir,
        const std::function<void(const std::filesystem::path&)>& visitor);

/**
 * Try to locate the highest revision of the encryption key file for the
 * provided key identifier.
 *
 * @param id The key identifier to search for
 * @param dir The directory containing the key files
 * @return The path to the newest key file for the given identifier if found
 */
std::optional<std::filesystem::path> locateNewestKeyFile(
        std::string_view id, const std::filesystem::path& dir);

/**
 * Copy the newest encryption key file for the given identifier located in
 * the source directory over to the destination directory.
 *
 * @param id The key identifier to copy
 * @param src The directory containing the source key files
 * @param dest The directory containing the destination key files
 * @return The path to the copied key file
 * @throws std::runtime_error if the key file could not be copied
 */
std::filesystem::path copyKeyFile(std::string_view id,
                                  const std::filesystem::path& src,
                                  const std::filesystem::path& dest);
} // namespace cb::dek::util
