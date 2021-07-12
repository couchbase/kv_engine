/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <boost/filesystem/path.hpp>
#include <string>
#include <vector>

/// The Error Map Manager provides access to the various versions of
/// the error map.
class ErrorMapManager {
public:
    /**
     * Request a version of the error map
     *
     * @param version The requested version
     * @return The requested version (if we have it) or the highest version
     *         we have
     */
    std::string_view getErrorMap(size_t version);

    /**
     * Initialize the ErrorMapManager. Must be called before the
     * ErrorMapManager::instance is being used
     *
     * @param directory The directory containing "all" of the files.
     *                  currently we'll only use a single file
     *                  (error_map_v2.json) and we'll generate version 1
     *                  out of that file.
     * @throws std::exception The subclass may differ depending on where
     *                        the error occurs (file IO, invalid JSON,
     *                        unexpected format etc)
     */
    static void initialize(const boost::filesystem::path& directory);

    /// Release memory allocated (and invalidate) the ErrorMapManager
    static void shutdown();

    /**
     * Get the handle to the ErrorMapManager.
     *
     * WARNING: Calling this method _before_ calling
     * ErrorMapManager::initialize() will CRASH your program
     */
    static ErrorMapManager& instance();

    virtual ~ErrorMapManager() = default;

protected:
    ErrorMapManager(std::vector<std::string> maps)
        : error_maps(std::move(maps)) {
    }

    /// The version is stored at the given index
    const std::vector<std::string> error_maps;
};
