/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace Collections {

/**
 * Manifest is an object that is constructed from JSON data as per
 * a set_collections command
 *
 * Users of this class can then obtain the revision, separator and
 * all collections that are included in the manifest.
 */
class Manifest {
public:
    /*
     * Initialise the default manifest
     */
    Manifest();

    /*
     * Create a manifest from json.
     * Validates the json as per SET_COLLECTIONS rules.
     */
    Manifest(const std::string& json);

    int getRevision() const {
        return revision;
    }

    const std::string& getSeparator() const {
        return separator;
    }

    std::vector<std::string>::const_iterator begin() const {
        return collections.begin();
    }

    std::vector<std::string>::const_iterator end() const {
        return collections.end();
    }

    size_t size() const {
        return collections.size();
    }

    std::vector<std::string>::const_iterator find(
            const std::string& collection) const {
        return std::find(begin(), end(), collection);
    }

private:
    /**
     * Check if the C-string input has a length > 0 and < 250.
     *
     * @param separator a C-string representing the separator.
     */
    static bool validSeparator(const char* separator);

    int revision;
    std::string separator;
    std::vector<std::string> collections;
};

} // end namespace Collections
