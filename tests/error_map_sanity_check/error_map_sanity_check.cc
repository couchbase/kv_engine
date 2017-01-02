/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <daemon/settings.h>
#include <iostream>

/**
 * Small standalone program used to load and parse all of the error maps.
 * The motivation behind this program is to easily detect syntax errors
 * etc in one of the error_maps when being run from the commit validators
 * (as we normally run memcached with the blackhole logger so that the
 * error messages printed out by memcached wouldn't be shown anywhere).
 */
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <directory containing error maps"
                  << std::endl;
        return EXIT_FAILURE;
    }

    Settings settings;
    try {
        settings.loadErrorMaps(argv[1]);
    } catch (const std::exception& exception) {
        std::cerr << exception.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}