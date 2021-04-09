/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
