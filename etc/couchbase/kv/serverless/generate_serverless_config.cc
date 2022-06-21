/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <nlohmann/json.hpp>
#include <serverless/config.h>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>

int main() {
    auto& instance = cb::serverless::Config::instance();
    try {
        std::ofstream of("configuration.json");
        of << std::setw(2) << instance.to_json() << std::endl;
        of.close();
    } catch (const std::exception& e) {
        std::cerr << "Failed to generate configuration.json: " << e.what()
                  << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
