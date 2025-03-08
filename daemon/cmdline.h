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

#include <filesystem>

/**
 * Parse the command line arguments and set the configuration file path.
 * @param argc The number of command line arguments
 * @param argv The command line arguments
 */
void parse_arguments(int argc, char **argv);

/**
 * Get the configuration file path
 * @return The configuration file path
 */
const std::filesystem::path& get_config_file();
