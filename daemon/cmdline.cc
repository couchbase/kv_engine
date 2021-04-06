/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "cmdline.h"
#include "config_parse.h"
#include "memcached.h"
#include "settings.h"

#include <getopt.h>
#include <iostream>
#include <vector>

static std::string config_file;

static void usage() {
    std::cerr << "memcached " << get_server_version() << R"(
    -C file       Read configuration from file
    -h            print this help and exit
)";
}

void parse_arguments(int argc, char** argv) {
    const std::vector<option> options = {
            {"config", required_argument, nullptr, 'C'},
            {"help", no_argument, nullptr, 'h'},
            {nullptr, 0, nullptr, 0}};

    int cmd;

    // Tell getopt to restart the parsing (if we used getopt before calling
    // this method)
    optind = 1;

    while ((cmd = getopt_long(argc, argv, "C:h", options.data(), nullptr)) !=
           EOF) {
        switch (cmd) {
        case 'C':
            config_file = optarg;
            break;

        case 'h':
            usage();
            std::exit(EXIT_SUCCESS);

        default:
            usage();
            std::exit(EXIT_FAILURE);
        }
    }

    if (config_file.empty()) {
        std::cerr << "Configuration file must be specified with -C"
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }

    load_config_file(config_file, Settings::instance());
}

const std::string& get_config_file() {
    return config_file;
}
