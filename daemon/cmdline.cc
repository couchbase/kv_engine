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

#include <dek/manager.h>
#include <getopt.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <vector>

static std::string config_file;

static void usage() {
    std::cerr << "memcached " << get_server_version() << R"(
    --stdin       Read extra commands from stdin (line by line modus)
    -C file       Read configuration from file
    -h            print this help and exit
)";
}

static void read_kv_settings_from_stdin() {
    using namespace std::string_view_literals;

    std::vector<char> buffer(4096);

    std::string_view cmd;
    do {
        if (!fgets(buffer.data(), static_cast<int>(buffer.size()), stdin)) {
            std::cerr << "Fatal error: fgets() returned NULL" << std::endl;
            std::_Exit(EXIT_FAILURE);
        }
        cmd = buffer.data();
        while (!cmd.empty() && (cmd.back() == '\n' || cmd.back() == '\r')) {
            cmd.remove_suffix(1);
        }
        std::string_view value;
        auto idx = cmd.find('=');
        if (idx != std::string_view::npos) {
            value = cmd.substr(idx + 1);
            cmd = {cmd.data(), idx};
        }

        if (cmd == "BOOTSTRAP_DEK"sv) {
            try {
                cb::dek::Manager::instance().reset(
                        nlohmann::json::parse(value));
            } catch (const std::exception& exception) {
                std::cerr << "Fatal error: Failed to parse JSON: "
                          << exception.what() << std::endl;
                std::_Exit(EXIT_FAILURE);
            }
        } else if (cmd == "DONE"sv) {
            return;
        } else {
            std::cerr << "Unknown command: \"" << cmd << "\". Ignored"
                      << std::endl;
        }

    } while (true);
}

void parse_arguments(int argc, char** argv) {
    const std::vector<option> options = {
            {"config", required_argument, nullptr, 'C'},
            {"stdin", no_argument, nullptr, 'S'},
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

        case 'S':
            read_kv_settings_from_stdin();
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
