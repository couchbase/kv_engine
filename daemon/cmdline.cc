/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "cmdline.h"
#include "config_parse.h"
#include "memcached.h"
#include "settings.h"

#include <getopt.h>
#include <iostream>

static std::string config_file;

static void usage() {
    std::cerr << "memcached " << get_server_version() << R"(
    -C file       Read configuration from file
    -h            print this help and exit
)";
}

void parse_arguments(int argc, char** argv) {
    struct option long_options[] = {
            {"config", required_argument, nullptr, 'C'},
            {"help", no_argument, nullptr, 'h'},
            {nullptr, 0, nullptr, 0}};

    int cmd;

    // Tell getopt to restart the parsing (if we used getopt before calling
    // this method)
    optind = 1;

    while ((cmd = getopt_long(argc, argv, "C:h", long_options, nullptr)) !=
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

    load_config_file(config_file.c_str(), Settings::instance());
}

const std::string& get_config_file() {
    return config_file;
}
