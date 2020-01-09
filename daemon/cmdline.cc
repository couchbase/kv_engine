/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "cmdline.h"
#include "config_parse.h"
#include "log_macros.h"
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
            {"threads", required_argument, nullptr, 't'},
            {"verbosity", no_argument, nullptr, 'v'},
            {"requests_per_event", required_argument, nullptr, 'R'},
            {"help", no_argument, nullptr, 'h'},
            {nullptr, 0, nullptr, 0}};

    int cmd;

    int verbosity = 0;
    int threads = 0;
    int requests = 0;

    // Tell getopt to restart the parsing (if we used getopt before calling
    // this method)
    optind = 1;

    while ((cmd = getopt_long(argc, argv, "C:t:vR:h", long_options, nullptr)) !=
           EOF) {
        switch (cmd) {
        case 'C':
            config_file = optarg;
            break;

        case 't':
            try {
                threads = std::stoi(optarg);
            } catch (const std::exception& e) {
                LOG_ERROR(R"(Failed to parse "{}": {})", optarg, e.what());
            }
            break;
        case 'v':
            ++verbosity;
            break;
        case 'R':
            try {
                requests = std::stoi(optarg);
            } catch (const std::exception& e) {
                LOG_ERROR(R"(Failed to parse "{}": {})", optarg, e.what());
            }
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

    if (verbosity > 0) {
        Settings::instance().setVerbose(verbosity);
    }

    if (threads > 0) {
        Settings::instance().setNumWorkerThreads(threads);
    }

    if (requests > 0) {
        Settings::instance().setRequestsPerEventNotification(
                requests, EventPriority::Default);
    }
}

const std::string& get_config_file() {
    return config_file;
}
