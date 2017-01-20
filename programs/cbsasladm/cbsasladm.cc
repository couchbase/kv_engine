/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "config.h"

#include <cbsasl/pwconv.h>
#include <cbsasl/user.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <getopt.h>
#include <iostream>
#include <map>
#include <memcached/protocol_binary.h>
#include <platform/platform.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_connection.h>
#include <string>
#include <utilities/protocol2text.h>

/**
 * Handle cbsasl refresh
 */
static int handle_refresh(int argc, char**,
                          MemcachedBinprotConnection& connection) {
    if (optind + 1 != argc) {
        std::cerr << "Error: cbsasl refresh don't take any arguments"
                  << std::endl;
        return EXIT_FAILURE;
    }

    BinprotIsaslRefreshCommand cmd;
    connection.sendCommand(cmd);

    BinprotIsaslRefreshResponse resp;
    connection.recvResponse(resp);

    if (resp.isSuccess()) {
        return EXIT_SUCCESS;
    } else {
        std::cerr << "Command failed: "
                  << memcached_status_2_text(resp.getStatus())
                  << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

int handle_pwconv(int argc, char** argv) {
    if (optind + 3 != argc) {
        std::cerr << "Usage: cbsasl pwconv inputfile outputfile" << std::endl;
        return EXIT_FAILURE;
    }

    try {
        cbsasl_pwconv(argv[optind + 1], argv[optind + 2]);
    } catch (std::runtime_error& e) {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::bad_alloc&) {
        std::cerr << "Error: out of memory" << std::endl;
        return EXIT_SUCCESS;
    }

    return EXIT_SUCCESS;
}

static void usage() {
    fprintf(stderr,
            "Usage: cbsasladm [-h host[:port]] [-p port] [-s] "
                "[-u user] [-P password] cmd\n"
                "   The following command(s) exists:\n"
                "\trefresh - tell memcached to reload its internal "
                "cache\n"
                "\tpwconv input ouput - convert isasl.pw to cbsasl.pw "
                "format\n");
    exit(EXIT_SUCCESS);
}

/**
 * Program entry point.
 *
 * @param argc argument count
 * @param argv argument vector
 * @return 0 if success, error code otherwise
 */
int main(int argc, char **argv) {
    int cmd;
    std::string port{"11210"};
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    std::string bucket{};
    sa_family_t family = AF_UNSPEC;
    bool secure = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "46h:p:u:b:P:si:")) != EOF) {
        switch (cmd) {
        case '6' :
            family = AF_INET6;
            break;
        case '4' :
            family = AF_INET;
            break;
        case 'h' :
            host.assign(optarg);
            break;
        case 'p':
            port.assign(optarg);
            break;
        case 'b' :
            bucket.assign(optarg);
            break;
        case 'u' :
            user.assign(optarg);
            break;
        case 'P':
            password.assign(optarg);
            break;
        case 's':
            secure = true;
            break;
        case 'i':
            try {
                using namespace Couchbase;
                UserFactory::setDefaultHmacIterationCount(std::stoi(optarg));
            } catch (...) {
                std::cerr << "Error: iteration count must be an integer"
                          << std::endl;
                return EXIT_FAILURE;
            }
            break;
        default:
            usage();
        }
    }

    if (optind == argc) {
        std::cerr << "You need to supply a command" << std::endl;
        usage();
    }

    std::string command{argv[optind]};
    if (command == "refresh") {
        try {
            in_port_t in_port;
            sa_family_t fam;
            std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

            if (family == AF_UNSPEC) { // The user may have used -4 or -6
                family = fam;
            }

            MemcachedBinprotConnection connection(host,
                                                  in_port,
                                                  family,
                                                  secure);

            // MEMCACHED_VERSION contains the git sha
            connection.hello("cbsasladm",
                             MEMCACHED_VERSION,
                             "command line utitilty to manage the internal sasl db");

            if (!user.empty()) {
                connection.authenticate(user, password,
                                        connection.getSaslMechanisms());
            }

            if (!bucket.empty()) {
                connection.selectBucket(bucket);
            }

            handle_refresh(argc, argv, connection);
        } catch (const ConnectionError& ex) {
            std::cerr << ex.what() << std::endl;
            return EXIT_FAILURE;
        } catch (const std::runtime_error& ex) {
            std::cerr << ex.what() << std::endl;
            return EXIT_FAILURE;
        }
    } else if (command == "pwconv") {
        return handle_pwconv(argc, argv);
    } else {
        std::cerr << "Error: Unknown command" << std::endl;
        usage();
    }

    return EXIT_SUCCESS;
}
