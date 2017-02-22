/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

/* mcctl - Utility program to perform IOCTL-style operations on a memcached
 *         process.
 */

#include "config.h"

#include <getopt.h>
#include <memcached/openssl.h>
#include <memcached/protocol_binary.h>
#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_connection.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <utilities/protocol2text.h>

/**
 * Get the verbosity level on the server.
 *
 * There isn't a single command to retrieve the current verbosity level,
 * but it is available through the settings stats...
 *
 * @param bio connection to the server.
 */
static int get_verbosity(MemcachedBinprotConnection& connection)
{
    auto stats = connection.stats("settings");
    if (stats) {
        auto* obj = cJSON_GetObjectItem(stats.get(), "verbosity");
        if (obj && obj->type == cJSON_Number) {
            const char* levels[] = {"warning",
                                    "info",
                                    "debug",
                                    "detail",
                                    "unknown"};
            const char* ptr = levels[4];

            if (obj->valueint > -1 && obj->valueint < 4) {
                ptr = levels[obj->valueint];
            }
            std::cerr << ptr << std::endl;
        } else if (obj) {
            std::cerr << "Invalid object type returned from the server: "
                      << obj->type << std::endl;
            return EXIT_FAILURE;
        } else {
            std::cerr << "Verbosity not returned from the server" << std::endl;
            return EXIT_FAILURE;
        }
    } else {
        std::cerr << "Verbosity not returned from the server" << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

/**
 * Sets the verbosity level on the server
 *
 * @param bio connection to the server.
 * @param value value to set the property to.
 */
static int set_verbosity(MemcachedBinprotConnection& connection,
                         const std::string& value)
{
    std::size_t pos;
    uint32_t level = std::stoi(value, &pos);
    if (pos != value.size()) {
        // Try to map it...
        if (value == "warning") {
            level = 0;
        } else if (value == "info") {
            level = 1;
        } else if (value == "debug") {
            level = 2;
        } else if (value == "detail") {
            level = 3;
        } else {
            std::cerr << "Unknown verbosity level \"" << value
                      << "\". Use warning/info/debug/detail" << std::endl;
            return EXIT_FAILURE;
        }
    }

    BinprotVerbosityCommand cmd;
    cmd.setLevel(level);
    connection.sendCommand(cmd);

    BinprotVerbosityResponse resp;
    connection.recvResponse(resp);

    if (resp.isSuccess()) {
        return EXIT_SUCCESS;
    } else {
        std::cerr << "Command failed: "
                  << memcached_status_2_text(resp.getStatus())
                  << std::endl;
        return EXIT_FAILURE;
    }
}

static void usage() {
    fprintf(stderr,
            "Usage: mcctl [-h host[:port]] [-p port] [-u user] [-P pass] [-s] <get|set> property [value]\n"
            "\n"
            "    get <property>           Returns the value of the given property.\n"
            "    set <property> [value]   Sets `property` to the given value.\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {
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

    while ((cmd = getopt(argc, argv, "46h:p:u:b:P:s")) != EOF) {
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
        default:
            usage();
        }
    }

    if (optind + 1 >= argc) {
         usage();
    }

    std::string command{argv[optind]};
    if (command != "get" && command != "set") {
        fprintf(stderr, "Unknown subcommand \"%s\"\n", argv[optind]);
        usage();
    }


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
        connection.hello("mcctl",
                         MEMCACHED_VERSION,
                         "command line utility to get/set properties");
        connection.setXerrorSupport(true);

        if (!user.empty()) {
            connection.authenticate(user, password,
                                    connection.getSaslMechanisms());
        }

        if (!bucket.empty()) {
            connection.selectBucket(bucket);
        }


        /* Need at least two more arguments: get/set and a property name. */
        std::string property = {argv[optind + 1]};

        if (command == "get") {
            if (property == "verbosity") {
                return get_verbosity(connection);
            } else {
                std::cout << connection.ioctl_get(property) << std::endl;
                return EXIT_SUCCESS;
            }
        } else {
            // we only support get and set
            std::string value;
            if (optind + 2 < argc) {
                value = argv[optind + 2];
            }

            if (property == "verbosity") {
                if (value.empty()) {
                    std::cerr
                        << "Error: 'set verbosity' requires a value argument."
                        << std::endl;
                    usage();
                } else {
                    return set_verbosity(connection, value);
                }
            } else {
                connection.ioctl_set(property, value);
                return EXIT_SUCCESS;
            }
        }
    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
