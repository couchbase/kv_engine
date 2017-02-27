/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <getopt.h>
#include <iostream>
#include <protocol/connection/client_mcbp_connection.h>
#include <programs/hostname_utils.h>

/**
 * Request a stat from the server
 * @param sock socket connected to the server
 * @param key the name of the stat to receive (empty == ALL)
 * @param json if true print as json otherwise print old-style
 */
static void request_stat(MemcachedBinprotConnection& connection,
                         const std::string& key,
                         bool json,
                         bool format) {
    try {
        auto stats = connection.stats(key);
        if (json) {
            std::cout << to_string(stats, format) << std::endl;
        } else {
            for (auto* obj = stats.get()->child;
                 obj != nullptr; obj = obj->next) {
                switch (obj->type) {
                case cJSON_String:
                    std::cout << obj->string << " " << obj->valuestring
                              << std::endl;
                    break;
                case cJSON_Number:
                    std::cout << obj->string << " " << obj->valueint
                              << std::endl;
                    break;
                case cJSON_False:
                    std::cout << obj->string << " false" << std::endl;
                    break;
                case cJSON_True:
                    std::cout << obj->string << " true" << std::endl;
                    break;
                case cJSON_NULL:
                    std::cout << obj->string << " null" << std::endl;
                    break;
                case cJSON_Array:
                case cJSON_Object:
                    std::cout << obj->string << " " << to_string(obj)
                              << std::endl;
                    break;
                default:
                    std::cerr << "Unknown element for: " << obj->string
                              << std::endl;
                    exit(EXIT_FAILURE);
                    break;
                }
            }
        }
    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
    }

}

static void usage() {
    std::cout << "Usage: mcstat [options] statkey ..." << std::endl
              << "  -h hostname[:port]  Host (and optional port number) to retrieve stats from"
              << std::endl
              << "                      (for IPv6 use: [address]:port if you'd like to specify port)"
              << std::endl
              << "  -p port      Port number" << std::endl
              << "  -u username  Username (currently synonymous with -b)"
              << std::endl
              << "  -b bucket    Bucket name" << std::endl
              << "  -P password  Password (if bucket is password-protected)"
              << std::endl
              << "  -s           Connect to node securely (using SSL)"
              << std::endl
              << "  -j           Print result as JSON (unformatted)"
              << std::endl
              << "  -J           Print result in JSON (formatted)"
              << std::endl
              << "  -4           Use IPv4 (default)" << std::endl
              << "  -6           Use IPv6" << std::endl
              << "  statkey ...  Statistic(s) to request" << std::endl;
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
    bool json = false;
    bool format = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "46h:p:u:b:P:sjJ")) != EOF) {
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
        case 'J':
            format = true;
            // FALLTHROUGH
        case 'j':
            json = true;
            break;
        default:
            usage();
            return EXIT_FAILURE;
        }
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
        connection.hello("mcstat", MEMCACHED_VERSION,
                         "command line utility to fetch stats");
        connection.setXerrorSupport(true);

        if (!user.empty()) {
            connection.authenticate(user, password,
                                    connection.getSaslMechanisms());
        }

        if (!bucket.empty()) {
            connection.selectBucket(bucket);
        }

        if (optind == argc) {
            request_stat(connection, "", json, format);
        } else {
            for (int ii = optind; ii < argc; ++ii) {
                request_stat(connection, argv[ii], json, format);
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
