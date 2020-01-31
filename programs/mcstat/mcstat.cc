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

#include <getopt.h>
#include <nlohmann/json.hpp>
#include <programs/getpass.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <utilities/terminate_handler.h>

#include <protocol/connection/frameinfo.h>
#include <iostream>

/**
 * Request a stat from the server
 * @param sock socket connected to the server
 * @param key the name of the stat to receive (empty == ALL)
 * @param json if true print as json otherwise print old-style
 */
static void request_stat(MemcachedConnection& connection,
                         const std::string& key,
                         bool json,
                         bool format,
                         const std::string& impersonate) {
    try {
        auto getFrameInfos = [&impersonate]() -> FrameInfoVector {
            if (impersonate.empty()) {
                return {};
            }
            FrameInfoVector ret;
            ret.emplace_back(
                    std::make_unique<ImpersonateUserFrameInfo>(impersonate));
            return ret;
        };
        if (json) {
            auto stats = connection.stats(key, getFrameInfos);
            std::cout << stats.dump(format ? 1 : -1, '\t') << std::endl;
        } else {
            connection.stats(
                    [](const std::string& key,
                       const std::string& value) -> void {
                        std::cout << key << " " << value << std::endl;
                    },
                    key,
                    getFrameInfos);
        }
    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
    }
}

static void usage() {
    std::cerr << R"(Usage: mcstat [options] statkey ...

Options:

  -h or --host hostname[:port]   The host (with an optional port) to connect to
                                 (for IPv6 use: [address]:port if you'd like to
                                 specify port)
  -p or --port port              The port number to connect to
  -b or --bucket bucketname      The name of the bucket to operate on
  -u or --user username          The name of the user to authenticate as
  -P or --password password      The passord to use for authentication
                                 (use '-' to read from standard input)
  -s or --ssl                    Connect to the server over SSL
  -C or --ssl-cert filename      Read the SSL certificate from the specified file
  -K or --ssl-key filename       Read the SSL private key from the specified file
  -4 or --ipv4                   Connect over IPv4
  -6 or --ipv6                   Connect over IPv6
  -j or --json                   Print result as JSON (unformatted)
  -J or --json=pretty            Print result in JSON (formatted)
  -I or --impersonate username   Try to impersonate the specified user
  --help                         This help text
  statkey ...  Statistic(s) to request
)";

    exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {
    // Make sure that we dump callstacks on the console
    install_backtrace_terminate_handler();

    int cmd;
    std::string port{"11210"};
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    std::string bucket{};
    std::string ssl_cert;
    std::string ssl_key;
    std::string impersonate;
    sa_family_t family = AF_UNSPEC;
    bool secure = false;
    bool json = false;
    bool format = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    struct option long_options[] = {
            {"ipv4", no_argument, nullptr, '4'},
            {"ipv6", no_argument, nullptr, '6'},
            {"host", required_argument, nullptr, 'h'},
            {"port", required_argument, nullptr, 'p'},
            {"bucket", required_argument, nullptr, 'b'},
            {"password", required_argument, nullptr, 'P'},
            {"user", required_argument, nullptr, 'u'},
            {"ssl", no_argument, nullptr, 's'},
            {"ssl-cert", required_argument, nullptr, 'C'},
            {"ssl-key", required_argument, nullptr, 'K'},
            {"json", optional_argument, nullptr, 'j'},
            {"impersonate", required_argument, nullptr, 'I'},
            {"help", no_argument, nullptr, 0},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc,
                              argv,
                              "46h:p:u:b:P:SsjJC:K:I:",
                              long_options,
                              nullptr)) != EOF) {
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
        case 'S':
            // Deprecated and not shown in the help text
            std::cerr << "mcstat: -S is deprecated. Use \'-P -\" instead"
                      << std::endl;
            password.assign(getpass());
            break;
        case 's':
            secure = true;
            break;
        case 'J':
            format = true;
            // FALLTHROUGH
        case 'j':
            json = true;
            if (optarg && std::string{optarg} == "pretty") {
                format = true;
            }
            break;
        case 'C':
            ssl_cert.assign(optarg);
            break;
        case 'K':
            ssl_key.assign(optarg);
            break;
        case 'I':
            impersonate.assign(optarg);
            break;
        default:
            usage();
            return EXIT_FAILURE;
        }
    }

    if (password == "-") {
        password.assign(getpass());
    } else if (password.empty()) {
        const char* env_password = std::getenv("CB_PASSWORD");
        if (env_password) {
            password = env_password;
        }
    }

    try {
        in_port_t in_port;
        sa_family_t fam;
        std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

        if (family == AF_UNSPEC) { // The user may have used -4 or -6
            family = fam;
        }
        MemcachedConnection connection(host, in_port, family, secure);
        connection.setSslCertFile(ssl_cert);
        connection.setSslKeyFile(ssl_key);

        connection.connect();

        // MEMCACHED_VERSION contains the git sha
        connection.hello("mcstat", MEMCACHED_VERSION,
                         "command line utility to fetch stats");
        try {
            connection.setXerrorSupport(true);
        } catch(std::runtime_error& e) {
            std::cerr << e.what() << std::endl;
        }

        if (!user.empty()) {
            connection.authenticate(user, password,
                                    connection.getSaslMechanisms());
        }

        if (!bucket.empty()) {
            connection.selectBucket(bucket);
        }

        if (optind == argc) {
            request_stat(connection, "", json, format, impersonate);
        } else {
            for (int ii = optind; ii < argc; ++ii) {
                request_stat(connection, argv[ii], json, format, impersonate);
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
