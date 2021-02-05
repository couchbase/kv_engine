/*
 *     Copyright 2021 Couchbase, Inc.
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
#include <programs/getpass.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <utilities/string_utilities.h>
#include <iostream>

static void usage() {
    std::cerr << R"(Usage: mctestauth [options]

Options:

  --host hostname[:port]   The host (with an optional port) to connect to
                           (for IPv6 use: [address]:port if you'd like to
                           specify port).
  --user username          The name of the user to authenticate as
  --password password      The passord to use for authentication
                           (use '-' to read from standard input)
  --tls                    Try to use TLS
  --ipv4                   Connect over IPv4
  --ipv6                   Connect over IPv6
  --help                   This help text
)";

    exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {
    int cmd;
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    sa_family_t family = AF_UNSPEC;
    bool tls = false;

    const std::vector<option> long_options = {
            {"host", required_argument, nullptr, 'h'},
            {"user", required_argument, nullptr, 'u'},
            {"password", required_argument, nullptr, 'P'},
            {"tls", no_argument, nullptr, 'T'},
            {"ipv4", no_argument, nullptr, '4'},
            {"ipv6", no_argument, nullptr, '6'},
            {"help", no_argument, nullptr, 0},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(
                    argc, argv, "h:u:P:T46", long_options.data(), nullptr)) !=
           EOF) {
        switch (cmd) {
        case 'h':
            host.assign(optarg);
            break;
        case 'u':
            user.assign(optarg);
            break;
        case 'P':
            password.assign(optarg);
            break;
        case 'T':
            tls = true;
            break;
        case '4':
            family = AF_INET;
            break;
        case '6':
            family = AF_INET6;
            break;
        default:
            usage();
            return EXIT_FAILURE;
        }
    }

    if (user.empty()) {
        std::cerr << "A user must be specified with --user" << std::endl;
        return EXIT_FAILURE;
    }

    if (password == "-") {
        password.assign(getpass());
    } else if (password.empty()) {
        const char* env_password = std::getenv("CB_PASSWORD");
        if (env_password) {
            password = env_password;
        }
    }

    // Initialize the socket subsystem (needed on win32)
    cb_initialize_sockets();

    try {
        in_port_t port;
        sa_family_t fam;
        std::tie(host, port, fam) =
                cb::inet::parse_hostname(host, tls ? "11207" : "11210");

        if (family == AF_UNSPEC) { // The user may have used -4 or -6
            family = fam;
        }
        MemcachedConnection connection(host, port, family, tls);
        connection.connect();
        connection.setAgentName("mctestauth");
        connection.setFeatures({cb::mcbp::Feature::XERROR});
        auto mechs = split_string(connection.getSaslMechanisms(), " ");
        for (const auto& mech : mechs) {
            std::cout << mech << ": ";
            std::cout.flush();
            try {
                auto c = connection.clone();
                c->authenticate(user, password, mech);
                std::cout << "OK" << std::endl;
            } catch (const std::exception& ex) {
                std::cout << "FAILED - " << ex.what() << std::endl;
            }
        }
    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::system_error& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
