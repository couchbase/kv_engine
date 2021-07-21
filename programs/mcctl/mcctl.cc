/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/* mcctl - Utility program to perform IOCTL-style operations on a memcached
 *         process.
 */
#include <getopt.h>
#include <memcached/openssl.h>
#include <memcached/protocol_binary.h>
#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <programs/getpass.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/terminate_handler.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>

/**
 * Get the verbosity level on the server.
 *
 * There isn't a single command to retrieve the current verbosity level,
 * but it is available through the settings stats...
 *
 * @param bio connection to the server.
 */
static int get_verbosity(MemcachedConnection& connection) {
    auto stats = connection.stats("settings");
    if (stats) {
        auto verbosity = stats.find("verbosity");
        if (verbosity == stats.end()) {
            std::cerr << "Verbosity not returned from the server" << std::endl;
            return EXIT_FAILURE;
        } else if (verbosity->type() ==
                   nlohmann::json::value_t::number_integer) {
            const char* levels[] = {"warning",
                                    "info",
                                    "debug",
                                    "detail",
                                    "unknown"};
            const char* ptr = levels[4];

            auto numVerbosity = verbosity->get<int>();

            if (numVerbosity > -1 && numVerbosity < 4) {
                ptr = levels[numVerbosity];
            }
            std::cerr << ptr << std::endl;
        } else {
            std::cerr << "Invalid object type returned from the server: "
                      << verbosity->type_name() << std::endl;
            return EXIT_FAILURE;
        }
    } else {
        std::cerr << "Settings stats not returned from the server" << std::endl;
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
static int set_verbosity(MemcachedConnection& connection,
                         const std::string& value) {
    uint32_t level;

    try {
        level = std::stoi(value);
    } catch (const std::invalid_argument&) {
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
        std::cerr << "Command failed: " << to_string(resp.getStatus())
                  << std::endl;
        return EXIT_FAILURE;
    }
}

static void usage() {
    std::cerr << R"(Usage mcctl [options] <get|set|reload> property [value]

Options:

  -h or --host hostname[:port]   The host (with an optional port) to connect to
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
  --help                         This help text

Commands:

   get <property>                Return the value of the given property
   set <property> [value]        Sets `property` to the given value
   reload <property>             Reload the named property (config, sasl, ...)

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
    sa_family_t family = AF_UNSPEC;
    bool secure = false;

    cb::net::initialize();

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
            {"help", no_argument, nullptr, 0},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(
                    argc, argv, "46h:p:u:b:P:sC:K:", long_options, nullptr)) !=
           EOF) {
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
        case 'C':
            ssl_cert.assign(optarg);
            break;
        case 'K':
            ssl_key.assign(optarg);
            break;

        default:
            usage();
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

    if (optind + 1 >= argc) {
         usage();
    }

    std::string command{argv[optind]};
    if (command != "get" && command != "set" && command != "reload") {
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

        MemcachedConnection connection(host, in_port, family, secure);
        connection.setSslCertFile(ssl_cert);
        connection.setSslKeyFile(ssl_key);

        connection.connect();

        // MEMCACHED_VERSION contains the git sha
        connection.setAgentName("mcctl " MEMCACHED_VERSION);
        connection.setFeatures({cb::mcbp::Feature::XERROR});

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
        } else if (command == "set") {
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
        } else if (command == "reload") {
            if (property == "config") {
                using BinprotConfigReloadCommand =
                        BinprotCommandT<BinprotGenericCommand,
                                        cb::mcbp::ClientOpcode::ConfigReload>;

                auto response =
                        connection.execute(BinprotConfigReloadCommand{});
                if (!response.isSuccess()) {
                    std::cerr << "Failed: " << to_string(response.getStatus())
                              << std::endl;
                    if (!response.getDataString().empty()) {
                        std::cerr << "\t" << response.getDataString()
                                  << std::endl;
                    }
                    return EXIT_FAILURE;
                }
            } else if (property == "sasl") {
                auto response =
                        connection.execute(BinprotIsaslRefreshCommand{});
                if (!response.isSuccess()) {
                    std::cerr << "Failed: " << to_string(response.getStatus())
                              << std::endl;
                    if (!response.getDataString().empty()) {
                        std::cerr << "\t" << response.getDataString()
                                  << std::endl;
                    }
                    return EXIT_FAILURE;
                }
            } else {
                std::cerr
                        << R"(Error: Unknown property. The only supported properties is "config" or "sasl")"
                        << std::endl;
                return EXIT_FAILURE;
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
