/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

// mcctl - Utility program to perform IOCTL-style operations on a memcached
//         process.

#include <memcached/io_control.h>
#include <memcached/protocol_binary.h>
#include <platform/terminal_color.h>
#include <platform/terminal_size.h>
#include <programs/getpass.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <cstdlib>
#include <iostream>

using namespace cb::terminal;

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
            std::cerr << TerminalColor::Red
                      << "Verbosity not returned from the server"
                      << TerminalColor::Reset << std::endl;
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
            std::cout << ptr << std::endl;
        } else {
            std::cerr << TerminalColor::Red
                      << "Invalid object type returned from the server: "
                      << verbosity->type_name() << TerminalColor::Reset
                      << std::endl;
            return EXIT_FAILURE;
        }
    } else {
        std::cerr << TerminalColor::Red
                  << "Settings stats not returned from the server"
                  << TerminalColor::Reset << std::endl;
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
    int level;

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

    connection.sendCommand(BinprotVerbosityCommand{level});

    BinprotVerbosityResponse resp;
    connection.recvResponse(resp);

    if (resp.isSuccess()) {
        return EXIT_SUCCESS;
    } else {
        std::cerr << TerminalColor::Red
                  << "Command failed: " << to_string(resp.getStatus())
                  << TerminalColor::Reset << std::endl;
        return EXIT_FAILURE;
    }
}

void printPropertyHelp() {
    size_t dw;
    try {
        auto [width, height] = getTerminalSize();
        (void)height;
        if (width < 60) {
            // if you've got a small terminal we'll just print it in the normal
            // way.
            width = std::numeric_limits<size_t>::max();
        };

        dw = width - 34;
    } catch (std::exception&) {
        dw = std::numeric_limits<size_t>::max();
    }

    std::cerr << "property may be one of: " << std::endl;
    cb::ioctl::Manager::getInstance().iterate([dw](const auto& e) {
        std::array<char, 45> kbuf;
        snprintf(kbuf.data(), kbuf.size(), "  %-37s  ", e.key.data());
        std::cerr << kbuf.data();

        switch (e.mode) {
        case cb::ioctl::Mode::RDONLY:
            std::cerr << TerminalColor::Yellow << "R " << TerminalColor::Reset;
            break;
        case cb::ioctl::Mode::WRONLY:
            std::cerr << TerminalColor::Yellow << " W" << TerminalColor::Reset;
            break;
        case cb::ioctl::Mode::RW:
            std::cerr << TerminalColor::Yellow << "RW" << TerminalColor::Reset;
            break;
        }

        std::cerr << " ";
        std::fill(kbuf.begin(), kbuf.end(), ' ');

        auto descr = e.description;
        while (true) {
            if (descr.size() < dw) {
                std::cerr << descr.data() << std::endl;
                return;
            }

            auto idx = descr.rfind(' ', std::min(dw, descr.size()));
            if (idx == std::string::npos) {
                std::cerr << descr.data() << std::endl;
                return;
            }
            std::cerr.write(descr.data(), idx);
            std::cerr << std::endl;
            descr.remove_prefix(idx + 1);
            std::cerr.write(kbuf.data(), kbuf.size() - 1);
        }
    });

    std::cerr << std::endl
              << "  R - available with 'get property'" << std::endl
              << "  W - available with 'set property'" << std::endl;
}

static void usage(McProgramGetopt& getopt, int exitcode) {
    std::cerr << R"(Usage mcctl [options] <get|set|reload> property [value]

Options:

)" << getopt << R"("

Commands:

   get <property>                Return the value of the given property
   set <property> [value]        Sets `property` to the given value
   reload <property>             Reload the named property (config, sasl, ...)

)";

    printPropertyHelp();
    exit(exitcode);
}

int main(int argc, char** argv) {
    using cb::getopt::Argument;
    McProgramGetopt getopt;
    std::string bucket;
    getopt.addOption({[&bucket](auto value) { bucket = std::string{value}; },
                      'b',
                      "bucket",
                      Argument::Required,
                      "bucketname",
                      "The name of the bucket to operate on"});

    getopt.addOption({[&getopt](auto) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    const auto arguments = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    if (arguments.size() < 2) {
        usage(getopt, EXIT_FAILURE);
    }

    std::string command{arguments.front()};
    if (command != "get" && command != "set" && command != "reload") {
        std::cerr << TerminalColor::Red << "Unknown subcommand \"" << command
                  << "\"" << TerminalColor::Reset << std::endl;
        usage(getopt, EXIT_FAILURE);
    }

    try {
        getopt.assemble();
        auto connection = getopt.getConnection();
        // MEMCACHED_VERSION contains the git sha
        connection->setAgentName("mcctl " MEMCACHED_VERSION);
        connection->setFeatures(
                {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});

        if (!bucket.empty()) {
            connection->selectBucket(bucket);
        }

        // Need at least two more arguments: get/set and a property name.
        std::string property{arguments[1]};

        if (command == "get") {
            if (property == "verbosity") {
                return get_verbosity(*connection);
            } else {
                std::cout << connection->ioctl_get(property) << std::endl;
                return EXIT_SUCCESS;
            }
        } else if (command == "set") {
            std::string value;
            if (arguments.size() > 2) {
                value = std::string{arguments[2]};
            }

            if (property == "verbosity") {
                if (value.empty()) {
                    std::cerr
                        << "Error: 'set verbosity' requires a value argument."
                        << std::endl;
                    usage(getopt, EXIT_FAILURE);
                } else {
                    return set_verbosity(*connection, value);
                }
            } else {
                connection->ioctl_set(property, value);
                return EXIT_SUCCESS;
            }
        } else if (command == "reload") {
            if (property == "config") {
                auto response = connection->execute(BinprotGenericCommand{
                        cb::mcbp::ClientOpcode::ConfigReload});
                if (!response.isSuccess()) {
                    std::cerr << TerminalColor::Red
                              << "Failed: " << to_string(response.getStatus());
                    if (!response.getDataString().empty()) {
                        std::cerr << std::endl
                                  << "\t" << response.getDataString();
                    }
                    std::cerr << TerminalColor::Reset << std::endl;
                    return EXIT_FAILURE;
                }
            } else if (property == "sasl") {
                auto response =
                        connection->execute(BinprotIsaslRefreshCommand{});
                if (!response.isSuccess()) {
                    std::cerr << TerminalColor::Red
                              << "Failed: " << to_string(response.getStatus());

                    if (!response.getDataString().empty()) {
                        std::cerr << std::endl
                                  << "\t" << response.getDataString();
                    }
                    std::cerr << TerminalColor::Reset << std::endl;

                    return EXIT_FAILURE;
                }
            } else {
                std::cerr
                        << TerminalColor::Red
                        << R"(Error: Unknown property. The only supported properties is "config" or "sasl")"
                        << TerminalColor::Reset << std::endl;
                return EXIT_FAILURE;
            }
        }
    } catch (const ConnectionError& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
