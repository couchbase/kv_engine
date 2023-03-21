/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/terminal_color.h>
#include <iostream>

static void usage(McProgramGetopt& getopt, int exitcode) {
    std::cerr << R"(Usage mcthrottlectl [options] <bucketname>

Options:

)" << getopt << std::endl
              << std::endl;
    std::exit(exitcode);
}

int main(int argc, char** argv) {
    size_t limit = 0;

    McProgramGetopt getopt;
    using cb::getopt::Argument;
    getopt.addOption({[&limit](auto value) {
                          try {
                              limit = std::stoul(std::string{value});
                          } catch (const std::exception& exception) {
                              std::cerr
                                      << TerminalColor::Red
                                      << "Failed to parse the provided limit: "
                                      << exception.what() << std::endl
                                      << TerminalColor::Reset;
                              std::exit(EXIT_FAILURE);
                          }
                      },
                      "throttle-limit",
                      Argument::Required,
                      "limit",
                      "The number of units per sec"});

    getopt.addOption({[&getopt](auto) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    auto arguments = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    if (arguments.size() != 1) {
        usage(getopt, EXIT_FAILURE);
    }

    try {
        getopt.assemble();
        auto connection = getopt.getConnection();
        connection->setAgentName("mcthrottlectl " MEMCACHED_VERSION);
        connection->setFeatures({cb::mcbp::Feature::XERROR});

        auto rsp = connection->execute(SetBucketUnitThrottleLimitCommand(
                std::string{arguments.front()}, limit));
        if (rsp.isSuccess()) {
            std::cout << TerminalColor::Green << rsp.getDataString()
                      << TerminalColor::Reset << std::endl;
        } else {
            std::cerr << TerminalColor::Red
                      << "Failed: " << to_string(rsp.getStatus())
                      << rsp.getDataString() << TerminalColor::Reset
                      << std::endl;
            std::exit(EXIT_FAILURE);
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
