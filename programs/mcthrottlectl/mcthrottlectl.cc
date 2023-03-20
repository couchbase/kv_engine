/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <platform/terminal_color.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <iostream>

using cb::terminal::TerminalColor;

static void usage(McProgramGetopt& getopt, int exitcode) {
    std::cerr << R"(Usage mcthrottlectl [options] [bucketname1 bucketname2]

Options:

)" << getopt << std::endl
              << std::endl;
    std::exit(exitcode);
}

std::size_t get_limit(std::string_view value, std::string_view key) {
    try {
        std::size_t idx = 0;
        auto ret = std::stoul(std::string{value}, &idx);
        if (idx != value.length()) {
            throw std::runtime_error("additional characters");
        }
        return ret;
    } catch (const std::exception& exception) {
        std::cerr << TerminalColor::Red << "Failed to parse the provided "
                  << key << " limit: " << exception.what() << std::endl
                  << TerminalColor::Reset;
        std::exit(EXIT_FAILURE);
    }
}

int main(int argc, char** argv) {
    std::optional<size_t> reserved;
    std::optional<size_t> hard_limit;
    std::optional<size_t> node_capacity;

    McProgramGetopt getopt;
    using cb::getopt::Argument;
    getopt.addOption({[&reserved](auto value) {
                          reserved = get_limit(value, "reserved");
                      },
                      "reserved",
                      Argument::Required,
                      "limit",
                      "The reserved number of units per sec"});
    getopt.addOption({[&hard_limit](auto value) {
                          hard_limit = get_limit(value, "hard");
                      },
                      "hard-limit",
                      Argument::Required,
                      "limit",
                      "The hard limit of units per sec"});
    getopt.addOption({[&node_capacity](auto value) {
                          node_capacity = get_limit(value, "capacity");
                      },
                      "node-capacity",
                      Argument::Required,
                      "limit",
                      "The node capacity in units per sec"});

    getopt.addOption({[&getopt](auto) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    auto arguments = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    try {
        getopt.assemble();
        auto connection = getopt.getConnection();
        connection->setAgentName("mcthrottlectl " MEMCACHED_VERSION);
        connection->setFeatures(
                {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});

        auto executeCommand = [&connection](const BinprotGenericCommand& cmd) {
            auto rsp = connection->execute(cmd);
            if (rsp.isSuccess()) {
                if (!rsp.getData().empty()) {
                    std::cout << TerminalColor::Green << rsp.getDataString()
                              << TerminalColor::Reset << std::endl;
                }
            } else {
                std::cerr << TerminalColor::Red
                          << "Failed: " << to_string(rsp.getStatus())
                          << rsp.getDataString() << TerminalColor::Reset
                          << std::endl;
                std::exit(EXIT_FAILURE);
            }
        };

        if (node_capacity.has_value()) {
            std::cout << TerminalColor::Green << "Setting node capacity to "
                      << *node_capacity << TerminalColor::Reset << std::endl;
            executeCommand(SetNodeThrottlePropertiesCommand(
                    nlohmann::json{{"capacity", *node_capacity}}));
        }

        if (reserved.has_value() || hard_limit.has_value()) {
            nlohmann::json doc;

            if (reserved.has_value()) {
                doc["reserved"] = *reserved;
            }
            if (hard_limit.has_value()) {
                doc["hard_limit"] = *hard_limit;
            }

            for (const auto& bucket : arguments) {
                std::cout << TerminalColor::Green << "Setting " << doc.dump()
                          << "for bucket " << bucket << TerminalColor::Reset
                          << std::endl;
                executeCommand(SetBucketThrottlePropertiesCommand(
                        std::string{bucket}, doc));
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
