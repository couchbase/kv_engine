/*
 *    Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <platform/dirutils.h>
#include <platform/terminal_color.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <filesystem>
#include <iostream>

using cb::terminal::TerminalColor;

static void usage(McProgramGetopt& getopt, int exitcode) {
    std::cerr << R"(Usage mcifconfig [options] <tls|list|define|delete>

Options:

)" << getopt << R"(

Commands:

   list                      List the defined interfaces
   define <filename/JSON>    Define a new interface
   delete <UUID>             Delete the interface with the provided UUID
   tls [filename/JSON]       Get (no argument) or set TLS properties

)";

    std::exit(exitcode);
}

/**
 * Get the payload to use. If param is a filename then read the file, if not
 * it should be the actual value
 *
 * @param param the parameter passed to the program
 */
std::string getPayload(std::string_view param) {
    std::filesystem::path nm(param);
    std::string value = std::string{param};
    if (exists(nm)) {
        value = cb::io::loadFile(nm);
    }
    try {
        const auto parsed = nlohmann::json::parse(value);
    } catch (const std::exception& e) {
        std::cerr << TerminalColor::Red
                  << "Failed to parse provided JSON: " << e.what()
                  << TerminalColor::Reset << std::endl;
        std::exit(EXIT_FAILURE);
    }
    return value;
}

int main(int argc, char** argv) {
    McProgramGetopt getopt;
    getopt.addOption({[&getopt](auto) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    const auto arguments = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    if (arguments.empty()) {
        usage(getopt, EXIT_FAILURE);
    }

    std::string key;
    std::string value;
    const std::string command{arguments.front()};
    if (command == "list") {
        key = command;
        if (arguments.size() != 1) {
            std::cerr << TerminalColor::Red
                      << "Error: list don't take parameters"
                      << TerminalColor::Reset << std::endl;
            std::exit(EXIT_FAILURE);
        }
    } else if (command == "tls") {
        key = command;
        if (arguments.size() > 2) {
            std::cerr << TerminalColor::Red
                      << "Error: tls have 1 (optional) parameter"
                      << TerminalColor::Reset << std::endl;
            std::exit(EXIT_FAILURE);
        }
        if (arguments.size() == 2) {
            value = getPayload(arguments.back());
        }
    } else if (command == "define") {
        key = command;
        if (arguments.size() != 2) {
            std::cerr << TerminalColor::Red << "Error: define have 1 parameter"
                      << TerminalColor::Reset << std::endl;
            std::exit(EXIT_FAILURE);
        }
        value = getPayload(arguments.back());
    } else if (command == "delete") {
        key = command;
        if (arguments.size() != 2) {
            std::cerr << TerminalColor::Red
                      << "Error: delete must have 1 parameter"
                      << TerminalColor::Reset << std::endl;
            std::exit(EXIT_FAILURE);
        }
        value = std::string{arguments.back()};
    } else {
        std::cerr << TerminalColor::Red << "Error: Unknown command \""
                  << command << "\"" << TerminalColor::Reset << std::endl;
        std::exit(EXIT_FAILURE);
    }

    try {
        getopt.assemble();
        auto connection = getopt.getConnection();
        connection->setAgentName("mcifconfig " MEMCACHED_VERSION);
        connection->setFeatures({cb::mcbp::Feature::XERROR});

        auto rsp = connection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::Ifconfig, key, value});
        if (rsp.isSuccess()) {
            std::cout << TerminalColor::Green << rsp.getDataView()
                      << TerminalColor::Reset << std::endl;
        } else {
            std::cerr << TerminalColor::Red << "Failed: " << rsp.getStatus()
                      << rsp.getDataView() << TerminalColor::Reset << std::endl;
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
