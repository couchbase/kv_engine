/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/Unistd.h>
#include <getopt.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/status.h>
#include <platform/string_hex.h>
#include <iostream>
#include <vector>

#ifdef WIN32
// Command.com don't support colors the same way as the terminals
static bool nocolor = true;
#else
static bool nocolor = false;
#endif

static void usage() {
    std::cerr << R"(Usage: mcbp_info [options]

Options:

)";
#ifdef WIN32
    std::cerr << "  --color            Use colors in the output" << std::endl;
#else
    std::cerr << "  --nocolor          Don't use colors in the output"
              << std::endl;
#endif
    std::cerr << R"(  --client_opcodes   Dump map of client opcodes
  --server_opcodes   Dump map of server opcodes
  --status           Dump map of status codes
  --help             This help text
)";

    exit(EXIT_FAILURE);
}

enum class Color { Red = 31, Green = 32, Yellow = 33 };

void printString(Color color, std::string_view view) {
    if (nocolor) {
        std::cout << view;
    } else {
        std::cout << "\033[" << std::to_string(int(color)) << "m" << view
                  << "\033[m";
    }
}

void dump_client_opcodes() {
    std::cout << std::endl << "Client opcodes" << std::endl;
    std::cout << "     0123456789abcdef";
    for (unsigned int opcode = 0; opcode < 256; opcode++) {
        if (opcode % 0x10 == 0) {
            std::cout << std::endl << cb::to_hex(uint8_t(opcode)) << " ";
        }
        auto op = cb::mcbp::ClientOpcode(opcode);
        if (cb::mcbp::is_valid_opcode(op)) {
            if (cb::mcbp::is_supported_opcode(op)) {
                if (cb::mcbp::is_deprecated(op)) {
                    printString(Color::Yellow, "d");
                } else if (cb::mcbp::is_reorder_supported(op)) {
                    if (cb::mcbp::is_durability_supported(op)) {
                        printString(Color::Green, "R");
                    } else {
                        printString(Color::Green, "r");
                    }
                } else {
                    printString(Color::Green, "X");
                }
            } else {
                printString(Color::Yellow, "U");
            }
        } else {
            printString(Color::Red, ".");
        }
    }

    std::cout << std::endl << std::endl << "Legend: " << std::endl;
    printString(Color::Green, "  r\tReorder supported\n");
    printString(Color::Green, "  R\tReorder and durability supported\n");
    printString(Color::Yellow, "  d\tDeprecated\n");
    printString(Color::Green, "  X\tSupported\n");
    printString(Color::Yellow, "  U\tNot supported\n");
    printString(Color::Red, "  .\tNot defined\n");
}

void dump_server_opcodes() {
    std::cout << std::endl << "Server opcodes" << std::endl;
    std::cout << "     0123456789abcdef";
    for (unsigned int opcode = 0; opcode < 256; opcode++) {
        if (opcode % 0x10 == 0) {
            std::cout << std::endl << cb::to_hex(uint8_t(opcode)) << " ";
        }
        auto op = cb::mcbp::ServerOpcode(opcode);
        if (cb::mcbp::is_valid_opcode(op)) {
            printString(Color::Green, "X");
        } else {
            printString(Color::Red, ".");
        }
    }

    std::cout << std::endl << std::endl << "Legend: " << std::endl;
    printString(Color::Green, "  X\tSupported\n");
    printString(Color::Red, "  .\tNot defined\n");
}

void dump_status() {
    // The status is uint16_t, but we only use a subset of them. Just
    // print out the first 256 values as that's the range containing all
    // of the ones we've defined (well if we start adding more we'll
    // automatically start printing them as well as COUNT is one higher than
    // the last one we've defined)
    std::cout << std::endl << "Status codes" << std::endl;
    std::cout << "     0123456789abcdef";
    for (unsigned int st = 0;
         st < std::max((unsigned int)cb::mcbp::Status::COUNT, 0x100U);
         st++) {
        if (st % 0x10 == 0) {
            std::cout << std::endl << cb::to_hex(uint8_t(st)) << " ";
        }
        if (st < (unsigned int)cb::mcbp::Status::COUNT) {
            try {
                const auto status = cb::mcbp::Status(st);
                to_string(status);
                if (cb::mcbp::isStatusSuccess(status)) {
                    printString(Color::Green, "S");
                } else {
                    printString(Color::Green, "E");
                }
            } catch (const std::exception&) {
                printString(Color::Red, ".");
            }
        } else {
            printString(Color::Red, ".");
        }
    }

    std::cout << std::endl << std::endl << "Legend: " << std::endl;
    printString(Color::Green, "  S\tSuccess\n");
    printString(Color::Green, "  E\tError\n");
    printString(Color::Red, "  .\tNot defined\n");
}

int main(int argc, char** argv) {
    int cmd;

    enum class Command { ClientOpcodes, ServerOpcodes, Status };
    std::vector<Command> commands;

#ifndef WIN32
    nocolor = isatty(STDOUT_FILENO) == 0;
#endif

    const std::vector<option> options = {
#ifdef WIN32
            {"color", no_argument, nullptr, 'n'},
#else
            {"nocolor", no_argument, nullptr, 'n'},
#endif
            {"client_opcodes", no_argument, nullptr, 'o'},
            {"server_opcodes", no_argument, nullptr, 'O'},
            {"status", no_argument, nullptr, 's'},
            {"help", no_argument, nullptr, 0},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc, argv, "o", options.data(), nullptr)) !=
           EOF) {
        switch (cmd) {
        case 'n':
#ifdef WIN32
            nocolor = false;
#else
            nocolor = true;
#endif
            break;
        case 'o':
            commands.emplace_back(Command::ClientOpcodes);
            break;
        case 'O':
            commands.emplace_back(Command::ServerOpcodes);
            break;
        case 's':
            commands.emplace_back(Command::Status);
            break;
        default:
            usage();
            return EXIT_FAILURE;
        }
    }

    if (commands.empty()) {
        usage();
        return EXIT_FAILURE;
    }

    for (const auto& c : commands) {
        switch (c) {
        case Command::ClientOpcodes:
            dump_client_opcodes();
            break;
        case Command::ServerOpcodes:
            dump_server_opcodes();
            break;
        case Command::Status:
            dump_status();
            break;
        }
    }

    return EXIT_SUCCESS;
}
