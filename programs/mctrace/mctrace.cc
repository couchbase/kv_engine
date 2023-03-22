/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * mctrace - Utility program to easily perform trace dumps on a running
 * memcached process
 */
#include <memcached/protocol_binary.h>
#include <platform/cb_malloc.h>
#include <platform/interrupt.h>
#include <platform/strerror.h>
#include <platform/terminal_color.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>

using namespace cb::terminal;

static bool caughtSigInt = false;

static void sigint_handler() {
    // We only want to soft-exit once, if we sigint twice just bail out
    if (caughtSigInt) {
        std::exit(EXIT_FAILURE);
    }
    caughtSigInt = true;
}

static void usage(McProgramGetopt& instance, int exitcode) {
    std::cerr << R"(Usage: mctrace [options]

Options:

)" << instance << std::endl
              << std::endl;
    std::exit(exitcode);
}

int main(int argc, char** argv) {
    std::string trace_config;
    std::string output("-");
    bool interactive = false;

    McProgramGetopt getopt;
    using cb::getopt::Argument;
    getopt.addOption(
            {[&trace_config](auto value) { trace_config = std::string{value}; },
             'c',
             "config",
             Argument::Required,
             "configuration",
             "Specify the trace configuration to use on the server (note that "
             "this will override the current configuration and the previous "
             "configuration will NOT be restored when the program terminates). "
             "ex: "
             "\"buffer-mode:ring;buffer-size:2000000;enabled-categories:*\""});

    getopt.addOption({[&output](auto value) { output = std::string{value}; },
                      'o',
                      "output",
                      Argument::Required,
                      "filename",
                      "Store the trace information in the named file."});

    getopt.addOption(
            {[&interactive](auto value) { interactive = true; },
             'w',
             "wait",
             "Wait until the user press ctrl-c before returning the data. This "
             "option clears the data on the server before waiting for the user "
             "to press ctrl-c and may be used to get information for a known "
             "window of time."});

    getopt.addOption({[&getopt](auto) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    getopt.parse(argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    try {
        getopt.assemble();
        auto connection = getopt.getConnection();
        connection->setAgentName("mctrace " MEMCACHED_VERSION);
        connection->setFeatures(
                {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});

        if (!trace_config.empty()) {
            // Start the trace
            connection->ioctl_set("trace.config", trace_config);
            connection->ioctl_set("trace.start", {});
        } else {
            if (connection->ioctl_get("trace.status") != "enabled") {
                std::cerr << TerminalColor::Red
                          << "Trace is not running. Specify a configuration."
                          << TerminalColor::Reset << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        if (interactive) {
            // Clear the trace by stopping and starting it
            connection->ioctl_set("trace.stop", {});
            connection->ioctl_set("trace.start", {});

            // Register our SIGINT handler
            cb::console::set_sigint_handler(sigint_handler);

            // This should be std::cout as it isn't an error, but given that
            // the user may want the output to go to stdout we use stderr
            // instead
            std::cerr << TerminalColor::Yellow << "Press CTRL-C to stop trace"
                      << TerminalColor::Reset << std::endl;
            // Wait for the trace to automatically stop or ctrl+c
            do {
                // In the ideal world we'd use a condition variable to do this
                // so we can bail out quickly. Unfortunately it's illegal to do
                // that from a signal handler.
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } while (!caughtSigInt);
        }

        FILE* destination = stdout;
        if (!output.empty() && output != "-") {
            destination = fopen(output.c_str(), "w");
            if (destination == nullptr) {
                std::cerr << TerminalColor::Red << "Failed to open \"" << output
                          << "\": " << cb_strerror() << TerminalColor::Reset
                          << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        // Start a dump
        auto uuid = connection->ioctl_get("trace.dump.begin");
        const std::string chunk_key = "trace.dump.chunk?id=" + uuid;

        // Print the dump to the destination (file or stdout)
        std::string chunk;
        do {
            chunk = connection->ioctl_get(chunk_key);
            fwrite(chunk.data(), chunk.size(), 1, destination);
        } while (!chunk.empty());
        fprintf(destination, "\n");

        if (destination != stdout) {
            fclose(destination);
        }

        // Remove the dump
        connection->ioctl_set("trace.dump.clear", uuid);
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
