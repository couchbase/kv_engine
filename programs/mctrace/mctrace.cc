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
#include <platform/interrupt.h>
#include <platform/strerror.h>
#include <platform/terminal_color.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <stdexcept>
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

static void closeDestination(FILE* destination, const std::string& output) {
    if (fclose(destination) != 0) {
        std::cerr << TerminalColor::Yellow << "Warning: Failed to close \""
                  << output << "\": " << cb_strerror() << TerminalColor::Reset
                  << std::endl;
    }
}

static void usage(McProgramGetopt& instance, int exitcode) {
    std::cerr << R"(Usage: mctrace [options]

Options:

)" << instance << std::endl
              << std::endl;
    std::exit(exitcode);
}

namespace {

/**
 * Stop tracing, retrieve the current trace dump and clear it from the
 * server, mirroring the kv_trace_dump workflow.
 *
 * Should the dump or write fail partway through, best-effort clear the dump
 * and restart tracing (if requested) before rethrowing, so a failure doesn't
 * leave the server with tracing disabled and/or an orphaned dump.
 *
 * destination must already be a valid, open stream (stdout is fine) so
 * that this function never needs to worry about output-file failures
 * unrelated to the actual trace dump.
 *
 * @param connection connection to the memcached server
 * @param destination stream to write the trace dump to
 * @param restart whether to restart tracing after dumping
 * @throws std::runtime_error if dumping or writing the trace fails
 */
void dumpTrace(MemcachedConnection& connection,
               FILE* destination,
               bool restart) {
    connection.ioctl_set("trace.stop", {});

    std::string uuid;
    try {
        uuid = connection.ioctl_get("trace.dump.begin");
        if (uuid.empty()) {
            throw std::runtime_error(
                    "Server returned empty UUID for trace dump");
        }

        const auto content = connection.ioctl_get("trace.dump.get?id=" + uuid);
        fwrite(content.data(), content.size(), 1, destination);
        fprintf(destination, "\n");
        fflush(destination);
        if (ferror(destination)) {
            throw std::runtime_error("Failed to write trace dump");
        }

        connection.ioctl_set("trace.dump.clear", uuid);
    } catch (...) {
        if (!uuid.empty()) {
            try {
                connection.ioctl_set("trace.dump.clear", uuid);
            } catch (const std::exception&) {
            }
        }
        if (restart) {
            try {
                connection.ioctl_set("trace.start", {});
            } catch (const std::exception&) {
            }
        }
        throw;
    }

    if (restart) {
        connection.ioctl_set("trace.start", {});
    }
}

} // namespace

int main(int argc, char** argv) {
    std::string trace_config;
    std::string output("-");
    bool interactive = false;
    bool restart = true;

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
            {[&interactive](auto) { interactive = true; },
             'w',
             "wait",
             "Wait until the user press ctrl-c before returning the data. This "
             "option clears the data on the server before waiting for the user "
             "to press ctrl-c and may be used to get information for a known "
             "window of time."});

    getopt.addOption({[&restart](auto) { restart = false; },
                      "norestart",
                      "Don't restart tracing after dumping the trace file"});

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

        // Validate (and open) the output destination before mutating any
        // server-side tracing state, so a bad path fails immediately
        // instead of after tracing has already been reconfigured/stopped.
        FILE* destination = nullptr;
        if (!output.empty() && output != "-") {
            destination = fopen(output.c_str(), "w");
            if (destination == nullptr) {
                std::cerr << TerminalColor::Red << "Failed to open \"" << output
                          << "\": " << cb_strerror() << TerminalColor::Reset
                          << std::endl;
                return EXIT_FAILURE;
            }
        }

        try {
            if (!trace_config.empty()) {
                // Start the trace
                connection->ioctl_set("trace.config", trace_config);
                connection->ioctl_set("trace.start", {});
            } else {
                if (connection->ioctl_get("trace.status") != "enabled") {
                    throw std::runtime_error(
                            "Trace is not running. Specify a configuration.");
                }
            }

            if (interactive) {
                // Clear the trace by stopping and starting it
                connection->ioctl_set("trace.stop", {});
                connection->ioctl_set("trace.start", {});

                // Register our SIGINT handler
                cb::console::set_sigint_handler(sigint_handler);

                // This should be std::cout as it isn't an error, but given
                // that the user may want the output to go to stdout we use
                // stderr instead
                std::cerr << TerminalColor::Yellow
                          << "Press CTRL-C to stop trace"
                          << TerminalColor::Reset << std::endl;
                // Wait for the trace to automatically stop or ctrl+c
                do {
                    // In the ideal world we'd use a condition variable to do
                    // this so we can bail out quickly. Unfortunately it's
                    // illegal to do that from a signal handler.
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                } while (!caughtSigInt);

                // Clear the handler before dumping the trace. A further
                // ctrl-c while dumpTrace is talking to the server would
                // otherwise be treated as a repeat press and hard-exit the
                // process, skipping dumpTrace's cleanup and potentially
                // leaving tracing stopped and the dump orphaned server-side.
                cb::console::clear_sigint_handler();
            }

            dumpTrace(*connection, destination ? destination : stdout, restart);
        } catch (...) {
            if (destination) {
                closeDestination(destination, output);
            }
            throw;
        }

        if (destination) {
            closeDestination(destination, output);
        }
    } catch (const std::exception& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
