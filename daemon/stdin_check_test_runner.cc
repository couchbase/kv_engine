/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "stdin_check.h"
#include <fmt/format.h>
#include <platform/command_line_options_parser.h>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <iostream>
#include <mutex>
#include <thread>

/**
 * Test program which utilize the "stdin check" functionality to allow
 * writing tests which to verify that it works as expected.
 */
int main(int argc, char** argv) {
    sigignore(SIGPIPE);
    std::mutex mutex;
    std::unique_lock lock(mutex);
    std::condition_variable condition;

    std::chrono::seconds waittime{10};

    cb::getopt::CommandLineOptionsParser getopt;
    using cb::getopt::Argument;
    getopt.addOption(
            {[&waittime](auto value) {
                 waittime = std::chrono::seconds{std::stoi(std::string{value})};
             },
             "wait",
             Argument::Required,
             "seconds",
             "Seconds to sleep before exiting after receiving an exit"});
    getopt.addOption({[](auto value) {
                          abrupt_shutdown_timeout_changed(std::chrono::seconds{
                                  std::stoi(std::string{value})});
                          ;
                      },
                      "abrupt_shutdown_timeout",
                      Argument::Required,
                      "seconds",
                      "Seconds to wait for abrupt shutdown timeout"});

    auto arguments = getopt.parse(argc, argv, [&getopt]() {
        std::cerr << R"(Usage: stdin_check_test_runner [options]

Options:

)" << getopt << std::endl
                  << std::endl;
        std::exit(EXIT_FAILURE);
    });

    setbuf(stdout, nullptr);
    setbuf(stderr, nullptr);

    start_stdin_listener([&mutex, &condition]() {
        // acquire the mutex to ensure we don't race with main
        std::unique_lock lock(mutex);
        condition.notify_all();
    });

    fmt::print(stdout, "Waiting for exit signal...\n");
    condition.wait(lock);
    fmt::print(stdout, "Shutdown started, sleep before exit\n");
    std::this_thread::sleep_for(waittime);
    return EXIT_FAILURE;
}
