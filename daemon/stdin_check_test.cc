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

#include <boost/process.hpp>
#include <fmt/format.h>
#include <folly/portability/GTest.h>
#include <cctype>
#include <csignal>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

class StdinCheckRunner {
public:
    StdinCheckRunner(int waittime = 0, int abrupt_shutdown_timeout = 0) {
        auto exe = std::filesystem::current_path() / "stdin_check_test_runner";
        std::vector<std::string> args = {{"--wait", std::to_string(waittime)}};
        if (abrupt_shutdown_timeout) {
            args.emplace_back(fmt::format("--abrupt_shutdown_timeout={}",
                                          abrupt_shutdown_timeout));
        }

        child = std::make_unique<boost::process::child>(
                boost::process::exe = exe.native(),
                boost::process::args = args,
                boost::process::std_in<in, boost::process::std_out> out,
                boost::process::std_err > err);

        child_stdin = fdopen(in.pipe().native_sink(), "w");
        child_stdout = fdopen(out.pipe().native_source(), "r");
        child_stderr = fdopen(err.pipe().native_source(), "r");

        if (!child_stdin || !child_stdout || !child_stderr) {
            throw std::runtime_error("Failed to open FILE* for process stdio");
        }

        auto message = readTestProgramMessage();
        if (!message.contains("Waiting for exit signal...")) {
            throw std::runtime_error(
                    std::string("Unexpected output from child: ") + message);
        }
    }

    void hangup() {
        fclose(child_stdin);
        child_stdin = nullptr;
    }

    void closeStderr() {
        fclose(child_stderr);
        child_stderr = nullptr;
    }

    void send(const std::string& command) {
        fmt::print(child_stdin, "{}\n", command);
        fflush(child_stdin);
    }

    std::string read() {
        std::array<char, 1024> buffer;
        if (fgets(buffer.data(), buffer.size(), child_stderr) == nullptr) {
            throw std::system_error(errno,
                                    std::system_category(),
                                    "Failed to read from child process");
        }
        return trim(buffer.data());
    }

    int wait() {
        child->wait();
        return child->exit_code();
    }

    std::string readTestProgramMessage() {
        std::array<char, 1024> buffer;
        if (fgets(buffer.data(), buffer.size(), child_stdout) == nullptr) {
            std::cout << "Failed to read from child process stdout: "
                      << strerror(errno) << std::endl;
            throw std::system_error(errno,
                                    std::system_category(),
                                    "Failed to read from child process");
        }
        return trim(buffer.data());
    }

protected:
    std::string trim(std::string s) {
        std::string result = s;
        while (!result.empty() && isspace(result.back())) {
            result.pop_back();
        }
        return result;
    }

    FILE* child_stdin = nullptr;
    FILE* child_stdout = nullptr;
    FILE* child_stderr = nullptr;
    boost::process::opstream in;
    boost::process::ipstream out;
    boost::process::ipstream err;
    std::unique_ptr<boost::process::child> child;
};

class StdinCheck : public ::testing::Test {
public:
    void SetUpTestSuite() {
        sigignore(SIGPIPE);
    }
};

TEST(StdinCheck, NormalShutdown) {
    StdinCheckRunner runner;
    runner.send("shutdown");
    auto message = runner.read();
    EXPECT_EQ("EOL on stdin. Initiating shutdown", message);
    message = runner.readTestProgramMessage();
    EXPECT_EQ("Shutdown started, sleep before exit", message);
    EXPECT_EQ(1, runner.wait());
}

TEST(StdinCheck, AbnormalShutdown) {
    StdinCheckRunner runner;
    runner.hangup();
    auto message = runner.read();
    EXPECT_EQ("EOF on stdin. Initiating shutdown", message);
    message = runner.readTestProgramMessage();
    EXPECT_EQ("Shutdown started, sleep before exit", message);
    EXPECT_EQ(1, runner.wait());
}

TEST(StdinCheck, ImmediateShutdown) {
    StdinCheckRunner runner;
    runner.send("die!");
    auto message = runner.read();
    EXPECT_EQ("'die!' on stdin. Exiting super-quickly", message);
    EXPECT_EQ(0, runner.wait());
}

TEST(StdinCheck, AbortGracefulShutdown) {
    StdinCheckRunner runner(30);
    runner.send("shutdown");
    auto message = runner.read();
    EXPECT_EQ("EOL on stdin. Initiating shutdown", message);
    runner.send("die!");
    message = runner.read();
    EXPECT_EQ("'die!' on stdin. Exiting super-quickly", message);
    EXPECT_EQ(0, runner.wait());
}

TEST(StdinCheck, UnknownCommand) {
    StdinCheckRunner runner;
    runner.send("foobar");
    auto message = runner.read();
    EXPECT_EQ("Unknown command received on stdin. Ignored", message);
}

TEST(StdinCheck, AbnormalShutdownTimeout) {
    StdinCheckRunner runner(60, 1);
    runner.send("get_abnormal_timeout");
    auto message = runner.read();
    EXPECT_EQ("1000ms", message);

    runner.hangup();
    message = runner.read();
    EXPECT_EQ("EOF on stdin. Initiating shutdown", message);
    message = runner.readTestProgramMessage();
    EXPECT_EQ("Shutdown started, sleep before exit", message);
    message = runner.read();
    EXPECT_EQ(fmt::format("Shutdown timed out! Exit({})",
                          abnormal_exit_handler_exit_code),
              message);
    EXPECT_EQ(abnormal_exit_handler_exit_code, runner.wait());
}

TEST(StdinCheck, MB68173_stderr_closed) {
    StdinCheckRunner runner;
    runner.closeStderr();
    runner.hangup();
    const auto message = runner.readTestProgramMessage();
    EXPECT_EQ("Shutdown started, sleep before exit", message);
    EXPECT_EQ(1, runner.wait());
}
