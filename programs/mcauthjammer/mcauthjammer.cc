/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "platform/dirutils.h"

#include <folly/MPMCQueue.h>
#include <folly/io/async/EventBase.h>
#include <platform/split_string.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <iostream>

static std::shared_ptr<spdlog::logger> logger;

static void usage(McProgramGetopt& instance, int exitcode) {
    std::cerr << R"(Usage: mcauthjammer [options]

Options:

)" << instance << std::endl
              << std::endl;
    std::exit(exitcode);
}

std::atomic_bool done{false};

struct Entry {
    std::string username;
    std::string password;
    std::string mech;
};

folly::MPMCQueue<Entry> auth_queue(1000);

class Authenticator {
public:
    Authenticator(std::string hostname, int port, bool tls)
        : connection(std::make_unique<MemcachedConnection>(
                  std::move(hostname), port, AF_UNSPEC, tls)) {
        connection->connect();
    }

    void run() {
        while (!done) {
            Entry entry;
            if (auth_queue.read(entry)) {
                while (!test_single_auth(
                        entry.username, entry.password, entry.mech)) {
                    // try again
                }
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds{10});
            }
        }
    }

protected:
    bool test_single_auth(const std::string& user,
                          const std::string& password,
                          const std::string& mech) {
        std::string reason;
        try {
            connection->authenticate(user, password, mech);
            logger->info("Successfully logged in {} with password {}",
                         user,
                         password);
            return true;
        } catch (const ConnectionError& exception) {
            if (exception.isAuthError()) {
                return true;
            }
            reason = exception.what();
        } catch (const std::system_error& exception) {
            if (static_cast<std::errc>(exception.code().value()) !=
                std::errc::connection_reset) {
                reason = exception.what();
            }
        } catch (const std::exception& exception) {
            reason = exception.what();
        }
        if (!reason.empty()) {
            logger->warn("test_single_auth({}): exception: {}", user, reason);
        }
        connection->reconnect();
        return false;
    }

    std::unique_ptr<MemcachedConnection> connection;
};

struct HostEntry {
    std::string hostname;
    int port;
    bool tls;
};
std::vector<HostEntry> server_hosts;

static void setupHosts(MemcachedConnection& connection) {
    auto rsp = connection.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetClusterConfig});
    if (!rsp.isSuccess()) {
        std::cerr << "Failed to fetch cluster map: " << rsp.getStatus()
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }

    auto json = rsp.getDataJson();
    if (!json.contains("nodesExt")) {
        std::cerr << "clustermap does not contain nodesExt" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    for (auto& obj : json["nodesExt"]) {
        if (!obj.is_object() || !obj.contains("services")) {
            continue;
        }
        const auto& services = obj["services"];
        const auto port_plain = services.value("kv", 0);
        const auto port_tls = services.value("kvSSL", 0);
        std::string hostname = connection.getHostname();
        if (!obj.value("thisNode", false)) {
            hostname = obj.value("hostname", "");
        }

        if (port_plain) {
            logger->info("Register server: {} at {}", hostname, port_plain);
            server_hosts.push_back(HostEntry{hostname, port_plain, false});
        }
        if (port_tls) {
            logger->info("Register server: {} at {} - TLS", hostname, port_tls);
            server_hosts.push_back(HostEntry{hostname, port_tls, true});
        }
    }
}

int main(int argc, char** argv) {
    std::optional<std::filesystem::path> password_file;
    std::string mechanism = "PLAIN";
    size_t num_threads = 4;

    McProgramGetopt getopt;
    using cb::getopt::Argument;

    getopt.addOption({[&num_threads](auto value) {
                          num_threads = stoul(std::string{value});
                      },
                      "threads",
                      Argument::Required,
                      "num",
                      fmt::format("The number of threads to use per server "
                                  "endpoint [Default: {}]",
                                  num_threads)});

    getopt.addOption({[&password_file](auto value) {
                          password_file = std::filesystem::path(value);
                      },
                      "password-file",
                      Argument::Required,
                      "filename",
                      "A file containing passwords to try"});

    getopt.addOption(
            {[&mechanism](auto value) { mechanism = std::string(value); },
             "mechanism",
             Argument::Required,
             "mech",
             "The mechanism to use for users [Default: PLAIN]"});

    getopt.addOption({[&getopt](auto value) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    auto arguments = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    if (arguments.empty()) {
        std::cerr << "No users specified" << std::endl;
        return EXIT_FAILURE;
    }

    if (!password_file.has_value()) {
        std::cerr << "Password file must be specified" << std::endl;
        return EXIT_FAILURE;
    }

    std::vector<std::string> passwords;
    try {
        auto content = cb::io::loadFile(*password_file);
        auto views = cb::string::split(content, '\n');
        for (const auto& v : views) {
            passwords.emplace_back(v);
        }
    } catch (const std::exception& exception) {
        std::cerr << "Failed to read password file: " << exception.what();
        return EXIT_FAILURE;
    }

    getopt.assemble();

    logger = std::make_shared<spdlog::logger>(
            "mcauthjammer",
            std::make_shared<spdlog::sinks::stderr_color_sink_st>());
    spdlog::register_logger(logger);
    logger->set_level(spdlog::level::info);
    logger->set_pattern("%^%Y-%m-%dT%T.%f%z %l %v%$");

    for (const auto& u : arguments) {
        logger->info("Test user {} with {} passwords", u, passwords.size());
    }

    setupHosts(*getopt.getConnection());

    std::vector<std::thread> threads;
    logger->info("Starting threads");
    for (std::size_t ii = 0; ii < num_threads; ++ii) {
        for (const auto& endpoint : server_hosts) {
            threads.emplace_back([&endpoint]() {
                Authenticator authenticator(
                        endpoint.hostname, endpoint.port, endpoint.tls);
                authenticator.run();
            });
        }
    }

    logger->info("Generating auth requests");

    for (const auto& user : arguments) {
        for (const auto& pw : passwords) {
            logger->debug("Test: {} [{}]", user, pw);
            auth_queue.blockingWrite(Entry{std::string{user}, pw, mechanism});
        }
    }

    done = true;
    logger->info("Waiting for threads to exit");
    for (auto& thread : threads) {
        thread.join();
    }

    logger->info("Shutdown complete");
    logger->flush();
    logger.reset();
    spdlog::details::registry::instance().shutdown();
    return EXIT_SUCCESS;
}
