/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/Unistd.h>
#include <mcbp/codec/frameinfo.h>
#include <memcached/stat_group.h>
#include <platform/terminal_color.h>
#include <platform/terminal_size.h>
#include <programs/getpass.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <iostream>
#include <limits>

using namespace cb::terminal;

/**
 * Request a stat from the server
 * @param sock socket connected to the server
 * @param key the name of the stat to receive (empty == ALL)
 * @param json if true print as json otherwise print old-style
 */
static void request_stat(MemcachedConnection& connection,
                         const std::string& key,
                         bool json,
                         bool format,
                         const std::string& impersonate) {
    try {
        auto getFrameInfos = [&impersonate]() -> FrameInfoVector {
            if (impersonate.empty()) {
                return {};
            }
            FrameInfoVector ret;
            ret.emplace_back(std::make_unique<
                             cb::mcbp::request::ImpersonateUserFrameInfo>(
                    impersonate));
            return ret;
        };
        if (json) {
            auto stats = connection.stats(key, getFrameInfos);
            std::cout << stats.dump(format ? 2 : -1) << std::endl;
        } else {
            connection.stats(
                    [](const std::string& key,
                       const std::string& value) -> void {
                        std::cout << key << " " << value << std::endl;
                    },
                    key,
                    getFrameInfos);
        }
    } catch (const ConnectionError& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
    }
}

static void usage(McProgramGetopt& instance, int exitcode) {
    std::cerr << R"(Usage: mcstat [options] statkey ...

Options:

)" << instance << std::endl
              << std::endl;
    std::exit(exitcode);
}

void printStatkeyHelp(std::string_view key) {
    size_t dw;
    try {
        auto [width, height] = getTerminalSize();
        (void)height;
        if (width < 60) {
            // if you've got a small terminal we'll just print it in the normal
            // way.
            width = std::numeric_limits<size_t>::max();
        };

        dw = width;
    } catch (std::exception&) {
        dw = std::numeric_limits<size_t>::max();
    }

    bool found = false;

    StatsGroupManager::getInstance().iterate([dw, &found, key](const auto& e) {
        if (key != e.key && !(key == "default" && e.key.empty())) {
            return;
        }

        found = true;
        std::cout << TerminalColor::Green << key << std::endl;
        for (const auto& c : key) {
            (void)c;
            std::cout << "=";
        }
        std::cout << std::endl << std::endl;
        auto descr = e.description;
        while (true) {
            if (descr.size() < dw) {
                std::cout << descr.data() << std::endl;
                break;
            }

            auto idx = descr.rfind(' ', std::min(dw, descr.size()));
            if (idx == std::string::npos) {
                std::cout << descr.data() << std::endl;
                break;
            }
            std::cout.write(descr.data(), idx);
            std::cout << std::endl;
            descr.remove_prefix(idx + 1);
        }
        std::cout << std::endl;
        if (e.bucket) {
            std::cout << TerminalColor::Yellow << "Bucket specific stat group"
                      << std::endl;
        }
        if (e.privileged) {
            std::cout << TerminalColor::Yellow << "Privileged stat group"
                      << std::endl;
        }
        std::cout << TerminalColor::Reset;
    });

    if (!found) {
        std::cerr << TerminalColor::Red << key << " is not a valid stat group"
                  << TerminalColor::Reset << std::endl;
        exit(EXIT_FAILURE);
    }
    exit(EXIT_SUCCESS);
}

void printStatkeyHelp() {
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

    std::cerr << "statkey may be one of: " << std::endl;

    StatsGroupManager::getInstance().iterate([dw](const auto& e) {
        std::array<char, 34> kbuf;
        snprintf(kbuf.data(), kbuf.size(), "    %-24s  ", e.key.data());
        std::cerr << kbuf.data();
        if (e.bucket) {
            std::cerr << TerminalColor::Yellow << 'B' << TerminalColor::Reset;
        } else {
            std::cerr << ' ';
        }

        if (e.privileged) {
            std::cerr << TerminalColor::Yellow << 'P' << TerminalColor::Reset;
        } else {
            std::cerr << ' ';
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

    std::cerr << "B - bucket specific stat group" << std::endl
              << "P - privileged stat" << std::endl;

    exit(EXIT_SUCCESS);
}

int main(int argc, char** argv) {
    std::string impersonate;
    bool json = false;
    bool format = false;
    bool allBuckets = false;
    std::vector<std::string> buckets;

    McProgramGetopt getopt;
    using cb::getopt::Argument;
    getopt.addOption({[&json, &format](auto value) {
                          json = true;
                          if (value == "pretty") {
                              format = true;
                          }
                      },
                      'j',
                      "json",
                      Argument::Optional,
                      "pretty",
                      "Print result in JSON"});

    getopt.addOption({[&buckets](auto value) {
                          buckets.emplace_back(std::string{value});
                      },
                      'b',
                      "bucket",
                      Argument::Required,
                      "bucketname",
                      "The name of the bucket to operate on"});

    getopt.addOption(
            {[&impersonate](auto value) { impersonate = std::string{value}; },
             'I',
             "impersonate",
             Argument::Required,
             "username",
             "Try to impersonate the specified user"});

    getopt.addOption(
            {[&allBuckets](auto) { allBuckets = true; },
             'a',
             "all-buckets",
             "Get list of buckets from the node and display stats per bucket "
             "basis."});

    getopt.addOption({[&getopt](auto value) {
                          if (value.empty()) {
                              usage(getopt, EXIT_SUCCESS);
                          }
                          if (value == "statkey") {
                              printStatkeyHelp();
                              std::exit(EXIT_SUCCESS);
                          }
                          printStatkeyHelp(value);
                          std::exit(EXIT_SUCCESS);
                      },
                      "help",
                      Argument::Optional,
                      "statkey",
                      "This help text (or description of statkeys)"});

    auto arguments = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    if (allBuckets && !buckets.empty()) {
        std::cerr << TerminalColor::Red
                  << "Cannot use both bucket and all-buckets options"
                  << TerminalColor::Reset << std::endl;
        return EXIT_FAILURE;
    }

    try {
        getopt.assemble();
        auto connection = getopt.getConnection();
        // MEMCACHED_VERSION contains the git sha
        connection->setAgentName("mcstat " MEMCACHED_VERSION);
        connection->setFeatures({cb::mcbp::Feature::XERROR});

        if (allBuckets) {
            buckets = connection->listBuckets();
        }

        // buckets can be empty, so do..while at least one stat call
        auto bucketItr = buckets.begin();
        do {
            if (bucketItr != buckets.end()) {
                // When all buckets is enabled, clone what cbstats does
                if (allBuckets) {
                    static std::string bucketSeparator(78, '*');
                    std::cout << TerminalColor::Green << bucketSeparator
                              << std::endl
                              << *bucketItr << TerminalColor::Reset << std::endl
                              << std::endl;
                }
                connection->selectBucket(*bucketItr);
                bucketItr++;
            }

            if (arguments.empty()) {
                request_stat(*connection, "", json, format, impersonate);
            } else {
                for (const auto& arg : arguments) {
                    request_stat(*connection,
                                 std::string{arg},
                                 json,
                                 format,
                                 impersonate);
                }
            }
        } while (bucketItr != buckets.end());

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
