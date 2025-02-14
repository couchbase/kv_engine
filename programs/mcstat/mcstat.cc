/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <mcbp/codec/frameinfo.h>
#include <memcached/stat_group.h>
#include <platform/split_string.h>
#include <platform/terminal_color.h>
#include <platform/terminal_size.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <utilities/timing_histogram_printer.h>
#include <cctype>
#include <iostream>
#include <limits>

using namespace cb::terminal;

/// Set to true if we should print the output in JSON format
bool json = false;

/// Split the requested string into the stat key and the value
/// (dcp allows for a JSON value to be passed in)
static std::pair<std::string, std::string> split_request_string(
        std::string_view request) {
    if (request.empty()) {
        return {{}, {}};
    }

    auto arguments = cb::string::split(request);
    auto* info = StatsGroupManager::getInstance().lookup(arguments.front());
    if (info == nullptr) {
        std::cerr << TerminalColor::Red
                  << "Unknown stat group: " << arguments.front()
                  << TerminalColor::Reset << std::endl;
        exit(EXIT_FAILURE);
    }

    if (info->id == StatGroupId::Dcp) {
        std::string_view view = request;
        view.remove_prefix(3);
        while (!view.empty() && std::isspace(view.front())) {
            view.remove_prefix(1);
        }
        if (!view.empty()) {
            try {
                // this must be JSON
                const auto obj = nlohmann::json::parse(view);
                if (!obj.is_object()) {
                    std::cerr << TerminalColor::Red
                              << "The value for the DCP stat must be a JSON "
                                 "object: "
                              << view << TerminalColor::Reset << std::endl;
                    exit(EXIT_FAILURE);
                }
            } catch (const std::exception& e) {
                std::cerr << TerminalColor::Red
                          << "Failed to parse the JSON value: " << view
                          << TerminalColor::Reset << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        return {std::string{arguments.front()}, std::string(view)};
    }

    return {std::string{request}, {}};
}

static void request_dcp_stat(MemcachedConnection& connection,
                             const std::string& value) {
    bool first_time = true;
    if (json) {
        std::cout << "{" << std::endl;
    }
    connection.stats(
            [&first_time](
                    const auto& key, const auto& value, auto datatype) -> void {
                if (json) {
                    if (first_time) {
                        first_time = false;
                    } else {
                        std::cout << "," << std::endl;
                    }
                }

                if (datatype == cb::mcbp::Datatype::JSON) {
                    auto payload = nlohmann::json::parse(value);
                    if (json) {
                        fmt::print(stdout, R"("{}":{})", key, payload.dump(2));
                    } else if (payload.is_object()) {
                        for (auto iter = payload.begin(); iter != payload.end();
                             ++iter) {
                            fmt::print(stdout,
                                       "{}_{} {}\n",
                                       key,
                                       iter.key(),
                                       iter.value().dump());
                        }
                    } else {
                        fmt::print(stdout, "{} {}\n", key, value);
                    }
                } else {
                    // dump it as a string value for now
                    if (json) {
                        fmt::print(stdout, R"("{}":"{}")", key, value);
                    } else {
                        fmt::print(stdout, "{} {}\n", key, value);
                    }
                }
            },
            "dcp",
            value);
    if (json) {
        std::cout << "}" << std::endl;
    }
}

static void print_key_value_pair(std::string_view group,
                                 std::string_view key,
                                 std::string_view value) {
    bool printed = false;
    if (value.find(R"("data":[)") != std::string::npos &&
        value.find(R"("bucketsLow":)") != std::string::npos) {
        // this might be a timing histogram... just try to
        // dump as such
        std::string_view nm;
        if (key.empty() || std::isdigit(key.front())) {
            nm = group;
        } else {
            nm = key;
        }

        try {
            TimingHistogramPrinter printer(nlohmann::json::parse(value));
            printer.dumpHistogram(nm);
            printed = true;
        } catch (const std::exception&) {
        }
    }

    if (!printed) {
        std::cout << key << " " << value << std::endl;
    }
}

/**
 * Request a stat from the server
 * @param connection socket connected to the server
 * @param statGroup the name of the stat to receive (empty == ALL)
 */
static void request_stat(MemcachedConnection& connection,
                         const std::string& statGroup) {
    auto [statKey, statValue] = split_request_string(statGroup);
    if (statKey == "dcp") {
        auto value = nlohmann::json::object();
        if (!statValue.empty()) {
            value = nlohmann::json::parse(statValue);
        }
        if (!value.contains("stream_format")) {
            value["stream_format"] = "json";
        }
        request_dcp_stat(connection, value.dump());
        return;
    }

    if (json) {
        auto stats = connection.stats(statGroup);
        std::cout << stats.dump() << std::endl;
    } else {
        connection.stats(
                [statGroup](const auto& key, const auto& value) -> void {
                    print_key_value_pair(statGroup, key, value);
                },
                statGroup);
    }
}

void request_sorted_stat(MemcachedConnection& connection,
                         const std::string& statGroup) {
    using KeyValue = std::pair<std::string, std::string>;
    std::vector<KeyValue> stats;

    auto [statKey, statValue] = split_request_string(statGroup);
    connection.stats(
            [&stats](const std::string& k, const std::string& v) -> void {
                stats.emplace_back(k, v);
            },
            statKey,
            statValue);

    std::ranges::sort(stats, [](const auto& a, const auto& b) {
        const auto& akey = a.first;
        const auto& bkey = b.first;

        if (!akey.empty() && !bkey.empty()) {
            if (std::isdigit(akey.front()) && std::isdigit(bkey.front())) {
                return std::stoull(akey) < std::stoull(bkey);
            }
        }

        return a.first < b.first;
    });

    for (const auto& [key, value] : stats) {
        print_key_value_pair(statGroup, key, value);
    }
}

static void usage(McProgramGetopt& instance, int exitcode) {
    std::cerr << R"(Usage: mcstat [options] statkey [arguments to statkey]

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
        }

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
        }

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
        std::ranges::fill(kbuf, ' ');

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

/// Build the stat string from the arguments provided to mcstat
/// For compat with cbstats allow "all" to be used
static std::string buildStatString(const std::vector<std::string_view>& args) {
    if (args.empty() || (args.size() == 1 && args.front() == "all")) {
        return {};
    }

    std::stringstream ss;
    for (const auto& arg : args) {
        ss << arg << " ";
    }
    auto ret = ss.str();
    ret.pop_back();
    return ret;
}

int main(int argc, char** argv) {
    bool sort = false;
    bool allBuckets = false;
    std::vector<std::string> buckets;

    McProgramGetopt getopt;
    using cb::getopt::Argument;
    getopt.addOption(
            {[](auto value) {
                 json = true;
                 if (value == "pretty") {
                     std::cerr
                             << "Pretty print is no longer supported. Use an "
                                "external tool such as jq to format the output"
                             << std::endl;
                 } else if (!value.empty()) {
                     std::cerr << TerminalColor::Red
                               << "Unknown json argument: " << value
                               << TerminalColor::Reset << std::endl;
                     std::exit(EXIT_FAILURE);
                 }
             },
             'j',
             "json",
             Argument::Optional,
             "",
             "Print result in JSON. Using pretty is no longer supported"});

    getopt.addOption({[&buckets](auto value) {
                          buckets.emplace_back(std::string{value});
                      },
                      'b',
                      "bucket",
                      Argument::Required,
                      "bucketname",
                      "The name of the bucket to operate on"});

    getopt.addOption(
            {[&allBuckets](auto) { allBuckets = true; },
             'a',
             "all-buckets",
             "Get list of buckets from the node and display stats per bucket "
             "basis."});

    getopt.addOption({[&sort](auto) { sort = true; },
                      "sort",
                      "sort output (only valid for non-JSON output)"});

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

    if (sort && json) {
        std::cerr << TerminalColor::Red
                  << "Cannot create JSON output while sorting"
                  << TerminalColor::Reset << std::endl;
        return EXIT_FAILURE;
    }

    if (allBuckets && !buckets.empty()) {
        std::cerr << TerminalColor::Red
                  << "Cannot use both bucket and all-buckets options"
                  << TerminalColor::Reset << std::endl;
        return EXIT_FAILURE;
    }

    try {
        const auto stat_key = buildStatString(arguments);

        getopt.assemble();
        auto connection = getopt.getConnection();
        // MEMCACHED_VERSION contains the git sha
        connection->setAgentName("mcstat/" PRODUCT_VERSION);
        connection->setFeatures(
                {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});

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

            if (sort) {
                request_sorted_stat(*connection, stat_key);
            } else {
                request_stat(*connection, stat_key);
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
