/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "stat_formatters.h"
#include "tasks_table_printer.h"
#include <mcbp/codec/frameinfo.h>
#include <memcached/stat_group.h>
#include <platform/split_string.h>
#include <platform/terminal_color.h>
#include <platform/terminal_size.h>
#include <programs/mc_program_getopt.h>
#include <programs/natsort.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/timing_histogram_printer.h>
#include <array>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <limits>

using namespace cb::terminal;

/**
 * Set to true if we should print the output in JSON format
 */
bool json = false;

/**
 * Set to true via mcstat's own "--disable-utf8" to render timing histogram
 * bars (see print_key_value_pair()'s use of TimingHistogramPrinter) with
 * plain ASCII rather than UTF-8 sparkline characters. mcstat defaults to
 * UTF-8, matching TimingHistogramPrinter's own default.
 */
bool disableUtf8 = false;

/**
 * The server's error map "errors" object (keyed by hex status code),
 * used to resolve mcbp status codes to human-readable names/descriptions.
 * Fetched once, right after the connection is established (see main()),
 * and reused for the lifetime of the connection - mirrors
 * mc_bin_client.py's hello() caching self.error_map rather than issuing a
 * fresh GetErrorMap request every time a status code needs resolving.
 */
nlohmann::json errorMap = nlohmann::json::object();

/**
 * Split the requested string into the stat key and the value
 * (dcp allows for a JSON value to be passed in)
 */
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

    if (info->id == StatGroupId::Tasks || info->id == StatGroupId::TasksAll ||
        info->id == StatGroupId::Hash || info->id == StatGroupId::Dispatcher ||
        info->id == StatGroupId::Responses) {
        if (arguments.size() > 1) {
            std::string_view view = request;
            view.remove_prefix(arguments.front().size());
            while (!view.empty() && std::isspace(view.front())) {
                view.remove_prefix(1);
            }
            return {std::string{arguments.front()}, std::string(view)};
        }
        return {std::string{arguments.front()}, {}};
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
            } catch (const std::exception&) {
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
            printer.setUseUtf8(!disableUtf8);
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
 * If statKey is "dcp", issue the (streaming) DCP stat request and print it;
 * otherwise do nothing. Returns true if the DCP stat was handled.
 */
static bool maybe_request_dcp_stat(MemcachedConnection& connection,
                                   const std::string& statKey,
                                   const std::string& statValue) {
    if (statKey != "dcp") {
        return false;
    }
    auto value = nlohmann::json::object();
    if (!statValue.empty()) {
        value = nlohmann::json::parse(statValue);
    }
    if (!value.contains("stream_format")) {
        value["stream_format"] = "json";
    }
    request_dcp_stat(connection, value.dump());
    return true;
}

/**
 * Fetch the server's error map and return its "errors" object (keyed by
 * hex status code), used to resolve status codes to their human-readable
 * names. Returns an empty object if the server doesn't support the
 * command.
 */
static nlohmann::json get_server_error_map_errors(
        MemcachedConnection& connection) {
    auto rsp = connection.execute(BinprotGetErrorMapCommand{});
    if (!rsp.isSuccess()) {
        return nlohmann::json::object();
    }
    return rsp.getDataJson().value("errors", nlohmann::json::object());
}

/**
 * Format a failed stat request as "Memcached error #<status>: <name> :
 * <desc> : <context>", resolving <name>/<desc> via the cached server error
 * map (falling back to the mcbp status text if the map doesn't have an
 * entry, e.g. a status code newer than this client binary knows about)
 * and <context> via any extended error JSON context (empty for a plain
 * protocol error).
 */
static std::string format_memcached_error(const ConnectionError& error) {
    auto status = uint16_t(error.getReason());
    auto entry = errorMap.find(fmt::format("{:x}", status));

    std::string name = fmt::format("{}", error.getReason());
    std::string desc;
    if (entry != errorMap.end()) {
        name = entry->value("name", name);
        desc = entry->value("desc", "");
    }

    std::string context;
    try {
        const auto ctx = error.getErrorJsonContext();
        if (ctx.is_object()) {
            auto errorObj = ctx.value("error", nlohmann::json::object());
            if (errorObj.is_object()) {
                context = errorObj.value("context", "");
            }
        }
    } catch (const std::exception&) {
    }

    return fmt::format(
            "Memcached error #{}:  {} : {} : {}", status, name, desc, context);
}

/**
 * Request a stat from the server
 * @param connection socket connected to the server
 * @param statGroup the name of the stat to receive (empty == ALL)
 */
static void request_stat(MemcachedConnection& connection,
                         const std::string& statGroup) {
    auto [statKey, statValue] = split_request_string(statGroup);
    if (maybe_request_dcp_stat(connection, statKey, statValue)) {
        return;
    }

    if (statKey == "tasks" || statKey == "tasks-all") {
        if (json) {
            auto stats = connection.stats(statKey);
            std::cout << stats.dump() << std::endl;
        } else {
            std::vector<std::pair<std::string, std::string>> stats;
            connection.stats(
                    [&stats](const auto& key, const auto& value) -> void {
                        stats.emplace_back(key, value);
                    },
                    statKey);
            printTasksTable(stats, statValue, std::cout);
        }
        return;
    }

    if (statKey == "hash") {
        if (json) {
            auto stats = connection.stats(statKey);
            std::cout << stats.dump() << std::endl;
        } else {
            std::vector<std::pair<std::string, std::string>> stats;
            connection.stats(
                    [&stats](const auto& key, const auto& value) -> void {
                        stats.emplace_back(key, value);
                    },
                    statKey);
            printHashStats(stats, statValue == "detail", std::cout);
        }
        return;
    }

    if (statKey == "dispatcher") {
        if (json) {
            auto stats = connection.stats(statKey);
            std::cout << stats.dump() << std::endl;
        } else {
            std::vector<std::pair<std::string, std::string>> stats;
            connection.stats(
                    [&stats](const auto& key, const auto& value) -> void {
                        stats.emplace_back(key, value);
                    },
                    statKey);
            printDispatcherStats(stats, statValue == "logs", std::cout);
        }
        return;
    }

    if (statKey == "responses") {
        if (json) {
            auto stats = connection.stats(statKey);
            std::cout << stats.dump() << std::endl;
        } else {
            std::vector<std::pair<std::string, std::string>> stats;
            connection.stats(
                    [&stats](const auto& key, const auto& value) -> void {
                        stats.emplace_back(key, value);
                    },
                    statKey);
            printResponsesStats(stats,
                                errorMap,
                                statValue == "all" || statValue == "1",
                                std::cout);
        }
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

/**
 * Send statGroup to the server exactly as given, without validating it
 * against the known stat groups (StatsGroupManager) first, and without any
 * specialized rendering (e.g. hash/dispatcher/tasks) - only the generic,
 * plain key/value output. Mirrors cbstats.py's own 'raw' command.
 *
 * Useful for stat groups this mcstat binary doesn't know about (e.g.
 * internal/underscore-prefixed debug stats such as "_hash-dump <vbid>", or
 * ones added to a newer server than this binary was built against).
 *
 * @param connection socket connected to the server
 * @param statGroup the exact stat group (and any arguments) to send
 */
static void request_raw_stat(MemcachedConnection& connection,
                             const std::string& statGroup) {
    if (json) {
        auto stats = connection.stats(statGroup);
        std::cout << stats.dump() << std::endl;
    } else {
        connection.stats(
                [&statGroup](const auto& key, const auto& value) -> void {
                    print_key_value_pair(statGroup, key, value);
                },
                statGroup);
    }
}

void request_sorted_stat(MemcachedConnection& connection,
                         const std::string& statGroup) {
    auto [statKey, statValue] = split_request_string(statGroup);
    if (statKey == "tasks" || statKey == "tasks-all" || statKey == "hash" ||
        statKey == "dispatcher" || statKey == "responses") {
        // These formatters have their own internal sorting logic
        request_stat(connection, statGroup);
        return;
    }

    using KeyValue = std::pair<std::string, std::string>;
    std::vector<KeyValue> stats;

    connection.stats(
            [&stats](const std::string& k, const std::string& v) -> void {
                stats.emplace_back(k, v);
            },
            statKey,
            statValue);

    std::ranges::sort(stats, [](const auto& a, const auto& b) {
        const auto& akey = a.first;
        const auto& bkey = b.first;
        return cb::natsort::less{}(akey, bkey);
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
    bool raw = false;
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

    getopt.addOption(
            {[&raw](auto) { raw = true; },
             "raw",
             "Send statkey to the server as-is, without validating it "
             "against the known stat groups first, and without any "
             "specialized formatting (mirrors cbstats' 'raw' command). "
             "Useful for internal/underscore-prefixed debug stat groups "
             "this mcstat binary doesn't know about."});

    getopt.addOption({[](auto) { disableUtf8 = true; },
                      "disable-utf8",
                      "Render timing histogram bars with plain ASCII "
                      "instead of UTF-8 sparkline characters (UTF-8 is "
                      "used by default)"});

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

    if (raw && sort) {
        std::cerr << TerminalColor::Red << "Cannot sort raw output"
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
        // Negotiate Collections so collection-aware stat groups (e.g.
        // per-collection stats) are reported in full.
        connection->setFeatures({cb::mcbp::Feature::XERROR,
                                 cb::mcbp::Feature::Collections,
                                 cb::mcbp::Feature::JSON});
        errorMap = get_server_error_map_errors(*connection);

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

            if (raw) {
                request_raw_stat(*connection, stat_key);
            } else if (sort) {
                request_sorted_stat(*connection, stat_key);
            } else {
                request_stat(*connection, stat_key);
            }
        } while (bucketItr != buckets.end());

    } catch (const ConnectionError& error) {
        std::cerr << TerminalColor::Red << format_memcached_error(error)
                  << TerminalColor::Reset << std::endl;
        return EXIT_FAILURE;
    } catch (const std::exception& ex) {
        std::cerr << TerminalColor::Red << ex.what() << TerminalColor::Reset
                  << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
