/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "programs/getpass.h"
#include "programs/hostname_utils.h"

#include <boost/algorithm/string/predicate.hpp>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <spdlog/fmt/fmt.h>
#include <utilities/json_utilities.h>
#include <utilities/terminate_handler.h>

#include <getopt.h>
#include <array>
#include <cinttypes>
#include <cstdlib>
#include <stdexcept>
#include <streambuf>
#include <string>

#define JSON_DUMP_INDENT_SIZE 4

class Timings {
public:
    explicit Timings(const nlohmann::json& json) {
        initialize(json);
    }

    uint64_t getTotal() const {
        return total;
    }

    void dumpHistogram(const std::string& opcode) {
        if (data.is_null()) {
            return;
        }

        fmt::print(
                stdout, "The following data is collected for \"{}\"\n", opcode);

        auto dataArray = data.get<std::vector<std::vector<nlohmann::json>>>();
        for (auto item : dataArray) {
            auto count = item[1].get<uint64_t>();
            if (count > maxCount) {
                maxCount = count;
            }
        }
        using namespace std::chrono;

        // loop though all the buckets in the json object and print them
        // to std out
        uint64_t lastBuckLow = bucketsLow;
        for (auto bucket : dataArray) {
            // Get the current bucket's highest value it would track counts for
            auto buckHigh = bucket[0].get<uint64_t>();
            // Get the counts for this bucket
            auto count = bucket[1].get<uint64_t>();
            // Get the percentile of counts that are <= buckHigh
            auto percentile = bucket[2].get<double>();

            if (lastBuckLow != buckHigh) {
                // Cast the high bucket width to us, ms and seconds so we
                // can check which units we should be using for this bucket
                auto buckHighUs = microseconds(buckHigh);
                auto buckHighMs = duration_cast<milliseconds>(buckHighUs);
                auto buckHighS = duration_cast<seconds>(buckHighUs);

                // If the bucket width values are in the order of tens of
                // seconds or milli seconds then print them as seconds or milli
                // seconds respectively. Otherwise print them as micro seconds

                // We're using tens of unit thresh holds so that each bucket
                // has 2 sig fig of differentiation in their width, so we dont
                // have buckets that are [1 - 1]s    100     (90.000%)
                if (buckHighS.count() > 10) {
                    auto low =
                            duration_cast<seconds>(microseconds(lastBuckLow));
                    dump("s",
                         low.count(),
                         buckHighS.count(),
                         count,
                         percentile);
                } else if (buckHighMs.count() > 10) {
                    auto low = duration_cast<milliseconds>(
                            microseconds(lastBuckLow));
                    dump("ms",
                         low.count(),
                         buckHighMs.count(),
                         count,
                         percentile);
                } else {
                    dump("us", lastBuckLow, buckHigh, count, percentile);
                }
            }
            // Set the low bucket value to this buckets high width value.
            lastBuckLow = buckHigh;
        }

        fmt::print(stdout, "Total: {} operations\n", total);
    }

private:
    void initialize(const nlohmann::json& root) {
        if (root.find("error") != root.end()) {
            // The server responded with an error.. send that to the user
            throw std::runtime_error(root["error"].get<std::string>());
        }
        if (root.find("data") != root.end()) {
            total = cb::jsonGet<uint64_t>(root, "total");
            data = cb::jsonGet<nlohmann::json>(root, "data");
            bucketsLow = cb::jsonGet<uint64_t>(root, "bucketsLow");
        }
    }

    void dump(const char* timeunit,
              int64_t low,
              int64_t high,
              int64_t count,
              double percentile) {
        // Calculations for histogram size rendering
        double factionOfHashes = (count / static_cast<double>(maxCount));
        int num = static_cast<int>(44.0 * factionOfHashes);

        // Calculations for padding around the count in each histogram bucket
        auto numberOfSpaces = fmt::formatted_size("{}", maxCount);

        fmt::print(stdout,
                   "[{:5} - {:5}]{} ({:6.4f}%) \t{}| {}\n",
                   low,
                   high,
                   timeunit,
                   percentile,
                   fmt::format("{:>" + std::to_string(numberOfSpaces) + "}",
                               count),
                   std::string(num, '#'));
    }

    /**
     * The highest value of all the samples (used to figure out the width
     * used for each sample in the printout)
     */
    uint64_t maxCount = 0;
    /**
     * Json object to store the data returned by memcached
     */
    nlohmann::json data;
    /**
     * The starting point of the lowest buckets width.
     * E.g. if buckets were [10 - 20][20 - 30] it would be 10.
     * Used to help reduce the amount the amount of json sent to
     * mctimings
     */
    uint64_t bucketsLow = 0;
    /**
     * Total number of counts recorded in the histogram
     */
    uint64_t total = 0;
};

std::string opcode2string(cb::mcbp::ClientOpcode opcode) {
    try {
        return to_string(opcode);
    } catch (const std::exception&) {
        return cb::to_hex(uint8_t(opcode));
    }
}

static void request_cmd_timings(MemcachedConnection& connection,
                                const std::string& bucket,
                                cb::mcbp::ClientOpcode opcode,
                                bool verbose,
                                bool skip,
                                bool json) {
    BinprotGetCmdTimerCommand cmd;
    cmd.setBucket(bucket);
    cmd.setOpcode(opcode);

    connection.sendCommand(cmd);

    BinprotGetCmdTimerResponse resp;
    connection.recvResponse(resp);

    if (!resp.isSuccess()) {
        switch (resp.getStatus()) {
        case cb::mcbp::Status::KeyEnoent:
            fmt::print(stderr, "Cannot find bucket: {}\n", bucket);
            break;
        case cb::mcbp::Status::Eaccess:
            if (bucket == "/all/") {
                fmt::print(stderr,
                           "Not authorized to access aggregated timings data.\n"
                           "Try specifying a bucket by using -b bucketname\n");

            } else {
                fmt::print(stderr, "Not authorized to access timings data\n");
            }
            break;
        default:
            fmt::print(stderr,
                       "Command failed: {}\n",
                       to_string(resp.getStatus()));
        }
        exit(EXIT_FAILURE);
    }

    try {
        auto command = opcode2string(opcode);
        if (json) {
            auto timings = resp.getTimings();
            if (timings == nullptr) {
                if (!skip) {
                    fmt::print(stderr,
                               "The server doesn't have information about "
                               "\"{}\"\n",
                               command);
                }
            } else {
                timings["command"] = command;
                fmt::print(stdout, "{}\n", timings.dump(JSON_DUMP_INDENT_SIZE));
            }
        } else {
            Timings timings(resp.getTimings());

            if (timings.getTotal() == 0) {
                if (skip == 0) {
                    fmt::print(stdout,
                               "The server doesn't have information about "
                               "\"{}\"\n",
                               command);
                }
            } else {
                if (verbose) {
                    timings.dumpHistogram(command);
                } else {
                    fmt::print(stdout,
                               "{} {} operations\n",
                               command,
                               timings.getTotal());
                }
            }
        }
    } catch (const std::exception& e) {
        fmt::print(stderr, "Fatal error: {}\n", e.what());
        exit(EXIT_FAILURE);
    }
}

static void request_stat_timings(MemcachedConnection& connection,
                                 const std::string& key,
                                 bool verbose,
                                 bool json_output) {
    std::map<std::string, std::string> map;
    try {
        map = connection.statsMap(key);
    } catch (const ConnectionError& ex) {
        if (ex.isNotFound()) {
            fmt::print(stderr, "Cannot find statistic: {}\n", key);
        } else if (ex.isAccessDenied()) {
            fmt::print(stderr, "Not authorized to access timings data\n");
        } else {
            fmt::print(stderr, "Fatal error: {}\n", ex.what());
        }

        exit(EXIT_FAILURE);
    }

    // The return value from stats injects the result in a k-v pair, but
    // these responses (i.e. subdoc_execute) don't include a key,
    // so the statsMap adds them into the map with a counter to make sure
    // that you can fetch all of them. We only expect a single entry, which
    // would be named "0"
    auto iter = map.find("0");
    if (iter == map.end()) {
        fmt::print(stderr, "Failed to fetch statistics for \"{}\"\n", key);
        exit(EXIT_FAILURE);
    }

    // And the value for the item should be valid JSON
    nlohmann::json json = nlohmann::json::parse(iter->second);
    if (json.is_null()) {
        fmt::print(stderr,
                   "Failed to fetch statistics for \"{}\". Not json\n",
                   key);
        exit(EXIT_FAILURE);
    }
    try {
        if (json_output) {
            json["command"] = key;
            fmt::print(stdout, "{}\n", json.dump(JSON_DUMP_INDENT_SIZE));
        } else {
            Timings timings(json);
            if (verbose) {
                timings.dumpHistogram(key);
            } else {
                fmt::print(
                        stdout, " {} {} operations\n", key, timings.getTotal());
            }
        }
    } catch (const std::exception& e) {
        fmt::print(stderr, "Fatal error: {}\n", e.what());
        exit(EXIT_FAILURE);
    }
}

void usage() {
    fmt::print(stderr,
               "Usage mctimings [options] [opcode / statname]\n{}\n",
               R"(Options:

  -h or --host hostname[:port]   The host (with an optional port) to connect to
  -p or --port port              The port number to connect to
  -b or --bucket bucketname      The name of the bucket to operate on
  -u or --user username          The name of the user to authenticate as
  -P or --password password      The passord to use for authentication
                                 (use '-' to read from standard input)
  -s or --ssl                    Connect to the server over SSL
  -4 or --ipv4                   Connect over IPv4
  -6 or --ipv6                   Connect over IPv6
  -v or --verbose                Use verbose output
  -S                             Read password from standard input
  -j or --json[=pretty]          Print JSON instead of histograms
  --help                         This help text

)");
    fmt::print(stderr,
               "Example:\n     mctimings --user operator --bucket /all/ "
               "--password - --verbose GET SET\n");
}

int main(int argc, char** argv) {
    // Make sure that we dump callstacks on the console
    install_backtrace_terminate_handler();

    int cmd;
    std::string port{"11210"};
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    std::string bucket{"/all/"};
    sa_family_t family = AF_UNSPEC;
    bool verbose = false;
    bool secure = false;
    bool json = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    struct option long_options[] = {
            {"ipv4", no_argument, nullptr, '4'},
            {"ipv6", no_argument, nullptr, '6'},
            {"host", required_argument, nullptr, 'h'},
            {"port", required_argument, nullptr, 'p'},
            {"bucket", required_argument, nullptr, 'b'},
            {"password", required_argument, nullptr, 'P'},
            {"user", required_argument, nullptr, 'u'},
            {"ssl", no_argument, nullptr, 's'},
            {"verbose", no_argument, nullptr, 'v'},
            {"json", optional_argument, nullptr, 'j'},
            {"help", no_argument, nullptr, 0},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(
                    argc, argv, "46h:p:u:b:P:sSvj", long_options, nullptr)) !=
           EOF) {
        switch (cmd) {
        case '6':
            family = AF_INET6;
            break;
        case '4':
            family = AF_INET;
            break;
        case 'h':
            host.assign(optarg);
            break;
        case 'p':
            port.assign(optarg);
            break;
        case 'S':
            password.assign("-");
            break;
        case 'b':
            bucket.assign(optarg);
            break;
        case 'u':
            user.assign(optarg);
            break;
        case 'P':
            password.assign(optarg);
            break;
        case 's':
            secure = true;
            break;
        case 'v':
            verbose = true;
            break;
        case 'j':
            json = true;
            if (optarg && boost::iequals(optarg, "pretty")) {
                verbose = true;
            }
            break;
        default:
            usage();
            return cmd == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
        }
    }

    if (password == "-") {
        password.assign(getpass());
    } else if (password.empty()) {
        const char* env_password = std::getenv("CB_PASSWORD");
        if (env_password) {
            password = env_password;
        }
    }

    try {
        in_port_t in_port;
        sa_family_t fam;
        std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

        if (family == AF_UNSPEC) { // The user may have used -4 or -6
            family = fam;
        }
        MemcachedConnection connection(host, in_port, family, secure);

        connection.connect();

        // MEMCACHED_VERSION contains the git sha
        connection.setAgentName("mctimings " MEMCACHED_VERSION);
        connection.setFeatures({cb::mcbp::Feature::XERROR});

        if (!user.empty()) {
            connection.authenticate(user, password,
                                    connection.getSaslMechanisms());
        }

        if (!bucket.empty() && bucket != "/all/") {
            connection.selectBucket(bucket);
        }

        if (optind == argc) {
            for (int ii = 0; ii < 256; ++ii) {
                request_cmd_timings(connection,
                                    bucket,
                                    cb::mcbp::ClientOpcode(ii),
                                    verbose,
                                    true,
                                    json);
            }
        } else {
            for (; optind < argc; ++optind) {
                try {
                    const auto opcode = to_opcode(argv[optind]);
                    request_cmd_timings(
                            connection, bucket, opcode, verbose, false, json);
                } catch (const std::invalid_argument&) {
                    // Not a command timing, try as statistic timing.
                    request_stat_timings(
                            connection, argv[optind], verbose, json);
                }
            }
        }
    } catch (const ConnectionError& ex) {
        fmt::print(stderr, "{}\n", ex.what());
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        fmt::print(stderr, "{}\n", ex.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
