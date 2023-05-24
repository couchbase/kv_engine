/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <fmt/core.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/string_hex.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/json_utilities.h>
#include <array>
#include <cinttypes>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

#define JSON_DUMP_NO_INDENT -1
#define JSON_DUMP_INDENT_SIZE 4

const static std::string_view histogramInfo = R"(Histogram Legend:
[1. - 2.]3. (4.)    5.|
    1. All values in this bucket were recorded for a higher value than this.
    2. The maximum value inclusive that could have been recorded in this bucket.
    3. The unit for the values of that (1.) and (2.) are in microseconds, milliseconds or seconds.
    4. Percentile of recorded values to the histogram that has values <= the value at (2.).
    5. The number of recorded values that were in the range (1.) to (2.) inclusive.

)";

class Timings {
public:
    explicit Timings(const nlohmann::json& json) {
        initialize(json);
    }

    uint64_t getTotal() const {
        return total + overflowed;
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
        maxCount = std::max(maxCount, overflowed);

        // If no buckets have no recorded values do not try to render buckets
        if (maxCount > 0) {
            // create double versions of sec, ms, us so we can print them to 2dp
            using namespace std::chrono;
            using doubleMicroseconds = duration<long double, std::micro>;
            using doubleMilliseconds = duration<long double, std::milli>;
            using doubleSeconds = duration<long double>;

            // loop though all the buckets in the json object and print them
            // to std out
            uint64_t lastBuckLow = bucketsLow;
            for (auto bucket : dataArray) {
                // Get the current bucket's highest value it would track counts
                // for
                auto buckHigh = bucket[0].get<int64_t>();
                // Get the counts for this bucket
                auto count = bucket[1].get<int64_t>();
                // Get the percentile of counts that are <= buckHigh
                auto percentile = bucket[2].get<double>();

                // Cast the high bucket width to us, ms and seconds so we
                // can check which units we should be using for this bucket
                auto buckHighUs = doubleMicroseconds(buckHigh);
                auto buckHighMs = duration_cast<doubleMilliseconds>(buckHighUs);
                auto buckHighS = duration_cast<doubleSeconds>(buckHighUs);

                if (buckHighS.count() > 1) {
                    auto low = duration_cast<doubleSeconds>(
                            microseconds(lastBuckLow));
                    dump("s",
                         low.count(),
                         buckHighS.count(),
                         count,
                         percentile);
                } else if (buckHighMs.count() > 1) {
                    auto low = duration_cast<doubleMilliseconds>(
                            doubleMicroseconds(lastBuckLow));
                    dump("ms",
                         low.count(),
                         buckHighMs.count(),
                         count,
                         percentile);
                } else {
                    dump("us", lastBuckLow, buckHigh, count, percentile);
                }

                // Set the low bucket value to this buckets high width value.
                lastBuckLow = buckHigh;
            }

            // Emit a pseudo-bucket for any overflowed samples which could not
            // be represented, if present.
            if (overflowed) {
                const auto barWidth = barChartWidth(overflowed);
                const auto countWidth = countFieldWidth();
                const doubleSeconds maxTrackableS =
                        doubleMicroseconds(maxTrackableValue);
                fmt::print("[{:6.2f} - {:6.2f}]s (overflowed)\t{}| {}\n",
                           maxTrackableS.count(),
                           std::numeric_limits<double>::infinity(),
                           fmt::format("{0:>{1}}", overflowed, countWidth),
                           std::string(barWidth, '#'));
            }
        }

        fmt::print(stdout, "Total: {} operations\n", getTotal());
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

            // "overflowed" and "maxTrackableValue" only added in 7.2.0; ignore
            // if not present
            overflowed =
                    cb::getOptionalJsonObject(root, "overflowed").value_or(0);
            maxTrackableValue = cb::getOptionalJsonObject(root, "max_trackable")
                                        .value_or(0);
        }
    }

    void dump(const char* timeunit,
              long double low,
              long double high,
              int64_t count,
              double percentile) {
        int num = barChartWidth(count);
        int numberOfSpaces = countFieldWidth();

        fmt::print(stdout,
                   "[{:6.2f} - {:6.2f}]{} ({:6.4f}%)\t{}| {}\n",
                   low,
                   high,
                   timeunit,
                   percentile,
                   fmt::format("{0:>{1}}", count, numberOfSpaces),
                   std::string(num, '#'));
    }

    // Calculation for padding around the count in each histogram bucket
    int countFieldWidth() const {
        return fmt::formatted_size("{}", maxCount) + 1;
    }

    // Calculation for histogram size rendering - how wide should the
    // ASCII bar be for the count of samples.
    int barChartWidth(int64_t count) const {
        double factionOfHashes =
                maxCount > 0 ? (count / static_cast<double>(maxCount)) : 0.0;
        int num = static_cast<int>(44.0 * factionOfHashes);
        return num;
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
     * Total number of counts recorded in the histogram buckets.
     */
    uint64_t total = 0;

    /**
     * Number of samples which overflowed the histograms' buckets.
     * (Added in 7.2.0).
     */
    uint64_t overflowed = 0;

    /**
     * Maximum value the histogram can track. Any values which are greater
     * than this are counted in `overflowed`.
     * (Added in 7.2.0).
     */
    uint64_t maxTrackableValue = 0;
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
                                std::optional<nlohmann::json>& jsonOutput) {
    connection.sendCommand(BinprotGetCmdTimerCommand{bucket, opcode});
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
        if (jsonOutput.has_value()) {
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
                jsonOutput->push_back(timings);
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
                                 std::optional<nlohmann::json>& json_output) {
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
        if (json_output.has_value()) {
            json["command"] = key;
            json_output->push_back(json);
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

static void printBucketHeader(const std::string& bucket) {
    fmt::print(stdout, "{}\n", std::string(78, '*'));
    fmt::print(stdout, "Bucket:'{}'\n\n", bucket);
}

void dumpHistogramFromFile(const std::string& file) {
    auto fileAsString = cb::io::loadFile(file);
    try {
        auto jsonData = nlohmann::json::parse(fileAsString);
        auto runDumpHisto = [](const nlohmann::json& obj) {
            try {
                Timings(obj).dumpHistogram(obj["command"]);
            } catch (std::exception& e) {
                fmt::print(stderr,
                           "Could not visualise object:{}, exception:{}\n",
                           obj.dump(),
                           e.what());
            }
        };

        if (jsonData.is_array()) {
            for (const auto& obj : jsonData) {
                if (obj.is_object()) {
                    if (obj.find("memcachedBucket") != obj.end()) {
                        printBucketHeader(obj["memcachedBucket"]);
                        for (const auto& histoObj :
                             obj["memcachedBucketData"]) {
                            runDumpHisto(histoObj);
                        }
                    } else {
                        runDumpHisto(obj);
                    }
                }
            }
        } else if (jsonData.is_object()) {
            runDumpHisto(jsonData);
        } else {
            throw std::runtime_error("Json is not valid!");
        }
    } catch (std::exception& e) {
        throw std::runtime_error(
                fmt::format("Could not parse json in file:{} exception:{}",
                            file,
                            e.what()));
    }
}

static void usage(McProgramGetopt& getopt, int exitcode) {
    std::cerr << R"(Usage mctimings [options] [opcode / statname]

Options:

)" << getopt << R"(

Example:
    mctimings --user operator --bucket /all/ --password - --verbose GET SET

)" << std::endl;
    std::exit(exitcode);
}

int main(int argc, char** argv) {
    std::vector<std::string> buckets{{"/all/"}};
    std::string file;
    bool verbose = false;
    bool json = false;

    McProgramGetopt getopt;
    using cb::getopt::Argument;
    getopt.addOption({[&verbose](auto) { verbose = true; },
                      'v',
                      "verbose",
                      "Use verbose output"});

    getopt.addOption({[&json, &verbose](auto value) {
                          json = true;
                          if (value == "pretty") {
                              verbose = true;
                          }
                      },
                      'j',
                      "json",
                      Argument::Optional,
                      "pretty",
                      "Print JSON instead of histograms"});

    getopt.addOption({[&buckets, &getopt](auto value) {
                          if (buckets.empty()) {
                              usage(getopt, EXIT_FAILURE);
                          }
                          buckets.front().assign(std::string{value});
                      },
                      'b',
                      "bucket",
                      Argument::Required,
                      "bucketname",
                      "The name of the bucket to operate on (specify \"@no "
                      "bucket@\" to get aggregated stats from all buckets. The "
                      "user must have the Stats privilege to do so)"});

    getopt.addOption({[&file](auto value) { file = std::string{value}; },
                      'f',
                      "file",
                      Argument::Required,
                      "path.json",
                      "Dump Histogram data from a json file produced from "
                      "mctimings using the --json arg"});

    getopt.addOption(
            {[&buckets, &getopt](auto value) {
                 if (!buckets.empty() && buckets.front() != "/all/") {
                     usage(getopt, EXIT_FAILURE);
                 }
             },
             'a',
             "all-buckets",
             "Get list of buckets from the node and display stats per bucket "
             "basis rather than aggregated e.g. --bucket /all/. Also -a and -b "
             "may not be used at the same time."});

    getopt.addOption({[&getopt](auto) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    auto extraArgs = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });
    if (!file.empty()) {
        try {
            fmt::print(stdout, histogramInfo);
            dumpHistogramFromFile(file);
        } catch (const std::exception& ex) {
            fmt::print(stderr, "{}\n", ex.what());
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }

    try {
        getopt.assemble();
        auto connection = getopt.getConnection();
        // MEMCACHED_VERSION contains the git sha
        connection->setAgentName("mctimings " MEMCACHED_VERSION);
        connection->setFeatures({cb::mcbp::Feature::XERROR});

        if (verbose && !json) {
            fmt::print(stdout, histogramInfo);
        }

        if (buckets.empty()) {
            buckets = connection->listBuckets();
        }

        std::optional<nlohmann::json> jsonOutput;
        if (json) {
            jsonOutput = nlohmann::json::array();
        }
        for (const auto& bucket : buckets) {
            if (bucket != "/all/") {
                connection->selectBucket(bucket);
            }

            if (!json) {
                printBucketHeader(bucket);
            }

            std::optional<nlohmann::json> jsonDataFromBucket;
            if (json) {
                jsonDataFromBucket = nlohmann::json::array();
            }
            if (extraArgs.empty()) {
                for (int i = 0; i < 256; ++i) {
                    request_cmd_timings(*connection,
                                        bucket,
                                        cb::mcbp::ClientOpcode(i),
                                        verbose,
                                        true,
                                        jsonDataFromBucket);
                }
            } else {
                for (const auto& arg : extraArgs) {
                    try {
                        request_cmd_timings(*connection,
                                            bucket,
                                            to_opcode(arg),
                                            verbose,
                                            false,
                                            jsonDataFromBucket);
                    } catch (const std::invalid_argument&) {
                        // Not a command timing, try as statistic timing.
                        request_stat_timings(*connection,
                                             std::string{arg},
                                             verbose,
                                             jsonDataFromBucket);
                    }
                }
            }
            if (jsonDataFromBucket) {
                jsonOutput->push_back(
                        {{"memcachedBucket", bucket},
                         {"memcachedBucketData", *jsonDataFromBucket}});
            }
        }
        if (jsonOutput.has_value()) {
            fmt::print(stdout,
                       "{}\n",
                       jsonOutput->dump(verbose ? JSON_DUMP_INDENT_SIZE
                                                : JSON_DUMP_NO_INDENT));
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
