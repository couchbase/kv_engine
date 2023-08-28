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
#include <utilities/timing_histogram_printer.h>
#include <array>
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
        if (json.find("error") != json.end()) {
            // The server responded with an error... send that to the user
            throw std::runtime_error(json["error"].get<std::string>());
        }
        if (json.find("data") != json.end()) {
            printer = std::make_unique<TimingHistogramPrinter>(json);
        }
    }

    uint64_t getTotal() const {
        if (printer) {
            return printer->getTotal();
        }
        return 0;
    }

    void dumpHistogram(const std::string& opcode) {
        if (!printer) {
            return;
        }

        printer->dumpHistogram(opcode);
    }

private:
    std::unique_ptr<TimingHistogramPrinter> printer;
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
    bool found = false;
    try {
        connection.stats(
                [&json_output, &key, verbose, &found](const auto&,
                                                      const auto& value) {
                    if (value.find(R"("data":[)") != std::string::npos &&
                        value.find(R"("bucketsLow":)") != std::string::npos) {
                        try {
                            auto json = nlohmann::json::parse(value);
                            if (json_output.has_value()) {
                                json["command"] = key;
                                json_output->push_back(json);
                            } else {
                                Timings timings(json);
                                if (verbose) {
                                    timings.dumpHistogram(key);
                                } else {
                                    fmt::print(stdout,
                                               " {} {} operations\n",
                                               key,
                                               timings.getTotal());
                                }
                            }
                            found = true;
                        } catch (const std::exception& ex) {
                            fmt::print(stderr, "Fatal error: {}\n", ex.what());
                            std::exit(EXIT_FAILURE);
                        }
                    }
                },
                key);
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

    if (!found) {
        fmt::print(stderr, "Failed to fetch statistics for \"{}\"\n", key);
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
