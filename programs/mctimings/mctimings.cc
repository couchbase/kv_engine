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

#include "config.h"
#include "programs/hostname_utils.h"

#include <array>
#include <cJSON.h>
#include <cstdlib>
#include <getopt.h>
#include <iostream>
#include <memcached/protocol_binary.h>
#include <protocol/connection/client_mcbp_connection.h>
#include <stdexcept>

static uint32_t getValue(cJSON *root, const char *key) {
    cJSON *obj = cJSON_GetObjectItem(root, key);
    if (obj == nullptr) {
        std::string msg = "Fatal error: missing key \"";
        msg += key;
        msg += "\"";
        throw std::runtime_error(msg);
    }
    return uint32_t(obj->valueint);
}

static cJSON *getArray(cJSON *root, const char *key) {
    cJSON *obj = cJSON_GetObjectItem(root, key);
    if (obj == nullptr) {
        std::string msg = "Fatal error: missing key \"";
        msg += key;
        msg += "\"";
        throw std::runtime_error(msg);
    }
    if (obj->type != cJSON_Array) {
        std::string msg = "Fatal error: key \"";
        msg += key;
        msg += "\" is not an array";
        throw std::runtime_error(msg);
    }
    return obj;
}

// A single bin of a histogram. Holds the raw count and cumulative total (to
// allow percentile to be calculated).
struct Bin {
    uint32_t count;
    uint64_t cumulative_count;
};

class Timings {
public:
    Timings(cJSON* json) : max(0), ns(Bin()), oldwayout(false) {
        us.fill(Bin());
        ms.fill(Bin());
        halfsec.fill(Bin());
        wayout.fill(Bin());
        initialize(json);
    }

    uint64_t getTotal() const {
        return total;
    }

    void dumpHistogram(const std::string &opcode)
    {
        std::cout << "The following data is collected for \""
                  << opcode << "\"" << std::endl;

        int ii;

        dump("ns", 0, 999, ns);
        for (ii = 0; ii < 100; ++ii) {
            dump("us", ii * 10, ((ii + 1) * 10 - 1), us[ii]);
        }

        for (ii = 1; ii < 50; ++ii) {
            dump("ms", ii, ii, ms[ii]);
        }

        dump("ms", 50, 499, halfsec[0]);
        for (ii = 1; ii < 10; ++ii) {
            dump("ms", ii * 500, ((ii + 1) * 500) - 1, halfsec[ii]);
        }

        if (oldwayout) {
            dump("ms", (10 * 500), 0, wayout[0]);
        } else {
            dump("s ", 5, 9, wayout[0]);
            dump("s ", 10, 19, wayout[1]);
            dump("s ", 20, 39, wayout[2]);
            dump("s ", 40, 79, wayout[3]);
            dump("s ", 80, 0, wayout[4]);
        }
        std::cout << "Total: " << total << " operations" << std::endl;
    }

private:

    // Helper function for initialize
    static void update_max_and_total(uint32_t& max, uint64_t& total, Bin& bin) {
        total += bin.count;
        bin.cumulative_count = total;
        if (bin.count > max) {
            max = bin.count;
        }
    }

    void initialize(cJSON *root) {
        auto *obj = cJSON_GetObjectItem(root, "error");
        if (obj != nullptr) {
            // The server responded with an error.. send that to the user
            std::string message(obj->valuestring);
            throw std::runtime_error(message);
        }

        ns.count = getValue(root, "ns");
        auto arr = getArray(root, "us");
        int ii = 0;
        cJSON* i = arr->child;
        while (i) {
            us[ii].count = i->valueint;
            ++ii;
            i = i->next;
            if (ii == 100 && i != NULL) {
                throw std::runtime_error(
                    "Internal error.. too many \"us\" samples");
            }
        }

        arr = getArray(root, "ms");
        ii = 1;
        i = arr->child;
        while (i) {
            ms[ii].count = i->valueint;
            ++ii;
            i = i->next;
            if (ii == 50 && i != NULL) {
                throw std::runtime_error(
                    "Internal error.. too many \"ms\" samples");
            }
        }

        arr = getArray(root, "500ms");
        ii = 0;
        i = arr->child;
        while (i) {
            halfsec[ii].count = i->valueint;
            ++ii;
            i = i->next;
            if (ii == 10 && i != NULL) {
                throw std::runtime_error(
                    "Internal error.. too many \"halfsec\" samples\"");
            }
        }

        try {
            wayout[0].count = getValue(root, "5s-9s");
            wayout[1].count = getValue(root, "10s-19s");
            wayout[2].count = getValue(root, "20s-39s");
            wayout[3].count = getValue(root, "40s-79s");
            wayout[4].count = getValue(root, "80s-inf");
        } catch (...) {
            wayout[0].count = getValue(root, "wayout");
            oldwayout = true;
        }

        // Calculate total and cumulative counts, and find the highest value.
        max = total = 0;

        update_max_and_total(max, total, ns);
        for (auto &val : us) {
            update_max_and_total(max, total, val);
        }
        for (auto &val : ms) {
            update_max_and_total(max, total, val);
        }
        for (auto &val : halfsec) {
            update_max_and_total(max, total, val);
        }
        for (auto &val : wayout) {
            update_max_and_total(max, total, val);
        }
    }

    void dump(const char *timeunit, uint32_t low, uint32_t high,
              const Bin& value)
    {
        if (value.count > 0) {
            char buffer[1024];
            int offset;
            if (low > 0 && high == 0) {
                offset = sprintf(buffer, "[%4u - inf.]%s", low, timeunit);
            } else {
                offset = sprintf(buffer, "[%4u - %4u]%s", low, high, timeunit);
            }
            offset += sprintf(buffer + offset, " (%6.2f%%) ",
                              double(value.cumulative_count) * 100.0 / total);

            // Determine how wide the max value would be, and pad all counts
            // to that width.
            int max_width = snprintf(buffer, 0, "%u", max);
            offset += sprintf(buffer + offset, " %*u", max_width, value.count);

            int num = (int)(44.0 * (float) value.count / (float)max);
            offset += sprintf(buffer + offset, " | ");
            for (int ii = 0; ii < num; ++ii) {
                offset += sprintf(buffer + offset, "#");
            }

            std::cout << buffer << std::endl;
        }
    }

    /**
     * The highest value of all the samples (used to figure out the width
     * used for each sample in the printout)
     */
    uint32_t max;

    /* We collect timings for <=1 us */
    Bin ns;

    /* We collect timings per 10usec */
    std::array<Bin, 100> us;

    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    std::array<Bin, 50> ms;

    std::array<Bin, 10> halfsec;

    // [5-9], [10-19], [20-39], [40-79], [80-inf].
    std::array<Bin, 5> wayout;

    bool oldwayout;

    uint64_t total;
};

std::string opcode2string(uint8_t opcode) {
    char opcode_buffer[8];
    const char *cmd = memcached_opcode_2_text(opcode);
    if (cmd == NULL) {
        snprintf(opcode_buffer, sizeof(opcode_buffer), "0x%02x", opcode);
        cmd = opcode_buffer;
    }

    return std::string(cmd);
}

static void request_cmd_timings(MemcachedBinprotConnection& connection,
                                const std::string bucket,
                                uint8_t opcode,
                                bool verbose,
                                bool skip) {
    BinprotGetCmdTimerCommand cmd;
    cmd.setBucket(bucket);
    cmd.setOpcode(opcode);

    connection.sendCommand(cmd);

    BinprotGetCmdTimerResponse resp;
    connection.recvResponse(resp);

    if (!resp.isSuccess()) {
        switch (resp.getStatus()) {
        case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
            std::cerr << "Cannot find bucket: " << bucket << std::endl;
            break;
        case PROTOCOL_BINARY_RESPONSE_EACCESS:
            std::cerr << "Not authorized to access timings data" << std::endl;
            break;
        default:
            std::cerr << "Command failed: "
                      << memcached_status_2_text(resp.getStatus())
                      << std::endl;
        }
        exit(EXIT_FAILURE);
    }

    try {
        Timings timings(resp.getTimings());

        auto cmd = opcode2string(opcode);

        if (timings.getTotal() == 0) {
            if (skip == 0) {
                std::cout << "The server don't have information about \""
                          << cmd << "\"" << std::endl;
            }
        } else {
            if (verbose) {
                timings.dumpHistogram(cmd);
            } else {
                std::cout << cmd << " " << timings.getTotal() << " operations"
                          << std::endl;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }
}

static void request_stat_timings(MemcachedBinprotConnection& connection,
                                 const std::string& key,
                                 bool verbose) {

    std::map<std::string, std::string> map;
    try {
        map = connection.statsMap(key);
    } catch (const BinprotConnectionError& ex) {
        if (ex.isNotFound()) {
            std::cerr <<"Cannot find statistic: " << key << std::endl;
        } else if (ex.isAccessDenied()) {
            std::cerr << "Not authorized to access timings data" << std::endl;
        } else {
            std::cerr << "Fatal error: " << ex.what() << std::endl;
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
        std::cerr << "Failed to fetch statistics for \"" << key << "\""
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    // And the value for the item should be valid JSON
    unique_cJSON_ptr json(cJSON_Parse(iter->second.c_str()));
    if (!json) {
        std::cerr << "Failed to fetch statistics for \"" << key << "\". Not json"
                  << std::endl;
        exit(EXIT_FAILURE);
    }
    try {
        Timings timings(json.get());
        if (verbose) {
            timings.dumpHistogram(key);
        } else {
            std::cout << key << " " << timings.getTotal() << " operations"
                      << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }
}

void usage() {
    std::cerr << "Usage mctimings [-h host[:port]] [-p port] [-u user]"
              << " [-P pass] [-b bucket] [-s] -v [opcode / stat_name]*" << std::endl
              << std::endl
              << "Example:" << std::endl
              << "    mctimings -h localhost:11210 -v GET SET";
}

int main(int argc, char** argv) {
    int cmd;
    std::string port{"11210"};
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    std::string bucket{};
    sa_family_t family = AF_UNSPEC;
    bool verbose = false;
    bool secure = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "46h:p:u:b:P:sv")) != EOF) {
        switch (cmd) {
        case '6' :
            family = AF_INET6;
            break;
        case '4' :
            family = AF_INET;
            break;
        case 'h' :
            host.assign(optarg);
            break;
        case 'p':
            port.assign(optarg);
            break;
        case 'b' :
            bucket.assign(optarg);
            break;
        case 'u' :
            user.assign(optarg);
            break;
        case 'P':
            password.assign(optarg);
            break;
        case 's':
            secure = true;
            break;
        case 'v' :
            verbose = true;
            break;
        default:
            usage();
            return EXIT_FAILURE;
        }
    }

    try {
        in_port_t in_port;
        sa_family_t fam;
        std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

        if (family == AF_UNSPEC) { // The user may have used -4 or -6
            family = fam;
        }
        MemcachedBinprotConnection connection(host,
                                              in_port,
                                              family,
                                              secure);

        if (!user.empty()) {
            connection.authenticate(user, password,
                                    connection.getSaslMechanisms());
        }

        // MEMCACHED_VERSION contains the git sha
        connection.hello("mctimings",
                         MEMCACHED_VERSION,
                         "command line utitilty to fetch command timings");
        connection.setXerrorSupport(true);

        if (!bucket.empty()) {
            connection.selectBucket(bucket);
        }

        if (optind == argc) {
            for (int ii = 0; ii < 256; ++ii) {
                request_cmd_timings(connection, bucket, (uint8_t)ii, verbose,
                                    true);
            }
        } else {
            for (; optind < argc; ++optind) {
                const uint8_t opcode = memcached_text_2_opcode(argv[optind]);
                if (opcode != PROTOCOL_BINARY_CMD_INVALID) {
                    request_cmd_timings(connection, bucket, opcode, verbose,
                                        false);
                } else {
                    // Not a command timing, try as statistic timing.
                    request_stat_timings(connection, argv[optind], verbose);
                }
            }
        }
    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
