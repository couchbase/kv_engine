/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#ifdef WIN32
#define NOMINMAX
#endif

#include "config.h"

#include <array>
#include <string>
#include <vector>
#include <iostream>
#include <cstdlib>

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/platform.h>

#include <getopt.h>
#include <cJSON.h>

#include "programs/utilities.h"
#include "utilities/protocol2text.h"

static uint32_t getValue(cJSON *root, const char *key) {
    cJSON *obj = cJSON_GetObjectItem(root, key);
    if (obj == nullptr) {
        std::string msg = "Fatal error: missing key \"";
        msg += key;
        msg += "\"";
        throw msg;
    }
    return uint32_t(obj->valueint);
}

static cJSON *getArray(cJSON *root, const char *key) {
    cJSON *obj = cJSON_GetObjectItem(root, key);
    if (obj == nullptr) {
        std::string msg = "Fatal error: missing key \"";
        msg += key;
        msg += "\"";
        throw msg;
    }
    if (obj->type != cJSON_Array) {
        std::string msg = "Fatal error: key \"";
        msg += key;
        msg += "\" is not an array";
        throw msg;
    }
    return obj;
}

class Timings {
public:
    Timings() : max(0), ns(0), oldwayout(false) {
        us.fill(0);
        ms.fill(0);
        halfsec.fill(0);
        wayout.fill(0);
    }

    void initialize(std::vector<char> &content) {
        auto *json = cJSON_Parse(content.data());
        if (json == nullptr) {
            std::string msg("Failed to decode json: \"");
            msg.append(content.data());
            msg.append("\"");
            throw msg;
        }

        auto *obj = cJSON_GetObjectItem(json, "error");
        if (obj) {
            std::string message(obj->valuestring);
            cJSON_Delete(json);
            throw message;
        }

        std::string msg;
        try {
            initialize(json);
        } catch (std::string &ex) {
            msg.assign(ex);
        }
        cJSON_Delete(json);
        if (!msg.empty()) {
            throw msg;
        }
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
            dump("s", 5, 9, wayout[0]);
            dump("s", 10, 19, wayout[1]);
            dump("s", 20, 39, wayout[2]);
            dump("s", 40, 79, wayout[3]);
            dump("s", 80, 0, wayout[4]);
        }
        std::cout << "Total: " << total << " operations" << std::endl;
    }

private:
    void initialize(cJSON *root) {
        ns = getValue(root, "ns");
        auto arr = getArray(root, "us");
        int ii = 0;
        cJSON* i = arr->child;
        while (i) {
            us[ii] = i->valueint;
            ++ii;
            i = i->next;
            if (ii == 100 && i != NULL) {
                throw std::string("Internal error.. too many \"us\" samples");
            }
        }

        arr = getArray(root, "ms");
        ii = 1;
        i = arr->child;
        while (i) {
            ms[ii] = i->valueint;
            ++ii;
            i = i->next;
            if (ii == 50 && i != NULL) {
                throw std::string("Internal error.. too many \"ms\" samples");
            }
        }

        arr = getArray(root, "500ms");
        ii = 0;
        i = arr->child;
        while (i) {
            halfsec[ii] = i->valueint;
            ++ii;
            i = i->next;
            if (ii == 10 && i != NULL) {
                throw std::string("Internal error.. too many \"halfsec\" samples\"");
            }
        }

        try {
            wayout[0] = getValue(root, "5s-9s");
            wayout[1] = getValue(root, "10s-19s");
            wayout[2] = getValue(root, "20s-39s");
            wayout[3] = getValue(root, "40s-79s");
            wayout[4] = getValue(root, "80s-inf");
        } catch (...) {
            wayout[0] = getValue(root, "wayout");
            oldwayout = true;
        }

        // Calculate total and find the highest value
        max = total = ns;
        for (auto &val : us) {
            total += val;
            if (val > max) {
                max = val;
            }
        }
        for (auto &val : ms) {
            total += val;
            if (val > max) {
                max = val;
            }
        }
        for (auto &val : halfsec) {
            total += val;
            if (val > max) {
                max = val;
            }
        }
        for (auto &val : wayout) {
            total += val;
            if (val > max) {
                max = val;
            }
        }
    }

    void dump(const char *timeunit, uint32_t low, uint32_t high, uint32_t value)
    {
        if (value > 0) {
            int ii;
            char buffer[1024];
            int offset;
            int num;
            if (low > 0 && high == 0) {
                offset = sprintf(buffer, "[%4u - inf.]%s", low, timeunit);
            } else {
                offset = sprintf(buffer, "[%4u - %4u]%s", low, high, timeunit);
            }
            num = (int)(40.0 * (float) value / (float)max);
            offset += sprintf(buffer + offset, " |");
            for (ii = 0; ii < num; ++ii) {
                offset += sprintf(buffer + offset, "#");
            }

            sprintf(buffer + offset, " - %u", value);
            std::cout << buffer << std::endl;
        }
    }

    /**
     * The highest value of all the samples (used to figure out the width
     * used for each sample in the printout)
     */
    uint32_t max;

    /* We collect timings for <=1 us */
    uint32_t ns;

    /* We collect timings per 10usec */
    std::array<uint32_t, 100> us;

    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    std::array<uint32_t, 50> ms;

    std::array<uint32_t, 10> halfsec;

    // [5-9], [10-19], [20-39], [40-79], [80-inf].
    std::array<uint32_t, 5> wayout;

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

static void request_timings(BIO *bio, uint8_t opcode, int verbose, int skip)
{
    protocol_binary_request_get_cmd_timer request;
    protocol_binary_response_no_extras response;

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET_CMD_TIMER;
    request.message.header.request.extlen = 1;
    request.message.header.request.bodylen = htonl(1);
    request.message.body.opcode = opcode;

    ensure_send(bio, &request, sizeof(request.bytes));
    ensure_recv(bio, &response, sizeof(response.bytes));
    uint32_t buffsize = ntohl(response.message.header.response.bodylen);
    std::vector<char> buffer(buffsize + 1, 0);

    ensure_recv(bio, buffer.data(), buffsize);
    if (response.message.header.response.status != 0) {
        switch (ntohs(response.message.header.response.status)) {
        case PROTOCOL_BINARY_RESPONSE_EACCESS:
            std::cerr << "Not authorized to access timings data" << std::endl;
            break;
        default:
            std::cerr << "Command failed: 0x" << std::hex
                      << ntohs(response.message.header.response.status)
                      << std::endl;
        }
        exit(EXIT_FAILURE);
    }

    Timings timings;
    try {
        timings.initialize(buffer);
    } catch (std::string &msg) {
        std::cerr << "Fatal error: " << msg << std::endl;
        exit(EXIT_FAILURE);
    }

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
}

int main(int argc, char** argv) {
    int cmd;
    const char *port = "11210";
    const char *host = "localhost";
    const char *user = NULL;
    const char *pass = NULL;
    int verbose = 0;
    int secure = 0;
    char *ptr;
    SSL_CTX* ctx;
    BIO* bio;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:u:P:sv")) != EOF) {
        switch (cmd) {
        case 'h' :
            host = optarg;
            ptr = strchr(optarg, ':');
            if (ptr != NULL) {
                *ptr = '\0';
                port = ptr + 1;
            }
            break;
        case 'p':
            port = optarg;
            break;
        case 'u' :
            user = optarg;
            break;
        case 'P':
            pass = optarg;
            break;
        case 's':
            secure = 1;
            break;
        case 'v':
            verbose = 1;
            break;
        default:
            std::cerr << "Usage mctimings [-h host[:port]] [-p port] [-u user]"
                      << " [-P pass] [-s] -v [opcode]*\n";
            exit(EXIT_FAILURE);
        }
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        exit(EXIT_FAILURE);
    }

    if (optind == argc) {
        for (int ii = 0; ii < 256; ++ii) {
            request_timings(bio, (uint8_t)ii, verbose, 1);
        }
    } else {
        for (; optind < argc; ++optind) {
            request_timings(bio, memcached_text_2_opcode(argv[optind]),
                            verbose, 0);
        }
    }

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return EXIT_SUCCESS;
}
