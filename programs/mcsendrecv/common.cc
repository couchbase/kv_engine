/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "common.h"
#include <iostream>
#include <vector>
#include <memcached/protocol_binary.h>
#include <utilities/protocol2text.h>
#include <programs/utilities.h>


static size_t try_read_bytes(BIO* bio, char* dest, size_t nbytes) {
    size_t total = 0;

    while (total < nbytes) {
        int nr = BIO_read(bio, dest + total, int(nbytes - total));
        if (nr <= 0) {
            if (BIO_should_retry(bio) == 0) {
                if (nr != 0) {
                    std::cerr << "Failed to read data" << std::endl;
                }
                return total;
            }
        } else {
            total += nr;
        }
    }

    return total;
}

static bool recvFrame(BIO* bio, std::vector<char>& frame) {
    protocol_binary_response_no_extras response;
    size_t nr = try_read_bytes(bio, reinterpret_cast<char*>(response.bytes),
                               sizeof(response.bytes));
    if (nr == 0) {
        frame.resize(0);
        return true;
    }

    uint32_t bodylen = ntohl(response.message.header.response.bodylen);
    frame.resize(sizeof(response.bytes) + bodylen);
    memcpy(frame.data(), response.bytes, sizeof(response.bytes));

    nr = try_read_bytes(bio, frame.data() + sizeof(response.bytes), bodylen);

    return nr == bodylen;
}

bool tap_cp(BIO* source, BIO* dest, bool verbose) {
    std::vector<char> frame;
    int counter = 0;
    while (recvFrame(source, frame)) {
        if (frame.empty()) {
            if (verbose) {
                fprintf(stderr, "\r");
                for (int ii = 0; ii < 2; ++ii) {
                    fprintf(stderr, "                                      ");
                }
                fprintf(stderr, "\r\n");
            }
            return true;
        }
        ++counter;
        if (verbose) {
            fprintf(stderr, "\r");
            for (int ii = 0; ii < 2; ++ii) {
                fprintf(stderr, "                                      ");
            }
            fprintf(stderr, "\r%08u", counter);
            fflush(stderr);
            uint8_t* ptr = reinterpret_cast<uint8_t*>(frame.data());
            if (*ptr == PROTOCOL_BINARY_REQ) {
                auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(ptr);
                if (req->message.header.request.opcode == PROTOCOL_BINARY_CMD_TAP_MUTATION) {
                    auto *m = reinterpret_cast<protocol_binary_request_tap_mutation*>(ptr);
                    uint16_t keylen = ntohs(req->message.header.request.keylen);
                    uint16_t eng = ntohs(m->message.body.tap.enginespecific_length);
                    std::string key(frame.data() + 24 + req->message.header.request.extlen + eng, keylen);
                    if (key.length() > 60) {
                        key.resize(60);
                    }
                    fprintf(stderr, " - %s", key.c_str());
                    fflush(stderr);
                }
            } else if (*ptr == PROTOCOL_BINARY_RES) {
                auto* res = reinterpret_cast<protocol_binary_response_no_extras*>(ptr);
                if (res->message.header.response.status != 0) {
                    auto st = ntohs(res->message.header.response.status);
                    fprintf(stderr, "Reveived: %d %s\n",st,
                            memcached_status_2_text((protocol_binary_response_status)st));
                }
            } else {
                fprintf(stderr, "Invalid magic received.. exiting\n");
                return false;
            }

        }

        ensure_send(dest, frame.data(), frame.size());
    }
    return true;
}
