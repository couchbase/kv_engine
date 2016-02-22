/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/platform.h>

#include <getopt.h>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <vector>
#include <iostream>
#include <utilities/protocol2text.h>

#include "programs/utilities.h"

/**
 * Request all features from the server and dump the available ones
 * @param sock socket connected to the server
 * @param key the name of the stat to receive (NULL == ALL)
 */
static void request_hello(BIO *bio, const char *key)
{
    const std::string useragent("mchello v1.0");
    uint16_t nkey = uint16_t(useragent.length());
    uint16_t features[MEMCACHED_TOTAL_HELLO_FEATURES];
    uint32_t total = nkey + MEMCACHED_TOTAL_HELLO_FEATURES * 2;
    for (unsigned int ii = 0; ii < MEMCACHED_TOTAL_HELLO_FEATURES; ++ii) {
        features[ii] = htons(MEMCACHED_FIRST_HELLO_FEATURE + ii);
    }

    protocol_binary_request_hello req;
    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_HELLO;
    req.message.header.request.bodylen = htonl(total);
    req.message.header.request.keylen = htons(nkey);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;

    std::vector<uint8_t> request(sizeof(req.bytes) + total);
    memcpy(request.data(), req.bytes, sizeof(req.bytes));
    auto offset = sizeof(req.bytes);
    memcpy(request.data() + offset, useragent.data(), useragent.length());
    offset += useragent.length();

    memcpy(request.data() + offset, features, MEMCACHED_TOTAL_HELLO_FEATURES * 2);

    sendCommand(bio, request);
    std::vector<uint8_t> response;

    readResponse(bio, response);
    auto* rsp = reinterpret_cast<protocol_binary_response_hello*>(response.data());
    total = rsp->message.header.response.bodylen;

    if (rsp->message.header.response.status != 0) {
        auto status = rsp->message.header.response.status;
        auto code = (protocol_binary_response_status)status;
        std::cerr << "Hello failed: " << memcached_status_2_text(code)
                  << std::endl;
        return;
    }

    if (total > sizeof(features)) {
        total = sizeof(features);
    }

    memcpy(features, response.data() + sizeof(rsp->bytes), total);
    total /= 2;

    std::cout << "The server accepted the following features:" << std::endl;
    for (unsigned int ii = 0; ii < total; ++ii) {
        const char *text = protocol_feature_2_text(ntohs(features[ii]));
        std::cout << "\t- " << text << std::endl;
    }
}

int main(int argc, char** argv) {
    int cmd;
    const char *port = "11210";
    const char *host = "localhost";
    const char *user = NULL;
    const char *pass = NULL;
    int secure = 0;
    char *ptr;
    SSL_CTX* ctx;
    BIO* bio;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:u:P:s")) != EOF) {
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
        default:
            fprintf(stderr,
                    "Usage mchello [-h host[:port]] [-p port] [-u user] [-p pass] [-s]\n");
            return 1;
        }
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return 1;
    }

    request_hello(bio, NULL);

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return EXIT_SUCCESS;
}
