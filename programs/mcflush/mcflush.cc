/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/platform.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <chrono>
#include <iostream>
#include <string>

#include "programs/utilities.h"


static void dumpbody(BIO *bio, uint32_t nbytes, bool swallow)
{
    if (nbytes > 0) {
        char *buffer = new char[nbytes];
        ensure_recv(bio, buffer, nbytes);
        if (swallow) {
            return;
        }
        std::string msg(buffer, nbytes);
        std::cerr <<  "Server response: [" << msg << "]" << std::endl;
        delete []buffer;
    }
}

static void execute_command(BIO *bio, uint8_t opcode)
{
    protocol_binary_request_no_extras request;
    protocol_binary_response_no_extras response;

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = opcode;

    ensure_send(bio, &request, sizeof(request));
    ensure_recv(bio, &response, sizeof(response.bytes));

    if (response.message.header.response.status != 0) {
        uint16_t err = ntohs(response.message.header.response.status);
        fprintf(stderr, "0x%02x failed with %u\n", opcode, err);
        dumpbody(bio, ntohl(response.message.header.response.bodylen), false);
        exit(EXIT_FAILURE);
    }

    dumpbody(bio, ntohl(response.message.header.response.bodylen), true);
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
    bool tcp_nodelay = false;
    int count = 1;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "Th:p:u:P:sn:")) != EOF) {
        switch (cmd) {
        case 'T' :
            tcp_nodelay = true;
            break;
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
        case 'n':
            count = atoi(optarg);
            break;
        default:
            fprintf(stderr,
                    "Usage mcflush [-h host[:port]] [-p port] [-u user] [-P pass] [-s] [-T]\n");
            return 1;
        }
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return 1;
    }

    if (tcp_nodelay && !enable_tcp_nodelay(bio)) {
        return 1;
    }

    using namespace std::chrono;

    auto start = high_resolution_clock::now();
    for (int ii = 0; ii < count; ++ii) {
        execute_command(bio, PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC);
        execute_command(bio, PROTOCOL_BINARY_CMD_FLUSH);
        execute_command(bio, PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC);
    }
    auto stop = high_resolution_clock::now();

    auto ms = duration_cast<milliseconds>(stop - start).count();
    std::cout << "Average flush time: " << ms/count << "ms" << std::endl;

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return EXIT_SUCCESS;
}
