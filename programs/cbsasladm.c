/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <platform/platform.h>

#include <getopt.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "utilities.h"

/**
 * Refresh the cbsasl password database
 * @param sock socket connected to the server
 */
static void refresh(BIO *bio)
{
    protocol_binary_response_no_extras response;
    protocol_binary_request_no_extras request;

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_ISASL_REFRESH;

    ensure_send(bio, &request, sizeof(request));

    ensure_recv(bio, &response, sizeof(response.bytes));
    if (response.message.header.response.status != 0) {
        uint16_t err = ntohs(response.message.header.response.status);
        fprintf(stderr, "Failed to refresh cbsasl passwd db: %d\n",
                err);
    }
}

/**
 * Program entry point.
 *
 * @param argc argument count
 * @param argv argument vector
 * @return 0 if success, error code otherwise
 */
int main(int argc, char **argv)
{
    int cmd;
    const char *port = "11210";
    const char *host = "localhost";
    const char *user = NULL;
    const char *pass = NULL;
    int secure = 0;
    char *ptr;
    SSL_CTX* ctx;
    BIO* bio;
    int ii;
    int ret = 0;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:su:P:")) != EOF) {
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
        case 's':
            secure = 1;
            break;
        case 'u' :
            user = optarg;
            break;
        case 'P':
            pass = optarg;
            break;
        default:
            fprintf(stderr,
                    "Usage: cbsasladm [-h host[:port]] [-p port] [-s] "
                    "[-u user] [-P password] [cmd]*\n"
                    "   The following command(s) exists:\n"
                    "\trefresh - tell memcached to reload its internal "
                    "cache\n");
            return 1;
        }
    }

    if (optind == argc) {
        fprintf(stderr, "You need to supply a command\n");
        return EXIT_FAILURE;
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return 1;
    }

    for (ii = optind; ii < argc; ++ii) {
        if (strcmp(argv[ii], "refresh") == 0) {
            refresh(bio);
        } else {
            fprintf(stderr, "Unknown command %s\n", argv[ii]);
            ret = 1;
        }
    }

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return ret;
}
