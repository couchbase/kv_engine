/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <memcached/util.h>
#include <platform/platform.h>
#include <memcached/util.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <utilities/protocol2text.h>


#include "programs/utilities.h"

/**
 * Print the key value pair
 * @param key key to print
 * @param keylen length of key to print
 * @param val value to print
 * @param vallen length of value
 */
static void print(const char *key, int keylen, const char *val, int vallen) {
    (void)fwrite(key, keylen, 1, stdout);
    fputs(" ", stdout);
    (void)fwrite(val, vallen, 1, stdout);
    fputs("\n", stdout);
    fflush(stdout);
}

/**
 * Request a stat from the server
 * @param sock socket connected to the server
 * @param key the name of the stat to receive (NULL == ALL)
 */
static void request_stat(BIO *bio, const char *key)
{
    uint32_t buffsize = 0;
    char *buffer = NULL;
    uint16_t keylen = 0;
    protocol_binary_request_stats request;
    protocol_binary_response_no_extras response;

    if (key != NULL) {
        keylen = (uint16_t)strlen(key);
    }

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_STAT;
    request.message.header.request.keylen = htons(keylen);
    request.message.header.request.bodylen = htonl(keylen);

    ensure_send(bio, &request, sizeof(request));
    if (keylen > 0) {
        ensure_send(bio, key, keylen);
    }

    do {
        ensure_recv(bio, &response, sizeof(response.bytes));
        /* Take any payload off the socket */
        const uint16_t keylen = ntohs(response.message.header.response.keylen);
        const uint32_t bodylen = ntohl(response.message.header.response.bodylen);
        if (bodylen > buffsize) {
            if ((buffer = realloc(buffer, bodylen)) == NULL) {
                fprintf(stderr, "Failed to allocate memory\n");
                exit(1);
            }
            buffsize = bodylen;
        }
        ensure_recv(bio, buffer, bodylen);

        /* If response was valid print it, otherwise print error string to stderr. */
        if (response.message.header.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            print(buffer, keylen, buffer + keylen, bodylen - keylen);
        } else {
            uint16_t err = ntohs(response.message.header.response.status);
            fprintf(stderr, "Error from server requesting stat");
            if (keylen > 0) {
                fprintf(stderr, " '%s': ", key);
            } else {
                fprintf(stderr, "s: ");
            }
            fprintf(stderr, "%s\n",  memcached_status_2_text(err));
        }
    } while (response.message.header.response.keylen != 0);
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

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "Th:p:u:b:P:s")) != EOF) {
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
        case 'b' :
        case 'u' :
            /* Currently -u and -b are synonymous - only allow the user to
             * specify one. */
            if (user == NULL) {
                user = optarg;
            } else {
                fprintf(stderr, "Error: cannot specify both -u (user) and -b (bucket).\n");
                return 1;
            }
            break;
        case 'P':
            pass = optarg;
            break;
        case 's':
            secure = 1;
            break;
        default:
            fprintf(stderr,
                    "Usage: mcstat [-h host[:port]] [-p port] [-b bucket] [-u user] [-P pass] [-s] [-T] statkey ...\n"
                    "\n"
                    "  -h hostname[:port]  Host (and optional port number) to retrieve stats from\n"
                    "  -p port             Port number\n"
                    "  -u username         Username (currently synonymous with -b)\n"
                    "  -b bucket           Bucket name\n"
                    "  -P password         Password (if bucket is password-protected)\n"
                    "  -s                  Connect to node securely (using SSL)\n"
                    "  -T                  Request TCP_NODELAY from the server\n"
                    "  statkey ...         Statistic(s) to request\n");
            return 1;
        }
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return 1;
    }

    if (tcp_nodelay && !enable_tcp_nodelay(bio)) {
        return 1;
    }

    if (optind == argc) {
        request_stat(bio, NULL);
    } else {
        int ii;
        for (ii = optind; ii < argc; ++ii) {
            request_stat(bio, argv[ii]);
        }
    }

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return EXIT_SUCCESS;
}
