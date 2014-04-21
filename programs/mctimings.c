/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/protocol_binary.h>
#include <memcached/openssl.h>
#include <platform/platform.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <cJSON.h>

#include "utilities.h"
#include "utilities/protocol2text.h"

typedef struct timings_st {
    uint32_t max;

    /* We collect timings for <=1 us */
    uint32_t ns;

    /* We collect timings per 10usec */
    uint32_t us[100];

    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    uint32_t ms[50];

    uint32_t halfsec[10];

    uint32_t wayout;
} timings_t;

timings_t timings;

static void callback(const char *timeunit, uint32_t min, uint32_t max, uint32_t total)
{
    if (total > 0) {
        int ii;
        char buffer[1024];
        int offset;
        int num;
        if (min > 0 && max == 0) {
            offset = sprintf(buffer, "[%4u - inf.]%s", min, timeunit);
        } else {
            offset = sprintf(buffer, "[%4u - %4u]%s", min, max, timeunit);
        }
        num = (float)40.0 * (float)total / (float)timings.max;
        offset += sprintf(buffer + offset, " |");
        for (ii = 0; ii < num; ++ii) {
            offset += sprintf(buffer + offset, "#");
        }

        offset += sprintf(buffer + offset, " - %u\n", total);
        fputs(buffer, stdout);
    }
}

static void dump_histogram(void)
{
    int ii;

    callback("ns", 0, 999, timings.ns);
    for (ii = 0; ii < 100; ++ii) {
        callback("us", ii * 10, ((ii + 1) * 10 - 1), timings.us[ii]);
    }

    for (ii = 1; ii < 50; ++ii) {
        callback("ms", ii, ii, timings.ms[ii]);
    }

    for (ii = 0; ii < 10; ++ii) {
        callback("ms", ii * 500, ((ii + 1) * 500) - 1, timings.halfsec[ii]);
    }

    callback("ms", (9 * 500), 0, timings.wayout);

}

static int json2internal(cJSON *r)
{
    int ii;
    cJSON *o = cJSON_GetObjectItem(r, "ns");
    cJSON *i;

    if (o == NULL) {
        fprintf(stderr, "Internal error.. failed to locate \"ns\"\n");
        return -1;
    }

    timings.max = timings.ns = o->valueint;
    o = cJSON_GetObjectItem(r, "us");
    if (o == NULL) {
        fprintf(stderr, "Internal error.. failed to locate \"us\"\n");
        return -1;
    }

    ii = 0;
    i = o->child;
    while (i) {
        timings.us[ii] = i->valueint;
        if (timings.us[ii] > timings.max) {
            timings.max = timings.us[ii];
        }

        ++ii;
        i = i->next;
        if (ii == 100 && i != NULL) {
            fprintf(stderr, "Internal error.. too many \"us\" samples\n");
            return -1;
        }
    }

    o = cJSON_GetObjectItem(r, "ms");
    if (o == NULL) {
        fprintf(stderr, "Internal error.. failed to locate \"ms\"\n");
        return -1;
    }
    ii = 1;
    i = o->child;
    while (i) {
        timings.ms[ii] = i->valueint;
        if (timings.ms[ii] > timings.max) {
            timings.max = timings.ms[ii];
        }

        ++ii;
        i = i->next;
        if (ii == 50 && i != NULL) {
            fprintf(stderr, "Internal error.. too many \"ms\" samples\n");
            return -1;
        }
    }

    o = cJSON_GetObjectItem(r, "500ms");
    if (o == NULL) {
        fprintf(stderr, "Internal error.. failed to locate \"500ms\"\n");
        return -1;
    }
    ii = 0;
    i = o->child;
    while (i) {
        timings.halfsec[ii] = i->valueint;
        if (timings.halfsec[ii] > timings.max) {
            timings.max = timings.halfsec[ii];
        }

        ++ii;
        i = i->next;
        if (ii == 10 && i != NULL) {
            fprintf(stderr, "Internal error.. too many \"halfsec\" samples\n");
            return -1;
        }
    }

    i = cJSON_GetObjectItem(r, "wayout");
    if (i == NULL) {
        fprintf(stderr, "Internal error.. failed to locate \"wayout\"\n");
        return -1;
    }
    timings.wayout = i->valueint;
    if (timings.wayout > timings.max) {
        timings.max = timings.wayout;
    }

    return 0;
}

static void request_timings(BIO *bio, uint8_t opcode)
{
    uint32_t buffsize;
    char *buffer;
    protocol_binary_request_get_cmd_timer request;
    protocol_binary_response_no_extras response;
    cJSON *json, *obj;

    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET_CMD_TIMER;
    request.message.header.request.extlen = 1;
    request.message.header.request.bodylen = htonl(1);
    request.message.body.opcode = opcode;

    ensure_send(bio, &request, sizeof(request.bytes));

    ensure_recv(bio, &response, sizeof(response.bytes));
    buffsize = ntohl(response.message.header.response.bodylen);
    buffer = malloc(buffsize + 1);
    if (buffer == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        exit(1);
    }

    ensure_recv(bio, buffer, buffsize);
    if (response.message.header.response.status != 0) {
        fprintf(stderr, "Command failed: %u\n",
                ntohs(response.message.header.response.status));
        exit(1);
    }

    buffer[buffsize] = '\0';
    json = cJSON_Parse(buffer);
    if (json == NULL) {
        fprintf(stderr, "Failed to parse json\n");
        exit(EXIT_FAILURE);
    }
    obj = cJSON_GetObjectItem(json, "error");
    if (obj == NULL) {
        if (json2internal(json) == -1) {
            fprintf(stderr, "Payload received:\n%s\n", buffer);
            fprintf(stderr, "cJSON representation:\n%s\n", cJSON_Print(json));
            exit(EXIT_FAILURE);
        }

        if (timings.max == 0) {
            const char *cmd = memcached_opcode_2_text(opcode);
            if (cmd) {
                fprintf(stdout,
                        "The server don't have information about \"%s\"\n",
                        cmd);
            } else {
                fprintf(stdout,
                        "The server don't have information about opcode %u\n",
                        opcode);
            }
        } else {
            const char *cmd = memcached_opcode_2_text(opcode);
            if (cmd) {
                fprintf(stdout,
                        "The following data is collected for \"%s\"\n",
                        cmd);
            } else {
                fprintf(stdout,
                        "The following data is collected for opcode %u\n",
                        opcode);
            }

            dump_histogram();
        }
    } else {
        fprintf(stderr, "Error: %s\n", obj->valuestring);
        exit(EXIT_FAILURE);
    }

    cJSON_Delete(json);
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
                    "Usage mctimings [-h host[:port]] [-p port] [-u user] [-P pass] [-s] [opcode]*\n");
            return 1;
        }
    }

    if (create_ssl_connection(&ctx, &bio, host, port, user, pass, secure) != 0) {
        return 1;
    }

    for (; optind < argc; ++optind) {
        request_timings(bio, memcached_text_2_opcode(argv[optind]));
    }

    BIO_free_all(bio);
    if (secure) {
        SSL_CTX_free(ctx);
    }

    return EXIT_SUCCESS;
}
