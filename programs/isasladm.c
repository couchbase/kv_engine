/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/protocol_binary.h>

#include <getopt.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void retry_send(int sock, const void* buf, size_t len);
static void retry_recv(int sock, void *buf, size_t len);

/**
 * Try to connect to the server
 * @param host the name of the server
 * @param port the port to connect to
 * @return a socket descriptor connected to host:port for success, -1 otherwise
 */
static int connect_server(const char *hostname, const char *port)
{
    struct addrinfo *ainfo = NULL;
    struct addrinfo hints = {
        .ai_flags = AI_ALL,
        .ai_family = PF_UNSPEC,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP};

    if (getaddrinfo(hostname, port, &hints, &ainfo) != 0) {
        return -1;
    }

    int sock = -1;
    struct addrinfo *ai = ainfo;
    while (ai != NULL) {
        sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock != -1) {
            if (connect(sock, ai->ai_addr, ai->ai_addrlen) != -1) {
                break;
            }
            close(sock);
            sock = -1;
        }
        ai = ai->ai_next;
    }

    freeaddrinfo(ainfo);

    if (sock == -1) {
        fprintf(stderr, "Failed to connect to memcached server (%s:%s): %s\n",
                hostname, port, strerror(errno));
    }

    return sock;
}

/**
 * Send the chunk of data to the other side, retry if an error occurs
 * (or terminate the program if retry wouldn't help us)
 * @param sock socket to write data to
 * @param buf buffer to send
 * @param len length of data to send
 */
static void retry_send(int sock, const void* buf, size_t len)
{
    off_t offset = 0;
    const char* ptr = buf;

    do {
        size_t num_bytes = len - offset;
        ssize_t nw = send(sock, ptr + offset, num_bytes, 0);
        if (nw == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to write: %s\n", strerror(errno));
                close(sock);
                exit(1);
            }
        } else {
            offset += nw;
        }
    } while (offset < len);
}

/**
 * Receive a fixed number of bytes from the socket.
 * (Terminate the program if we encounter a hard error...)
 * @param sock socket to receive data from
 * @param buf buffer to store data to
 * @param len length of data to receive
 */
static void retry_recv(int sock, void *buf, size_t len)
{
    if (len == 0) {
        return;
    }
    off_t offset = 0;
    do {
        ssize_t nr = recv(sock, ((char*)buf) + offset, len - offset, 0);
        if (nr == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to read: %s\n", strerror(errno));
                close(sock);
                exit(1);
            }
        } else {
            if (nr == 0) {
                fprintf(stderr, "Connection closed\n");
                close(sock);
                exit(1);
            }
            offset += nr;
        }
    } while (offset < len);
}

/**
 * Refresh the iSASL password database
 * @param sock socket connected to the server
 */
static void refresh(int sock)
{
    protocol_binary_request_no_extras request = {
        .message.header.request = {
            .magic = PROTOCOL_BINARY_REQ,
            .opcode = PROTOCOL_BINARY_CMD_ISASL_REFRESH
        }
    };

    retry_send(sock, &request, sizeof(request));

    protocol_binary_response_no_extras response;
    retry_recv(sock, &response, sizeof(response.bytes));
    if (response.message.header.response.status != 0) {
        uint16_t err = ntohs(response.message.header.response.status);
        fprintf(stderr, "Failed to refresh isasl passwd db: %d\n",
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
    const char * const default_ports[] = { "memcache", "11211", NULL };
    const char *port = NULL;
    const char *host = NULL;
    char *ptr;

    /* Initialize the socket subsystem */
    initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:u:P:")) != EOF) {
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
        default:
            fprintf(stderr,
                    "Usage mcstat [-h host[:port]] [-p port] [cmd]*\n");
            return 1;
        }
    }

    if (host == NULL) {
        host = "localhost";
    }

    if (optind == argc) {
        fprintf(stderr, "You need to supply a command\n");
        return EXIT_FAILURE;
    }

    int sock = -1;
    if (port == NULL) {
        int ii = 0;
        do {
            port = default_ports[ii++];
            sock = connect_server(host, port);
        } while (sock == -1 && default_ports[ii] != NULL);
    } else {
        sock = connect_server(host, port);
    }

    if (sock == -1) {
        return 1;
    }

    for (int ii = optind; ii < argc; ++ii) {
        if (strcmp(argv[ii], "refresh") == 0) {
            refresh(sock);
        } else {
            fprintf(stderr, "Unknown command %s\n", argv[ii]);
            close(sock);
            return 1;
        }
    }

    close(sock);

    return 0;
}
