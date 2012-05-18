/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// mcbasher is a program that keeps on running commands to a memcached
// server in order to try to "crash" it. It does <b>not</b> try to verify
// the correctness of the response it gets, because it's inteded to be used
// with different mock engines and all we care about is if we're able to
// run the commands or not.

#include "config.h"

#include <memcached/protocol_binary.h>

#include <getopt.h>
#include <pthread.h>
#include <cstdlib>
#include <cstdio>
#include <string>
#include <string.h>
#include <list>
#include <cassert>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cerrno>


using namespace std;


class Connection {
public:
    Connection(const string &_host, const string &_port,
               char *sndbuf, size_t sndbufsz, bool loop) :
        sock(-1),
        devZeroSize(8192), devZero(new char[devZeroSize]),
        host(_host), port(_port),
        sendBuffer(sndbuf), sendBufferSize(sndbufsz), loopSendBuffer(loop),
        sendBufferOffset(0)
    {

    }

    ~Connection() {
        if (sock != -1) {
            close(sock);
        }
        delete []devZero;
    }


    void main(void) {
        while (true) {
            if (sock == -1) {
                connect();
            }

            while (sock != -1) {
                struct pollfd fds[1];
                fds[0].fd = sock;
                if (toSend == 0) {
                    fds[0].events = POLLIN;
                } else {
                    fds[0].events = POLLIN | POLLOUT;
                }
                assert(poll(fds, 1, -1) != -1);

                if (fds[0].revents & POLLIN) {
                    drainInput();
                } else if (fds[0].revents & POLLOUT) {
                    doSendData();
                } else {
                    // what is this?
                    abort();
                }
            }
        }
    }

protected:
    /**
     * Try to connect to the server
     * @return false if we failed to connect to the server
     */
    bool connect(void)
    {
        struct addrinfo *ai = NULL;
        struct addrinfo hints;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_protocol = IPPROTO_TCP;
        hints.ai_socktype = SOCK_STREAM;

        if (getaddrinfo(host.c_str(), port.c_str(), &hints, &ai) != 0) {
            return false;
        }

        struct addrinfo *e = ai;
        do {
            if ((sock = socket(e->ai_family, e->ai_socktype,
                               e->ai_protocol)) != -1) {
                if (::connect(sock, ai->ai_addr, ai->ai_addrlen) == -1) {
                    close(sock);
                    sock = -1;
                }
            }
            if (sock == -1) {
                e = e->ai_next;
            } else {
                break;
            }
        } while (e) ;

        fcntl(sock, F_SETFL, fcntl(sock, F_GETFL) | O_NONBLOCK);

        freeaddrinfo(ai);

        sendBufferOffset = 0;
        toSend = sendBufferSize - sendBufferOffset;
        return true;
    }

    void drainInput(void) {
        ssize_t nr;

        while ((nr = recv(sock, devZero, devZeroSize, 0)) > 0) {
            // Discard the data :)
        }

        if ((nr == -1 && errno != EWOULDBLOCK) || nr == 0) {
            close(sock);
            sock = -1;
        }
    }

    void doSendData(void) {
        ssize_t nw;
        assert(toSend > 0);

        while ((nw = send(sock, sendBuffer+sendBufferOffset, toSend, 0)) > 0) {
            sendBufferOffset += nw;
            toSend = sendBufferSize - sendBufferOffset;
            if (toSend == 0) {
                if (loopSendBuffer) {
                    sendBufferOffset = 0;
                    toSend = sendBufferSize - sendBufferOffset;
                } else {
                    return;
                }
            }
        }

        if (nw == -1 && errno != EWOULDBLOCK) {
            close(sock);
            sock = -1;
        }
    }

    char *sendBuffer;
    size_t toSend;
    size_t sendBufferSize;
    bool loopSendBuffer;
    size_t sendBufferOffset;

    size_t devZeroSize;
    char* devZero;
    int sock;
    const string host;
    const string port;
};



extern "C" {
    void *connection_main(void *arg) {
        Connection *c = reinterpret_cast<Connection*>(arg);
        c->main();
        return reinterpret_cast<void*>(c);
    }
}

static void bangit(list<Connection*> conn)
{
    // @todo rewrite to use libevent and async io ;)
    list<pthread_t> tids;
    list<Connection*>::iterator iter;
    for (iter = conn.begin(); iter != conn.end(); ++iter) {
        pthread_t tid;
        void *arg = reinterpret_cast<void*>(*iter);
        assert(pthread_create(&tid, NULL, connection_main, arg) == 0);
        tids.push_back(tid);
    }

    list<pthread_t>::iterator ii;
    for (ii = tids.begin(); ii != tids.end(); ++ii) {
        assert(pthread_join(*ii, NULL) == 0);
    }
}

Connection *createTapConnection(const char *host, const char *port)
{
    protocol_binary_request_tap_connect *req;
    char *message = new char[sizeof(req->bytes)];
    size_t messagesize = sizeof(req->bytes);
    req = reinterpret_cast<protocol_binary_request_tap_connect *>(message);
    memset(req->bytes, 0, sizeof(req->bytes));

    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_CONNECT;
    req->message.header.request.keylen = htons(0);
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = htonl(4);
    req->message.body.flags = ntohl(TAP_CONNECT_FLAG_DUMP);

    return new Connection(host, port, message, messagesize, false);
}

static size_t insertRandomGet(char *ptr, size_t buffsz)
{
    protocol_binary_request_get *req;

    if (buffsz < (sizeof(req->bytes) + 1)) {
        return 0;
    }

    req = reinterpret_cast<protocol_binary_request_get*>(ptr);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    req->message.header.request.keylen = htons(1);
    req->message.header.request.extlen = 0;
    req->message.header.request.bodylen = htonl(1);
    ptr += sizeof(req->bytes);
    *ptr = 'a';
    return sizeof(req->bytes) + 1;
}

static size_t insertRandomAdd(char *ptr, size_t buffsz)
{
    protocol_binary_request_add *req;

    if (buffsz < (sizeof(req->bytes) + 1)) {
        return 0;
    }

    req = reinterpret_cast<protocol_binary_request_add*>(ptr);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_ADD;
    req->message.header.request.keylen = htons(1);
    req->message.header.request.extlen = 8;
    req->message.header.request.bodylen = htonl(9);
    req->message.body.flags = 0;
    req->message.body.expiration = 0;
    ptr += sizeof(req->bytes);
    *ptr = 'a';
    return sizeof(req->bytes) + 1;
}

static size_t insertRandomSet(char *ptr, size_t buffsz)
{
    protocol_binary_request_set *req;

    if (buffsz < (sizeof(req->bytes) + 1)) {
        return 0;
    }

    req = reinterpret_cast<protocol_binary_request_set*>(ptr);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_SET;
    req->message.header.request.keylen = htons(1);
    req->message.header.request.extlen = 8;
    req->message.header.request.bodylen = htonl(9);
    req->message.body.flags = 0;
    req->message.body.expiration = 0;
    ptr += sizeof(req->bytes);
    *ptr = 'a';
    return sizeof(req->bytes) + 1;
}

static size_t insertRandomReplace(char *ptr, size_t buffsz)
{
    protocol_binary_request_replace *req;

    if (buffsz < (sizeof(req->bytes) + 1)) {
        return 0;
    }

    req = reinterpret_cast<protocol_binary_request_replace*>(ptr);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_REPLACE;
    req->message.header.request.keylen = htons(1);
    req->message.header.request.extlen = 8;
    req->message.header.request.bodylen = htonl(9);
    req->message.body.flags = 0;
    req->message.body.expiration = 0;
    ptr += sizeof(req->bytes);
    *ptr = 'a';
    return sizeof(req->bytes) + 1;
}

static size_t insertRandomDelete(char *ptr, size_t buffsz)
{
    protocol_binary_request_delete *req;

    if (buffsz < (sizeof(req->bytes) + 1)) {
        return 0;
    }

    req = reinterpret_cast<protocol_binary_request_delete*>(ptr);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_DELETE;
    req->message.header.request.keylen = htons(1);
    req->message.header.request.extlen = 0;
    req->message.header.request.bodylen = htonl(1);
    ptr += sizeof(req->bytes);
    *ptr = 'a';
    return sizeof(req->bytes) + 1;
}

static size_t insertRandomFlush(char *ptr, size_t buffsz)
{
    protocol_binary_request_flush *req;

    if (buffsz < sizeof(req->bytes)) {
        return 0;
    }

    req = reinterpret_cast<protocol_binary_request_flush*>(ptr);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_FLUSH;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = htonl(4);
    return sizeof(req->bytes);
}

static size_t insertRandomNoop(char *ptr, size_t buffsz)
{
    protocol_binary_request_noop *req;

    if (buffsz < sizeof(req->bytes)) {
        return 0;
    }

    req = reinterpret_cast<protocol_binary_request_noop*>(ptr);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_NOOP;
    return sizeof(req->bytes);
}

static size_t insertRandomVersion(char *ptr, size_t buffsz)
{
    protocol_binary_request_version *req;

    if (buffsz < sizeof(req->bytes)) {
        return 0;
    }

    req = reinterpret_cast<protocol_binary_request_version*>(ptr);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_VERSION;
    return sizeof(req->bytes);
}

Connection *createGetSetConnection(const char *host, const char *port)
{
    const size_t buffersize = 1024*1024;
    char *message = new char[buffersize];
    memset(message, 0, buffersize);
    size_t msgsize = 0;
    char *curr = message;
    while (msgsize < buffersize) {
        long id = random() % 10;
        size_t size;
        switch (id) {
        case 0:
            size = insertRandomGet(curr, buffersize - msgsize);
            break;
        case 1:
            size = insertRandomAdd(curr, buffersize - msgsize);
            break;
        case 2:
            size = insertRandomReplace(curr, buffersize - msgsize);
            break;
        case 3:
            size = insertRandomDelete(curr, buffersize - msgsize);
            break;
        case 4:
            size = insertRandomFlush(curr, buffersize - msgsize);
            break;
        case 5:
            size = insertRandomNoop(curr, buffersize - msgsize);
            break;
        case 6:
            size = insertRandomVersion(curr, buffersize - msgsize);
            break;
        default:
            size = insertRandomSet(curr, buffersize - msgsize);
        }
        curr += size;
        msgsize += size;
        if (size == 0) {
            break;
        }
    }

    return new Connection(host, port, message, msgsize, true);
}


/**
 * Program entry point. Connect to a memcached server and use the binary
 * protocol to retrieve a given set of stats.
 *
 * @param argc argument count
 * @param argv argument vector
 * @return 0 if success, error code otherwise
 */
int main(int argc, char **argv)
{
    int cmd;
    const char *port = "11211";
    const char *host = NULL;
    int connections = 10;
    char *ptr;

    /* Initialize the socket subsystem */
    initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:c:")) != EOF) {
        switch (cmd) {
        case 'h' :
            host = optarg;
            ptr = strchr(optarg, ':');
            if (ptr != NULL) {
                *ptr = '\0';
                port = ptr + 1;
            }
            break;
        case 'p' :
            port = optarg;
            break;
        case 'c' :
            connections = atoi(optarg);
            break;
        default:
            fprintf(stderr,
                    "Usage mcbasher [-h host[:port]] [-p port] [-c connections]*\n");
            return 1;
        }
    }

    if (host == NULL) {
        host = "localhost";
    }

    list<Connection*> conns;
    for (int ii = 0; ii < connections; ++ii) {
        Connection *c;
        if ((ii & 1) == 0) {
            c = createTapConnection(host, port);
        } else {
            c = createGetSetConnection(host, port);
        }

        conns.push_back(c);
    }

    bangit(conns);

    return 0;
}
