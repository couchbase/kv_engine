/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc.
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

#include <errno.h>
#include <event.h>
#include <limits.h>
#include <memcached/protocol_binary.h>

#include <time.h>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <fstream>
#include <list>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>

#include "mccouch.h"

using namespace std;
using namespace mccouch;

/*
 * libevent2 define evutil_socket_t so that it'll automagically work
 * on windows
 */
#ifndef evutil_socket_t
#define evutil_socket_t int
#endif

class Buffer
{
public:
    char *data;
    size_t size;
    size_t avail;
    size_t curr;

    Buffer() : data(NULL), size(0), avail(0), curr(0) {
        /* EMPTY */
    }

    Buffer(size_t s) : data((char *)malloc(s)), size(s), avail(s), curr(0) {
        cb_assert(data != NULL);
    }

    Buffer(const Buffer &other) :
        data((char *)malloc(other.size)), size(other.size), avail(other.avail),
        curr(other.curr) {
        cb_assert(data != NULL);
        memcpy(data, other.data, size);
    }

    ~Buffer() {
        free(data);
    }

    bool grow() {
        return grow(8192);
    }

    bool grow(size_t minFree) {
        if (minFree == 0) {
            // no minimum size requested, just ensure that there is at least
            // one byte there...
            minFree = 1;
        }

        if (size - avail < minFree) {
            size_t next = size ? size << 1 : 8192;
            char *ptr;

            while ((next - avail) < minFree) {
                next <<= 1;
            }

            ptr = (char *)realloc(data, next);
            if (ptr == NULL) {
                return false;
            }
            data = ptr;
            size = next;
        }

        return true;
    }
};

namespace mccouch
{
    static const int mockIgnoreAccept    =  0;
    static const int mockCloseSocket     =  1;
    static const int mockDropResponse    =  2;
    static const int mockGarbageResponse =  3;
    static const int mockEtmpfail        =  4;
    static const int mockSlowResponse    =  5;
    static const int mockInvalid         = -1;

    static bool mockRandomFailure = false;
    static int  mockRandomRange;

    class McConnection
    {
    public:
        McConnection(evutil_socket_t s, struct event_base *b, McCouchMockServerInstance *svr);
        virtual ~McConnection();

        bool doFillInput();
        bool doDrainOutput();
        bool isDisconnected();

        virtual void step(short which);

    protected:
        virtual void handleRequest(protocol_binary_request_header *req);
        virtual void handleDeleteVbucket(protocol_binary_request_header *req);
        virtual void handleFlush(protocol_binary_request_header *req);

        void response(const string &key, const uint8_t *ext,
                      uint8_t extlen, const string &body, uint16_t status,
                      uint64_t cas, protocol_binary_request_header *req);

        void response(protocol_binary_request_header *req, uint16_t error);

        void doDisconnect();

        void updateEvent();

        void genRandAction();
        bool mockFailure(int failure);

        Buffer output;
        Buffer input;
        evutil_socket_t sock;
        struct event ev_event;
        short ev_flags;
        struct event_base *base;
        bool disconnected;
        int  mock_failure;
        McCouchMockServerInstance *server;
    };

    class McEndpoint : public McConnection
    {
    public:
        McEndpoint(evutil_socket_t s, struct event_base *b, McCouchMockServerInstance *svr);
        virtual ~McEndpoint() {}
        virtual void step(short which);
    };

    class McCouchMockServerInstance
    {
    private:
        list<McConnection *> conns;

    public:
        McCouchMockServerInstance(int &port);
        ~McCouchMockServerInstance();

        void run();

        void addConnection(McConnection *conn) {
            conns.push_back(conn);
        }
        void removeConnection(McConnection *conn) {
            conns.remove(conn);
        }

    protected:
        cb_thread_t threadid;

        /** The event base this instance is connected to */
        struct event_base *ev_base;
    };

} // namespace

extern "C" {
    static void start_mccouch(void *);
    static void mccouch_libevent_callback(evutil_socket_t , short, void *);
}

McConnection::McConnection(evutil_socket_t s, struct event_base *b,
                           McCouchMockServerInstance *svr)
    : sock(s), ev_flags(0), base(b), disconnected(false), mock_failure(mockInvalid), server(svr)
{
    updateEvent();
    if( server ) {
        server->addConnection(this);
    }
}

McConnection::~McConnection()
{
    EVUTIL_CLOSESOCKET(sock);
    if (ev_flags != 0) {
        event_del(&ev_event);
    }
}

void McConnection::genRandAction()
{
    if (mccouch::mockRandomFailure) {
        mock_failure = rand() % mockRandomRange;
    }
}

bool McConnection::mockFailure(int failure)
{
    if (mock_failure != failure) {
        return false;
    }

    bool rev = true;
    switch( mock_failure ) {
        case mockCloseSocket:
            shutdown(sock, 1);
            EVUTIL_CLOSESOCKET(sock);
            break;
        case mockDropResponse:
        case mockGarbageResponse:
        case mockIgnoreAccept:
        case mockEtmpfail:
        case mockSlowResponse:
            break;
        default:
            rev = false;
            break;
    }
    return rev;
}

bool McConnection::doFillInput()
{
    cb_assert(sock != INVALID_SOCKET);
    size_t processed;

    input.grow(8192);
    protocol_binary_request_header *req;
    req = (protocol_binary_request_header *)input.data;

    do {
        ssize_t nr;
        while (input.avail >= sizeof(*req) && input.avail >= (ntohl(req->request.bodylen) + sizeof(*req))) {
            if (req->request.magic != PROTOCOL_BINARY_REQ) {
                doDisconnect();
                return false;
            }
            handleRequest(req);
            processed = ntohl(req->request.bodylen) + sizeof(*req);
            memmove(input.data, input.data + processed, input.avail - processed);
            input.avail -= processed;
        }

        if (input.size - input.avail == 0) {
            input.grow();
        }

        nr = recv(sock, input.data + input.avail, input.size - input.avail, 0);
        if (nr == -1) {
            int error = errno;
#ifdef _MSC_VER
            DWORD err = GetLastError();
            if (err == WSAEWOULDBLOCK) {
                error = EWOULDBLOCK;
            }
#endif
            switch (error) {
            case EINTR:
                break;
            case EWOULDBLOCK:
                return true;
            default:
                doDisconnect();
                return false;
            }
        } else if (nr == 0) {
            doDisconnect();
            return false;
        } else {
            input.avail += (size_t)nr;
        }
    } while (true);

    return true;
}

bool McConnection::doDrainOutput()
{
    cb_assert(sock != INVALID_SOCKET);
    do {
        if (output.avail == 0) {
            return true;
        }

        ssize_t nw = send(sock, output.data + output.curr,
                          output.avail - output.curr, 0);
        if (nw == -1) {
            int error = errno;
#ifdef _MSC_VER
            DWORD err = GetLastError();
            if (err == WSAEWOULDBLOCK) {
                error = EWOULDBLOCK;
            }
#endif
            switch (error) {
            case EINTR:
                // retry
                break;
            case EWOULDBLOCK:
                // The network can't accept more..
                return true;
            default:
                doDisconnect();
                return false;
            }
        } else {
            output.curr += (size_t)nw;
            if (output.curr == output.avail) {
                output.curr = output.avail = 0;
            }
        }
    } while (true);
    return true;
}

bool McConnection::isDisconnected()
{
    return disconnected;
}

void McConnection::step(short which)
{
    if (which & (EV_READ|EV_WRITE)) {
        genRandAction();
    }

    if (which & EV_READ) {
        if (!doFillInput()) {
            return;
        }
    }

    if (which & EV_WRITE) {
        if (!doDrainOutput()) {
            return;
        }
    }
    updateEvent();
}

void McConnection::handleRequest(protocol_binary_request_header *req)
{
    if (mockFailure(mockDropResponse) || mockFailure(mockCloseSocket)) {
        return;
    }

    switch (req->request.opcode) {
    case 0xac: /* CMD_NOTIFY_VBUCKET_UPDATE */
        if (mockFailure(mockEtmpfail)) {
            response(req, PROTOCOL_BINARY_RESPONSE_ETMPFAIL);
        } else {
            response(req, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        }
        break;
    case 0x89: /* SELECT_BUCKET */
        response(req, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
        // @todo handle other commands
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
        handleDeleteVbucket(req);
        break;
    case PROTOCOL_BINARY_CMD_FLUSH:
        handleFlush(req);
        break;
    default:
        cb_assert(0);
    }
}

static void removeVbucketFile(uint16_t vbucket)
{
    std::stringstream fname;
    const char *dir = getenv("EP_TEST_DIR");
    dir = dir ? dir : "/tmp/test";
    fname << dir << "/" << vbucket << ".couch.1";
    if (FILE *fp = fopen(fname.str().c_str(), "r")) {
        fclose(fp);
        int ret = remove(fname.str().c_str());
        cb_assert(!ret);
    }
}

void McConnection::handleDeleteVbucket(protocol_binary_request_header *req)
{
    removeVbucketFile(ntohs(req->request.vbucket));
    response(req, PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

void McConnection::handleFlush(protocol_binary_request_header *req)
{
    for (int ii = 0; ii < 32; ++ii) {
        removeVbucketFile(ii);
    }
    response(req, PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

void McConnection::response(const string &key, const uint8_t *ext,
                            uint8_t extlen, const string &body,
                            uint16_t status, uint64_t cas,
                            protocol_binary_request_header *req)
{
    protocol_binary_response_header res;

    if (!mockFailure(mockGarbageResponse)) {
        memset(res.bytes, 0, sizeof(res.bytes));
        res.response.magic = PROTOCOL_BINARY_RES;
        res.response.opcode = req->request.opcode;
        res.response.extlen = extlen;
        res.response.keylen = htons(static_cast<uint16_t>(key.length()));
        res.response.status = htons(status);
        res.response.bodylen = ntohl(body.length() + key.length() + extlen);
        res.response.opaque = req->request.opaque;
        res.response.cas = cas;
    }

    output.grow(body.length() + key.length() + extlen + sizeof(res.bytes));
    memcpy(output.data + output.avail, res.bytes, sizeof(res.bytes));
    output.avail += sizeof(res.bytes);

    if (extlen) {
        memcpy(output.data + output.avail, ext, extlen);
        output.avail += extlen;
    }

    if (key.length()) {
        memcpy(output.data + output.avail, key.c_str(), key.length());
        output.avail += key.length();
    }

    if (body.length()) {
        memcpy(output.data + output.avail, body.c_str(), body.length());
        output.avail += body.length();
    }

    if (mockFailure(mockSlowResponse)) {
        sleep(10);
    }
}

void McConnection::response(protocol_binary_request_header *req,
                            uint16_t error)
{
    response("", NULL, 0, "", error, 0, req);
}

void McConnection::doDisconnect()
{
    if (ev_flags != 0) {
        event_del(&ev_event);
    }
    ev_flags = 0;
    disconnected = true;
    server->removeConnection(this);
}

void McConnection::updateEvent()
{
    if (sock == INVALID_SOCKET) {
        return;
    }
    int flags = EV_TIMEOUT | EV_PERSIST;
    if (output.avail == 0) {
        flags |= EV_READ;
    } else {
        flags |= EV_WRITE ;
    }

    if (flags == ev_flags || ev_flags != 0) {
        event_del(&ev_event);
    }

    ev_flags = flags;
    event_set(&ev_event, sock, ev_flags,
              mccouch_libevent_callback,
              this);
    event_base_set(base, &ev_event);
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    if (event_add(&ev_event, &tv) == -1) {
        cb_assert(0);
    }
}

McEndpoint::McEndpoint(evutil_socket_t s, struct event_base *b, McCouchMockServerInstance *svr) :
    McConnection(s, b, svr)
{}

void McEndpoint::step(short which)
{
    if (which & EV_READ) {
        struct sockaddr_storage addr;
        socklen_t addrlen = sizeof(addr);

        genRandAction();
        if (!mockFailure(mockIgnoreAccept)) {
            evutil_socket_t c = accept(sock, (struct sockaddr *)&addr, &addrlen);

            if (c == INVALID_SOCKET ||
                evutil_make_socket_nonblocking(c) == -1) {
                cb_assert(0);
            }

            new McConnection(c, base, server);
        }
    }
    updateEvent();
}

McCouchMockServerInstance::McCouchMockServerInstance(int &port) :
    ev_base(event_base_new())
{
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_INET; // The testsuite don't use ipv6

    struct addrinfo *ainfo = NULL;
    int error = getaddrinfo(NULL, "0", &hints, &ainfo);
    if (error != 0) {
        throw runtime_error("Failed to lookup ports");
    }

    struct addrinfo *ai = ainfo;
    while (ai != NULL) {
        evutil_socket_t sock = socket(ai->ai_family,
                                      ai->ai_socktype,
                                      ai->ai_protocol);
        if (sock != -1) {
            if (::bind(sock, ai->ai_addr, ai->ai_addrlen) == 0 &&
                ::listen(sock, 1) == 0 &&
                evutil_make_socket_nonblocking(sock) == 0) {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                if (getsockname(sock, (struct sockaddr *)&my_sockaddr, &len) == 0) {
                    if (ai->ai_addr->sa_family == AF_INET) {
                        port = ntohs(my_sockaddr.in.sin_port);
                    } else {
                        port = ntohs(my_sockaddr.in6.sin6_port);
                    }
                    new McEndpoint(sock, ev_base, this);
                    break;
                } else {
                    EVUTIL_CLOSESOCKET(sock);
                }
            } else {
                EVUTIL_CLOSESOCKET(sock);
            }
        }
        ai = ai->ai_next;
    }

    freeaddrinfo(ainfo);
    if (conns.empty()) {
        throw runtime_error("Failed to create temp mock server");
    }

    if (cb_create_thread(&threadid, start_mccouch, this, 0) != 0) {
        throw runtime_error("Error creating memcached engine thread");
    }
}

McCouchMockServerInstance::~McCouchMockServerInstance()
{
    event_base_loopbreak(ev_base);
    int ret = cb_join_thread(threadid);
    if (ret != 0) {
        cb_assert(0);
    }
    list<McConnection *>::iterator it = conns.begin();
    for (; it != conns.end(); ++it) {
        delete *it;
    }
    event_base_free(ev_base);
}

void McCouchMockServerInstance::run()
{
    /* seed for randomized failure tests */
    srand(time(NULL));
    event_base_loop(ev_base, 0);
}

void start_mccouch(void *arg)
{
    static_cast<McCouchMockServerInstance *>(arg)->run();
}

void mccouch_libevent_callback(evutil_socket_t sock, short which, void *arg)
{
    (void)sock;
    McConnection *c = static_cast<McConnection *>(arg);
    c->step(which);
    if (c->isDisconnected()) {
        delete c;
    }
}

McCouchMockServer::McCouchMockServer(int &port, bool randomFailure, int randomRange)
{
    instance = new McCouchMockServerInstance(port);
    mccouch::mockRandomFailure = randomFailure;
    mccouch::mockRandomRange = randomRange;
}

McCouchMockServer::~McCouchMockServer()
{
    delete instance;
}
