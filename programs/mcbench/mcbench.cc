/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

/*
 * mcbench is a program used to test the single frontend thread performance
 * on a memcached service
 */
#include "config.h"

#include <memcached/protocol_binary.h>

#include <getopt.h>
#include <cstdlib>
#include <cstdio>
#include <string>
#include <string.h>
#include <list>
#include <vector>
#include <iostream>
#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <chrono>
#include <thread>
#include <atomic>
#include <platform/platform.h>



class Connection;
static void thread_main(Connection *c);


class Connection {
public:
    Connection(const std::string &_host, const std::string &_port,
               uint8_t *sndbuf, size_t sndbufsz) :
        sendBuffer(sndbuf), sendBufferSize(sndbufsz),
        sendBufferOffset(0),
        sock(INVALID_SOCKET), host(_host), port(_port), currentOps(0)
    {
        currentOps.store(0, std::memory_order_release);
        recvBuffer.reserve(2 * 1024 * 1024);
    }

    void start() {
        running.store(true, std::memory_order_release);
        tid = std::thread(thread_main, this);
    }


    void stop() {
        running.store(false, std::memory_order_release);
        tid.join();

    }

    size_t getOpsPerSec() const {
        using namespace std::chrono;

        std::chrono::time_point<std::chrono::steady_clock> now;
        if (running) {
            now = steady_clock::now();
        } else {
            now = stopTime;
        }

        auto delta = duration_cast<seconds>(now - startTime).count();
        if (delta == 0) {
            return currentOps.load(std::memory_order_acquire);
        } else {
            return currentOps.load(std::memory_order_acquire) / delta;
        }
    }

    size_t getDuration() const {
        using namespace std::chrono;
        return duration_cast<seconds>(stopTime - startTime).count();
    }

    size_t getTotalOps() const {
        return currentOps.load(std::memory_order_acquire);
    }

    ~Connection() {
        if (sock != INVALID_SOCKET) {
            closesocket(sock);
        }
    }

protected:
    friend void thread_main(Connection *c);


    void run(void) {
        startTime = std::chrono::steady_clock::now();

        while (running.load(std::memory_order_acquire)) {
            if (sock == INVALID_SOCKET) {
                connect();
            }

            while (sock != INVALID_SOCKET && running.load(std::memory_order_acquire)) {
                struct pollfd fds[1];
                fds[0].fd = sock;
                if (toSend == 0) {
                    fds[0].events = POLLIN;
                } else {
                    fds[0].events = POLLIN | POLLOUT;
                }
                cb_assert(poll(fds, 1, -1) != -1);

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
        stopTime = std::chrono::steady_clock::now();
    }



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
        size_t currsize = recvBuffer.size();
        recvBuffer.resize(2 * 1024 * 1024);
        while ((nr = recv(sock, recvBuffer.data() + currsize,
                          (2 * 1024 * 1024) - currsize, 0)) > 0) {
            /* Parse packets */
            recvBuffer.resize(currsize + nr);
            size_t size = recvBuffer.size();
            uint8_t *curr = recvBuffer.data();

            while (size > 24) {
                protocol_binary_response_no_extras *res;
                res = reinterpret_cast<protocol_binary_response_no_extras *>(curr);
                cb_assert(res->message.header.response.magic == 0x81);
                uint32_t bodylen = ntohl(res->message.header.response.bodylen);

                if (size < (24 + bodylen)) {
                    break;
                }

                size -= (24 + bodylen);
                currentOps.fetch_add(1, std::memory_order_release);;
                curr += 24 + bodylen;
            }

            /* repack the buffer */
            memmove(recvBuffer.data(), curr, size);
            currsize = size;
            recvBuffer.resize(2 * 1024 * 1024);
        }

        recvBuffer.resize(currsize);

        if ((nr == -1 && errno != EWOULDBLOCK) || nr == 0) {
            close(sock);
            sock = -1;
        }
    }

    void doSendData(void) {
        ssize_t nw;
        cb_assert(toSend > 0);

        while ((nw = send(sock, sendBuffer+sendBufferOffset, toSend, 0)) > 0) {
            sendBufferOffset += nw;
            toSend = sendBufferSize - sendBufferOffset;
            if (toSend == 0) {
                sendBufferOffset = 0;
                toSend = sendBufferSize - sendBufferOffset;
            }
        }

        if (nw == -1 && errno != EWOULDBLOCK) {
            close(sock);
            sock = -1;
        }
    }

    uint8_t *sendBuffer;
    size_t toSend;
    size_t sendBufferSize;
    size_t sendBufferOffset;

    std::vector<uint8_t> recvBuffer;
    int sock;
    const std::string host;
    const std::string port;
    std::chrono::time_point<std::chrono::steady_clock> startTime;
    std::chrono::time_point<std::chrono::steady_clock> stopTime;
    std::atomic<size_t> currentOps;
    std::thread tid;
    std::atomic<bool> running;
};

static void thread_main(Connection *c)
{
    c->run();
}

static void buildSetStream(std::vector<uint8_t> &vector, size_t size) {
    /* preformat a buffer that's roughly 2MB big */
    vector.reserve(2 * 1024 * 1024);
    while (vector.size() < (2 * 1024 * 1024)) {
        protocol_binary_request_set req;
        memset(&req, 0, sizeof(req));

        req.message.header.request.magic = PROTOCOL_BINARY_REQ;
        req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET;
        req.message.header.request.keylen = htons(3);
        req.message.header.request.extlen = 8;
        req.message.header.request.bodylen = htonl(11 + size);
        req.message.body.flags = 0;
        req.message.body.expiration = 0;

        for (size_t ii = 0; ii < sizeof(req.bytes); ++ii) {
            vector.push_back(req.bytes[ii]);
        }

        vector.push_back('f');
        vector.push_back('o');
        vector.push_back('o');

        vector.resize(vector.size() + size, ' ');
    }
}

static void set_test(const std::string &host, const std::string &port, int duration) {
    std::list<int> sizes;
    sizes.push_back(256);
    sizes.push_back(512);
    sizes.push_back(1 * 1024);
    sizes.push_back(2 * 1024);
    sizes.push_back(4 * 1024);
    sizes.push_back(8 * 1024);
    sizes.push_back(16 * 1024);
    sizes.push_back(512 * 1024);

    for (auto iter = sizes.begin(); iter != sizes.end(); ++iter) {
        std::vector<uint8_t> message;
        buildSetStream(message, *iter);
        Connection c(host, port, message.data(), message.size());

        int end = time(NULL) + duration;
        c.start();

        while (time(NULL) < (end)) {
            std::cout << "\rSet test with objects of " << *iter << " bytes: "
                      << c.getOpsPerSec() << " set/sec";
            std::cout.flush();
            sleep(1);
        }

        // Loop and print stats for the duration
        c.stop();

        std::cout << "\r " << *iter << " bytes: "
                  << "Duration " << c.getDuration()
                  << "s Total ops " << c.getTotalOps()
                  << " avg: " << c.getOpsPerSec() << std::endl;
        std::cout.flush();
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
    std::string host("localhost");
    std::string port("12000");
    int duration = 60;
    char *ptr;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:d:")) != EOF) {
        switch (cmd) {
        case 'h' :
            ptr = strchr(optarg, ':');
            if (ptr != NULL) {
                *ptr = '\0';
                port.assign(ptr + 1);
            }
            host.assign(optarg);
            break;
        case 'p' :
            port.assign(optarg);
            break;
        case 'd':
            duration = atoi(optarg);
            break;
        default:
            fprintf(stderr,
                    "Usage mcbench [-h host[:port]] [-p port] [-d duration]\n");
            return 1;
        }
    }

    set_test(host, port, duration);

    return 0;
}
