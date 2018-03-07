/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

// moxi_hammer is a command line utility to drive load to a moxi server
// to see how it works under pressure. It works by populating a buffer with
// commands it wants to send to the server, and then loops over and over
// sending that buffer to the server. It spins up a configurable number
// of connections to the server and they all send the same buffer, but
// start at a different offset (all by sending a set command).

#include "config.h"

#include <event2/event.h>
#include <platform/socket.h>

#include <getopt.h>
#include <algorithm>
#include <array>
#include <gsl/gsl>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <random>
#include <sstream>
#include <system_error>
#include <vector>

// We don't care about the response sent from moxi
static std::array<char, 65 * 1024> sink;

// This is the preformatted stream of commands we want to send to moxi
std::vector<char> data;

class Connection {
public:
    Connection(SOCKET sock, size_t off) : sfd(sock), offset(off) {
        // empty
    }

    void drain() {
        while (cb::net::recv(sfd, sink.data(), sink.size(), 0) > 0) {
            // drop data;
        }
    }

    void fill() {
        do {
            ssize_t nw = cb::net::send(
                    sfd, data.data() + offset, data.size() - offset, 0);
            if (nw > 0) {
                offset += nw;
                if (offset == data.size()) {
                    offset = 0;
                }
            } else if (nw == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return;
            } else {
                throw std::system_error(
                        errno, std::system_category(), "Failed to send data");
            }
        } while (true);
    }

    SOCKET getSocket() {
        return sfd;
    }

private:
    // The socket used in communication
    SOCKET sfd;

    // Current offset in the buffer
    size_t offset;
};

static void event_handler(evutil_socket_t, short which, void* arg) {
    auto* conn = reinterpret_cast<Connection*>(arg);

    if ((which & EV_READ) == EV_READ) {
        conn->drain();
    }

    if ((which & EV_WRITE) == EV_WRITE) {
        conn->fill();
    }
}

SOCKET new_socket(std::string host,
                  const std::string& port,
                  sa_family_t family) {
    addrinfo hints = {};
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = family;

    if (host.empty() || host == "localhost") {
        if (family == AF_INET) {
            host.assign("127.0.0.1");
        } else if (family == AF_INET6) {
            host.assign("::1");
        } else if (family == AF_UNSPEC) {
            host.assign("localhost");
        }
    }

    int error;
    addrinfo* ai;
    error = getaddrinfo(host.c_str(), port.c_str(), &hints, &ai);

    if (error != 0) {
        throw std::system_error(error,
                                std::system_category(),
                                "Failed to resolve address \"" + host + "\"");
    }

    for (struct addrinfo* next = ai; next; next = next->ai_next) {
        SOCKET sfd =
                socket(next->ai_family, next->ai_socktype, next->ai_protocol);
        if (sfd != INVALID_SOCKET) {
            if (cb::net::connect(sfd,
                                 next->ai_addr,
                                 gsl::narrow<size_t>(next->ai_addrlen)) !=
                SOCKET_ERROR) {
                if (evutil_make_socket_nonblocking(sfd) != -1) {
                    freeaddrinfo(ai);
                    return sfd;
                }
            }
            cb::net::closesocket(sfd);
        }
    }

    freeaddrinfo(ai);
    return INVALID_SOCKET;
}

static std::string getKey(uint64_t num) {
    return std::to_string(num % 1000);
}

/**
 * Build up the preformatted stream of operations we're going to send to
 * moxi.
 *
 * @todo add support for all kinds of operations
 * @todo add the option to configure the type of load
 * @todo add support to configure the keyspace
 *
 * @param streamsize The size of the stream to send
 * @param itemsize The size of each object to store
 */
static void build_stream(size_t streamsize, size_t itemsize) {
    // to avoid a ton of reallocs initially
    data.reserve(streamsize + 1024 * 1024);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    std::string command;
    std::string key;
    uint16_t total;
    size_t sz;

    while (data.size() < streamsize) {
        switch (dis(gen) % 10) {
        case 0:
        case 1:
        case 2:
            sz = dis(gen) % itemsize;
            command.assign("set ");
            command.append(getKey(dis(gen)));
            command.append(" 0 0 ");
            command.append(std::to_string(sz));
            command.append("\r\n");
            std::copy(command.begin(), command.end(), std::back_inserter(data));
            for (size_t ii = 0; ii < sz; ++ii) {
                data.push_back('a');
            }
            data.push_back('\r');
            data.push_back('\n');
            break;
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
        case 8:
        case 9:
            // insert get(s)
            total = gsl::narrow<uint16_t>(dis(gen) + 1);
            command.assign("get ");
            for (uint16_t ii = 0; ii < total; ii++) {
                command.append(getKey(dis(gen)));
                command.append(" ");
            }
            command.append("\r\n");
            std::copy(command.begin(), command.end(), std::back_inserter(data));
            break;
        }
    }
}

static void hammer_thread(std::string hostname,
                          std::string port,
                          sa_family_t family,
                          size_t streamsize,
                          size_t num_connections) {
    std::cout << "Setting up connections...";
    std::cout.flush();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;

    auto* base = event_base_new();
    // Add all of the connections
    for (size_t ii = 0; ii < num_connections; ++ii) {
        // Connect and make socket non-blocking
        auto sock = new_socket(hostname, port, family);
        if (sock == INVALID_SOCKET) {
            std::cerr << "Failed to connect to " << hostname << ":" << port
                      << std::endl;
            exit(EXIT_FAILURE);
        }

        // Let each connect attempt start somewhere in the stream, and start
        // with a set command...
        auto offset = dis(gen) % streamsize;
        auto start = std::find(data.begin() + offset, data.end(), 's');
        if (start == data.end()) {
            start = data.begin();
        }
        auto* c = new Connection(sock, size_t(start - data.begin()));

        // add to libevent
        auto* event = event_new(base,
                                c->getSocket(),
                                EV_READ | EV_WRITE | EV_PERSIST,
                                event_handler,
                                c);
        if (event_add(event, nullptr) == -1) {
            std::cerr << "Failed to add event" << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    std::cout << "done" << std::endl;
    std::cout << "Hammer moxi....";
    std::cout.flush();

    // Hammer moxi
    try {
        event_base_loop(base, 0);
    } catch (const std::system_error& e) {
        std::cout << std::endl;
        std::cerr << e.what() << std::endl;
        std::cerr << "code: " << e.code().value() << std::endl;
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char** argv) {
    struct option long_options[] = {
            {"streamsize", required_argument, nullptr, 's'},
            {"host", required_argument, nullptr, 'h'},
            {"port", required_argument, nullptr, 'p'},
            {"itemsize", required_argument, nullptr, 'i'},
            {"ipv6", no_argument, nullptr, '6'},
            {"connections", required_argument, nullptr, 'c'},
            {"help", no_argument, nullptr, '?'},
            {nullptr, 0, nullptr, 0}};

    std::string hostname;
    std::string port{"11211"};
    sa_family_t family{AF_INET};
    size_t streamsize = /* 64* */ 1024 * 1024;
    size_t itemsize = 100;
    size_t num_connections = 100;

    // @todo add support for multiple threads
    // @todo add support for specifying the chunk size to send to the server

    int cmd;
    while ((cmd = getopt_long(
                    argc, argv, "s:h:p:i:6c:?", long_options, nullptr)) != EOF) {
        switch (cmd) {
        case 's':
            streamsize = size_t(atoi(optarg) * 1024 * 1024);
            break;
        case 'h':
            hostname.assign(optarg);
            break;
        case 'p':
            port.assign(optarg);
            break;
        case 'i':
            itemsize = size_t(atoi(optarg));
            break;
        case '6':
            family = sa_family_t(AF_INET6);
            break;
        case 'c':
            num_connections = size_t(atoi(optarg));
            break;
        default:
            std::cerr << "moxi_hammer [options]" << std::endl
                      << "  --streamsize=xx   The size of the stream in MB\n"
                      << "  --itemsize=xx     The size of each item in bytes\n"
                      << "  --host=name       The host moxi is running at\n"
                      << "  --port=port       The port moxi is listening at\n"
                      << "  --ipv6            Use IPv6\n"
                      << "  --connections=nn  The number of connections to open"
                      << std::endl;
            return EXIT_FAILURE;
        }
    }

    // Format the stream
    std::cout << "Formatting the stream...";
    std::cout.flush();
    build_stream(streamsize, itemsize);
    std::cout << " done" << std::endl;

    hammer_thread(hostname, port, family, streamsize, num_connections);

    return EXIT_SUCCESS;
}
