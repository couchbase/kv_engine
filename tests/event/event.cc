/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

// This is a small test program used to test / experiment with libevent
// functions and behavior. It is not needed by Couchbase
#include <event2/event.h>
#include <string.h>
#include <iostream>
#include <getopt.h>
#ifndef WIN32
#include <netinet/in.h>
#include <signal.h>

#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#endif

static evutil_socket_t new_socket(struct addrinfo* ai, bool nonblock = true) {
    evutil_socket_t sfd;

    sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (sfd == INVALID_SOCKET) {
        return INVALID_SOCKET;
    }

    if (nonblock) {
        if (evutil_make_socket_nonblocking(sfd) == -1) {
            evutil_closesocket(sfd);
            return INVALID_SOCKET;
        }
    }

    return sfd;
}

void client_callback(evutil_socket_t sock, short mask, void* arg) {
    std::cerr << "Client wakeup for " << sock << std::endl;
    char block[8192];

    // try to fill the pipe
    int ret;
    do {
        ret = send(sock, block, sizeof(block), 0);
    } while (ret > 0);

    std::cout << "send return " << ret << std::endl;
    if (ret == -1) {
        std::cerr << "Client return with " << strerror(errno) << std::endl;
        switch (errno) {
        case EPIPE:
        case ECONNRESET:
           std::cerr << "Connection reset.. Terminating" << std::endl;
           exit(EXIT_SUCCESS);
        default:
           ;
        }
    }
}

void base_callback(evutil_socket_t sock, short mask, void* arg) {
    auto client = accept(sock, nullptr, nullptr);

    if (evutil_make_socket_nonblocking(client) == -1) {
       std::cerr << "Failed to make the client nonblocking" << std::endl;
        evutil_closesocket(client);
        exit(EXIT_FAILURE);
    }


    auto* evbase = reinterpret_cast<event_base*>(arg);

    auto ev = event_new(evbase, client, EV_WRITE | EV_PERSIST, client_callback,
                        reinterpret_cast<void*>(evbase));
    if (ev == nullptr) {
        std::cerr << "Failed to allocate event for client " << client <<
        std::endl;
        exit(EXIT_FAILURE);
    }
    if (event_add(ev, nullptr) == -1) {
        std::cerr << "Failed to add client event" << std::endl;
        exit(EXIT_FAILURE);
    }
}

int server(const std::string &hostname, const std::string &port) {
#ifndef WIN32
    sigignore(SIGPIPE);
#endif
    auto evbase = event_base_new();

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_INET;


    struct addrinfo* ai = nullptr;
    int error = getaddrinfo(hostname.c_str(), port.c_str(), &hints, &ai);

    if (error != 0) {
        std::cerr << "getaddrinfo(): " << strerror(error) << std::endl;

        return EXIT_FAILURE;
    }

    for (struct addrinfo* next = ai; next != nullptr; next = next->ai_next) {
        evutil_socket_t sfd;

        if ((sfd = new_socket(next)) == INVALID_SOCKET) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            continue;
        }

#ifndef WIN32
        const struct linger ling = {0, 0};
        const int flags = 1;
        const void* ling_ptr = reinterpret_cast<const char*>(&ling);
        const void* flags_ptr = reinterpret_cast<const void*>(&flags);

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, flags_ptr, sizeof(flags));
        error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, flags_ptr,
                           sizeof(flags));
        if (error != 0) {
            std::cerr << "setsockopt(SO_KEEPALIVE): " << strerror(errno) <<
            std::endl;
        }
        error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, ling_ptr, sizeof(ling));
        if (error != 0) {
            std::cerr << "setsockopt(SO_LINGER): " << strerror(errno) <<
            std::endl;
        }
#endif

        if (bind(sfd, next->ai_addr, (socklen_t) next->ai_addrlen) ==
            SOCKET_ERROR) {
            std::cerr << "bind(): " << strerror(errno) << std::endl;
            evutil_closesocket(sfd);
            freeaddrinfo(ai);
            continue;
        } else {
            if (listen(sfd, 10) == SOCKET_ERROR) {
                std::cerr << "listen(): " << strerror(errno) << std::endl;
                evutil_closesocket(sfd);
                freeaddrinfo(ai);
                return EXIT_FAILURE;
            }
        }

        auto ev = event_new(evbase, sfd, EV_READ | EV_PERSIST, base_callback,
                            reinterpret_cast<void*>(evbase));
        if (ev == nullptr) {
            std::cerr << "Failed to allocate event" << std::endl;
            return EXIT_FAILURE;
        }
        if (event_add(ev, nullptr) == -1) {
            std::cerr << "Failed to add event" << std::endl;
            return EXIT_FAILURE;
        }
    }

    freeaddrinfo(ai);

    std::cout << "Server ready to use on " << hostname.c_str()
             << ":" << port.c_str() << std::endl;
    event_base_loop(evbase, 0);

    return EXIT_SUCCESS;
}

int client(const std::string &hostname, const std::string &port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_INET;

    struct addrinfo* ai = nullptr;
    int error = getaddrinfo(hostname.c_str(), port.c_str(), &hints, &ai);

    if (error != 0) {
        std::cerr << "getaddrinfo(): " << strerror(error) << std::endl;

        return EXIT_FAILURE;
    }

    int clients = 0;
    for (struct addrinfo* next = ai; next != nullptr; next = next->ai_next) {
        evutil_socket_t sfd;

        if ((sfd = new_socket(next, false)) == INVALID_SOCKET) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            continue;
        }

        if (connect(sfd, ai->ai_addr, ai->ai_addrlen) == SOCKET_ERROR) {
            std::cerr << "Failed to connect to server: " << strerror(errno) << std::endl;
            evutil_closesocket(sfd);
            continue;
        } else {
            ++clients;
        }
    }
    freeaddrinfo(ai);


    std::cerr << "connected " << clients << "..." << std::endl;
    int a;
    std::cin >> a;

    return EXIT_SUCCESS;
}

int main(int argc, char** argv) {

    int cmd;

    while ((cmd = getopt(argc, argv, "cs")) != -1) {
        switch (cmd) {
        case 'c' :
            return client("127.0.0.1", "6666");
        case 's' :
            return server("127.0.0.1", "6666");
        default:
            std::cerr << "Usage: eventtest [-s] [-c]" << std::endl;
        }
    }

    return EXIT_FAILURE;
}
