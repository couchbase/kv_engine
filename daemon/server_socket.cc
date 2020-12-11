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

#include "server_socket.h"

#include "front_end_thread.h"
#include "listening_port.h"
#include "memcached.h"
#include "network_interface.h"
#include "settings.h"
#include "stats.h"

#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <exception>
#include <memory>
#include <string>

std::atomic<uint64_t> ServerSocket::numInstances{0};

/**
 * The listen_event_handler is the callback from libevent when someone is
 * connecting to one of the server sockets. It runs in the context of the
 * listen thread
 */
void ServerSocket::listen_event_handler(evutil_socket_t, short, void* arg) {
    auto& c = *reinterpret_cast<ServerSocket*>(arg);

    if (is_memcached_shutting_down()) {
        // Someone requested memcached to shut down. The listen thread should
        // be stopped immediately to avoid new connections
        LOG_INFO("Stopping listen thread");
        event_base_loopbreak(event_get_base(c.ev.get()));
        return;
    }

    try {
        c.acceptNewClient();
    } catch (std::invalid_argument& e) {
        LOG_WARNING("{}: exception occurred while accepting clients: {}",
                    c.getSocket(),
                    e.what());
    }
}

ServerSocket::ServerSocket(SOCKET fd,
                           event_base* b,
                           std::shared_ptr<ListeningPort> interf)
    : sfd(fd),
      interface(std::move(interf)),
      sockname(cb::net::getsockname(fd)),
      ev(event_new(b,
                   sfd,
                   EV_READ | EV_PERSIST,
                   listen_event_handler,
                   reinterpret_cast<void*>(this))) {
    if (!ev) {
        throw std::bad_alloc();
    }

    std::string tagstr;
    if (!interface->tag.empty()) {
        tagstr = " \"" + interface->tag + "\"";
    }
    LOG_INFO("{} Listen on IPv{}{}: {}",
             sfd,
             interface->family == AF_INET ? "4" : "6",
             tagstr,
             sockname);
    if (cb::net::listen(sfd, backlog) == SOCKET_ERROR) {
        LOG_WARNING("{}: Failed to listen on {}: {}",
                    sfd,
                    sockname,
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (event_add(ev.get(), nullptr) == -1) {
        LOG_WARNING("Failed to add connection to libevent: {}", cb_strerror());
        ev.reset();
    }
    numInstances++;
}

ServerSocket::~ServerSocket() {
    std::string tagstr;
    if (!interface->tag.empty()) {
        tagstr = " \"" + interface->tag + "\"";
    }
    LOG_INFO("Shutting down IPv{} interface{}: {}",
             interface->family == AF_INET ? "4" : "6",
             tagstr,
             sockname);

    if (ev) {
        if (event_del(ev.get()) == -1) {
            LOG_WARNING("Failed to remove connection to libevent: {}",
                        cb_strerror());
        }
    }
    safe_close(sfd);
    numInstances--;
}

void ServerSocket::acceptNewClient() {
    sockaddr_storage addr{};
    socklen_t addrlen = sizeof(addr);
    auto client = cb::net::accept(
            sfd, reinterpret_cast<struct sockaddr*>(&addr), &addrlen);

    if (client == INVALID_SOCKET) {
        auto error = cb::net::get_socket_error();
        LOG_WARNING("Failed to accept new client: {}", cb_strerror(error));
        return;
    }

    stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
    if (cb::net::set_socket_noblocking(client) == -1) {
        LOG_WARNING("Failed to make socket non-blocking. closing it");
        safe_close(client);
        return;
    }

    // Check if we're exceeding the connection limits
    size_t current;
    size_t limit;

    if (interface->system) {
        ++stats.system_conns;
        current = stats.getSystemConnections();
        limit = Settings::instance().getSystemConnections();
    } else {
        current = stats.getUserConnections();
        limit = Settings::instance().getMaxUserConnections();
    }

    LOG_DEBUG("Accepting client {} of {}{}",
              current,
              limit,
              interface->system ? " on system port" : "");
    if (current > limit) {
        stats.rejected_conns++;
        LOG_WARNING(
                "Shutting down client as we're running "
                "out of connections{}: {} of {}",
                interface->system ? " on system interface" : "",
                current,
                limit);
        safe_close(client);
        if (interface->system) {
            --stats.system_conns;
        }
        return;
    }

    uniqueSslPtr ssl;
    bool failed = false;
    if (interface->isSslPort()) {
        try {
            ssl = createSslStructure(*interface);
            if (!ssl) {
                failed = true;
            }
        } catch (const std::exception&) {
            failed = true;
        }
    }

    if (failed) {
        LOG_WARNING(
                "{} Failed to create OpenSSL SSL structure, "
                "closing connection",
                client);
        if (interface->system) {
            --stats.system_conns;
        }
        safe_close(client);
        return;
    }

    FrontEndThread::dispatch(
            client, interface->system, interface->port, std::move(ssl));
}

nlohmann::json ServerSocket::toJson() const {
    nlohmann::json ret;

    ret["ssl"] = interface->isSslPort();

    if (interface->family == AF_INET) {
        ret["family"] = "AF_INET";
    } else {
        ret["family"] = "AF_INET6";
    }

    ret["name"] = sockname;
    ret["port"] = interface->port;
    ret["system"] = interface->system;
    ret["tag"] = interface->tag;

    return ret;
}

void ServerSocket::updateSSL(const std::string& key, const std::string& cert) {
    std::stringstream ss;
    ss << sfd << ": Update interface properties for IPv";
    if (interface->family == AF_INET) {
        ss << "4";
    } else {
        ss << "6";
    }
    ss << ": " << sockname << " ";
    if (!key.empty()) {
        ss << "SSL: key: " << key << " cert: " << cert;
    }
    if (!interface->tag.empty()) {
        ss << " (" << interface->tag << ")";
    }
    LOG_INFO(ss.str());
    interface = std::make_shared<ListeningPort>(interface->tag,
                                                interface->host,
                                                interface->port,
                                                interface->family,
                                                interface->system,
                                                key,
                                                cert);
}
