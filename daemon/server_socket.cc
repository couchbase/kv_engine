/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "server_socket.h"

#include "front_end_thread.h"
#include "listening_port.h"
#include "memcached.h"
#include "network_interface_manager.h"
#include "settings.h"
#include "stats.h"

#include <folly/io/async/EventBase.h>
#include <libevent/utilities.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/uuid.h>
#include <exception>
#include <memory>
#include <string>

std::atomic<uint64_t> ServerSocket::numInstances{0};

class LibeventServerSocketImpl : public ServerSocket {
public:
    LibeventServerSocketImpl() = delete;
    LibeventServerSocketImpl(const LibeventServerSocketImpl&) = delete;

    /**
     * Create a new instance
     *
     * @param sfd The socket to operate on
     * @param b The event base to use (the caller owns the event base)
     * @param interf The interface object containing properties to use
     */
    LibeventServerSocketImpl(SOCKET sfd,
                             folly::EventBase& b,
                             std::shared_ptr<ListeningPort> interf);

    ~LibeventServerSocketImpl() override;

    /**
     * Get the socket (used for logging)
     */
    SOCKET getSocket() {
        return sfd;
    }

    void acceptNewClient();

    const ListeningPort& getInterfaceDescription() const override {
        return *interface;
    }

    /**
     * Get the details for this connection to put in the portnumber
     * file so that the test framework may pick up the port numbers
     */
    nlohmann::json toJson() const override;

    const std::string& getUuid() const override {
        return uuid;
    }

protected:
    /// The socket object to accept clients from
    const SOCKET sfd;

    /// The unique id used to identify _this_ port
    const std::string uuid;

    std::shared_ptr<ListeningPort> interface;

    /// The sockets name (used for debug)
    const std::string sockname;

    /// The backlog to specify to bind
    const int backlog = 1024;

    /// The libevent object we're using
    cb::libevent::unique_event_ptr ev;

    /// The notification handler registered in libevent
    static void listen_event_handler(evutil_socket_t, short, void* arg);
};

/**
 * The listen_event_handler is the callback from libevent when someone is
 * connecting to one of the server sockets. It runs in the context of the
 * listen thread
 */
void LibeventServerSocketImpl::listen_event_handler(evutil_socket_t,
                                                    short,
                                                    void* arg) {
    auto& c = *reinterpret_cast<LibeventServerSocketImpl*>(arg);

    if (is_memcached_shutting_down()) {
        // Someone requested memcached to shut down. The listen thread should
        // be stopped immediately to avoid new connections
        LOG_INFO_RAW("Stopping listen thread");
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

LibeventServerSocketImpl::LibeventServerSocketImpl(
        SOCKET fd, folly::EventBase& b, std::shared_ptr<ListeningPort> interf)
    : sfd(fd),
      uuid(to_string(cb::uuid::random())),
      interface(std::move(interf)),
      sockname(cb::net::getsockname(fd)),
      ev(event_new(b.getLibeventBase(),
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

LibeventServerSocketImpl::~LibeventServerSocketImpl() {
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

void LibeventServerSocketImpl::acceptNewClient() {
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
        LOG_WARNING_RAW("Failed to make socket non-blocking. closing it");
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
    if (interface->tls) {
        try {
            ssl = networkInterfaceManager->createClientSslHandle();
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

nlohmann::json LibeventServerSocketImpl::toJson() const {
    nlohmann::json ret;

    const auto sockinfo = cb::net::getSockNameAsJson(sfd);
    ret["host"] = sockinfo["ip"];
    ret["port"] = sockinfo["port"];

    if (interface->family == AF_INET) {
        ret["family"] = "inet";
    } else {
        ret["family"] = "inet6";
    }

    ret["tls"] = interface->tls;
    ret["system"] = interface->system;
    ret["tag"] = interface->tag;
    ret["type"] = "mcbp";
    ret["uuid"] = uuid;

    return ret;
}

std::unique_ptr<ServerSocket> ServerSocket::create(
        SOCKET sfd,
        folly::EventBase& b,
        std::shared_ptr<ListeningPort> interf) {
    return std::make_unique<LibeventServerSocketImpl>(sfd, b, interf);
}
