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
    nlohmann::json to_json() const override;

    const std::string& getUuid() const override {
        return uuid;
    }

protected:
    /// Set the various TCP Keepalive options to the provided socket.
    void setTcpKeepalive(SOCKET client);

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

    /// Is TLS configured yet?
    bool tls_configured = false;

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
    try {
        c.acceptNewClient();
    } catch (std::invalid_argument& e) {
        LOG_WARNING_CTX("exception occurred while accepting clients",
                        {"socket", c.getSocket()},
                        {"error", e.what()});
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
                   this)) {
    if (!ev) {
        throw std::bad_alloc();
    }

#ifdef __linux__
    if (!interface->system) {
        // Set the current configured value so that it appears in the logs.
        // We will however try to set it to the "current" value once the
        // client connects (as the value is dynamic)
        auto timeout =
                Settings::instance().getTcpUnauthenticatedUserTimeout().count();
        if (!cb::net::setSocketOption<uint32_t>(
                    sfd,
                    IPPROTO_TCP,
                    TCP_USER_TIMEOUT,
                    gsl::narrow_cast<uint32_t>(timeout))) {
            LOG_WARNING_CTX(
                    "Failed to set TCP_USER_TIMEOUT",
                    {"socket", sfd},
                    {"to", timeout},
                    {"error", cb_strerror(cb::net::get_socket_error())});
        }
    }
#endif

    auto properties = cb::net::getSocketOptions(sfd);
    if (!interface->tag.empty()) {
        properties["tag"] = interface->tag;
    }

    LOG_INFO_CTX("Listen",
                 {"protocol", interface->family == AF_INET ? "IPv4" : "IPv6"},
                 {"socket", sfd},
                 {"address", sockname},
                 {"tls", interface->tls},
                 {"properties", properties});
    if (cb::net::listen(sfd, backlog) == SOCKET_ERROR) {
        LOG_WARNING_CTX("Failed to listen",
                        {"socket", sfd},
                        {"address", sockname},
                        {"error", cb_strerror(cb::net::get_socket_error())});
    }

    if (event_add(ev.get(), nullptr) == -1) {
        LOG_WARNING_CTX("Failed to add connection to libevent",
                        {"error", cb_strerror()});
        ev.reset();
    }
    numInstances++;
}

LibeventServerSocketImpl::~LibeventServerSocketImpl() {
    interface->valid.store(false, std::memory_order_release);
    std::string tagstr;
    if (!interface->tag.empty()) {
        tagstr = " \"" + interface->tag + "\"";
    }
    LOG_INFO_CTX("Shutting down interface",
                 {"protocol", interface->family == AF_INET ? "IPv4" : "IPv6"},
                 {"tag", tagstr},
                 {"address", sockname});

    if (ev) {
        if (event_del(ev.get()) == -1) {
            LOG_WARNING_CTX("Failed to remove connection from libevent",
                            {"error", cb_strerror()});
        }
    }
    close_server_socket(sfd);
    numInstances--;
}

void LibeventServerSocketImpl::acceptNewClient() {
    static const bool dont_disconnect_during_shutdown =
            getenv("MEMCACHED_UNIT_TEST");
    sockaddr_storage addr{};
    socklen_t addrlen = sizeof(addr);
    auto client = cb::net::accept(
            sfd, reinterpret_cast<struct sockaddr*>(&addr), &addrlen);

    if (client == INVALID_SOCKET) {
        auto error = cb::net::get_socket_error();
        LOG_WARNING_CTX("Failed to accept new client",
                        {"error", cb_strerror(error)});
        return;
    }

    ++global_statistics.curr_conns;
    if (cb::net::set_socket_noblocking(client) == -1) {
        LOG_WARNING_RAW("Failed to make socket non-blocking. closing it");
        close_client_socket(client);
        return;
    }

    if (is_memcached_shutting_down() && dont_disconnect_during_shutdown) {
        close_client_socket(client);
        return;
    }

    if (interface->system) {
        try {
            LOG_INFO_CTX("Accepting client on system interface",
                         {"conn_id", client},
                         {"peer", cb::net::getPeerNameAsJson(client)});
        } catch (const std::exception& e) {
            LOG_INFO_CTX("Accepting client on system interface",
                         {"conn_id", client},
                         {"error", e.what()});
        }
    }

    if (interface->tls && !tls_configured) {
        tls_configured = networkInterfaceManager->isTlsConfigured();
        if (!tls_configured) {
            try {
                LOG_INFO_CTX(
                        "MB-58154: Closing connection as TLS isn't configured "
                        "yet",
                        {"conn_id", client},
                        {"peer", cb::net::getPeerNameAsJson(client)});

            } catch (const std::exception& e) {
                LOG_INFO_CTX(
                        "MB-58154: Closing connection as TLS isn't configured "
                        "yet",
                        {"conn_id", client},
                        {"error", e.what()});
            }
            close_client_socket(client);
            return;
        }
    }

    // Check if we're exceeding the connection limits
    size_t current;
    size_t limit;

    if (interface->system) {
        ++global_statistics.system_conns;
        current = global_statistics.getSystemConnections();
        limit = Settings::instance().getSystemConnections();
    } else {
        current = global_statistics.getUserConnections();
        limit = Settings::instance().getMaxUserConnections();
    }

    if (current > limit) {
        global_statistics.rejected_conns++;
        LOG_WARNING_CTX(
                "Shutting down client as we're running out of connections",
                {"system", interface->system},
                {"current", current},
                {"limit", limit});
        close_client_socket(client);
        if (interface->system) {
            --global_statistics.system_conns;
        }
        return;
    }

    // Connections connecting to a system port should use the TCP Keepalive
    // configuration configured in the OS and be protected against
    // TCP_USER_TIMEOUT
    if (!interface->system) {
        setTcpKeepalive(client);

#ifdef __linux__
        auto timeout = gsl::narrow_cast<uint32_t>(
                Settings::instance()
                        .getTcpUnauthenticatedUserTimeout()
                        .count());
        if (!cb::net::setSocketOption<uint32_t>(
                    sfd, IPPROTO_TCP, TCP_USER_TIMEOUT, timeout)) {
            LOG_WARNING_CTX(
                    "Failed to set TCP_USER_TIMEOUT",
                    {"socket", sfd},
                    {"to", timeout},
                    {"error", cb_strerror(cb::net::get_socket_error())});
        }
#endif
    }

    constexpr int flags = 1;
    if (cb::net::setsockopt(
                client, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags)) ==
        -1) {
        LOG_WARNING_CTX("cb::net::setsockopt TCP_NODELAY failed",
                        {"socket", static_cast<uint64_t>(client)},
                        {"error", cb_strerror(cb::net::get_socket_error())});
    }

    FrontEndThread::dispatch(client, interface);
}

void LibeventServerSocketImpl::setTcpKeepalive(SOCKET client) {
    auto& settings = Settings::instance();
    const auto idle = settings.getTcpKeepAliveIdle();
    if (idle.count() != 0) {
        auto t = gsl::narrow_cast<uint32_t>(idle.count());
        if (cb::net::setsockopt(
                    client, IPPROTO_TCP, TCP_KEEPIDLE, &t, sizeof(t)) == -1) {
            LOG_WARNING_CTX(
                    "Failed to set TCP_KEEPIDLE",
                    {"conn_id", client},
                    {"error", cb_strerror(cb::net::get_socket_error())});
        }
    }
    const auto interval = settings.getTcpKeepAliveInterval();
    if (interval.count() != 0) {
        auto val = gsl::narrow_cast<uint32_t>(interval.count());
        if (cb::net::setsockopt(
                    client, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) ==
            -1) {
            LOG_WARNING_CTX(
                    "Failed to set TCP_KEEPINTVL",
                    {"conn_id", client},
                    {"error", cb_strerror(cb::net::get_socket_error())});
        }
    }
    const auto probes = settings.getTcpKeepAliveProbes();
    if (probes) {
        if (cb::net::setsockopt(client,
                                IPPROTO_TCP,
                                TCP_KEEPCNT,
                                &probes,
                                sizeof(probes)) == -1) {
            LOG_WARNING_CTX(
                    "Failed to set TCP_KEEPCNT",
                    {"conn_id", client},
                    {"error", cb_strerror(cb::net::get_socket_error())});
        }
    }
}

nlohmann::json LibeventServerSocketImpl::to_json() const {
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
