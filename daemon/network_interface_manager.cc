/*
 *     Copyright 2020 Couchbase, Inc.
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
#include "network_interface_manager.h"

#include "front_end_thread.h"
#include "listening_port.h"
#include "log_macros.h"
#include "memcached.h"
#include "server_socket.h"
#include "settings.h"
#include "ssl_utils.h"
#include "stats.h"

#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/strerror.h>

namespace cb::prometheus {
// forward declaration
std::pair<in_port_t, sa_family_t> getRunningConfig();
} // namespace cb::prometheus

std::unique_ptr<NetworkInterfaceManager> networkInterfaceManager;

NetworkInterfaceManager::NetworkInterfaceManager(event_base* base) {
    if (!create_nonblocking_socketpair(pipe)) {
        FATAL_ERROR(EXIT_FAILURE, "Unable to create notification pipe");
    }

    event.reset(event_new(base,
                          pipe[0],
                          EV_READ | EV_PERSIST,
                          NetworkInterfaceManager::event_handler,
                          this));

    if (!event) {
        FATAL_ERROR(EXIT_FAILURE, "Failed to allocate network manager event");
    }

    if (event_add(event.get(), nullptr) == -1) {
        FATAL_ERROR(EXIT_FAILURE, "Can't setup network manager event");
    }

    LOG_INFO("Enable port(s)");
    for (auto& interf : Settings::instance().getInterfaces()) {
        if (!createInterface(interf.tag,
                             interf.host,
                             interf.port,
                             interf.system,
                             interf.ssl.key,
                             interf.ssl.cert,
                             interf.ipv4,
                             interf.ipv6)) {
            FATAL_ERROR(EXIT_FAILURE,
                        "Failed to create required listening socket(s). "
                        "Terminating.");
        }
    }

    writeInterfaceFile(true);
}

void NetworkInterfaceManager::signal() {
    check_listen_conn = true;
    if (pipe[1] != INVALID_SOCKET) {
        if (cb::net::send(pipe[1], "", 1, 0) != 1 &&
            !cb::net::is_blocking(cb::net::get_socket_error())) {
            LOG_WARNING("Failed to notify network manager: {}",
                        cb_strerror(cb::net::get_socket_error()));
        }
    }
}

void NetworkInterfaceManager::event_handler(evutil_socket_t fd,
                                            short,
                                            void* arg) {
    auto& me = *reinterpret_cast<NetworkInterfaceManager*>(arg);
    // Start by draining the notification pipe fist
    drain_notification_channel(fd);
    me.event_handler();
}

void NetworkInterfaceManager::event_handler() {
    if (check_listen_conn) {
        invalidateSslCache();
        check_listen_conn = false;

        bool changes = false;
        auto interfaces = Settings::instance().getInterfaces();

        // Step one, enable all new ports
        bool success = true;
        for (const auto& interface : interfaces) {
            const bool useTag = interface.port == 0;
            bool ipv4 = interface.ipv4 != NetworkInterface::Protocol::Off;
            bool ipv6 = interface.ipv6 != NetworkInterface::Protocol::Off;

            for (const auto& connection : listen_conn) {
                const auto& descr = connection->getInterfaceDescription();

                if ((useTag && (descr.tag == interface.tag)) ||
                    (!useTag && (descr.port == interface.port))) {
                    if (descr.family == AF_INET) {
                        ipv4 = false;
                    } else {
                        ipv6 = false;
                    }

                    if (!ipv4 && !ipv6) {
                        // no need to search anymore as we've got both
                        break;
                    }
                }
            }

            if (ipv4) {
                // create an IPv4 interface
                changes = true;
                success &= createInterface(interface.tag,
                                           interface.host,
                                           interface.port,
                                           interface.system,
                                           interface.ssl.key,
                                           interface.ssl.cert,
                                           interface.ipv4,
                                           NetworkInterface::Protocol::Off);
            }

            if (ipv6) {
                // create an IPv6 interface
                changes = true;
                success &= createInterface(interface.tag,
                                           interface.host,
                                           interface.port,
                                           interface.system,
                                           interface.ssl.key,
                                           interface.ssl.cert,
                                           NetworkInterface::Protocol::Off,
                                           interface.ipv6);
            }
        }

        // Step two, shut down ports if we didn't fail opening new ports
        if (success) {
            for (auto iter = listen_conn.begin(); iter != listen_conn.end();
                 /* empty */) {
                auto& connection = *iter;
                const auto& descr = connection->getInterfaceDescription();
                // should this entry be here:
                bool drop = true;
                for (const auto& interface : interfaces) {
                    if (descr.tag.empty()) {
                        if (interface.port != descr.port) {
                            // port mismatch... look at the next
                            continue;
                        }
                    } else if (descr.tag != interface.tag) {
                        // Tag mismatch... look at the next
                        continue;
                    }

                    if ((descr.family == AF_INET &&
                         interface.ipv4 != NetworkInterface::Protocol::Off) ||
                        (descr.family == AF_INET6 &&
                         interface.ipv6 != NetworkInterface::Protocol::Off)) {
                        drop = false;
                    }

                    if (!drop) {
                        if (descr.sslKey != interface.ssl.key ||
                            descr.sslCert != interface.ssl.cert) {
                            // change the associated description
                            connection->updateSSL(interface.ssl.key,
                                                  interface.ssl.cert);
                        }
                    }

                    break;
                }
                if (drop) {
                    // erase returns the element following this one (or end())
                    changes = true;
                    iter = listen_conn.erase(iter);
                } else {
                    // look at the next element
                    ++iter;
                }
            }
        }

        auto prometheus_config = cb::prometheus::getRunningConfig();
        if (prometheus_conn != prometheus_config) {
            // current prometheus MetricServer config does not match
            // what was last written to disk.
            prometheus_conn = prometheus_config;
            changes = true;
        }

        if (changes) {
            // Try to write all of the changes, ignore errors
            writeInterfaceFile(false);
        }
    }
}

void NetworkInterfaceManager::writeInterfaceFile(bool terminate) {
    auto& settings = Settings::instance();

    auto filename = settings.getPortnumberFile();
    if (!filename.empty()) {
        nlohmann::json json;
        json["ports"] = nlohmann::json::array();

        for (const auto& connection : listen_conn) {
            json["ports"].push_back(connection->toJson());
        }

        {
            auto [port, family] = cb::prometheus::getRunningConfig();
            if (family == AF_INET || family == AF_INET6) {
                json["prometheus"]["port"] = port;
                json["prometheus"]["family"] =
                        (family == AF_INET) ? "inet" : "inet6";
            }
        }

        std::string tempname;
        tempname.assign(filename);
        tempname.append(".lck");

        FILE* file = nullptr;
        file = fopen(tempname.c_str(), "a");
        if (file == nullptr) {
            LOG_CRITICAL(R"(Failed to open "{}": {})", tempname, cb_strerror());
            if (terminate) {
                exit(EXIT_FAILURE);
            }
            return;
        }

        fprintf(file, "%s\n", json.dump().c_str());
        fclose(file);
        if (cb::io::isFile(filename)) {
            cb::io::rmrf(filename);
        }

        LOG_INFO("Port numbers available in {}", filename);
        if (rename(tempname.c_str(), filename.c_str()) == -1) {
            LOG_CRITICAL(R"(Failed to rename "{}" to "{}": {})",
                         tempname,
                         filename,
                         cb_strerror());
            if (terminate) {
                exit(EXIT_FAILURE);
            }
            cb::io::rmrf(tempname);
        }
    }
}

/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const SOCKET sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int old_size;

    /* Start with the default size. */
    if (cb::net::getsockopt(sfd,
                            SOL_SOCKET,
                            SO_SNDBUF,
                            reinterpret_cast<void*>(&old_size),
                            &intsize) != 0) {
        LOG_WARNING("getsockopt(SO_SNDBUF): {}", strerror(errno));
        return;
    }

    /* Binary-search for the real maximum. */
    int min = old_size;
    int max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        int avg = ((unsigned int)(min + max)) / 2;
        if (cb::net::setsockopt(sfd,
                                SOL_SOCKET,
                                SO_SNDBUF,
                                reinterpret_cast<void*>(&avg),
                                intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    LOG_DEBUG("<{} send buffer was {}, now {}", sfd, old_size, last_good);
}

static SOCKET new_server_socket(struct addrinfo* ai) {
    auto sfd = cb::net::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (sfd == INVALID_SOCKET) {
        return INVALID_SOCKET;
    }

    if (evutil_make_socket_nonblocking(sfd) == -1) {
        safe_close(sfd);
        return INVALID_SOCKET;
    }

    maximize_sndbuf(sfd);

    const struct linger ling = {0, 0};
    const int flags = 1;
    int error;

#ifdef IPV6_V6ONLY
    if (ai->ai_family == AF_INET6) {
        error = cb::net::setsockopt(sfd,
                                    IPPROTO_IPV6,
                                    IPV6_V6ONLY,
                                    reinterpret_cast<const void*>(&flags),
                                    sizeof(flags));
        if (error != 0) {
            LOG_WARNING("setsockopt(IPV6_V6ONLY): {}", strerror(errno));
            safe_close(sfd);
            return INVALID_SOCKET;
        }
    }
#endif

    if (cb::net::setsockopt(sfd,
                            SOL_SOCKET,
                            SO_REUSEADDR,
                            reinterpret_cast<const void*>(&flags),
                            sizeof(flags)) != 0) {
        LOG_WARNING("setsockopt(SO_REUSEADDR): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (cb::net::setsockopt(sfd,
                            SOL_SOCKET,
                            SO_KEEPALIVE,
                            reinterpret_cast<const void*>(&flags),
                            sizeof(flags)) != 0) {
        LOG_WARNING("setsockopt(SO_KEEPALIVE): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (cb::net::setsockopt(sfd,
                            SOL_SOCKET,
                            SO_LINGER,
                            reinterpret_cast<const char*>(&ling),
                            sizeof(ling)) != 0) {
        LOG_WARNING("setsockopt(SO_LINGER): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    return sfd;
}

bool NetworkInterfaceManager::createInterface(const std::string& tag,
                                              const std::string& host,
                                              in_port_t port,
                                              bool system_port,
                                              const std::string& sslkey,
                                              const std::string& sslcert,
                                              NetworkInterface::Protocol iv4,
                                              NetworkInterface::Protocol iv6) {
    SOCKET sfd;
    addrinfo hints = {};

    // Set to true when we create an IPv4 interface
    bool ipv4 = false;
    // Set to true when we create an IPv6 interface
    bool ipv6 = false;

    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;

    if (iv4 != NetworkInterface::Protocol::Off &&
        iv6 != NetworkInterface::Protocol::Off) {
        hints.ai_family = AF_UNSPEC;
    } else if (iv4 != NetworkInterface::Protocol::Off) {
        hints.ai_family = AF_INET;
    } else if (iv6 != NetworkInterface::Protocol::Off) {
        hints.ai_family = AF_INET6;
    } else {
        throw std::invalid_argument(
                "server_socket: can't create a socket without IPv4 or IPv6");
    }

    const char* host_buf = nullptr;
    if (!host.empty() && host != "*") {
        host_buf = host.c_str();
    }

    struct addrinfo* ai;
    int error =
            getaddrinfo(host_buf, std::to_string(port).c_str(), &hints, &ai);
    if (error != 0) {
#ifdef WIN32
        LOG_WARNING("getaddrinfo(): {}", cb_strerror(error));
#else
        if (error != EAI_SYSTEM) {
            LOG_WARNING("getaddrinfo(): {}", gai_strerror(error));
        } else {
            LOG_WARNING("getaddrinfo(): {}", cb_strerror(error));
        }
#endif
        return false;
    }

    // getaddrinfo may return multiple entries for a given name/port pair.
    // Iterate over all of them and try to set up a listen object.
    // We need at least _one_ entry per requested configuration (IPv4/6) in
    // order to call it a success.
    for (struct addrinfo* next = ai; next; next = next->ai_next) {
        if (next->ai_addr->sa_family != AF_INET &&
            next->ai_addr->sa_family != AF_INET6) {
            // Ignore unsupported address families
            continue;
        }

        if ((sfd = new_server_socket(next)) == INVALID_SOCKET) {
            // getaddrinfo can return "junk" addresses,
            continue;
        }

        if (bind(sfd, next->ai_addr, (socklen_t)next->ai_addrlen) ==
            SOCKET_ERROR) {
            const auto bind_error = cb::net::get_socket_error();
            auto name = cb::net::to_string(
                    reinterpret_cast<sockaddr_storage*>(next->ai_addr),
                    static_cast<socklen_t>(next->ai_addrlen));
            LOG_WARNING(
                    "Failed to bind to {} - {}", name, cb_strerror(bind_error));
            safe_close(sfd);
            continue;
        }

        in_port_t listenport = port;
        if (listenport == 0) {
            // The interface description requested an ephemeral port to
            // be allocated. Pick up the real port number
            union {
                struct sockaddr_in in;
                struct sockaddr_in6 in6;
            } my_sockaddr{};
            socklen_t len = sizeof(my_sockaddr);
            if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len) == 0) {
                if (next->ai_addr->sa_family == AF_INET) {
                    listenport = ntohs(my_sockaddr.in.sin_port);
                } else {
                    listenport = ntohs(my_sockaddr.in6.sin6_port);
                }
            } else {
                const auto error = cb::net::get_socket_error();
                auto name = cb::net::to_string(
                        reinterpret_cast<sockaddr_storage*>(next->ai_addr),
                        static_cast<socklen_t>(next->ai_addrlen));
                LOG_WARNING(
                        "Failed to look up the assigned port for the ephemeral "
                        "port: {} error: {}",
                        name,
                        cb_strerror(error));
                safe_close(sfd);
                continue;
            }
        } else {
            listenport = port;
        }

        // We've configured this port.
        if (next->ai_addr->sa_family == AF_INET) {
            // We have at least one entry
            ipv4 = true;
        } else {
            // We have at least one entry
            ipv6 = true;
        }

        auto inter = std::make_shared<ListeningPort>(tag,
                                                     host,
                                                     listenport,
                                                     next->ai_addr->sa_family,
                                                     system_port,
                                                     sslkey,
                                                     sslcert);
        listen_conn.emplace_back(std::make_unique<ServerSocket>(
                sfd, event_get_base(event.get()), inter));
        stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
    }

    freeaddrinfo(ai);

    // Check if we successfully listened on all required protocols.
    bool required_proto_missing = false;

    // Check if the specified (missing) protocol was requested; if so log a
    // message, and if required return true.
    auto checkIfProtocolRequired =
            [host, port](const NetworkInterface::Protocol& protoMode,
                         const char* protoName) -> bool {
        if (protoMode != NetworkInterface::Protocol::Off) {
            // Failed to create a socket for this protocol; and it's not
            // disabled
            auto level = spdlog::level::level_enum::warn;
            if (protoMode == NetworkInterface::Protocol::Required) {
                level = spdlog::level::level_enum::critical;
            }
            CB_LOG_ENTRY(level,
                         R"(Failed to create {} {} socket for "{}:{}")",
                         to_string(protoMode),
                         protoName,
                         host.empty() ? "*" : host,
                         port);
        }
        return protoMode == NetworkInterface::Protocol::Required;
    };
    if (!ipv4) {
        required_proto_missing |= checkIfProtocolRequired(iv4, "IPv4");
    }
    if (!ipv6) {
        required_proto_missing |= checkIfProtocolRequired(iv6, "IPv6");
    }

    // Return success as long as we managed to create a listening port
    // for all non-optional protocols.
    return !required_proto_missing;
}
