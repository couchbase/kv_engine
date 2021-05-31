/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Danga Interactive
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
 */
#include "network_interface_manager.h"

#include "listening_port.h"
#include "log_macros.h"
#include "memcached.h"
#include "network_interface_description.h"
#include "server_socket.h"
#include "settings.h"
#include "ssl_utils.h"
#include "stats.h"
#include "tls_configuration.h"

#include <folly/io/async/EventBase.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <statistics/prometheus.h>

std::unique_ptr<NetworkInterfaceManager> networkInterfaceManager;

NetworkInterfaceManager::NetworkInterfaceManager(
        folly::EventBase& base, cb::prometheus::AuthCallback authCB)
    : eventBase(base), authCallback(std::move(authCB)) {
    LOG_INFO_RAW("Enable port(s)");
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
    eventBase.runInEventBaseThread([this]() { updateDeprecatedInterfaces(); });
}

void NetworkInterfaceManager::updateDeprecatedInterfaces() {
    invalidateSslCache();

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
                (!useTag && ((descr.port == interface.port)) &&
                 descr.host == interface.host)) {
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
                    if (interface.port != descr.port ||
                        interface.host != descr.host) {
                        // port mismatch... look at the next
                        continue;
                    }
                } else if (descr.tag != interface.tag) {
                    // Tag mismatch... look at the next
                    continue;
                }

                if ((descr.family == AF_INET &&
                     interface.ipv4 == NetworkInterface::Protocol::Off) ||
                    (descr.family == AF_INET6 &&
                     interface.ipv6 == NetworkInterface::Protocol::Off)) {
                    // address family mismatch... look at the next
                    continue;
                }

                drop = false;
                if (descr.sslKey != interface.ssl.key ||
                    descr.sslCert != interface.ssl.cert) {
                    // change the associated description
                    connection->updateSSL(interface.ssl.key,
                                          interface.ssl.cert);
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

        FILE* file = fopen(tempname.c_str(), "a");
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

/// The max send buffer size we want
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)

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
                            SO_REUSEPORT,
                            reinterpret_cast<const void*>(&flags),
                            sizeof(flags)) != 0) {
        LOG_WARNING("setsockopt(SO_REUSEPORT): {}",
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
                const auto e = cb::net::get_socket_error();
                auto name = cb::net::to_string(
                        reinterpret_cast<sockaddr_storage*>(next->ai_addr),
                        static_cast<socklen_t>(next->ai_addrlen));
                LOG_WARNING(
                        "Failed to look up the assigned port for the ephemeral "
                        "port: {} error: {}",
                        name,
                        cb_strerror(e));
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
        listen_conn.emplace_back(ServerSocket::create(sfd, eventBase, inter));
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

std::pair<nlohmann::json, nlohmann::json>
NetworkInterfaceManager::createInterface(
        const NetworkInterfaceDescription& description) {
    SOCKET sfd;
    addrinfo hints = {};

    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = description.getFamily();

    const char* host_buf = nullptr;
    const auto host = description.getHost();
    if (!host.empty() && host != "*") {
        host_buf = host.c_str();
    }

    struct addrinfo* ai;
    int error = getaddrinfo(host_buf,
                            std::to_string(description.getPort()).c_str(),
                            &hints,
                            &ai);
    if (error != 0) {
#ifdef WIN32
        throw std::system_error(
                GetLastError(), std::system_category(), "getaddrinfo()");
#else
        if (error != EAI_SYSTEM) {
            throw std::runtime_error(std::string("getaddrinfo(): ") +
                                     gai_strerror(error));
        } else {
            throw std::system_error(
                    errno, std::system_category(), "getaddrinfo");
        }
#endif
    }

    nlohmann::json errors = nlohmann::json::array();
    nlohmann::json ret = nlohmann::json::array();

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
            errors.push_back("Failed to bind to " + name + " - " +
                             cb_strerror(bind_error));
            safe_close(sfd);
            continue;
        }

        auto inter = std::make_shared<ListeningPort>(
                description.getTag(),
                host,
                cb::net::getSockNameAsJson(sfd)["port"].get<in_port_t>(),
                next->ai_addr->sa_family,
                description.isSystem(),
                "",
                "");
        listen_conn.emplace_back(ServerSocket::create(sfd, eventBase, inter));
        stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
        ret.push_back(listen_conn.back()->toJson());
    }

    freeaddrinfo(ai);
    return std::make_pair<nlohmann::json, nlohmann::json>(std::move(ret),
                                                          std::move(errors));
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::doDefineInterface(const nlohmann::json& spec) {
    auto descr = NetworkInterfaceDescription(spec);

    if (descr.isTls()) {
        // @todo needs the refactor to get off multiple ssl files
        return {cb::mcbp::Status::NotSupported, {}};
    }

    if (descr.getType() == NetworkInterfaceDescription::Type::Prometheus) {
        const auto prometheus = cb::prometheus::getRunningConfigAsJson();
        if (!prometheus.empty()) {
            return {cb::mcbp::Status::KeyEexists, {}};
        }
        try {
            const auto port = cb::prometheus::initialize(
                    {descr.getPort(), descr.getFamily()},
                    server_prometheus_stats,
                    authCallback);
            nlohmann::json json;
            json["errors"] = nlohmann::json::array();
            json["ports"].push_back(port);
            return {cb::mcbp::Status::Success, json.dump(2)};
        } catch (const std::exception& e) {
            return {cb::mcbp::Status::Einternal, e.what()};
        }
        // not reached
    }

    // I need to handle any for interface.. I'm tempted to drop that!
    if (descr.getPort() != 0) {
        // We can't add the port if it already exist with the same address
        for (const auto& c : listen_conn) {
            const auto& d = c->getInterfaceDescription();
            if (d.port == descr.getPort() && d.family == descr.getFamily() &&
                d.getHostname() == descr.getHostname()) {
                return {cb::mcbp::Status::KeyEexists, {}};
            }
        }
    }

    try {
        auto [result, errors] = createInterface(descr);
        if (result.empty()) {
            nlohmann::json json;
            json["error"]["context"] = "Failed to create any ports";
            json["error"]["errors"] = std::move(errors);
            return {cb::mcbp::Status::KeyEnoent, json.dump(2)};
        }

        nlohmann::json json;
        json["ports"] = std::move(result);
        json["errors"] = std::move(errors);
        writeInterfaceFile(false);
        return {cb::mcbp::Status::Success, json.dump()};
    } catch (const std::exception& exception) {
        return {cb::mcbp::Status::Einternal, exception.what()};
    }
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::doDeleteInterface(const std::string& uuid) {
    for (auto iter = listen_conn.begin(); iter != listen_conn.end(); iter++) {
        if (iter->get()->getUuid() == uuid) {
            if (listen_conn.size() == 1) {
                nlohmann::json json;
                json["error"]["context"] = "Can't delete the last interface";
                return {cb::mcbp::Status::Eaccess, json.dump()};
            }

            listen_conn.erase(iter);
            writeInterfaceFile(false);
            return {cb::mcbp::Status::Success, {}};
        }
    }

    const auto prometheus = cb::prometheus::getRunningConfigAsJson();
    if (!prometheus.empty() && prometheus["uuid"] == uuid) {
        cb::prometheus::shutdown();
        writeInterfaceFile(false);
        return {cb::mcbp::Status::Success, {}};
    }

    return {cb::mcbp::Status::KeyEnoent, {}};
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::doListInterface() {
    nlohmann::json ret = nlohmann::json::array();

    for (const auto& connection : listen_conn) {
        ret.push_back(connection->toJson());
    }

    auto prometheus = cb::prometheus::getRunningConfigAsJson();
    if (!prometheus.empty()) {
        ret.push_back(std::move(prometheus));
    }

    return {cb::mcbp::Status::Success, ret.dump(2)};
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::doTlsReconfigure(const nlohmann::json& spec) {
    try {
        auto next = std::make_unique<TlsConfiguration>(spec);
        auto desc = next->to_json().dump(2);
        tlsConfiguration.wlock()->swap(next);
        return {cb::mcbp::Status::Success, std::move(desc)};
    } catch (const std::exception& e) {
        return {cb::mcbp::Status::Einternal, e.what()};
    }
}

uniqueSslPtr NetworkInterfaceManager::createClientSslHandle() {
    uniqueSslPtr ret;
    tlsConfiguration.withRLock([&ret](auto& config) {
        if (config) {
            ret = config->createClientSslHandle();
        }
    });
    return ret;
}
