/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Danga Interactive
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
 */
#include "network_interface_manager.h"

#include "connection.h"
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
#include <serverless/config.h>
#include <statistics/prometheus.h>

/**
 * Initialise Prometheus metric server with the provided config, and enable
 * all required endpoints.
 */
static nlohmann::json prometheus_init(
        const std::pair<in_port_t, sa_family_t>& config,
        cb::prometheus::AuthCallback authCB) {
    auto port = cb::prometheus::initialize(config, std::move(authCB));

    using cb::prometheus::IncludeMetaMetrics;
    using cb::prometheus::IncludeTimestamps;
    // Only the low cardinality metrics include the exposer_ metrics.
    // This is because ns_server will concat the outputs from both endpoints,
    // and joining the two sets of exposer_ naively will result in duplicates,
    // which is not valid Prometheus exposition format.
    cb::prometheus::addEndpoint("/_prometheusMetrics",
                                IncludeTimestamps::Yes,
                                IncludeMetaMetrics::Yes,
                                server_prometheus_stats_low);
    cb::prometheus::addEndpoint("/_prometheusMetricsNoTS",
                                IncludeTimestamps::No,
                                IncludeMetaMetrics::Yes,
                                server_prometheus_stats_low);
    cb::prometheus::addEndpoint("/_prometheusMetricsHigh",
                                IncludeTimestamps::Yes,
                                IncludeMetaMetrics::No,
                                server_prometheus_stats_high);
    cb::prometheus::addEndpoint("/_prometheusMetricsHighNoTS",
                                IncludeTimestamps::No,
                                IncludeMetaMetrics::No,
                                server_prometheus_stats_high);
    if (cb::serverless::isEnabled()) {
        cb::prometheus::addEndpoint("/_metering",
                                    IncludeTimestamps::No,
                                    IncludeMetaMetrics::No,
                                    server_prometheus_metering);
    }

    return port;
}

std::unique_ptr<NetworkInterfaceManager> networkInterfaceManager;

NetworkInterfaceManager::NetworkInterfaceManager(
        folly::EventBase& base, cb::prometheus::AuthCallback authCB)
    : eventBase(base), authCallback(std::move(authCB)) {
}

void NetworkInterfaceManager::createBootstrapInterface() {
    auto [ipv4, ipv6] = cb::net::getIpAddresses(false);
    auto& settings = Settings::instance();
    if (settings.has.interfaces) {
        LOG_INFO_RAW("Enable port(s)");
        for (auto& interf : settings.getInterfaces()) {
            auto createfunc = [this](const nlohmann::json& spec,
                                     bool required) {
                try {
                    auto [status, error] = doDefineInterface(spec);
                    if (status != cb::mcbp::Status::Success && required) {
                        FATAL_ERROR(EXIT_FAILURE,
                                    "Failed to create required listening "
                                    "socket: \"{}\". Errors: {}. Terminating.",
                                    spec.dump(),
                                    error);
                    }
                } catch (const std::exception& e) {
                    FATAL_ERROR(EXIT_FAILURE,
                                "Failed to create required listening "
                                "socket: \"{}\". Error: {}. Terminating.",
                                spec.dump(),
                                e.what());
                }
            };

            if (interf.ipv4 != NetworkInterface::Protocol::Off) {
                createfunc(nlohmann::json{{"type", "mcbp"},
                                          {"family", "inet"},
                                          {"host", interf.host},
                                          {"port", interf.port},
                                          {"tag", interf.tag},
                                          {"system", interf.system},
                                          {"tls", interf.tls}},
                           interf.ipv4 == NetworkInterface::Protocol::Required);
            }

            if (interf.ipv6 != NetworkInterface::Protocol::Off) {
                createfunc(nlohmann::json{{"type", "mcbp"},
                                          {"family", "inet6"},
                                          {"host", interf.host},
                                          {"port", interf.port},
                                          {"system", interf.system},
                                          {"tls", interf.tls}},
                           interf.ipv6 == NetworkInterface::Protocol::Required);
            }
        }
    } else {
        auto createfunc = [this](sa_family_t fam) {
            const std::string hostname = fam == AF_INET ? "127.0.0.1" : "::1";
            const std::string family = fam == AF_INET ? "inet" : "inet6";
            NetworkInterfaceDescription bootstrap({{"host", hostname},
                                                   {"port", 0},
                                                   {"family", family},
                                                   {"system", true},
                                                   {"type", "mcbp"},
                                                   {"tag", "bootstrap"}});
            auto [ifc, errors] = createInterface(bootstrap);
            if (ifc.empty()) {
                FATAL_ERROR(EXIT_FAILURE,
                            "Failed to create a {} bootstrap interface: {}",
                            family,
                            errors.dump());
            }
        };

        LOG_INFO_RAW("Enable bootstrap port(s)");
        if (!ipv4.empty()) {
            // Create an IPv4 bootstrap interface
            createfunc(AF_INET);
        }
        if (!ipv6.empty()) {
            // Create an IPv4 bootstrap interface
            createfunc(AF_INET6);
        }
    }

    try {
        if (settings.has.prometheus_config) {
            prometheus_init(settings.getPrometheusConfig(), authCallback);
        } else {
            prometheus_init({0, ipv4.empty() ? AF_INET6 : AF_INET},
                            authCallback);
        }
    } catch (const std::exception& exception) {
        // Error message already formatted in the exception
        FATAL_ERROR(EXIT_FAILURE, "{}", exception.what());
    }

    writeInterfaceFile(true);
}

bool NetworkInterfaceManager::isTlsConfigured() {
    return tlsConfiguration.rlock()->get();
}

void NetworkInterfaceManager::writeInterfaceFile(bool terminate) {
    auto& settings = Settings::instance();

    auto filename = settings.getPortnumberFile();
    if (!filename.empty()) {
        nlohmann::json json;
        json["ports"] = nlohmann::json::array();

        for (const auto& connection : listen_conn) {
            json["ports"].push_back(connection->to_json());
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
        std::filesystem::remove(filename);

        LOG_INFO("Port numbers available in {}", filename);
        if (rename(tempname.c_str(), filename.c_str()) == -1) {
            LOG_CRITICAL(R"(Failed to rename "{}" to "{}": {})",
                         tempname,
                         filename,
                         cb_strerror());
            if (terminate) {
                exit(EXIT_FAILURE);
            }
            std::filesystem::remove(tempname);
        }
    }
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

    const struct linger ling = {0, 0};
    const int flags = 1;
    int error;

#ifdef IPV6_V6ONLY
    if (ai->ai_family == AF_INET6) {
        error = cb::net::setsockopt(
                sfd, IPPROTO_IPV6, IPV6_V6ONLY, &flags, sizeof(flags));
        if (error != 0) {
            LOG_WARNING("setsockopt(IPV6_V6ONLY): {}", strerror(errno));
            safe_close(sfd);
            return INVALID_SOCKET;
        }
    }
#endif

    if (cb::net::setsockopt(
                sfd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags)) != 0) {
        LOG_WARNING("setsockopt(SO_REUSEADDR): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (cb::net::setsockopt(
                sfd, SOL_SOCKET, SO_REUSEPORT, &flags, sizeof(flags)) != 0) {
        LOG_WARNING("setsockopt(SO_REUSEPORT): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (cb::net::setsockopt(
                sfd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof(flags)) != 0) {
        LOG_WARNING("setsockopt(SO_KEEPALIVE): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    if (cb::net::setsockopt(sfd, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling)) !=
        0) {
        LOG_WARNING("setsockopt(SO_LINGER): {}",
                    cb_strerror(cb::net::get_socket_error()));
    }

    return sfd;
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
        }
        throw std::system_error(errno, std::system_category(), "getaddrinfo");
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

        if (bind(sfd, next->ai_addr, next->ai_addrlen) == SOCKET_ERROR) {
            const auto bind_error = cb::net::get_socket_error();
            auto name = cb::net::to_string(
                    reinterpret_cast<sockaddr_storage*>(next->ai_addr),
                    next->ai_addrlen);
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
                description.isTls());
        listen_conn.emplace_back(ServerSocket::create(sfd, eventBase, inter));
        ++stats.curr_conns;
        ret.push_back(listen_conn.back()->to_json());
    }

    freeaddrinfo(ai);
    return std::make_pair<nlohmann::json, nlohmann::json>(std::move(ret),
                                                          std::move(errors));
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::defineInterface(const nlohmann::json& spec) {
    std::pair<cb::mcbp::Status, std::string> ret;
    eventBase.runInEventBaseThreadAndWait(
            [this, &spec, &ret]() { ret = doDefineInterface(spec); });
    return ret;
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::doDefineInterface(const nlohmann::json& spec) {
    if (!eventBase.isInEventBaseThread()) {
        throw std::logic_error(
                "NetworkInterfaceManager::doDefineInterface(): Must be running "
                "in the event base thread as it updates the list of server "
                "sockets");
    }

    auto descr = NetworkInterfaceDescription(spec);

    if (descr.getType() == NetworkInterfaceDescription::Type::Prometheus) {
        const auto prometheus = cb::prometheus::getRunningConfigAsJson();
        if (!prometheus.empty()) {
            return {cb::mcbp::Status::KeyEexists, {}};
        }
        try {
            const auto port = prometheus_init(
                    {descr.getPort(), descr.getFamily()}, authCallback);
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
NetworkInterfaceManager::deleteInterface(const std::string& uuid) {
    std::pair<cb::mcbp::Status, std::string> ret;
    eventBase.runInEventBaseThreadAndWait(
            [this, &uuid, &ret]() { ret = doDeleteInterface(uuid); });
    return ret;
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::doDeleteInterface(const std::string& uuid) {
    if (!eventBase.isInEventBaseThread()) {
        throw std::logic_error(
                "NetworkInterfaceManager::doDeleteInterface(): Must be running "
                "in the event base thread as it updates the list of server "
                "sockets");
    }

    for (auto iter = listen_conn.begin(); iter != listen_conn.end(); iter++) {
        if (iter->get()->getUuid() == uuid) {
            if (listen_conn.size() == 1) {
                nlohmann::json json;
                json["error"]["context"] = "Can't delete the last interface";
                return {cb::mcbp::Status::Eaccess, json.dump()};
            }

            listen_conn.erase(iter);
            writeInterfaceFile(false);
            iterate_all_connections(
                    [](auto& conn) { conn.reEvaluateParentPort(); });
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
NetworkInterfaceManager::listInterface() {
    std::pair<cb::mcbp::Status, std::string> ret;
    eventBase.runInEventBaseThreadAndWait(
            [this, &ret]() { ret = doListInterface(); });
    return ret;
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::doListInterface() {
    if (!eventBase.isInEventBaseThread()) {
        throw std::logic_error(
                "NetworkInterfaceManager::doListInterface(): Must be running "
                "in the event base thread as it operates on the list of server "
                "sockets");
    }

    nlohmann::json ret = nlohmann::json::array();

    for (const auto& connection : listen_conn) {
        ret.push_back(connection->to_json());
    }

    auto prometheus = cb::prometheus::getRunningConfigAsJson();
    if (!prometheus.empty()) {
        ret.push_back(std::move(prometheus));
    }

    return {cb::mcbp::Status::Success, ret.dump(2)};
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::getTlsConfig() {
    try {
        return tlsConfiguration.withRLock([](auto& config)
                                                  -> std::pair<cb::mcbp::Status,
                                                               std::string> {
            if (config) {
                return {cb::mcbp::Status::Success, config->to_json().dump(2)};
            }
            return {cb::mcbp::Status::KeyEnoent, ""};
        });
    } catch (const std::exception& e) {
        return {cb::mcbp::Status::Einternal, e.what()};
    }
}

std::pair<cb::mcbp::Status, std::string>
NetworkInterfaceManager::reconfigureTlsConfig(const nlohmann::json& spec) {
    try {
        auto next = std::make_unique<TlsConfiguration>(spec);
        auto desc = next->to_json().dump();
        tlsConfiguration.wlock()->swap(next);
        LOG_INFO("TLS configuration changed to: {}", desc);
        return {cb::mcbp::Status::Success, std::move(desc)};
    } catch (const std::exception& e) {
        LOG_WARNING("TLS configuration failed: {}", e.what());
        return {cb::mcbp::Status::Einternal, e.what()};
    }
}

uniqueSslPtr NetworkInterfaceManager::createClientSslHandle() {
    return tlsConfiguration.withRLock([](auto& config) -> uniqueSslPtr {
        if (config) {
            return config->createClientSslHandle();
        }
        return {};
    });
}

std::size_t NetworkInterfaceManager::getNumberOfDaemonConnections() const {
    return ServerSocket::getNumInstances();
}
