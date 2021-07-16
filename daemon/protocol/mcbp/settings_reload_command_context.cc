/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "settings_reload_command_context.h"

#include <daemon/cmdline.h>
#include <daemon/config_parse.h>
#include <daemon/connection.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/network_interface_description.h>
#include <daemon/network_interface_manager.h>
#include <daemon/one_shot_task.h>
#include <daemon/settings.h>
#include <executor/executorpool.h>
#include <logger/logger.h>
#include <platform/dirutils.h>

SettingsReloadCommandContext::SettingsReloadCommandContext(Cookie& cookie)
    : FileReloadCommandContext(cookie) {
}

class NetworkInterfaceManagerException : public std::runtime_error {
public:
    NetworkInterfaceManagerException(std::string message, nlohmann::json error)
        : std::runtime_error(message + ": " + error.dump()),
          message(std::move(message)),
          error(std::move(error)) {
    }

    const std::string message;
    const nlohmann::json error;
};

std::vector<NetworkInterfaceDescription>
SettingsReloadCommandContext::getInterfaces() {
    auto [status, list] = networkInterfaceManager->doListInterface();
    if (status != cb::mcbp::Status::Success) {
        throw std::runtime_error("Failed to fetch the list of interfaces: " +
                                 to_string(status));
    }
    std::vector<NetworkInterfaceDescription> ret;
    auto json = nlohmann::json::parse(list);
    for (const auto& entry : json) {
        ret.emplace_back(NetworkInterfaceDescription{entry});
    }
    return ret;
}

void SettingsReloadCommandContext::deleteInterface(const std::string& uuid) {
    auto [status, error] = networkInterfaceManager->doDeleteInterface(uuid);
    if (status != cb::mcbp::Status::Success) {
        throw NetworkInterfaceManagerException(
                "Failed to delete interface " + uuid + ": " + to_string(status),
                nlohmann::json::parse(error));
    }
}

void SettingsReloadCommandContext::createInterface(const nlohmann::json& spec) {
    auto [status, error] = networkInterfaceManager->doDefineInterface(spec);
    if (status != cb::mcbp::Status::Success) {
        auto json = nlohmann::json::parse(error);
        json["spec"] = spec;
        throw NetworkInterfaceManagerException(
                "Failed to create interface: " + to_string(status), json);
    }
}

void SettingsReloadCommandContext::maybeReconfigurePrometheus(Settings& next) {
    if (!next.has.prometheus_config) {
        return;
    }

    auto [next_port, next_family] = next.getPrometheusConfig();
    auto [curr_port, curr_family] = cb::prometheus::getRunningConfig();
    if (next_port == curr_port && next_family == curr_family) {
        // Nothing changed
        return;
    }

    auto ifc = getInterfaces();
    for (const auto& e : ifc) {
        if (e.getType() == NetworkInterfaceDescription::Type::Prometheus) {
            deleteInterface(e.getUuid());
        }
    }

    // Time to define the interface
    createInterface(nlohmann::json{
            {"type", "prometheus"},
            {"family", next_family == AF_INET ? "inet" : "inet6"},
            {"host", next_family == AF_INET ? "127.0.0.1" : "::1"},
            {"port", next_port}});
}

void SettingsReloadCommandContext::maybeReconfigureInterfaces(Settings& next) {
    maybeReconfigurePrometheus(next);

    if (!next.has.interfaces) {
        return;
    }

    auto interfaces = getInterfaces();

    // Iterate over the new interface list and create the interfaces
    // we don't already have
    auto new_ifc = next.getInterfaces();
    for (const auto& e : new_ifc) {
        // do we have the interface already
        bool found_ipv4 = false;
        bool found_ipv6 = false;
        for (const auto& iface : interfaces) {
            if (e.port == iface.getPort() &&
                ((e.host == "*" &&
                  (iface.getHost() == "::" || iface.getHost() == "0.0.0.0")) ||
                 e.host == iface.getHost())) {
                // This _might_ be the correct interface
                if (e.ipv4 != NetworkInterface::Protocol::Off &&
                    iface.getFamily() == AF_INET) {
                    found_ipv4 = true;
                }
                if (e.ipv6 != NetworkInterface::Protocol::Off &&
                    iface.getFamily() == AF_INET6) {
                    found_ipv6 = true;
                }
            }
        }

        if (e.ipv4 != NetworkInterface::Protocol::Off && !found_ipv4) {
            // we need to at least try to create an IPv4 interface
            try {
                createInterface(nlohmann::json{{"type", "mcbp"},
                                               {"family", "inet"},
                                               {"host", e.host},
                                               {"port", e.port},
                                               {"tag", e.tag},
                                               {"system", e.system},
                                               {"tls", !e.ssl.key.empty()}});
            } catch (const std::exception& exception) {
                if (e.ipv4 == NetworkInterface::Protocol::Required) {
                    // It is required; Throw the exception so we'll
                    // return an error
                    throw;
                }
            }
        }

        if (e.ipv6 != NetworkInterface::Protocol::Off && !found_ipv6) {
            // we need to at least try to create an IPv6 interface
            try {
                createInterface(nlohmann::json{{"type", "mcbp"},
                                               {"family", "inet6"},
                                               {"host", e.host},
                                               {"port", e.port},
                                               {"system", e.system},
                                               {"tls", !e.ssl.key.empty()}});
            } catch (const std::exception& exception) {
                if (e.ipv6 == NetworkInterface::Protocol::Required) {
                    // It is required; Throw the exception so we'll
                    // return an error
                    throw;
                }
            }
        }
    }

    // Iterate over the interfaces and this time delete the ones
    // not present in the current configuration
    for (const auto& iface : interfaces) {
        if (iface.getType() != NetworkInterfaceDescription::Type::Mcbp) {
            continue;
        }
        bool found = false;
        for (const auto& e : new_ifc) {
            if (e.port == iface.getPort() &&
                ((e.host == "*" &&
                  (iface.getHost() == "::" || iface.getHost() == "0.0.0.0")) ||
                 e.host == iface.getHost()) &&
                ((e.ipv4 != NetworkInterface::Protocol::Off &&
                  iface.getFamily() == AF_INET) ||
                 (e.ipv6 != NetworkInterface::Protocol::Off &&
                  iface.getFamily() == AF_INET6))) {
                found = true;
                break;
            }
        }

        if (!found) {
            deleteInterface(iface.getUuid());
        }
    }
}

cb::engine_errc SettingsReloadCommandContext::doSettingsReload() {
    const auto old_priv_debug = Settings::instance().isPrivilegeDebug();
    try {
        LOG_INFO("Reloading config file {}", get_config_file());
        const auto content =
                cb::io::loadFile(get_config_file(), std::chrono::seconds{5});
        Settings new_settings(nlohmann::json::parse(content));

        // Unfortunately ns_server won't keep its commitment to implement
        // MB-46863 for 7.1. Until they do we need to work around it
        // by trying to update the prometheus, interface and TLS
        // configuration as part of reloading the configuration.
        //
        // The following section may be deleted once they get around
        // fixing MB-46863

        // Verify that static members hasn't been changed (It'll throw
        // an exception for errors which will cause the error message to
        // be returned to the client)
        Settings::instance().updateSettings(new_settings, false);

        // Try to reconfigure the Prometheus listener if it changed
        maybeReconfigurePrometheus(new_settings);

        // Try to reconfigure interfaces. If an error occurs it'll throw
        // an exception which will cause an error to be sent back to the
        // client.
        maybeReconfigureInterfaces(new_settings);

        // Finally; update the TLS configuration
        auto tls = new_settings.getTlsConfiguration();
        if (!tls.empty() &&
            networkInterfaceManager->allowTlsSettingsInConfigFile()) {
            networkInterfaceManager->doTlsReconfigure(tls);
        }

        // END workaround for ns_server limitations (MB-46863)

        // interfaces updated, time to do the rest of them
        Settings::instance().updateSettings(new_settings, true);

        // We need to produce an audit trail if someone tried to enable /
        // disable privilege debug mode
        if (Settings::instance().isPrivilegeDebug() != old_priv_debug) {
            audit_set_privilege_debug_mode(
                    cookie, Settings::instance().isPrivilegeDebug());
        }
        return cb::engine_errc::success;
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: Failed reloading config file. not enough memory",
                    cookie.getConnectionId());
        return cb::engine_errc::no_memory;
    } catch (const std::system_error& error) {
        if (error.code() == std::errc::too_many_files_open) {
            LOG_WARNING("{}: Failed reloading config file. too many files open",
                        cookie.getConnectionId());
            return cb::engine_errc::temporary_failure;
        }
        cookie.setErrorContext(error.what());
    } catch (const NetworkInterfaceManagerException& exception) {
        cookie.setErrorContext(exception.message);
        nlohmann::json extras;
        // Unfortunately "error" isn't allowed to use in the extras
        // field as we would automatically overwrite it in the framework.
        // Lets just put it in details for now
        extras["details"] = exception.error;
        cookie.setErrorJsonExtras(extras);
        LOG_WARNING(
                "{}: {} - Failed reloading config file. Error: \"{}\". Extra: "
                "{}",
                cookie.getConnectionId(),
                cookie.getEventId(),
                cookie.getErrorContext(),
                extras);
        return cb::engine_errc::failed;
    } catch (const std::exception& exception) {
        cookie.setErrorContext(exception.what());
    } catch (...) {
        cookie.setErrorContext("Unknown error");
    }

    LOG_WARNING("{}: {} - Failed reloading config file '{}'. Error: {}",
                cookie.getConnectionId(),
                cookie.getEventId(),
                get_config_file(),
                cookie.getErrorContext());
    return cb::engine_errc::failed;
}

cb::engine_errc SettingsReloadCommandContext::reload() {
    ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
            TaskId::Core_SettingsReloadTask, "Reload memcached.json", [this]() {
                ::notifyIoComplete(cookie, doSettingsReload());
            }));
    return cb::engine_errc::would_block;
}
