/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "ifconfig_context.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/memcached.h>
#include <daemon/network_interface_manager.h>

IfconfigCommandContext::IfconfigCommandContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_Ifconfig,
              fmt::format("Ifconfig {}", cookie.getRequest().getKeyString()),
              ConcurrencySemaphores::instance().ifconfig) {
}

cb::engine_errc IfconfigCommandContext::execute() {
    auto ret = doExecute();
    if (!response.empty()) {
        datatype = cb::mcbp::Datatype::JSON;
    }
    return ret;
}

cb::engine_errc IfconfigCommandContext::doExecute() {
    const auto key = cookie.getRequest().getKeyString();
    const auto value = cookie.getRequest().getValueString();

    if (key == "define") {
        auto json = nlohmann::json::parse(value);
        auto [s, p] = networkInterfaceManager->defineInterface(json);
        response = std::move(p);
        return s;
    }

    if (key == "delete") {
        auto [s, p] =
                networkInterfaceManager->deleteInterface(std::string(value));
        response = std::move(p);
        return s;
    }

    if (key == "list") {
        auto [s, p] = networkInterfaceManager->listInterface();
        response = std::move(p);
        return s;
    }

    // The validator blocked all other keys
    Expects(key == "tls");
    if (value.empty()) {
        auto [s, p] = networkInterfaceManager->getTlsConfig();
        response = std::move(p);
        return s;
    }

    auto json = nlohmann::json::parse(value);
    auto [s, p] = networkInterfaceManager->reconfigureTlsConfig(json);
    response = std::move(p);
    return s;
}
