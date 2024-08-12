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
#include <daemon/one_shot_limited_concurrency_task.h>
#include <executor/executorpool.h>

cb::engine_errc IfconfigCommandContext::scheduleTask() {
    const std::string_view key = cookie.getRequest().getKeyString();
    std::string value(cookie.getRequest().getValueString());
    auto& semaphore = ConcurrencySemaphores::instance().ifconfig;

    if (key == "define") {
        ExecutorPool::get()->schedule(
                std::make_shared<OneShotLimitedConcurrencyTask>(
                        TaskId::Core_Ifconfig,
                        "Ifconfig define",
                        [this, spec = std::move(value)]() {
                            auto json = nlohmann::json::parse(spec);
                            auto [s, p] =
                                    networkInterfaceManager->defineInterface(
                                            json);
                            status = s;
                            payload = std::move(p);
                            cookie.notifyIoComplete(cb::engine_errc::success);
                        },
                        semaphore));
    } else if (key == "delete") {
        ExecutorPool::get()->schedule(
                std::make_shared<OneShotLimitedConcurrencyTask>(
                        TaskId::Core_Ifconfig,
                        "Ifconfig delete",
                        [this, uuid = std::move(value)]() {
                            auto [s, p] =
                                    networkInterfaceManager->deleteInterface(
                                            uuid);
                            status = s;
                            payload = std::move(p);
                            cookie.notifyIoComplete(cb::engine_errc::success);
                        },
                        semaphore));
    } else if (key == "list") {
        ExecutorPool::get()->schedule(
                std::make_shared<OneShotLimitedConcurrencyTask>(
                        TaskId::Core_Ifconfig,
                        "Ifconfig list",
                        [this]() {
                            auto [s, p] =
                                    networkInterfaceManager->listInterface();
                            status = s;
                            payload = std::move(p);
                            cookie.notifyIoComplete(cb::engine_errc::success);
                        },
                        semaphore));
    } else if (key == "tls") {
        if (value.empty()) {
            ExecutorPool::get()->schedule(
                    std::make_shared<OneShotLimitedConcurrencyTask>(
                            TaskId::Core_Ifconfig,
                            "Ifconfig get TLS configuration",
                            [this]() {
                                auto [s, p] =
                                        networkInterfaceManager->getTlsConfig();
                                status = s;
                                payload = std::move(p);
                                cookie.notifyIoComplete(
                                        cb::engine_errc::success);
                            },
                            semaphore));
        } else {
            ExecutorPool::get()->schedule(
                    std::make_shared<OneShotLimitedConcurrencyTask>(
                            TaskId::Core_Ifconfig,
                            "Ifconfig set TLS configuration",
                            [this, spec = std::move(value)]() {
                                auto json = nlohmann::json::parse(spec);
                                auto [s, p] =
                                        networkInterfaceManager
                                                ->reconfigureTlsConfig(json);
                                status = s;
                                payload = std::move(p);
                                cookie.notifyIoComplete(
                                        cb::engine_errc::success);
                            },
                            semaphore));
        }
    } else {
        throw std::runtime_error(
                "IfconfigCommandContext::scheduleTask(): unknown command");
    }

    state = State::Done;
    return cb::engine_errc::would_block;
}

cb::engine_errc IfconfigCommandContext::done() {
    if (status != cb::mcbp::Status::Success) {
        cookie.setErrorContext(std::string{payload.data(), payload.size()});
    }
    cookie.sendResponse(status,
                        {},
                        {},
                        payload,
                        payload.empty() ? cb::mcbp::Datatype::Raw
                                        : cb::mcbp::Datatype::JSON,
                        cb::mcbp::cas::Wildcard);
    return cb::engine_errc::success;
}

cb::engine_errc IfconfigCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::Done:
            return done();

        case State::scheduleTask:
            ret = scheduleTask();
            break;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}

IfconfigCommandContext::IfconfigCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie) {
}
