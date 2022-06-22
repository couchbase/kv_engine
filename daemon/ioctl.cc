/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ioctl.h"

#include "connection.h"
#include "connections.h"
#include "cookie.h"
#include "settings.h"
#include "tracing.h"
#include "utilities/string_utilities.h"
#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <memcached/io_control.h>
#include <nlohmann/json.hpp>
#include <platform/cb_arena_malloc.h>
#include <serverless/config.h>

/*
 * Implement ioctl-style memcached commands (ioctl_get / ioctl_set).
 */

/**
 * Function interface for ioctl_get callbacks
 */
using GetCallbackFunc =
        std::function<cb::engine_errc(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      std::string& value,
                                      cb::mcbp::Datatype& datatype)>;

/**
 * Function interface for ioctl_set callbacks
 */
using SetCallbackFunc =
        std::function<cb::engine_errc(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      const std::string& value)>;

/**
 * Callback for calling allocator specific memory release
 */
static cb::engine_errc setReleaseFreeMemory(Cookie& cookie,
                                            const StrToStrMap&,
                                            const std::string& value) {
    cb::ArenaMalloc::releaseMemory();
    auto& c = cookie.getConnection();
    LOG_INFO("{}: IOCTL_SET: release_free_memory called", c.getId());
    return cb::engine_errc::success;
}

static cb::engine_errc setJemallocProfActive(Cookie& cookie,
                                             const StrToStrMap&,
                                             const std::string& value) {
    bool enable;
    if (value == "true") {
        enable = true;
    } else if (value == "false") {
        enable = false;
    } else {
        return cb::engine_errc::invalid_arguments;
    }

    int res = cb::ArenaMalloc::setProperty(
            "prof.active", &enable, sizeof(enable));
    auto& c = cookie.getConnection();
    LOG_INFO("{}: {} IOCTL_SET: setJemallocProfActive:{} called, result:{}",
             c.getId(),
             c.getDescription(),
             value,
             (res == 0) ? "success" : "failure");

    return (res == 0) ? cb::engine_errc::success
                      : cb::engine_errc::invalid_arguments;
}

static cb::engine_errc setJemallocProfDump(Cookie& cookie,
                                           const StrToStrMap&,
                                           const std::string&) {
    int res = cb::ArenaMalloc::setProperty("prof.dump", nullptr, 0);
    auto& c = cookie.getConnection();
    LOG_INFO("{}: {} IOCTL_SET: setJemallocProfDump called, result:{}",
             c.getId(),
             c.getDescription(),
             (res == 0) ? "success" : "failure");

    return (res == 0) ? cb::engine_errc::success
                      : cb::engine_errc::invalid_arguments;
}

cb::engine_errc ioctlGetMcbpSla(Cookie& cookie,
                                const StrToStrMap& arguments,
                                std::string& value,
                                cb::mcbp::Datatype& datatype) {
    if (!arguments.empty() || !value.empty()) {
        return cb::engine_errc::invalid_arguments;
    }

    value = cb::mcbp::sla::to_json().dump();
    datatype = cb::mcbp::Datatype::JSON;
    return cb::engine_errc::success;
}

cb::engine_errc ioctlRbacDbDump(Cookie& cookie,
                                const StrToStrMap& arguments,
                                std::string& value,
                                cb::mcbp::Datatype& datatype) {
    if (cookie.checkPrivilege(cb::rbac::Privilege::SecurityManagement)
                .failed()) {
        return cb::engine_errc::no_access;
    }

    if (!value.empty()) {
        return cb::engine_errc::invalid_arguments;
    }

    cb::sasl::Domain domain = cb::sasl::Domain::Local;
    if (!arguments.empty()) {
        for (auto& arg : arguments) {
            if (arg.first == "domain") {
                try {
                    domain = cb::sasl::to_domain(arg.second);
                } catch (const std::exception&) {
                    return cb::engine_errc::invalid_arguments;
                }
            } else {
                return cb::engine_errc::invalid_arguments;
            }
        }
    }

    value = cb::rbac::to_json(domain).dump();
    datatype = cb::mcbp::Datatype::JSON;
    return cb::engine_errc::success;
}

cb::engine_errc ioctl_get_property(Cookie& cookie,
                                   const std::string& key,
                                   std::string& value,
                                   cb::mcbp::Datatype& datatype) {
    datatype = cb::mcbp::Datatype::Raw;
    std::pair<std::string, StrToStrMap> request;

    try {
        request = decode_query(key);
    } catch (const std::invalid_argument&) {
        return cb::engine_errc::invalid_arguments;
    }

    auto& manager = cb::ioctl::Manager::getInstance();
    auto* id = manager.lookup(request.first);
    if (id) {
        switch (id->id) {
        case cb::ioctl::Id::RbacDbDump:
            return ioctlRbacDbDump(cookie, request.second, value, datatype);
        case cb::ioctl::Id::Sla:
            return ioctlGetMcbpSla(cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceConfig:
            return ioctlGetTracingConfig(
                    cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceStatus:
            return ioctlGetTracingStatus(
                    cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceDumpList:
            return ioctlGetTracingList(cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceDumpBegin:
            return ioctlGetTracingBeginDump(
                    cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceDumpGet:
            return ioctlGetTraceDump(cookie, request.second, value, datatype);

        case cb::ioctl::Id::JemallocProfActive: // may only be used with Set
        case cb::ioctl::Id::JemallocProfDump: // may only be used with Set
        case cb::ioctl::Id::ReleaseFreeMemory: // may only be used with Set
        case cb::ioctl::Id::ServerlessMaxConnectionsPerBucket: // set only
        case cb::ioctl::Id::ServerlessReadUnitSize: // set only
        case cb::ioctl::Id::ServerlessWriteUnitSize: // set only
        case cb::ioctl::Id::TraceDumpClear: // may only be used with Set
        case cb::ioctl::Id::TraceStart: // may only be used with Set
        case cb::ioctl::Id::TraceStop: // may only be used with Set
        case cb::ioctl::Id::enum_max:
            break;
        }
    }
    return cb::engine_errc::invalid_arguments;
}

static cb::engine_errc ioctlSetMcbpSla(Cookie& cookie,
                                       const StrToStrMap&,
                                       const std::string& value) {
    try {
        cb::mcbp::sla::reconfigure(nlohmann::json::parse(value));
        LOG_INFO("SLA configuration changed to: {}",
                 cb::mcbp::sla::to_json().dump());
    } catch (const std::exception& e) {
        cookie.getEventId();
        auto& c = cookie.getConnection();
        LOG_WARNING("{}: Failed to set MCBP SLA. UUID:[{}]: {}",
                    c.getId(),
                    cookie.getEventId(),
                    e.what());
        return cb::engine_errc::invalid_arguments;
    }

    return cb::engine_errc::success;
}

static cb::engine_errc ioctlSetServerlessMaxConnectionsPerBucket(
        Cookie& cookie, const StrToStrMap&, const std::string& value) {
    if (isServerlessDeployment()) {
        try {
            auto& config = cb::serverless::Config::instance();
            auto val = std::stoul(value);
            if (val < 100) {
                cookie.setErrorContext(
                        "Maximum number of connections cannot be below 100");
                return cb::engine_errc::invalid_arguments;
            }
            config.maxConnectionsPerBucket.store(val,
                                                 std::memory_order_release);
            LOG_INFO("Set maximum connections to a bucket to {}", val);
        } catch (const std::exception& e) {
            cookie.setErrorContext(
                    "Failed to convert the provided value to an integer");
            return cb::engine_errc::invalid_arguments;
        }

        return cb::engine_errc::success;
    }

    std::string reason{
            cb::ioctl::Manager::getInstance()
                    .lookup(cb::ioctl::Id::ServerlessMaxConnectionsPerBucket)
                    .key};
    reason.append(" may only be used on serverless deployments");
    cookie.setErrorContext(std::move(reason));
    return cb::engine_errc::invalid_arguments;
}

static cb::engine_errc ioctlSetServerlessUnitSize(Cookie& cookie,
                                                  cb::ioctl::Id id,
                                                  const std::string& value) {
    if (isServerlessDeployment()) {
        try {
            auto& config = cb::serverless::Config::instance();
            auto val = std::stoul(value);
            if (id == cb::ioctl::Id::ServerlessReadUnitSize) {
                LOG_INFO("Change RCU size from {} to {}",
                         config.readUnitSize.load(std::memory_order_acquire),
                         val);
                config.readUnitSize.store(val, std::memory_order_release);
            } else if (id == cb::ioctl::Id::ServerlessWriteUnitSize) {
                LOG_INFO("Change WCU size from {} to {}",
                         config.writeUnitSize.load(std::memory_order_acquire),
                         val);
                config.writeUnitSize.store(val, std::memory_order_release);
            } else {
                LOG_WARNING_RAW(
                        "ioctlSetServerlessUnitSize: Internal error, "
                        "called for unknown id. request ignored");
                cookie.setErrorContext("Internal error");
                return cb::engine_errc::invalid_arguments;
            }
        } catch (const std::exception& e) {
            cookie.setErrorContext(
                    "Failed to convert the provided value to an integer");
            return cb::engine_errc::invalid_arguments;
        }

        return cb::engine_errc::success;
    }

    std::string reason{cb::ioctl::Manager::getInstance().lookup(id).key};
    reason.append(" may only be used on serverless deployments");
    cookie.setErrorContext(std::move(reason));
    return cb::engine_errc::invalid_arguments;
}

cb::engine_errc ioctl_set_property(Cookie& cookie,
                                   const std::string& key,
                                   const std::string& value) {
    std::pair<std::string, StrToStrMap> request;

    try {
        request = decode_query(key);
    } catch (const std::invalid_argument&) {
        return cb::engine_errc::invalid_arguments;
    }

    auto& manager = cb::ioctl::Manager::getInstance();
    auto* id = manager.lookup(request.first);
    if (id) {
        switch (id->id) {
        case cb::ioctl::Id::JemallocProfActive:
            return setJemallocProfActive(cookie, request.second, value);
        case cb::ioctl::Id::JemallocProfDump:
            return setJemallocProfDump(cookie, request.second, value);
        case cb::ioctl::Id::ReleaseFreeMemory:
            return setReleaseFreeMemory(cookie, request.second, value);
        case cb::ioctl::Id::Sla:
            return ioctlSetMcbpSla(cookie, request.second, value);
        case cb::ioctl::Id::ServerlessMaxConnectionsPerBucket:
            return ioctlSetServerlessMaxConnectionsPerBucket(
                    cookie, request.second, value);
        case cb::ioctl::Id::ServerlessReadUnitSize:
        case cb::ioctl::Id::ServerlessWriteUnitSize:
            return ioctlSetServerlessUnitSize(cookie, id->id, value);
        case cb::ioctl::Id::TraceConfig:
            return ioctlSetTracingConfig(cookie, request.second, value);
        case cb::ioctl::Id::TraceStart:
            return ioctlSetTracingStart(cookie, request.second, value);
        case cb::ioctl::Id::TraceStop:
            return ioctlSetTracingStop(cookie, request.second, value);
        case cb::ioctl::Id::TraceDumpClear:
            return ioctlSetTracingClearDump(cookie, request.second, value);

        case cb::ioctl::Id::TraceDumpBegin: // may only be used with Get
        case cb::ioctl::Id::TraceDumpGet: // may only be used with Get
        case cb::ioctl::Id::TraceDumpList: // may only be used with Get
        case cb::ioctl::Id::TraceStatus: // may only be used with Get
        case cb::ioctl::Id::RbacDbDump: // may only be used with Get
        case cb::ioctl::Id::enum_max:
            break;
        }
    }

    return cb::engine_errc::invalid_arguments;
}
