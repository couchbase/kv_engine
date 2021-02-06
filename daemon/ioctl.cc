/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include "ioctl.h"

#include "connection.h"
#include "connections.h"
#include "cookie.h"
#include "tracing.h"
#include "utilities/string_utilities.h"
#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <nlohmann/json.hpp>
#include <platform/cb_arena_malloc.h>

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

static const std::unordered_map<std::string, GetCallbackFunc> ioctl_get_map{
        {"trace.config", ioctlGetTracingConfig},
        {"trace.status", ioctlGetTracingStatus},
        {"trace.dump.list", ioctlGetTracingList},
        {"trace.dump.begin", ioctlGetTracingBeginDump},
        {"trace.dump.get", ioctlGetTraceDump},
        {"sla", ioctlGetMcbpSla},
        {"rbac.db.dump", ioctlRbacDbDump}};

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

    auto entry = ioctl_get_map.find(request.first);
    if (entry != ioctl_get_map.end()) {
        return entry->second(cookie, request.second, value, datatype);
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
        LOG_INFO("{}: Failed to set MCBP SLA. UUID:[{}]: {}",
                 c.getId(),
                 cookie.getEventId(),
                 e.what());
        return cb::engine_errc::invalid_arguments;
    }

    return cb::engine_errc::success;
}

static const std::unordered_map<std::string, SetCallbackFunc> ioctl_set_map{
        {"jemalloc.prof.active", setJemallocProfActive},
        {"jemalloc.prof.dump", setJemallocProfDump},
        {"release_free_memory", setReleaseFreeMemory},
        {"trace.config", ioctlSetTracingConfig},
        {"trace.start", ioctlSetTracingStart},
        {"trace.stop", ioctlSetTracingStop},
        {"trace.dump.clear", ioctlSetTracingClearDump},
        {"sla", ioctlSetMcbpSla}};

cb::engine_errc ioctl_set_property(Cookie& cookie,
                                   const std::string& key,
                                   const std::string& value) {
    std::pair<std::string, StrToStrMap> request;

    try {
        request = decode_query(key);
    } catch (const std::invalid_argument&) {
        return cb::engine_errc::invalid_arguments;
    }

    auto entry = ioctl_set_map.find(request.first);
    if (entry != ioctl_set_map.end()) {
        return entry->second(cookie, request.second, value);
    }
    return cb::engine_errc::invalid_arguments;
}
