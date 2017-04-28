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

#include "config.h"
#include "alloc_hooks.h"
#include "connections.h"
#include "utilities/string_utilities.h"
#include "tracing.h"

/*
 * Implement ioctl-style memcached commands (ioctl_get / ioctl_set).
 */

/**
 * Function interface for ioctl_get callbacks
 */
using GetCallbackFunc = std::function<ENGINE_ERROR_CODE(
        Connection* c,
        const StrToStrMap& arguments,
        std::string& value)>;

/**
 * Function interface for ioctl_set callbacks
 */
using SetCallbackFunc = std::function<ENGINE_ERROR_CODE(
        Connection* c,
        const StrToStrMap& arguments,
        const std::string& value)>;

/**
 * Callback for calling allocator specific memory release
 */
static ENGINE_ERROR_CODE setReleaseFreeMemory(Connection* c,
                                              const StrToStrMap&,
                                              const std::string& value) {
    AllocHooks::release_free_memory();
    LOG_NOTICE(c, "%u: IOCTL_SET: release_free_memory called", c->getId());
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE setJemallocProfActive(Connection* c,
                                               const StrToStrMap&,
                                               const std::string& value) {
    bool enable;
    if (value == "true") {
        enable = true;
    } else if (value == "false") {
        enable = false;
    } else {
        return ENGINE_EINVAL;
    }

    int res = AllocHooks::set_allocator_property(
            "prof.active", &enable, sizeof(enable));
    LOG_NOTICE(c,
               "%u: %s IOCTL_SET: setJemallocProfActive:%s called, result:%s",
               c->getId(),
               c->getDescription().c_str(),
               value.c_str(),
               (res == 0) ? "success" : "failure");

    return (res == 0) ? ENGINE_SUCCESS : ENGINE_EINVAL;
}

static ENGINE_ERROR_CODE setJemallocProfDump(Connection* c,
                                             const StrToStrMap&,
                                             const std::string&) {
    int res = AllocHooks::set_allocator_property("prof.dump", nullptr, 0);
    LOG_NOTICE(c,
               "%u: %s IOCTL_SET: setJemallocProfDump called, result:%s",
               c->getId(),
               c->getDescription().c_str(),
               (res == 0) ? "success" : "failure");

    return (res == 0) ? ENGINE_SUCCESS : ENGINE_EINVAL;
}

/**
 * Callback for setting the trace status of a specific connection
 */
static ENGINE_ERROR_CODE setTraceConnection(Connection* c,
                                            const StrToStrMap& arguments,
                                            const std::string& value) {
    auto id = arguments.find("id");
    if (id == arguments.end()) {
        return ENGINE_EINVAL;
    }
    return apply_connection_trace_mask(id->second, value);
}

static const std::unordered_map<std::string, GetCallbackFunc> ioctl_get_map{
        {"trace.config", ioctlGetTracingConfig},
        {"trace.status", ioctlGetTracingStatus},
        {"trace.dump.begin", ioctlGetTracingBeginDump},
        {"trace.dump.chunk", ioctlGetTracingDumpChunk},
};

ENGINE_ERROR_CODE ioctl_get_property(Connection* c,
                                     const std::string& key,
                                     std::string& value) {

    std::pair<std::string, StrToStrMap> request;

    try {
        request = decode_query(key);
    } catch (const std::invalid_argument&) {
        return ENGINE_EINVAL;
    }

    auto entry = ioctl_get_map.find(request.first);
    if (entry != ioctl_get_map.end()) {
        return entry->second(c, request.second, value);
    }
    return ENGINE_EINVAL;
}

static const std::unordered_map<std::string, SetCallbackFunc> ioctl_set_map{
        {"jemalloc.prof.active", setJemallocProfActive},
        {"jemalloc.prof.dump", setJemallocProfDump},
        {"release_free_memory", setReleaseFreeMemory},
        {"trace.connection", setTraceConnection},
        {"trace.config", ioctlSetTracingConfig},
        {"trace.start", ioctlSetTracingStart},
        {"trace.stop", ioctlSetTracingStop},
        {"trace.dump.clear", ioctlSetTracingClearDump},
};

ENGINE_ERROR_CODE ioctl_set_property(Connection* c,
                                     const std::string& key,
                                     const std::string& value) {

    std::pair<std::string, StrToStrMap> request;

    try {
        request = decode_query(key);
    } catch (const std::invalid_argument&) {
        return ENGINE_EINVAL;
    }

    auto entry = ioctl_set_map.find(request.first);
    if (entry != ioctl_set_map.end()) {
        return entry->second(c, request.second, value);
    }
    return ENGINE_EINVAL;
}
