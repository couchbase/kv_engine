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
    mc_release_free_memory();
    LOG_NOTICE(c, "%u: IOCTL_SET: release_free_memory called", c->getId());
    return ENGINE_SUCCESS;
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

static const std::unordered_map<std::string, GetCallbackFunc> ioctl_get_map {
    {"trace.config", ioctlGetTracingConfig},
    {"trace.status", ioctlGetTracingStatus},
    {"trace.dump", ioctlGetTracingDump},
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


static const std::unordered_map<std::string, SetCallbackFunc> ioctl_set_map {
    {"release_free_memory", setReleaseFreeMemory},
    {"trace.connection", setTraceConnection},
    {"trace.config", ioctlSetTracingConfig},
    {"trace.start", ioctlSetTracingStart},
    {"trace.stop", ioctlSetTracingStop},
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
