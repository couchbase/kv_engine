/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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


#include "tracing.h"

#include <phosphor/phosphor.h>
#include <phosphor/tools/export.h>
#include <mutex>

// TODO: MB-20640 The default config should be configurable from memcached.json
static phosphor::TraceConfig lastConfig{
        phosphor::TraceConfig(phosphor::BufferMode::ring, 20 * 1024 * 1024)};
static std::mutex configMutex;


ENGINE_ERROR_CODE ioctlGetTracingStatus(Connection*,
                                        const StrToStrMap&,
                                        std::string& value) {
    value = PHOSPHOR_INSTANCE.isEnabled() ? "enabled" : "disabled";
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlGetTracingConfig(Connection*,
                                        const StrToStrMap&,
                                        std::string& value) {
    std::lock_guard<std::mutex> lh(configMutex);
    value = *lastConfig.toString();
    return ENGINE_SUCCESS;
}

struct DumpContext {
    DumpContext(phosphor::TraceContext&& _context)
        : context(std::move(_context)), json_export(context) {
    }

    // Moving is dangerous as json_export contains a reference to
    // context.
    DumpContext(DumpContext&& other) = delete;

    phosphor::TraceContext context;
    phosphor::tools::JSONExport json_export;
};

static std::map<cb::uuid::uuid_t, std::unique_ptr<DumpContext>> dumps;
static std::mutex dumpsMutex;

ENGINE_ERROR_CODE ioctlGetTracingBeginDump(Connection*,
                                           const StrToStrMap&,
                                           std::string& value) {
    std::lock_guard<phosphor::TraceLog> lh(PHOSPHOR_INSTANCE);
    if (PHOSPHOR_INSTANCE.isEnabled()) {
        PHOSPHOR_INSTANCE.stop(lh);
    }

    phosphor::TraceContext context = PHOSPHOR_INSTANCE.getTraceContext(lh);
    if (context.getBuffer() == nullptr) {
        return ENGINE_EINVAL;
    }

    // Create the new dump associated with a random uuid
    cb::uuid::uuid_t uuid = cb::uuid::random();
    {
        std::lock_guard<std::mutex> lh(dumpsMutex);
        dumps.emplace(uuid, std::make_unique<DumpContext>(std::move(context)));
    }

    // Return the textual form of the uuid back to the user with success
    value = to_string(uuid);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlGetTracingDumpChunk(Connection*,
                                           const StrToStrMap& arguments,
                                           std::string& value) {
    auto id = arguments.find("id");
    if (id == arguments.end()) {
        // id argument must be specified
        return ENGINE_EINVAL;
    }

    cb::uuid::uuid_t uuid;
    try {
        uuid = cb::uuid::from_string(id->second);
    } catch (const std::invalid_argument&) {
        // id argument must be a valid uuid
        return ENGINE_EINVAL;
    }

    {
        std::lock_guard<std::mutex> lh(dumpsMutex);
        auto dump = dumps.find(uuid);
        if (dump == dumps.end()) {
            // uuid must exist in dumps map
            return ENGINE_EINVAL;
        }

        // @todo make configurable
        const size_t chunk_size = 1024 * 1024;

        if (dump->second->json_export.done()) {
            value = "";
        } else {
            // @todo generate on background thread
            // and add ewouldblock functionality
            value.resize(chunk_size);
            size_t count = dump->second->json_export.read(&value[0],
                                                          chunk_size);
            value.resize(count);
        }
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingClearDump(Connection*,
                                           const StrToStrMap& arguments,
                                           const std::string& value) {
    cb::uuid::uuid_t uuid;
    try {
        uuid = cb::uuid::from_string(value);
    } catch (const std::invalid_argument&) {
        // id argument must be a valid uuid
        return ENGINE_EINVAL;
    }

    {
        std::lock_guard<std::mutex> lh(dumpsMutex);
        auto dump = dumps.find(uuid);
        if (dump == dumps.end()) {
            // uuid must exist in dumps map
            return ENGINE_EINVAL;
        }

        dumps.erase(dump);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingConfig(Connection*,
                                        const StrToStrMap&,
                                        const std::string& value) {
    if (value == "") {
        return ENGINE_EINVAL;
    }
    try {
        std::lock_guard<std::mutex> lh(configMutex);
        lastConfig = phosphor::TraceConfig::fromString(value);
    } catch (const std::invalid_argument&) {
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingStart(Connection*,
                                       const StrToStrMap&,
                                       const std::string& value) {
    std::lock_guard<std::mutex> lh(configMutex);
    PHOSPHOR_INSTANCE.start(lastConfig);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingStop(Connection*,
                                      const StrToStrMap&,
                                      const std::string& value) {
    PHOSPHOR_INSTANCE.stop();
    return ENGINE_SUCCESS;
}
