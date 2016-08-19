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
    value = lastConfig.toString();
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
