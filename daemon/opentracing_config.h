/*
 *     Copyright 2019 Couchbase, Inc
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
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <string>

class OpenTracingConfig {
public:
    /**
     * Pick out the configuration parameters from the provided JSON
     * structure. The format is expected to be:
     *
     *    {
     *       "module" : "/path/to/shared/library",
     *       "config" : "The configuration to use",
     *       "enabled" : false
     *    }
     */
    explicit OpenTracingConfig(const nlohmann::json& json)
        : enabled(isEnabled(json)),
          module(lookupEntry(json, "module")),
          config(lookupEntry(json, "config")) {
    }

    const bool enabled;
    const std::string module;
    const std::string config;

protected:
    /// Get the value of enabled (or false if not present)
    static bool isEnabled(const nlohmann::json& json);
    /// Get the value for the key (or "" if not present)
    static std::string lookupEntry(const nlohmann::json& json,
                                   const std::string& key);
};
