/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "logger/visibility.h"

#include <nlohmann/json_fwd.hpp>
#include <spdlog/common.h>

#include <string>

namespace cb::logger {

struct LOGGER_PUBLIC_API Config {
    Config() = default;
    explicit Config(const nlohmann::json& json);

    bool operator==(const Config& other) const;
    bool operator!=(const Config& other) const;

    /// The base name of the log files (we'll append: .000000.txt where
    /// the numbers is a sequence counter. The higher the newer ;)
    std::string filename;
    /// 8192 item size for the logging queue. This is equivalent to 2 MB
    size_t buffersize = 8192;
    /// 100 MB per cycled file
    size_t cyclesize = 100 * 1024 * 1024;
    /// if running in a unit test or not
    bool unit_test = false;
    /// Should messages be passed on to the console via stderr
    bool console = true;
    /// The default log level to initialize the logger to
    spdlog::level::level_enum log_level = spdlog::level::level_enum::info;
};

} // namespace cb::logger
