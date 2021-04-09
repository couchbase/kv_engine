/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
