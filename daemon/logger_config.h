/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <cJSON.h>
#include <gsl/gsl>
#include <string>

/* The settings supported by the file logger */
struct LoggerConfig {
    LoggerConfig() = default;
    explicit LoggerConfig(gsl::not_null<const cJSON*> json);

    bool operator==(const LoggerConfig& other) const;
    bool operator!=(const LoggerConfig& other) const;

    std::string filename;
    size_t buffersize = 2048 * 1024; // 2 MB for the logging queue
    size_t cyclesize = 100 * 1024 * 1024; // 100 MB per cycled file
    size_t sleeptime = 60; // time between forced flushes of the buffer
    bool unit_test = false; // if running in a unit test or not
};
