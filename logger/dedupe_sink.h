/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "config.h"

#include <platform/processclock.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

/*
 * This class acts as a wrapper around other spdlog sinks and performs
 * deduplication of log messages.
 */
template <class Mutex>
class dedupe_sink : public spdlog::sinks::base_sink<Mutex> {
public:
    explicit dedupe_sink(std::shared_ptr<spdlog::sinks::sink> s,
                         const std::string& log_pattern);

    dedupe_sink() = delete;
    dedupe_sink(const dedupe_sink&) = delete;
    dedupe_sink& operator=(const dedupe_sink&) = delete;
    virtual ~dedupe_sink() = default;

protected:
    void _sink_it(const spdlog::details::log_msg& msg) override;
    void _flush() override;

private:
    void flushLastLog();
    void deduplicate(const spdlog::details::log_msg& msg);

    struct {
        /* The latest message received */
        std::string message;
        /* The number of times we've seen this message */
        int count;
        /* The time point when the message was first seen */
        ProcessClock::time_point created;
    } lastLog;

    std::shared_ptr<spdlog::sinks::sink> sink;
    spdlog::formatter_ptr formatter;
};

using dedupe_sink_mt = dedupe_sink<std::mutex>;
using dedupe_sink_st = dedupe_sink<spdlog::details::null_mutex>;
