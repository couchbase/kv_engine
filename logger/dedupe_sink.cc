/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "dedupe_sink.h"

/* This sink should only be used as a wrapper around another sink */
template <class Mutex>
dedupe_sink<Mutex>::dedupe_sink(std::shared_ptr<spdlog::sinks::sink> s,
                                const std::string& log_pattern)
    : spdlog::sinks::base_sink<Mutex>(), lastLog(), sink(s) {
    formatter = std::make_shared<spdlog::pattern_formatter>(
            log_pattern, spdlog::pattern_time_type::local);
}

/* Checks if the underlying sink should log this message and if so,
 * perform deduplication on it.
 */
template <class Mutex>
void dedupe_sink<Mutex>::_sink_it(const spdlog::details::log_msg& msg) {
    if (sink->should_log(msg.level)) {
        deduplicate(msg);
    }
}

/* Tells the underlying sink to flush its contents. */
template <class Mutex>
void dedupe_sink<Mutex>::_flush() {
    std::lock_guard<Mutex> lock(spdlog::sinks::base_sink<Mutex>::_mutex);
    flushLastLog();
    sink->flush();
}

/* Generates an info message of the form "Message repeated xxx times"
 * and resets lastLog.
 */
template <class Mutex>
void dedupe_sink<Mutex>::flushLastLog() {
    if (lastLog.count > 1) {
        spdlog::details::log_msg repeated;
        repeated.time = spdlog::details::os::now();
        repeated.level = spdlog::level::info;

#if defined(SPDLOG_FMT_PRINTF)
        fmt::printf(repeated.raw, "Message repeated %u times", lastLog.count);
#else
        repeated.raw.write("Message repeated {} times", lastLog.count);
#endif
        formatter->format(repeated);
        sink->log(repeated);
    }
    lastLog.message.clear();
    lastLog.created = ProcessClock::time_point();
    lastLog.count = 0;
}

/*
 * Counts how many times a message has been seen.
 * Logs every new message and saves a copy of it to compare against future
 * messages for deduplication.
 */
template <class Mutex>
void dedupe_sink<Mutex>::deduplicate(const spdlog::details::log_msg& log_msg) {
    // Only run dedupe for ~5 seconds
    bool timeToFlush =
            (ProcessClock::now() - lastLog.created) > std::chrono::seconds(5);

    if (lastLog.message == log_msg.raw.str() && !timeToFlush) {
        ++lastLog.count;
    } else {
        flushLastLog();
        sink->log(log_msg);
        lastLog.message = log_msg.raw.str();
        lastLog.created = ProcessClock::now();
        lastLog.count = 1;
    }
}

/* Explicit instantiation of all the template instances we'll use.
 * Required so the compiler can link the source and header files.
 */
template class dedupe_sink<std::mutex>;
template class dedupe_sink<spdlog::details::null_mutex>;
