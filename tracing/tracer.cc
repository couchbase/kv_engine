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
#include <memcached/tracer.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <sstream>

std::chrono::microseconds to_micros(
        const std::chrono::steady_clock::time_point tp) {
    return std::chrono::time_point_cast<std::chrono::microseconds>(tp)
            .time_since_epoch();
}

namespace cb::tracing {

SpanId Tracer::begin(Code tracecode,
                     std::chrono::steady_clock::time_point startTime) {
    vecSpans.emplace_back(tracecode, startTime);
    return vecSpans.size() - 1;
}

bool Tracer::end(SpanId spanId, std::chrono::steady_clock::time_point endTime) {
    if (spanId >= vecSpans.size()) {
        return false;
    }
    auto& span = vecSpans[spanId];
    span.duration =
            std::chrono::duration_cast<Span::Duration>(endTime - span.start);
    return true;
}

bool Tracer::end(Code tracecode,
                 std::chrono::steady_clock::time_point endTime) {
    // Locate the ID for this tracecode (when we begin the Span).
    SpanId spanId = 0;
    for (const auto& span : vecSpans) {
        if (span.code == tracecode) {
            break;
        }
        spanId++;
    }
    return end(spanId, endTime);
}

const std::vector<Span>& Tracer::getDurations() const {
    return vecSpans;
}

Span::Duration Tracer::getTotalMicros() const {
    if (vecSpans.empty()) {
        return std::chrono::microseconds(0);
    }
    const auto& top = vecSpans[0];
    // If the Span has not yet been closed; return the duration up to now.
    if (top.duration == Span::Duration::max()) {
        return std::chrono::duration_cast<Span::Duration>(
                std::chrono::steady_clock::now() - top.start);
    }
    return top.duration;
}

/**
 * Encode the total micros in 2 bytes. Gives a much better coverage
 * and reasonable error rates on larger values.
 * Idea by Brett Lawson [@brett19]
 * Max Time: 02:00.125042 (120125042)
 */
uint16_t Tracer::getEncodedMicros() const {
    return encodeMicros(getTotalMicros().count());
}

uint16_t Tracer::encodeMicros(uint64_t actual) {
    static const uint64_t maxVal = 120125042;
    actual = std::min(actual, maxVal);
    return uint16_t(std::round(std::pow(actual * 2, 1.0 / 1.74)));
}

std::chrono::microseconds Tracer::decodeMicros(uint16_t encoded) {
    auto usecs = uint64_t(std::pow(encoded, 1.74) / 2);
    return std::chrono::microseconds(usecs);
}

void Tracer::clear() {
    vecSpans.clear();
}

} // namespace cb::tracing

MEMCACHED_PUBLIC_API std::ostream& operator<<(
        std::ostream& os, const cb::tracing::Tracer& tracer) {
    return os << to_string(tracer);
}

MEMCACHED_PUBLIC_API std::string to_string(const cb::tracing::Tracer& tracer,
                                           bool raw) {
    const auto& vecSpans = tracer.getDurations();
    std::ostringstream os;
    auto size = vecSpans.size();
    for (const auto& span : vecSpans) {
        os << to_string(span.code) << "="
           << span.start.time_since_epoch().count() << ":";
        if (span.duration == std::chrono::microseconds::max()) {
            os << "--";
        } else {
            os << span.duration.count();
        }
        size--;
        if (size > 0) {
            os << (raw ? " " : "\n");
        }
    }
    return os.str();
}

MEMCACHED_PUBLIC_API std::string to_string(const cb::tracing::Code tracecode) {
    using cb::tracing::Code;
    switch (tracecode) {
    case Code::Request:
        return "request";
    case Code::BackgroundWait:
        return "bg.wait";
    case Code::BackgroundLoad:
        return "bg.load";
    case Code::Get:
        return "get";
    case Code::GetIf:
        return "get.if";
    case Code::GetStats:
        return "get.stats";
    case Code::SetWithMeta:
        return "set.with.meta";
    case Code::Store:
        return "store";
    case Code::SyncWritePrepare:
        return "sync_write.prepare";
    case Code::SyncWriteAckLocal:
        return "sync_write.ack_local";
    case Code::SyncWriteAckRemote:
        return "sync_write.ack_remote";
    }
    return "unknown tracecode";
}
