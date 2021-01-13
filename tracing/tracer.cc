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

namespace cb::tracing {

SpanId Tracer::begin(Code tracecode, Clock::time_point startTime) {
    return vecSpans.withLock([tracecode, startTime](auto& spans) {
        spans.emplace_back(tracecode, startTime);
        return spans.size() - 1;
    });
}

bool Tracer::end(SpanId spanId, Clock::time_point endTime) {
    return vecSpans.withLock([spanId, endTime](auto& spans) {
        if (spanId >= spans.size()) {
            return false;
        }

        auto& span = spans.at(spanId);
        span.duration = std::chrono::duration_cast<Span::Duration>(endTime -
                                                                   span.start);
        return true;
    });
}

std::vector<Span> Tracer::extractDurations() {
    std::vector<Span> ret;
    vecSpans.swap(ret);
    return ret;
}

Span::Duration Tracer::getTotalMicros() const {
    return vecSpans.withLock([](auto& spans) -> Span::Duration {
        if (spans.empty()) {
            return {};
        }
        const auto& top = spans.at(0);
        // If the Span has not yet been closed; return the duration up to now.
        if (top.duration == Span::Duration::max()) {
            return std::chrono::duration_cast<Span::Duration>(Clock::now() -
                                                              top.start);
        }
        return top.duration;
    });
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
    vecSpans.lock()->clear();
}

std::string Tracer::to_string() const {
    return vecSpans.withLock([](auto& spans) {
        std::ostringstream os;
        auto size = spans.size();
        for (const auto& span : spans) {
            os << ::to_string(span.code) << "="
               << span.start.time_since_epoch().count() << ":";
            if (span.duration == std::chrono::microseconds::max()) {
                os << "--";
            } else {
                os << span.duration.count();
            }
            size--;
            if (size > 0) {
                os << " ";
            }
        }
        return os.str();
    });
}

} // namespace cb::tracing

MEMCACHED_PUBLIC_API std::ostream& operator<<(
        std::ostream& os, const cb::tracing::Tracer& tracer) {
    return os << tracer.to_string();
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
