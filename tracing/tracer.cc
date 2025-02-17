/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <memcached/tracer.h>
#include <algorithm>
#include <cmath>
#include <sstream>

namespace cb::tracing {

void Tracer::record(Code code, Clock::time_point start, Clock::time_point end) {
    auto duration = std::chrono::duration_cast<Span::Duration>(end - start);
    vecSpans.withLock([code, start, duration](auto& spans) {
        if (spans.size() < MaxTraceSpans || code == Code::Request) {
            spans.emplace_back(code, start, duration);
        }
    });
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
    static constexpr uint64_t maxVal = 120125042;
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

        if (spans.size() == MaxTraceSpans + 1) {
            // If we've got "an overflow" it mans that we've (most likely)
            // dropped some frames (we could in theory hit the exact number
            // of trace spans; Ignore that and assume that we've dropped
            // elements). The last entry in the vector is the Request span,
            // so we should take the start time from the one before that
            // and use as the start time for our overflow entry.
            os << " overflow="
               << spans.at(MaxTraceSpans - 1).start.time_since_epoch().count()
               << ":--";
        }

        return os.str();
    });
}

} // namespace cb::tracing

std::ostream& operator<<(std::ostream& os, const cb::tracing::Tracer& tracer) {
    return os << tracer.to_string();
}
