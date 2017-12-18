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
#include "config.h"
#include <tracing/tracer.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <sstream>

std::chrono::microseconds to_micros(const ProcessClock::time_point tp) {
    return std::chrono::time_point_cast<std::chrono::microseconds>(tp)
            .time_since_epoch();
}

namespace cb {
namespace tracing {

Tracer::SpanId Tracer::invalidSpanId() {
    return std::numeric_limits<SpanId>::max();
}

Tracer::SpanId Tracer::begin(const TraceCode tracecode) {
    std::lock_guard<std::mutex> lock(spanMutex);
    auto t = to_micros(ProcessClock::now());
    vecSpans.push_back(Span(tracecode, t));
    return vecSpans.size() - 1;
}

bool Tracer::end(SpanId spanId) {
    std::lock_guard<std::mutex> lock(spanMutex);
    if (spanId >= vecSpans.size())
        return false;
    auto& span = vecSpans[spanId];
    span.duration = to_micros(ProcessClock::now()) - span.start;
    return true;
}

bool Tracer::end(const TraceCode tracecode) {
    std::lock_guard<std::mutex> lock(spanMutex);
    SpanId spanId = 0;
    for (const auto& span : vecSpans) {
        if (span.code == tracecode) {
            return end(spanId);
        }
        spanId++;
    }
    return false;
}

const std::vector<Span>& Tracer::getDurations() const {
    return vecSpans;
}

std::chrono::microseconds Tracer::getTotalMicros() const {
    if (vecSpans.empty()) {
        return std::chrono::microseconds(0);
    }
    return vecSpans[0].duration;
}

/**
 * Encode the total micros in 2 bytes using the below scheme
 * m11 = m.wxyz = log11(micros)
 * m <=7 => first 3 bits of the 16 bits
 * wxyz <= 8191 ==> remaining 13 bits
 * ---
 * To Decode, get the log11 value & then raise it to 11.
 * Max time period represented here :
 * pow(7.8191,11)
 *   = 138949722 micros
 *   = 02:18.916408 (2m 18s)
 */
uint16_t Tracer::getEncodedMicros(uint64_t actual) const {
    static const uint8_t base = 11;
    static const double ln11 = std::log(base);

    double const MAX_11_REP = 7.8191f;

    if (0 == actual) {
        actual = getTotalMicros().count();
    }

    auto repMicros = std::log(actual) / ln11;

    // make sure it is <= than 7.8191
    repMicros = std::min(repMicros, MAX_11_REP);

    // first the integral part
    uint16_t encodedMicros = uint8_t(repMicros);

    encodedMicros <<= 13;

    // remove the integral part
    repMicros -= int(repMicros);

    // we need the first 4 significant digits only
    uint16_t mantissa = uint16_t(repMicros *= 10000);
    mantissa = std::min(mantissa, uint16_t(8191));

    encodedMicros += mantissa;

    return encodedMicros;
}

std::chrono::microseconds Tracer::decodeMicros(uint16_t encoded) const {
    static const uint8_t base = 11;

    auto repMicros = (encoded >> 13) + // first 3 bits
                     (encoded & 0x1FFF) / 10000.0; // last 13 bits
    return std::chrono::microseconds(uint64_t(std::pow(base, repMicros)));
}

void Tracer::clear() {
    vecSpans.clear();
}

} // end namespace tracing
} // end namespace cb

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
        os << to_string(span.code) << "=" << span.start.count() << ":"
           << span.duration.count();
        size--;
        if (size > 0) {
            os << (raw ? " " : "\n");
        }
    }
    return os.str();
}

MEMCACHED_PUBLIC_API std::string to_string(
        const cb::tracing::TraceCode tracecode) {
    using cb::tracing::TraceCode;
    switch (tracecode) {
    case TraceCode::REQUEST:
        return "request";

    case TraceCode::ALLOCATE:
        return "allocate";
    case TraceCode::BGFETCH:
        return "bg.fetch";
    case TraceCode::FLUSH:
        return "flush";
    case TraceCode::GAT:
        return "gat";
    case TraceCode::GET:
        return "get";
    case TraceCode::GETIF:
        return "get.if";
    case TraceCode::GETLOCKED:
        return "get.locked";
    case TraceCode::GETMETA:
        return "get.meta";
    case TraceCode::GETSTATS:
        return "get.stats";
    case TraceCode::ITEMDELETE:
        return "item.delete";
    case TraceCode::LOCK:
        return "lock";
    case TraceCode::OBSERVE:
        return "observe";
    case TraceCode::REMOVE:
        return "remove";
    case TraceCode::SETITEMINFO:
        return "set.item.info";
    case TraceCode::SETWITHMETA:
        return "set.with.meta";
    case TraceCode::STORE:
        return "store";
    case TraceCode::STOREIF:
        return "store.if";
    case TraceCode::UNLOCK:
        return "unlock";
    }
    return "unknown tracecode";
}
