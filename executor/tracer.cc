/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "platform/cb_arena_malloc.h"
#include <executor/tracer.h>

#include <folly/Chrono.h>
#include <folly/container/F14Map.h>

namespace cb::executor {

Tracer::Tracer() {
    cb::NoArenaGuard guard;
    data = std::make_unique<Tracer::Profile>();
}

Tracer::~Tracer() {
    cb::NoArenaGuard guard;
    data.reset();
}

CoarseSteadyClock::time_point CoarseSteadyClock::now() {
    return time_point(
            folly::chrono::coarse_steady_clock::now().time_since_epoch());
}

const Tracer::Profile& Tracer::getProfile() const {
    return *data;
}

void Tracer::record(EventLiteral eventId,
                    cb::tracing::Clock::time_point start,
                    cb::tracing::Clock::time_point end) {
    cb::NoArenaGuard guard;
    // Larger type used for calculation.
    using LongDuration = std::chrono::duration<int64_t, Duration::period>;

    auto& [count, duration] = (*data)[eventId];

    const auto delta = std::chrono::duration_cast<LongDuration>(end - start);
    if (delta.count() < 0) {
        throw std::range_error("Duration cannot be negative: " +
                               std::to_string(delta.count()));
    }
    const auto newDuration =
            std::chrono::duration_cast<LongDuration>(duration) + delta;

    ++count;
    // Max duration is 71 minutes - shouldn't overflow, but clamp to max.
    if (newDuration.count() > std::numeric_limits<Duration::rep>::max()) {
        duration = Duration::max();
    } else {
        duration = newDuration;
    }
}

void Tracer::clear() {
    cb::NoArenaGuard guard;
    data->clear();
}

} // namespace cb::executor
