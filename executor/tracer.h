/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <folly/container/F14Map-fwd.h>
#include <memcached/tracer.h>
#include <platform/cb_time.h>

namespace cb::executor {

/**
 * Uses the coarse system clock (via folly) and has a time_point compatible with
 * the cb::time::steady_clock.
 */
struct CoarseSteadyClock {
    using duration = cb::time::steady_clock::duration;
    using period = cb::time::steady_clock::period;
    using rep = cb::time::steady_clock::rep;
    using time_point = cb::time::steady_clock::time_point;
    static constexpr bool is_steady{true};

    static time_point now();
};

/**
 * An EventLiteral is simply a wrapper for a const char* with the contract that
 * the string is a compile-time literal (and does not need to be copied).
 * It's the type we use to identfy trace events.
 */
struct EventLiteral {
    template <size_t N>
    /* NOLINTNEXTLINE(modernize-avoid-c-arrays) */
    constexpr EventLiteral(const char (&value)[N]) : value(value) {
    }

    operator std::string_view() const {
        return value;
    }

    const char* const value;
};

inline bool operator==(const EventLiteral& lhs, const EventLiteral& rhs) {
    return lhs.value == rhs.value || static_cast<std::string_view>(lhs) ==
                                             static_cast<std::string_view>(rhs);
}

inline bool operator!=(const EventLiteral& lhs, const EventLiteral& rhs) {
    return !(lhs == rhs);
}

} // namespace cb::executor

template <>
struct std::hash<cb::executor::EventLiteral> {
    std::size_t operator()(const cb::executor::EventLiteral& s) const noexcept {
        return std::hash<std::string_view>{}(s.value);
    }
};

namespace cb::executor {

class Traceable : public cb::tracing::Traceable<cb::executor::EventLiteral> {};

/**
 * Tracer used by the executor. Allows profiling the execution of tasks.
 * Produces a Profile which contains the cumulative time spent for an event.
 */
class Tracer : public cb::tracing::TraceRecorder<EventLiteral> {
public:
    Tracer();
    ~Tracer() override;

    /**
     * Type used for storing durations - 32bit microsecond.
     * gives maximum duration of 71.58minutes.
     */
    using Duration = std::chrono::duration<uint32_t, std::micro>;

    /**
     * Typed used for storing data for an event.
     */
    using EventData = std::pair<uint32_t, Duration>;

    /**
     * Type used to store the map of events and total durations.
     * F14Vector map has the nice property that we can clear the map without
     * freeing up the memory.
     */
    using Profile = folly::F14VectorMap<EventLiteral, EventData>;

    /**
     * Returns the current profile data.
     */
    const Profile& getProfile() const;

    void record(EventLiteral eventId,
                cb::tracing::Clock::time_point start,
                cb::tracing::Clock::time_point end) override;

    /**
     * Clears the recoded durations without performing memory deallocation.
     */
    virtual void clear();

private:
    /// Holds the recorded data. Uses a pointer to avoid including F14Map.
    std::unique_ptr<Profile> data;
};

using Profile = Tracer::Profile;

} // namespace cb::executor
