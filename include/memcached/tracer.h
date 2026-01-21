/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/tracecode.h>

#include <folly/Synchronized.h>
#include <chrono>
#include <string>
#include <vector>

namespace cb::tracing {

using Clock = std::chrono::steady_clock;

class Span {
public:
    /// Type used for storing durations - 32bit microsecond.
    /// gives maximum duration of 35.79minutes.
    using Duration = std::chrono::duration<uint32_t, std::micro>;

    Span(Code code,
         Clock::time_point start,
         Duration duration = Duration::max())
        : start(start), duration(duration), code(code) {
    }
    Clock::time_point start;
    Duration duration;
    Code code;
};

/**
 * A TraceRecorer can record the event timing information.
 * @tparam EventId identifier for the event
 */
template <typename EventId>
class TraceRecorder {
public:
    /// Record a complete span (when both start and end are already known).
    virtual void record(EventId id,
                        Clock::time_point start,
                        Clock::time_point end) = 0;

    virtual ~TraceRecorder() = default;
};

/**
 * Tracer maintains an ordered vector of tracepoints
 * with name:time(micros)
 */
class Tracer : public TraceRecorder<Code> {
public:
    /// The maximum number of trace spans added (in addition to the entire
    /// Request span)
    static constexpr const std::size_t MaxTraceSpans = 500;

    void record(Code code,
                Clock::time_point start,
                Clock::time_point end) override;

    Span::Duration getTotalMicros() const;

    uint16_t getEncodedMicros() const;
    /**
     * Max time period represented here is 02:00.125042
     */
    static uint16_t encodeMicros(uint64_t actual);

    static std::chrono::microseconds decodeMicros(uint16_t encoded);

    // clear the collected trace data;
    void clear();

    /// Get a string representation of all of the spans
    std::string to_string() const;

protected:
    folly::Synchronized<std::vector<Span>, std::mutex> vecSpans;
};

/**
 * Anything which can provide a TraceRecorder to record event timing
 * information.
 */
template <typename EventId_>
class Traceable {
public:
    using EventId = EventId_;

    virtual ~Traceable() = default;
    /**
     * Returns a TraceRecorder which is valid only within the current thread and
     * for the lifetime of this object.
     */
    virtual TraceRecorder<EventId>& getTracer() = 0;
    /**
     * Returns a TraceRecorder which is valid only within the current thread and
     * for the lifetime of this object.
     */
    virtual const TraceRecorder<EventId>& getTracer() const = 0;
};

/**
 * Base class for a Traceable type. The type of event ID is cb::tracing::Code
 * and is suitable for command tracing.
 */
class TracerMixin : public Traceable<Code> {
public:
    Tracer& getTracer() override {
        return tracer;
    }
    const Tracer& getTracer() const override {
        return tracer;
    }

protected:
    Tracer tracer;
};

/**
 * Helper class to assist in recording a Span into Tracer objects via
 * ScopeTimerN<> classes.
 *
 * The start and stop methods record the duration of the span and it is
 * injected into the provided traceable object as part of object destruction (as
 * long as start() was at least called).
 *
 * To allow simplification of code which might or might not be executed in
 * a client-specific context it is possible to specify nullptr as the traceable
 * and this is a "noop".
 */
template <typename EventId>
class SpanStopwatch {
public:
    SpanStopwatch(Traceable<EventId>& traceable, EventId eventId)
        : traceable(&traceable), eventId(eventId) {
    }
    SpanStopwatch(Traceable<EventId>* traceable, EventId eventId)
        : traceable(traceable), eventId(eventId) {
    }

    ~SpanStopwatch() {
        if (traceable) {
            traceable->getTracer().record(eventId, startTime, stopTime);
        }
    }

    void start(Clock::time_point tp) {
        startTime = tp;
    }

    void stop(Clock::time_point tp) {
        stopTime = tp;
    }

private:
    Traceable<EventId>* const traceable;
    Clock::time_point startTime;
    Clock::time_point stopTime;
    const EventId eventId;
};

template <typename EventId, typename Mutex>
class MutexSpan {
public:
    /**
     * Acquire the specified mutex and push a trace element for the time
     * waiting for acquire the mutex and one span for how long it was held
     * if one of the values exceeds the provided threshold
     */
    MutexSpan(Traceable<EventId>* traceable,
              Mutex& mutex_,
              EventId wait,
              EventId held,
              Clock::duration threshold_ = Clock::duration::zero())
        : traceable(traceable),
          mutex(mutex_),
          threshold(threshold_),
          wait(wait),
          held(held) {
        if (traceable) {
            start = Clock::now();
            mutex.lock();
            lockedAt = Clock::now();
        } else {
            mutex.lock();
        }
    }

    ~MutexSpan() {
        mutex.unlock();
        if (traceable) {
            releasedAt = std::chrono::steady_clock::now();
            const auto waitTime = lockedAt - start;
            const auto heldTime = releasedAt - lockedAt;
            if (waitTime > threshold || heldTime > threshold) {
                auto& tracer = traceable->getTracer();
                tracer.record(wait, start, lockedAt);
                tracer.record(held, lockedAt, releasedAt);
            }
        }
    }

private:
    Traceable<EventId>* const traceable;
    Mutex& mutex;
    const Clock::duration threshold;
    const Code wait;
    const Code held;
    Clock::time_point start;
    Clock::time_point lockedAt;
    Clock::time_point releasedAt;
};

} // namespace cb::tracing

std::ostream& operator<<(std::ostream& os, const cb::tracing::Tracer& tracer);
