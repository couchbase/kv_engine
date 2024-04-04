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

#include <folly/Synchronized.h>
#include <chrono>
#include <string>
#include <vector>

namespace cb::tracing {

enum class Code : uint8_t {
    /// Time spent in the entire request
    Request,
    /// Time spent throttled
    Throttled,
    /// The time spent during execution on front end thread
    Execute,
    /// The time spent associating a bucket
    AssociateBucket,
    /// The time spent disassociating a bucket
    DisassociateBucket,
    /// The time spent waiting to acquire the bucket lock
    BucketLockWait,
    /// The time spent holding the bucket lock
    BucketLockHeld,
    /// Time spent creating the RBAC context
    CreateRbacContext,
    /// Time spent updating privilege context when toggling buckets
    UpdatePrivilegeContext,
    /// Time spent generating audit event
    Audit,
    /// Time spent reconfiguring audit daemon
    AuditReconfigure,
    /// Time spent generating audit stats
    AuditStats,
    /// Time spend validating audit input
    AuditValidate,
    /// Time spent decompressing Snappy data.
    SnappyDecompress,
    /// Time spent validating if incoming value is JSON.
    JsonValidate,
    /// Time spent parsing JSON.
    JsonParse,
    /// Time spent performing subdoc lookup / mutation (all paths).
    SubdocOperate,
    /// Time spent waiting for a background fetch operation to be scheduled.
    BackgroundWait,
    /// Time spent performing the actual background load from disk.
    BackgroundLoad,
    /// Time spent in EngineIface::get
    Get,
    /// Time spent in EngineIface::get_if
    GetIf,
    /// Time spent in EngineIface::getStats
    GetStats,
    /// Time spent in EngineIface::setWithMeta
    SetWithMeta,
    /// Time spent in EngineIface::store and EngineIface::store_if
    Store,
    /// Time spent by a SyncWrite in Prepared state before being completed.
    SyncWritePrepare,
    /// Time when a SyncWrite local ACK is received by the Active.
    SyncWriteAckLocal,
    /// Time when a SyncWrite replica ACK is received by the Active.
    SyncWriteAckRemote,
    /// Time spent in Select Bucket
    SelectBucket,
    /// Time spent in running the SASL start/step call on the executor
    /// thread
    Sasl,
    /// Time spent looking up stats from the underlying Storage engine
    StorageEngineStats,
};

using Clock = std::chrono::steady_clock;

class Span {
public:
    /// Type used for storing durations - 32bit microsecond.
    /// gives maximum duration of 35.79minutes.
    using Duration = std::chrono::duration<int32_t, std::micro>;

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
 * Tracer maintains an ordered vector of tracepoints
 * with name:time(micros)
 */
class Tracer {
public:
    /// The maximum number of trace spans added (in addition to the entire
    /// Request span)
    static constexpr const std::size_t MaxTraceSpans = 500;
    // Record a complete Span (when both start and end are already known).
    void record(Code code, Clock::time_point start, Clock::time_point end);

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

class Traceable {
public:
    virtual ~Traceable() = default;
    Tracer& getTracer() {
        return tracer;
    }
    const Tracer& getTracer() const {
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
class SpanStopwatch {
public:
    SpanStopwatch(Traceable& traceable, Code code)
        : traceable(&traceable), code(code) {
    }
    SpanStopwatch(Traceable* traceable, Code code)
        : traceable(traceable), code(code) {
    }

    ~SpanStopwatch() {
        if (traceable) {
            traceable->getTracer().record(code, startTime, stopTime);
        }
    }

    void start(Clock::time_point tp) {
        startTime = tp;
    }

    void stop(Clock::time_point tp) {
        stopTime = tp;
    }

private:
    Traceable* const traceable;
    Clock::time_point startTime;
    Clock::time_point stopTime;
    const Code code;
};

template <class Mutex>
class MutexSpan {
public:
    /**
     * Acquire the specified mutex and push a trace element for the time
     * waiting for acquire the mutex and one span for how long it was held
     * if one of the values exceeds the provided threshold
     */
    MutexSpan(Traceable* traceable,
              Mutex& mutex_,
              Code wait,
              Code held,
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
                auto tracer = traceable->getTracer();
                tracer.record(wait, start, lockedAt);
                tracer.record(held, lockedAt, releasedAt);
            }
        }
    }

private:
    Traceable* const traceable;
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
std::string to_string(cb::tracing::Code tracecode);
