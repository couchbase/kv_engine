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

/*
 * Benchmarks relating to Session Tracing.
 */

#include <benchmark/benchmark.h>
#include <memcached/tracer.h>
#include <platform/scope_timer.h>

#include "programs/engine_testapp/mock_cookie.h"
#include "trace_helpers.h"

#define TRACE_SCOPE(ck, code) ScopedTracer __st__##__LINE__(ck, code)

void SessionTracingRecordMutationSpan(benchmark::State& state) {
    auto* cookie = create_mock_cookie();
    cookie_to_mock_cookie(cookie)->setTracingEnabled(true);

    while (state.KeepRunning()) {
        // Representative set of TRACE_BLOCKS for recording a mutation's work.
        { TRACE_SCOPE(cookie, cb::tracing::Code::Request); }
        { TRACE_SCOPE(cookie, cb::tracing::Code::Store); }

        cookie_to_mock_cookie(cookie)->getTracer().clear();
    }
    destroy_mock_cookie(cookie);
}

// Benchmark the performance when using a ScopeTimer instead of the TRACE_SCOPE
// macros.
void SessionTracingScopeTimer(benchmark::State& state) {
    auto* cookie = create_mock_cookie();
    cookie_to_mock_cookie(cookie)->setTracingEnabled(true);

    while (state.KeepRunning()) {
        // Representative set of scopes for recording a mutation's work.
        {
            ScopeTimer1<TracerStopwatch> timer(cookie,
                                               cb::tracing::Code::Request);
        }
        {
            ScopeTimer1<TracerStopwatch> timer(cookie,
                                               cb::tracing::Code::Store);
        }

        cookie_to_mock_cookie(cookie)->getTracer().clear();
    }
    destroy_mock_cookie(cookie);
}

// Benchmark the performance of encoding a duration into a 2-byte compressed
// format.
void SessionTracingEncode(benchmark::State& state) {
    auto* cookie = create_mock_cookie();

    // Record a single span so we have something to encode. Don't care what
    // the value is, but want it runtime-calculated so we don't constant-fold
    // away the encoding below.
    auto& traceable = *cookie_to_mock_cookie(cookie);
    traceable.setTracingEnabled(true);
    {
        ScopeTimer1<TracerStopwatch> timer(
                TracerStopwatch(cookie, cb::tracing::Code::Request));
    }

    while (state.KeepRunning()) {
        // Unroll 5x to amortise KeepRunning() overhead.
        benchmark::DoNotOptimize(traceable.getTracer().getEncodedMicros());
        benchmark::DoNotOptimize(traceable.getTracer().getEncodedMicros());
        benchmark::DoNotOptimize(traceable.getTracer().getEncodedMicros());
        benchmark::DoNotOptimize(traceable.getTracer().getEncodedMicros());
        benchmark::DoNotOptimize(traceable.getTracer().getEncodedMicros());
    }
    destroy_mock_cookie(cookie);
}

BENCHMARK(SessionTracingRecordMutationSpan);
BENCHMARK(SessionTracingScopeTimer);
BENCHMARK(SessionTracingEncode);
