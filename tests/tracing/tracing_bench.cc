/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

/*
 * Benchmarks relating to Session Tracing.
 */

#include <benchmark/benchmark.h>

#include "programs/engine_testapp/mock_server.h"
#include "tracing/trace_helpers.h"
#include "tracing/tracetypes.h"

void SessionTracingRecordMutationSpan(benchmark::State& state) {
    auto* cookie = create_mock_cookie();
    mock_get_traceable(cookie).setTracingEnabled(true);

    while (state.KeepRunning()) {
        // Representative set of TRACE_BLOCKS for recording a mutation's work.
        {
            TRACE_SCOPE(get_mock_server_api(),
                        cookie,
                        cb::tracing::TraceCode::REQUEST);
        }
        {
            TRACE_SCOPE(get_mock_server_api(),
                        cookie,
                        cb::tracing::TraceCode::ALLOCATE);
        }
        {
            TRACE_SCOPE(get_mock_server_api(),
                        cookie,
                        cb::tracing::TraceCode::STORE);
        }

        mock_get_traceable(cookie).getTracer().clear();
    }
}

BENCHMARK(SessionTracingRecordMutationSpan);

BENCHMARK_MAIN()
