/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <benchmark/benchmark.h>
#include <memcached/isotime.h>
#include <platform/sysinfo.h>
#include <platform/timeutils.h>

static void ISOTimeClassBackend(benchmark::State& state) {
    const auto length = ISOTime::generatetimestamp().length();
    if (length < 26) {
        std::abort();
    }
    while (state.KeepRunning()) {
        if (ISOTime::generatetimestamp().length() != length) {
            std::abort();
        }
    }
}

static void SpdlogBackend(benchmark::State& state) {
    const auto length = cb::time::timestamp().length();
    if (length < 26) {
        std::abort();
    }

    while (state.KeepRunning()) {
        if (cb::time::timestamp().length() != length) {
            std::abort();
        }
    }
}

BENCHMARK(ISOTimeClassBackend)->ThreadRange(1, int(cb::get_cpu_count()));
BENCHMARK(SpdlogBackend)->ThreadRange(1, int(cb::get_cpu_count()));
