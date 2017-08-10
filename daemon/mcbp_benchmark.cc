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

#include "mcbp.h"

#include <benchmark/benchmark.h>

// Benchmark trying to format into the net_buf structure

// Our pipe keeps track of the memory we read, so we need to benchmark
// the time it takes to reset the pipe (so we know how much to subtract from
// the pipe tests
void PipeClearBenchmark(benchmark::State& state) {
    cb::Pipe pipe(1024);
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(&pipe);
        pipe.clear();
    }
}
BENCHMARK(PipeClearBenchmark);

// Benchmark the overhead of calling produced() to initialize data
void PipeProducedAndClearBenchmark(benchmark::State& state) {
    cb::Pipe pipe(100);
    while (state.KeepRunning()) {
        pipe.produced(24);
        benchmark::DoNotOptimize(&pipe);
        pipe.clear();
        benchmark::DoNotOptimize(&pipe);
    }
}
BENCHMARK(PipeProducedAndClearBenchmark);

// Benchmark inserting into the pipe by using the wdata() and produced()
// interface (without calling any lambda functions so that we can see if
// that adds an extra overhead)
void McbpAddHeaderBenchmark(benchmark::State& state) {
    cb::Pipe pipe(1024);
    while (state.KeepRunning()) {
        mcbp_add_header(pipe,
                        PROTOCOL_BINARY_CMD_ADD,
                        0,
                        0,
                        0,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        0,
                        0);
        benchmark::DoNotOptimize(&pipe);
        pipe.clear();
    }
}
BENCHMARK(McbpAddHeaderBenchmark);

// Benchmark keeping the pipe "up to date" by inspecting if the entry
// in the io-vector just sent was the tip of the pipe.

/**
 * Initialize the msghdr we're simulating that we've sent by using sendmsg.
 *
 * The numbers simulate a get response where the 24 first byte is the
 * generated header we stored in the buffer, followed by a 4 byte extras
 * section (the flags section). Then a 5 byte key, and finally the 199 bytes
 * big body.
 *
 * The base address doesn't really matter, as we don't really look at them
 * _except_ for the address where we stored the 24 byte header (as thats
 * the one we generated and want to keep track of)
 *
 * @param iov The IO vector to insert the various segments into
 * @param hdr The message object to create
 * @param header The location of the header
 */
static void initialize_msg_header(std::array<iovec, 4>& iov,
                                  struct msghdr& hdr,
                                  uint8_t* header) {
    // Add header
    iov[0].iov_base = header;
    iov[0].iov_len = 24;

    // Add flags somewhere outside the header object
    iov[1].iov_base = header - 200;
    iov[1].iov_len = 4;

    // Add a key somewhere outside the header object
    iov[2].iov_base = header - 200;
    iov[2].iov_len = 50;

    // Add a body somewhere outside the header object
    iov[3].iov_base = header - 200;
    iov[3].iov_len = 199;

    // Link it into the message header structure
    hdr.msg_iov = iov.data();
    hdr.msg_iovlen = 4;
}

// Create a baseline of the overhead of initializing the message header
void InitializeMsgHeaderBenchmark(benchmark::State& state) {
    msghdr hdr{};
    std::array<iovec, 4> iov{};

    uint8_t blob[1024];
    while (state.KeepRunning()) {
        initialize_msg_header(iov, hdr, blob);
        benchmark::DoNotOptimize(&hdr);
    }
}
BENCHMARK(InitializeMsgHeaderBenchmark);

// Benchmark the proposed implementation where we keep track of the
// number of bytes processed from the input pipe
void AdjustMsgHeaderBenchmark(benchmark::State& state) {
    struct msghdr hdr {};
    std::array<iovec, 4> iov{};
    cb::Pipe pipe(1024);
    const uint8_t* rbuf = pipe.rdata().data();

    while (state.KeepRunning()) {
        pipe.produced(24);
        initialize_msg_header(iov, hdr, const_cast<uint8_t*>(rbuf));
        benchmark::DoNotOptimize(&pipe);
        adjust_msghdr(pipe, &hdr, 24 + 4 + 50 + 199);
        benchmark::DoNotOptimize(&pipe);
    }
}
BENCHMARK(AdjustMsgHeaderBenchmark);

BENCHMARK_MAIN()
