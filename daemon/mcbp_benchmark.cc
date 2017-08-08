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
void McbpAddHeaderBenchmark(benchmark::State& state) {
    net_buf buf;
    char buffer[1024];
    buf.buf = buffer;
    while (state.KeepRunning()) {
        mcbp_add_header(buf,
                        PROTOCOL_BINARY_CMD_ADD,
                        0,
                        0,
                        0,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        0,
                        0);
        benchmark::DoNotOptimize(&buf);
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

// Benchmark the current implementation of just updating the message header
void AdjustMsgHeaderBenchmark(benchmark::State& state) {
    msghdr hdr{};
    std::array<iovec, 4> iov{};
    uint8_t blob[1024];

    while (state.KeepRunning()) {
        initialize_msg_header(iov, hdr, blob);
        benchmark::DoNotOptimize(&hdr);
        adjust_msghdr(&hdr, 24 + 4 + 50 + 199);
        benchmark::DoNotOptimize(&hdr);
    }
}
BENCHMARK(AdjustMsgHeaderBenchmark);

BENCHMARK_MAIN()
