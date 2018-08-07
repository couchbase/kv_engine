/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "mock_connection.h"
#include <benchmark/benchmark.h>
#include <daemon/mcbp_validators.h>

/**
 * Test the performance of the command validators for the some of the most
 * frequent commands.
 */
class McbpValidatorBench : public ::benchmark::Fixture {
public:
    void SetUp(benchmark::State& st) override {
        McbpValidatorChains::initializeMcbpValidatorChains(validatorChains);
        memset(request.bytes, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    McbpValidatorChains validatorChains;
    MockConnection connection;

    union {
        protocol_binary_request_no_extras request;
        uint8_t blob[4096];
    };
};

BENCHMARK_DEFINE_F(McbpValidatorBench, GetBench)(benchmark::State& state) {
    request.message.header.request.extlen = 0;
    request.message.header.request.keylen = htons(10);
    request.message.header.request.bodylen = htonl(10);

    void* packet = static_cast<void*>(&request);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    Cookie cookie(connection);

    while (state.KeepRunning()) {
        cookie.reset();
        cookie.setPacket(Cookie::PacketContent::Full, buffer);
        validatorChains.invoke(PROTOCOL_BINARY_CMD_GET, cookie);
    }
}

BENCHMARK_DEFINE_F(McbpValidatorBench, SetBench)(benchmark::State& state) {
    request.message.header.request.extlen = 8;
    request.message.header.request.keylen = htons(10);
    request.message.header.request.bodylen = htonl(20);

    void* packet = static_cast<void*>(&request);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    Cookie cookie(connection);

    while (state.KeepRunning()) {
        cookie.reset();
        cookie.setPacket(Cookie::PacketContent::Full, buffer);
        validatorChains.invoke(PROTOCOL_BINARY_CMD_SET, cookie);
    }
}

BENCHMARK_DEFINE_F(McbpValidatorBench, AddBench)(benchmark::State& state) {
    request.message.header.request.extlen = 8;
    request.message.header.request.keylen = htons(10);
    request.message.header.request.bodylen = htonl(20);

    void* packet = static_cast<void*>(&request);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    Cookie cookie(connection);

    while (state.KeepRunning()) {
        cookie.reset();
        cookie.setPacket(Cookie::PacketContent::Full, buffer);
        validatorChains.invoke(PROTOCOL_BINARY_CMD_ADD, cookie);
    }
}

BENCHMARK_REGISTER_F(McbpValidatorBench, GetBench);
BENCHMARK_REGISTER_F(McbpValidatorBench, SetBench);
BENCHMARK_REGISTER_F(McbpValidatorBench, AddBench);
BENCHMARK_MAIN()
