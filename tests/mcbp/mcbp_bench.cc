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
#include <daemon/cookie.h>
#include <daemon/front_end_thread.h>
#include <daemon/mcbp_validators.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>

FrontEndThread thread;
/**
 * Test the performance of the command validators for the some of the most
 * frequent commands.
 */
class McbpValidatorBench : public ::benchmark::Fixture {
public:
    void SetUp(benchmark::State& st) override {
        memset(request.bytes, 0, sizeof(request));
        request.message.header.request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.message.header.request.setDatatype(cb::mcbp::Datatype::Raw);
    }

    McbpValidatorBench() : connection(thread) {
    }

protected:
    McbpValidator validator;
    MockConnection connection;

    union {
        protocol_binary_request_no_extras request;
        uint8_t blob[4096];
    };
};

BENCHMARK_DEFINE_F(McbpValidatorBench, GetBench)(benchmark::State& state) {
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(10);
    request.message.header.request.setBodylen(10);

    void* packet = static_cast<void*>(&request);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    Cookie cookie(connection);

    while (state.KeepRunning()) {
        cookie.reset();
        cookie.setPacket(req);
        validator.validate(cb::mcbp::ClientOpcode::Get, cookie);
    }
}

BENCHMARK_DEFINE_F(McbpValidatorBench, SetBench)(benchmark::State& state) {
    request.message.header.request.setExtlen(8);
    request.message.header.request.setKeylen(10);
    request.message.header.request.setBodylen(20);

    void* packet = static_cast<void*>(&request);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    Cookie cookie(connection);

    while (state.KeepRunning()) {
        cookie.reset();
        cookie.setPacket(req);
        validator.validate(cb::mcbp::ClientOpcode::Set, cookie);
    }
}

BENCHMARK_DEFINE_F(McbpValidatorBench, AddBench)(benchmark::State& state) {
    request.message.header.request.setExtlen(8);
    request.message.header.request.setKeylen(10);
    request.message.header.request.setBodylen(20);

    void* packet = static_cast<void*>(&request);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    Cookie cookie(connection);

    while (state.KeepRunning()) {
        cookie.reset();
        cookie.setPacket(req);
        validator.validate(cb::mcbp::ClientOpcode::Add, cookie);
    }
}

BENCHMARK_REGISTER_F(McbpValidatorBench, GetBench);
BENCHMARK_REGISTER_F(McbpValidatorBench, SetBench);
BENCHMARK_REGISTER_F(McbpValidatorBench, AddBench);
BENCHMARK_MAIN()
