/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
