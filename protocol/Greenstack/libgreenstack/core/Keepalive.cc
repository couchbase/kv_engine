/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include <libgreenstack/Opcodes.h>
#include <libgreenstack/core/Keepalive.h>
#include <Greenstack/payload/KeepaliveRequest_generated.h>
#include <libgreenstack/Status.h>
#include <iostream>
#include <stdexcept>

Greenstack::KeepaliveRequest::KeepaliveRequest()
    : Request(Greenstack::Opcode::Keepalive) {

    flatbuffers::FlatBufferBuilder fbb;
    Greenstack::Payload::KeepaliveRequestBuilder builder(fbb);
    auto stuff = builder.Finish();
    Greenstack::Payload::FinishKeepaliveRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

Greenstack::KeepaliveRequest::KeepaliveRequest(uint16_t interval)
    : Request(Greenstack::Opcode::Keepalive) {

    flatbuffers::FlatBufferBuilder fbb;
    Greenstack::Payload::KeepaliveRequestBuilder builder(fbb);
    builder.add_interval(interval);
    auto stuff = builder.Finish();
    Greenstack::Payload::FinishKeepaliveRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

uint16_t Greenstack::KeepaliveRequest::getInterval() const {
    auto request = Greenstack::Payload::GetKeepaliveRequest(payload.data());
    return request->interval();
}

void Greenstack::KeepaliveRequest::validate() {

    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifyKeepaliveRequestBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for KeepaliveRequest");
    }
}
