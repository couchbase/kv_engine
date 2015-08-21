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
#include <libgreenstack/core/SaslAuth.h>
#include <Greenstack/payload/SaslAuthResponse_generated.h>
#include <Greenstack/payload/SaslAuthRequest_generated.h>

#include <iostream>
#include <stdexcept>

Greenstack::SaslAuthRequest::SaslAuthRequest(const std::string& mechanism,
                                             const std::string& challenge)
    : Request() {
    setOpcode(Greenstack::Opcode::SaslAuth);
    flatbuffers::FlatBufferBuilder fbb;
    auto mec = fbb.CreateString(mechanism);
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(challenge.data());
    auto chal = fbb.CreateVector(ptr, challenge.length());

    Greenstack::Payload::SaslAuthRequestBuilder builder(fbb);
    builder.add_Mechanism(mec);
    builder.add_Challenge(chal);

    auto stuff = builder.Finish();
    Greenstack::Payload::FinishSaslAuthRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

Greenstack::SaslAuthRequest::SaslAuthRequest()
    : Request() {
    setOpcode(Greenstack::Opcode::SaslAuth);
}

const std::string Greenstack::SaslAuthRequest::getMechanism() const {
    auto request = Greenstack::Payload::GetSaslAuthRequest(payload.data());
    std::string ret(request->Mechanism()->c_str());
    return ret;
}

const std::string Greenstack::SaslAuthRequest::getChallenge() const {
    auto request = Greenstack::Payload::GetSaslAuthRequest(payload.data());
    auto vec = request->Challenge();
    return std::string(vec->begin(), vec->end());
}

void Greenstack::SaslAuthRequest::validate() {
    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifySaslAuthRequestBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for SaslAuthRequest");
    }
}

Greenstack::SaslAuthResponse::SaslAuthResponse(const std::string& challenge)
    : Response() {
    setOpcode(Greenstack::Opcode::SaslAuth);
    setStatus(Greenstack::Status::Success);

    flatbuffers::FlatBufferBuilder fbb;

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(challenge.data());
    auto chal = fbb.CreateVector(ptr, challenge.length());

    Greenstack::Payload::SaslAuthResponseBuilder builder(fbb);
    builder.add_Challenge(chal);

    auto stuff = builder.Finish();
    Greenstack::Payload::FinishSaslAuthResponseBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

Greenstack::SaslAuthResponse::SaslAuthResponse()
    : Response() {
    setOpcode(Greenstack::Opcode::SaslAuth);
    // @todo fixme
}

const std::string Greenstack::SaslAuthResponse::getChallenge() const {
    auto response = Greenstack::Payload::GetSaslAuthResponse(payload.data());
    auto vec = response->Challenge();

    if (vec) {
        return std::string(vec->begin(), vec->end());
    } else {
        return std::string();
    }
}

void Greenstack::SaslAuthResponse::validate() {
    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifySaslAuthResponseBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for SaslAuthResponse");
    }
}

Greenstack::SaslAuthResponse::SaslAuthResponse(const Greenstack::Status& status) {
    setOpcode(Greenstack::Opcode::SaslAuth);
    setStatus(status);
    flatbuffers::FlatBufferBuilder fbb;
    Greenstack::Payload::SaslAuthResponseBuilder builder(fbb);
    auto stuff = builder.Finish();
    Greenstack::Payload::FinishSaslAuthResponseBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}
