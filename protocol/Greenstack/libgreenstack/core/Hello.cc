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
#include <libgreenstack/core/Hello.h>
#include <Greenstack/payload/HelloRequest_generated.h>
#include <Greenstack/payload/HelloResponse_generated.h>
#include <libgreenstack/Status.h>
#include <iostream>
#include <stdexcept>

Greenstack::HelloRequest::HelloRequest(const std::string& userAgent,
                                       const std::string& agentVersion,
                                       const std::string& comment)
    : Request() {
    if (userAgent.empty()) {
        throw std::runtime_error("User Agent shall be present");
    }

    setOpcode(Greenstack::Opcode::Hello);
    flatbuffers::FlatBufferBuilder fbb;
    auto agent = fbb.CreateString(userAgent);
    auto usecase = fbb.CreateString(comment);
    auto version = fbb.CreateString(agentVersion);

    Greenstack::Payload::HelloRequestBuilder builder(fbb);
    builder.add_UserAgent(agent);
    builder.add_UserAgentVersion(version);
    builder.add_Comment(usecase);
    auto stuff = builder.Finish();
    Greenstack::Payload::FinishHelloRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

Greenstack::HelloRequest::HelloRequest()
    : Request() {
    setOpcode(Greenstack::Opcode::Hello);
}

const std::string Greenstack::HelloRequest::getUserAgent() const {
    auto request = Greenstack::Payload::GetHelloRequest(payload.data());
    std::string ret(request->UserAgent()->c_str());
    return ret;
}

void Greenstack::HelloRequest::validate() {

    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifyHelloRequestBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for HelloRequest");
    }
}

const std::string Greenstack::HelloRequest::getUserAgentVersion() const {
    auto request = Greenstack::Payload::GetHelloRequest(payload.data());
    std::string ret(request->UserAgentVersion()->c_str());
    return ret;
}

const std::string Greenstack::HelloRequest::getComment() const {
    auto request = Greenstack::Payload::GetHelloRequest(payload.data());
    std::string ret(request->Comment()->c_str());
    return ret;
}

Greenstack::HelloResponse::HelloResponse(const std::string& userAgent,
                                         const std::string& userAgentVersion,
                                         const std::string& mechanisms)
    : Response() {
    setOpcode(Greenstack::Opcode::Hello);
    setStatus(Greenstack::Status::Success);
    if (userAgent.empty()) {
        throw std::runtime_error("User Agent shall be present");
    }

    flatbuffers::FlatBufferBuilder fbb;
    auto agent = fbb.CreateString(userAgent);
    auto version = fbb.CreateString(userAgentVersion);
    auto mec = fbb.CreateString(mechanisms);

    Greenstack::Payload::HelloResponseBuilder builder(fbb);
    builder.add_UserAgent(agent);
    builder.add_UserAgentVersion(version);
    builder.add_SaslMechanisms(mec);
    auto stuff = builder.Finish();
    Greenstack::Payload::FinishHelloResponseBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

Greenstack::HelloResponse::HelloResponse()
    : Response() {
    setOpcode(Greenstack::Opcode::Hello);
}

const std::string Greenstack::HelloResponse::getUserAgent() const {
    auto Response = Greenstack::Payload::GetHelloResponse(payload.data());
    std::string ret(Response->UserAgent()->c_str());
    return ret;
}

const std::string Greenstack::HelloResponse::getSaslMechanisms() const {
    auto Response = Greenstack::Payload::GetHelloResponse(payload.data());
    std::string ret(Response->SaslMechanisms()->c_str());
    return ret;
}

void Greenstack::HelloResponse::validate() {
    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifyHelloResponseBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for HelloRequest");
    }
}

const std::string Greenstack::HelloResponse::getUserAgentVersion() const {
    auto Response = Greenstack::Payload::GetHelloResponse(payload.data());
    std::string ret(Response->UserAgentVersion()->c_str());
    return ret;
}

void Greenstack::HelloRequestBuilder::setPayload(Greenstack::Message& message) {
    if (userAgent.empty()) {
        throw std::runtime_error("User Agent shall be present");
    }

    flatbuffers::FlatBufferBuilder fbb;
    auto agent = fbb.CreateString(userAgent);
    auto version = fbb.CreateString(userAgentVersion);
    auto usecase = fbb.CreateString(comment);

    Greenstack::Payload::HelloRequestBuilder builder(fbb);
    builder.add_UserAgent(agent);
    builder.add_UserAgentVersion(version);
    builder.add_Comment(usecase);
    auto stuff = builder.Finish();
    Greenstack::Payload::FinishHelloRequestBuffer(fbb, stuff);

    std::vector<uint8_t> payload;
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
    message.setPayload(payload);
}
