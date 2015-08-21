/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <libgreenstack/memcached/AssumeRole.h>
#include <flatbuffers/flatbuffers.h>
#include <Greenstack/payload/AssumeRoleRequest_generated.h>


Greenstack::AssumeRoleRequest::AssumeRoleRequest(const std::string& roleName)
    : Request(Greenstack::Opcode::AssumeRole) {
    if (roleName.empty()) {
        throw std::runtime_error("Role name can't be empty");
    }
    flatbuffers::FlatBufferBuilder fbb;
    auto name = fbb.CreateString(roleName);

    Greenstack::Payload::AssumeRoleRequestBuilder builder(fbb);
    builder.add_Role(name);

    auto stuff = builder.Finish();
    Greenstack::Payload::FinishAssumeRoleRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

const std::string Greenstack::AssumeRoleRequest::getRole() const {
    auto request = Greenstack::Payload::GetAssumeRoleRequest(payload.data());
    std::string ret(request->Role()->c_str());
    return ret;
}

Greenstack::AssumeRoleRequest::AssumeRoleRequest()
    : Request(Greenstack::Opcode::AssumeRole) {
}

void Greenstack::AssumeRoleRequest::validate() {
    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifyAssumeRoleRequestBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for AssumeRole");
    }
}

Greenstack::AssumeRoleResponse::AssumeRoleResponse()
    : Response(Greenstack::Opcode::AssumeRole, Greenstack::Status::Success) {
}

Greenstack::AssumeRoleResponse::AssumeRoleResponse(const Greenstack::Status& status)
    : Response(Greenstack::Opcode::AssumeRole, status) {
}
