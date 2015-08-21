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

#include "config.h"

#include <strings.h>
#include <map>

#include <libgreenstack/memcached/Mutation.h>
#include <flatbuffers/flatbuffers.h>
#include <Greenstack/payload/MutationRequest_generated.h>
#include <Greenstack/payload/MutationResponse_generated.h>


const std::map<Greenstack::mutation_type_t, std::string> mappings = {
    {Greenstack::MutationType::Add,     "Add"},
    {Greenstack::MutationType::Set,     "Set"},
    {Greenstack::MutationType::Replace, "Replace"},
    {Greenstack::MutationType::Append,  "Append"},
    {Greenstack::MutationType::Prepend, "Prepend"},
    {Greenstack::MutationType::Patch,   "Patch"}
};

std::string Greenstack::MutationType::to_string(
    Greenstack::mutation_type_t opcode) {
    const auto iter = mappings.find(opcode);
    if (iter == mappings.end()) {
        return std::to_string(opcode);
    } else {
        return iter->second;
    }
}

Greenstack::mutation_type_t Greenstack::MutationType::from_string(
    const std::string& val) {
    for (auto iter : mappings) {
        if (strcasecmp(val.c_str(), iter.second.c_str()) == 0) {
            return iter.first;
        }
    }

    std::string msg = "Unknown command [";
    msg.append(val);
    msg.append("]");
    throw std::runtime_error(msg);
}

Greenstack::MutationRequest::MutationRequest()
    : Request(Opcode::Mutation) {

}

static Greenstack::Payload::compression_t convertCompression(
    const Greenstack::Compression& compression) {
    switch (compression) {
    case Greenstack::Compression::None:
        return Greenstack::Payload::compression_t_None;
    case Greenstack::Compression::Snappy:
        return Greenstack::Payload::compression_t_Snappy;
    default:
        throw std::runtime_error(
            Greenstack::to_string(compression));
    }
}

static Greenstack::Payload::datatype_t convertDatatype(
    const Greenstack::Datatype& datatype) {
    switch (datatype) {
    case Greenstack::Datatype::Raw:
        return Greenstack::Payload::datatype_t_Raw;
    case Greenstack::Datatype::Json:
        return Greenstack::Payload::datatype_t_Json;
    default:
        throw std::runtime_error(Greenstack::to_string(datatype));
    }
}

static Greenstack::Payload::mutation_type_t convertOperation(
    Greenstack::mutation_type_t operation) {
    switch (operation) {
    case Greenstack::MutationType::Add:
        return Greenstack::Payload::mutation_type_t_Add;
    case Greenstack::MutationType::Set:
        return Greenstack::Payload::mutation_type_t_Set;
    case Greenstack::MutationType::Replace:
        return Greenstack::Payload::mutation_type_t_Replace;
    case Greenstack::MutationType::Append:
        return Greenstack::Payload::mutation_type_t_Append;
    case Greenstack::MutationType::Prepend:
        return Greenstack::Payload::mutation_type_t_Prepend;
    case Greenstack::MutationType::Patch:
        return Greenstack::Payload::mutation_type_t_Patch;
    default:
        throw std::runtime_error(
            Greenstack::MutationType::to_string(operation));
    }
}

void Greenstack::MutationRequest::validate() {
    Message::validate();

    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifyMutationRequestBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for MutationRequest");
    }

    if (!getFlexHeader().haveVbucketId()) {
        throw std::runtime_error("MutationRequest needs vbucket id");
    }
}

Greenstack::mutation_type_t Greenstack::MutationRequest::getMutationType() const {
    return mutationType;
}

void Greenstack::MutationRequest::setMutationType(mutation_type_t type) {
    MutationRequest::mutationType = type;
}

void Greenstack::MutationRequest::assemble() {
    flatbuffers::FlatBufferBuilder fbb;

    auto docid = fbb.CreateString(documentInfo->getId());
    auto exp = fbb.CreateString(documentInfo->getExpiration());

    const uint8_t* ptr = value->getData();
    auto docValue = fbb.CreateVector(ptr, value->getSize());

    Greenstack::Payload::compression_t compr = convertCompression(
        documentInfo->getCompression());
    Greenstack::Payload::datatype_t datatype = convertDatatype(
        documentInfo->getDatatype());

    Greenstack::Payload::DocumentInfoBuilder docinfoBuilder(fbb);
    docinfoBuilder.add_Expiration(exp);
    docinfoBuilder.add_Flags(documentInfo->getFlags());
    docinfoBuilder.add_Id(docid);
    docinfoBuilder.add_Datatype(datatype);
    docinfoBuilder.add_Compression(compr);
    docinfoBuilder.add_Cas(documentInfo->getCas());
    auto docinfoOffset = docinfoBuilder.Finish();

    Greenstack::Payload::DocumentBuilder docBuilder(fbb);
    docBuilder.add_Info(docinfoOffset);
    docBuilder.add_Value(docValue);
    auto docBuilderOffset = docBuilder.Finish();

    Greenstack::Payload::MutationRequestBuilder mutationRequestBuilder(fbb);
    mutationRequestBuilder.add_Document(docBuilderOffset);
    mutationRequestBuilder.add_Operation(convertOperation(mutationType));

    auto stuff = mutationRequestBuilder.Finish();
    Greenstack::Payload::FinishMutationRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

static Greenstack::Compression convertCompression(
    Greenstack::Payload::compression_t compression) {
    switch (compression) {
    case Greenstack::Payload::compression_t_None:
        return Greenstack::Compression::None;
    case Greenstack::Payload::compression_t_Snappy:
        return Greenstack::Compression::Snappy;
    default:
        throw std::runtime_error("Received unknown compression");
    }
}

static Greenstack::Datatype convertDatatype(
    const Greenstack::Payload::datatype_t& datatype) {
    switch (datatype) {
    case Greenstack::Payload::datatype_t_Raw:
        return Greenstack::Datatype::Raw;
    case Greenstack::Payload::datatype_t_Json:
        return Greenstack::Datatype::Json;
    default:
        throw std::runtime_error("Received unknown datatype");
    }
}

static Greenstack::mutation_type_t convertOperation(
    Greenstack::Payload::mutation_type_t operation) {
    switch (operation) {
    case Greenstack::Payload::mutation_type_t_Add:
        return Greenstack::MutationType::Add;
    case Greenstack::Payload::mutation_type_t_Set:
        return Greenstack::MutationType::Set;
    case Greenstack::Payload::mutation_type_t_Replace:
        return Greenstack::MutationType::Replace;
    case Greenstack::Payload::mutation_type_t_Append:
        return Greenstack::MutationType::Append;
    case Greenstack::Payload::mutation_type_t_Prepend:
        return Greenstack::MutationType::Prepend;
    case Greenstack::Payload::mutation_type_t_Patch:
        return Greenstack::MutationType::Patch;
    default:
        throw std::runtime_error("Unknown mutation type received");
    }
}

void Greenstack::MutationRequest::disassemble() {
    using namespace Greenstack::Payload;
    auto request = GetMutationRequest(payload.data());

    mutationType = convertOperation(request->Operation());
    auto document = request->Document();
    auto val = document->Value();
    // @todo verify that this is safe!!!!
    value = std::make_shared<FixedByteArrayBuffer>(const_cast<uint8_t*>(val->Data()),
                                                   val->Length());

    // Decode the DocumentInfo
    auto info = document->Info();
    documentInfo = std::make_shared<Greenstack::DocumentInfo>();

    documentInfo->setId(info->Id()->c_str());
    documentInfo->setFlags(info->Flags());
    documentInfo->setExpiration(info->Expiration()->c_str());
    documentInfo->setCas(info->Cas());
    documentInfo->setCompression(convertCompression(info->Compression()));
    documentInfo->setDatatype(convertDatatype(info->Datatype()));
}

Greenstack::MutationResponse::MutationResponse(cas_t cas, uint32_t size,
                                               uint64_t seqno,
                                               uint64_t vbucketuuid)
    : Response(Opcode::Mutation, Status::Success) {
    flatbuffers::FlatBufferBuilder fbb;
    Greenstack::Payload::MutationResponseBuilder builder(fbb);
    builder.add_Cas(cas);
    builder.add_Size(size);
    builder.add_Seqno(seqno);
    builder.add_VBucketUuid(vbucketuuid);
    auto stuff = builder.Finish();
    Greenstack::Payload::FinishMutationResponseBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

Greenstack::MutationResponse::MutationResponse(const Greenstack::Status& status)
    : Response(Opcode::Mutation, status) {
    // empty
}

Greenstack::MutationResponse::MutationResponse()
    : Response(Opcode::Mutation, Status::Success) {
    // empty
}

void Greenstack::MutationResponse::validate() {
    Message::validate();
    if (getStatus() == Status::Success) {
        using namespace Greenstack::Payload;
        using namespace flatbuffers;
        Verifier verifier(payload.data(), payload.size());

        if (!VerifyMutationResponseBuffer(verifier)) {
            throw std::runtime_error("Incorrect payload for MutationResponse");
        }
    } else {
        if (!payload.empty()) {
            throw std::runtime_error("Incorrect payload for MutationResponse");
        }
    }
}

Greenstack::cas_t Greenstack::MutationResponse::getCas() const {
    auto response = Greenstack::Payload::GetMutationResponse(payload.data());
    return response->Cas();
}

size_t Greenstack::MutationResponse::getSize() const {
    auto response = Greenstack::Payload::GetMutationResponse(payload.data());
    return response->Size();
}

uint64_t Greenstack::MutationResponse::getSeqno() const {
    auto response = Greenstack::Payload::GetMutationResponse(payload.data());
    return response->Seqno();
}

uint64_t Greenstack::MutationResponse::getVbucketUuid() const {
    auto response = Greenstack::Payload::GetMutationResponse(payload.data());
    return response->VBucketUuid();
}
