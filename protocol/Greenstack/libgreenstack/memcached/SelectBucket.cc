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

#include <libgreenstack/memcached/SelectBucket.h>
#include <Greenstack/payload/SelectBucketRequest_generated.h>
#include <iostream>
#include <stdexcept>

Greenstack::SelectBucketRequest::SelectBucketRequest(
    const std::string& bucketName)
    : Request(Greenstack::Opcode::SelectBucket) {
    if (bucketName.empty()) {
        throw std::runtime_error("Bucket name can't be empty");
    }

    flatbuffers::FlatBufferBuilder fbb;
    auto name = fbb.CreateString(bucketName);

    Greenstack::Payload::SelectBucketRequestBuilder builder(fbb);
    builder.add_BucketName(name);

    auto stuff = builder.Finish();
    Greenstack::Payload::FinishSelectBucketRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

Greenstack::SelectBucketRequest::SelectBucketRequest()
    : Request(Greenstack::Opcode::SelectBucket) {
}

const std::string Greenstack::SelectBucketRequest::getName() const {
    auto request = Greenstack::Payload::GetSelectBucketRequest(payload.data());
    std::string ret(request->BucketName()->c_str());
    return ret;
}

void Greenstack::SelectBucketRequest::validate() {
    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifySelectBucketRequestBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for SelectBucketRequest");
    }
}

Greenstack::SelectBucketResponse::SelectBucketResponse(const Greenstack::Status& status)
    : Response(Greenstack::Opcode::SelectBucket, status) {
}

Greenstack::SelectBucketResponse::SelectBucketResponse()
    : Response(Greenstack::Opcode::SelectBucket, Greenstack::Status::Success) {
}
