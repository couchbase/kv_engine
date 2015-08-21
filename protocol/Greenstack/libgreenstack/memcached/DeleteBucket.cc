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

#include <libgreenstack/memcached/DeleteBucket.h>
#include <flatbuffers/flatbuffers.h>
#include <Greenstack/payload/DeleteBucketRequest_generated.h>

Greenstack::DeleteBucketRequest::DeleteBucketRequest(const std::string& name,
                                                     bool force)
    : Request(Greenstack::Opcode::DeleteBucket) {
    if (name.empty()) {
        throw std::runtime_error("Bucket name can't be empty");
    }

    flatbuffers::FlatBufferBuilder fbb;
    auto bucketName = fbb.CreateString(name);

    Greenstack::Payload::DeleteBucketRequestBuilder builder(fbb);
    builder.add_BucketName(bucketName);
    builder.add_Force(force);

    auto stuff = builder.Finish();
    Greenstack::Payload::FinishDeleteBucketRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

const std::string Greenstack::DeleteBucketRequest::getName() const {
    auto request = Greenstack::Payload::GetDeleteBucketRequest(payload.data());
    std::string ret(request->BucketName()->c_str());
    return ret;
}

bool Greenstack::DeleteBucketRequest::isForce() const {
    auto request = Greenstack::Payload::GetDeleteBucketRequest(payload.data());
    return request->Force();
}

Greenstack::DeleteBucketRequest::DeleteBucketRequest()
    : Request(Greenstack::Opcode::DeleteBucket) {
}

void Greenstack::DeleteBucketRequest::validate() {
    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifyDeleteBucketRequestBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for DeleteBucket");
    }
}

Greenstack::DeleteBucketResponse::DeleteBucketResponse()
    : Response(Greenstack::Opcode::DeleteBucket, Greenstack::Status::Success) {

}

Greenstack::DeleteBucketResponse::DeleteBucketResponse(
    const Greenstack::Status& status)
    : Response(Greenstack::Opcode::DeleteBucket, status) {

}
