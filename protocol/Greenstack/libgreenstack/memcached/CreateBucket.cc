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

#include <map>
#include <strings.h>
#include <libgreenstack/memcached/CreateBucket.h>
#include <flatbuffers/flatbuffers.h>
#include <Greenstack/payload/CreateBucketRequest_generated.h>

Greenstack::BucketType const Greenstack::CreateBucketRequest::getType() const {
    auto request = Payload::GetCreateBucketRequest(payload.data());
    auto type = request->Type();
    switch (type) {
    case Payload::bucket_type_t_Memcached:
        return BucketType::Memcached;
    case Payload::bucket_type_t_Couchbase:
        return BucketType::Couchbase;
    case Payload::bucket_type_t_EWouldBlock:
        return BucketType::EWouldBlock;
    default:
        throw std::runtime_error(
            "Unsupported bucket type" + std::to_string(type));
    }
}

const std::string Greenstack::CreateBucketRequest::getName() const {
    auto request = Payload::GetCreateBucketRequest(payload.data());
    std::string ret(request->Name()->c_str());
    return ret;
}

const std::string Greenstack::CreateBucketRequest::getConfig() const {
    auto request = Payload::GetCreateBucketRequest(payload.data());
    std::string ret(request->Config()->c_str());
    return ret;
}

Greenstack::CreateBucketRequest::CreateBucketRequest(const std::string& name,
                                                     const std::string& config,
                                                     const Greenstack::BucketType& type)
    : Request(Opcode::CreateBucket) {
    if (name.empty()) {
        throw std::runtime_error("Bucket name can't be empty");
    }
    flatbuffers::FlatBufferBuilder fbb;
    auto bucketName = fbb.CreateString(name);
    auto bucketConfig = fbb.CreateString(config);

    Payload::CreateBucketRequestBuilder builder(fbb);
    builder.add_Name(bucketName);
    builder.add_Config(bucketConfig);
    switch (type) {
    case BucketType::Memcached:
        builder.add_Type(Payload::bucket_type_t_Memcached);
        break;
    case BucketType::Couchbase:
        builder.add_Type(Payload::bucket_type_t_Couchbase);
        break;
    case BucketType::EWouldBlock:
        builder.add_Type(Payload::bucket_type_t_EWouldBlock);
        break;
    default:
        std::string msg = "Unsupported bucket type";
        throw std::runtime_error(msg);
    }

    auto stuff = builder.Finish();
    Payload::FinishCreateBucketRequestBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
}

Greenstack::CreateBucketRequest::CreateBucketRequest()
    : Request(Opcode::CreateBucket) {

}

void Greenstack::CreateBucketRequest::validate() {
    Message::validate();
    using namespace Greenstack::Payload;
    using namespace flatbuffers;
    Verifier verifier(payload.data(), payload.size());

    if (!VerifyCreateBucketRequestBuffer(verifier)) {
        throw std::runtime_error("Incorrect payload for CreateBucketRequest");
    }
}

Greenstack::CreateBucketResponse::CreateBucketResponse()
    : Response(Opcode::CreateBucket, Status::Success) {

}

Greenstack::CreateBucketResponse::CreateBucketResponse(
    const Greenstack::Status& status)
    : Response(Opcode::CreateBucket, status) {

}
