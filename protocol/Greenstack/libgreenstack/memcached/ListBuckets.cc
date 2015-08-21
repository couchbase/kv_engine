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

#include <libgreenstack/memcached/ListBuckets.h>
#include <flatbuffers/flatbuffers.h>
#include <Greenstack/payload/ListBucketsResponse_generated.h>

Greenstack::ListBucketsRequest::ListBucketsRequest()
    : Request(Opcode::ListBuckets) {

}

Greenstack::ListBucketsResponse::ListBucketsResponse(
    const std::vector<std::string>& buckets)
    : Response(Opcode::ListBuckets, Status::Success) {
    flatbuffers::FlatBufferBuilder fbb;

    flatbuffers::Offset<flatbuffers::String>* strings = new flatbuffers::Offset<flatbuffers::String>[buckets.size()];
    int ii = 0;
    for (auto bucket : buckets) {
        strings[ii++] = fbb.CreateString(bucket);
    }

    auto vector = fbb.CreateVector(strings, buckets.size());

    Greenstack::Payload::ListBucketsResponseBuilder builder(fbb);
    builder.add_Buckets(vector);

    auto stuff = builder.Finish();
    Greenstack::Payload::FinishListBucketsResponseBuffer(fbb, stuff);
    payload.resize(fbb.GetSize());
    memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
    delete[]strings;
}

Greenstack::ListBucketsResponse::ListBucketsResponse(
    const Greenstack::Status& status)
    : Response(Opcode::ListBuckets, status) {
    if (status == Greenstack::Status::Success) {
        flatbuffers::FlatBufferBuilder fbb;
        std::vector<std::string> buckets;
        flatbuffers::Offset<flatbuffers::String>* strings = new flatbuffers::Offset<flatbuffers::String>[buckets.size()];
        int ii = 0;
        for (auto bucket : buckets) {
            strings[ii++] = fbb.CreateString(bucket);
        }
        auto vector = fbb.CreateVector(strings, buckets.size());

        Greenstack::Payload::ListBucketsResponseBuilder builder(fbb);
        builder.add_Buckets(vector);

        auto stuff = builder.Finish();
        Greenstack::Payload::FinishListBucketsResponseBuffer(fbb, stuff);
        payload.resize(fbb.GetSize());
        memcpy(payload.data(), fbb.GetBufferPointer(), fbb.GetSize());
        delete[]strings;
    }
}

const std::vector<std::string> Greenstack::ListBucketsResponse::getBuckets() const {
    std::vector<std::string> ret;
    if (getStatus() != Status::Success) {
        std::string msg = "Buckets list not present for non-successful operations";
        throw std::runtime_error(msg);
    }

    auto request = Greenstack::Payload::GetListBucketsResponse(payload.data());
    auto buckets = request->Buckets();
    for (auto iter = buckets->begin(); iter != buckets->end(); ++iter) {
        ret.push_back(std::string(iter->c_str()));
    }

    return ret;
}

void Greenstack::ListBucketsResponse::validate() {
    Message::validate();
    if (getStatus() == Status::Success) {
        using namespace Greenstack::Payload;
        using namespace flatbuffers;
        Verifier verifier(payload.data(), payload.size());

        if (!VerifyListBucketsResponseBuffer(verifier)) {
            throw std::runtime_error(
                "Incorrect payload for ListBucketsResponse");
        }
    } else {
        if (payload.size() > 0) {
            throw std::runtime_error(
                "Incorrect payload for ListBucketsResponse");
        }
    }
}
