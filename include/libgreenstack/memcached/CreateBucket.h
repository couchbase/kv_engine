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
#pragma once

#include <libgreenstack/Request.h>
#include <libgreenstack/Response.h>
#include <libgreenstack/memcached/Bucket.h>
#include <string>

namespace Greenstack {
    class Message;

    class CreateBucketRequest
        : public Greenstack::Request {
    public:
        CreateBucketRequest(const std::string& name, const std::string& config,
                            const BucketType& type);

        const std::string getName() const;

        const std::string getConfig() const;

        const BucketType getType() const;

    protected:
        CreateBucketRequest();

        friend class Message;

        virtual void validate() override;
    };

    class CreateBucketResponse
        : public Greenstack::Response {
    public:
        CreateBucketResponse();

        CreateBucketResponse(const Greenstack::Status& status);

    protected:
        friend class Message;
    };
}
