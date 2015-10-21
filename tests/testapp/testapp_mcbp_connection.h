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

#include "config.h"

#include "testapp_connection.h"

class MemcachedBinprotConnection : public MemcachedConnection {
public:
    MemcachedBinprotConnection(in_port_t port, sa_family_t family, bool ssl)
        : MemcachedConnection(port, family, ssl, Protocol::Memcached) { }


    virtual std::string to_string() override;

    virtual void authenticate(const std::string& username,
                              const std::string& password,
                              const std::string& mech) override;


    virtual void createBucket(const std::string& name,
                              const std::string& config,
                              Greenstack::Bucket::bucket_type_t type) override;


    virtual void deleteBucket(const std::string& name) override;

    virtual std::vector<std::string> listBuckets() override;

    virtual void recvFrame(Frame& frame) override;
};
