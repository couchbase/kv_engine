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

#include <array>
#include "config.h"

#include "client_connection.h"

class MemcachedBinprotConnection : public MemcachedConnection {
public:
    MemcachedBinprotConnection(in_port_t port, sa_family_t family, bool ssl)
        : MemcachedConnection(port, family, ssl, Protocol::Memcached) {
        std::fill(features.begin(), features.end(), false);
    }

    virtual std::string to_string() override;

    virtual void authenticate(const std::string& username,
                              const std::string& password,
                              const std::string& mech) override;

    virtual void assumeRole(const std::string& role) override;

    virtual void createBucket(const std::string& name,
                              const std::string& config,
                              const Greenstack::BucketType& type) override;


    virtual void deleteBucket(const std::string& name) override;

    virtual void selectBucket(const std::string& name) override;

    virtual std::vector<std::string> listBuckets() override;

    virtual Document get(const std::string& id, uint16_t vbucket) override;

    virtual MutationInfo mutate(const Document& doc, uint16_t vbucket,
                                const Greenstack::mutation_type_t type) override;

    virtual unique_cJSON_ptr stats(const std::string& subcommand) override;

    virtual void sendFrame(const Frame& frame) override;

    virtual void recvFrame(Frame& frame) override;

    void setDatatypeSupport(bool enable);

    void setMutationSeqnoSupport(bool enable);


    virtual void configureEwouldBlockEngine(const EWBEngineMode& mode,
                                            ENGINE_ERROR_CODE err_code,
                                            uint32_t value) override;

    std::array<bool, 3> features;
};
