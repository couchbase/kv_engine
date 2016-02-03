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

#include "testapp_connection.h"
#include <libgreenstack/Greenstack.h>

class GreenstackError : public ConnectionError {
public:
    explicit GreenstackError(const char* what_arg, Greenstack::Status stat)
        : ConnectionError(what_arg, Protocol::Greenstack, uint16_t(stat)),
          status(stat) {
    }

    explicit GreenstackError(const std::string& what_arg,
                             Greenstack::Status stat)
        : ConnectionError(what_arg, Protocol::Greenstack, uint16_t(stat)),
          status(stat) {
    }

    Greenstack::Status getStatus() const {
        return status;
    }

#ifdef WIN32
#define NOEXCEPT
#else
#define NOEXCEPT noexcept
#endif

    virtual const char* what() const NOEXCEPT override {
        std::string msg(std::runtime_error::what());
        msg.append(" ");
        msg.append(Greenstack::to_string(status));

        return msg.c_str();
    }

private:
    Greenstack::Status status;
};

class MemcachedGreenstackConnection : public MemcachedConnection {
public:
    MemcachedGreenstackConnection(in_port_t port, sa_family_t family, bool ssl)
        : MemcachedConnection(port, family, ssl, Protocol::Greenstack) { }

    virtual std::string to_string() override;

    virtual void authenticate(const std::string& username,
                              const std::string& password,
                              const std::string& mech) override;


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

    virtual void recvFrame(Frame& frame) override;

    /**
     * Run Hello to the server and identify ourself with the userAgent,
     * userAgentVersion and comment.
     *
     * @throws std::runtime_error if an error occurs
     */
    void hello(const std::string& userAgent,
               const std::string& userAgentVersion,
               const std::string& comment);


    /**
     * Get the servers SASL mechanisms. This is only valid after running a
     * successful HELLO
     */
    const std::string& getSaslMechanisms() const {
        return saslMechanisms;
    }


    virtual void configureEwouldBlockEngine(const EWBEngineMode& mode,
                                            ENGINE_ERROR_CODE err_code,
                                            uint32_t value) override;

protected:
    Greenstack::UniqueMessagePtr recvMessage();

    std::string saslMechanisms;

};
