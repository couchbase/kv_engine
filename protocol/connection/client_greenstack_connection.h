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

#include "client_connection.h"
#include <libgreenstack/Greenstack.h>

inline std::string formatGreenstackExceptionMsg(const std::string& prefix,
                                                Greenstack::Status reason) {
    // Format the error message
    std::string errormessage(prefix);
    errormessage.append(": ");
    errormessage.append(Greenstack::to_string(reason));
    errormessage.append(" (");
    errormessage.append(std::to_string(uint16_t(reason)));
    errormessage.append(")");
    return errormessage;
}

class GreenstackConnectionError : public ConnectionError {
public:
    explicit GreenstackConnectionError(const char* prefix,
                                       Greenstack::Status reason_)
        : ConnectionError(formatGreenstackExceptionMsg(prefix,
                                                       reason_).c_str()),
          reason(reason_) {
    }

    explicit GreenstackConnectionError(const std::string& prefix,
                                       Greenstack::Status reason_)
        : GreenstackConnectionError(prefix.c_str(), reason_) {
    }

    uint16_t getReason() const override {
        return static_cast<uint16_t>(reason);
    }

    Protocol getProtocol() const override {
        return Protocol::Greenstack;
    }

    bool isInvalidArguments() const override {
        return reason == Greenstack::Status::InvalidArguments;
    }

    bool isAlreadyExists() const override {
        return reason == Greenstack::Status::AlreadyExists;
    }

    bool isNotFound() const override {
        return reason == Greenstack::Status::NotFound;
    }

    bool isNotMyVbucket() const override {
        return reason == Greenstack::Status::NotMyVBucket;
    }

    bool isNotStored() const override {
        return reason == Greenstack::Status::NotStored;
    }

    bool isAccessDenied() const override {
        return reason == Greenstack::Status::NoAccess;
    }

    bool isDeltaBadval() const override {
        throw std::runtime_error("Not implemented");
    }

    bool isAuthError() const override {
        return reason == Greenstack::Status::AuthenticationError;
    }

private:
    Greenstack::Status reason;
};

class MemcachedGreenstackConnection : public MemcachedConnection {
public:
    MemcachedGreenstackConnection(const std::string& host, in_port_t port,
                                  sa_family_t family, bool ssl)
        : MemcachedConnection(host, port, family, ssl, Protocol::Greenstack) {}

    virtual std::string to_string() override;

    std::unique_ptr<MemcachedConnection> clone() override;

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

    Frame encodeCmdGet(const std::string& id, uint16_t vbucket) override;

    Frame encodeCmdDcpOpen() override;

    Frame encodeCmdDcpStreamReq() override;

    virtual MutationInfo mutate(const Document& doc, uint16_t vbucket,
                                const Greenstack::mutation_type_t type) override;

    virtual unique_cJSON_ptr stats(const std::string& subcommand) override;

    virtual void reloadAuditConfiguration() override;

    virtual void recvFrame(Frame& frame) override;

    virtual void hello(const std::string& userAgent,
                       const std::string& userAgentVersion,
                       const std::string& comment) override;


    virtual void configureEwouldBlockEngine(const EWBEngineMode& mode,
                                            ENGINE_ERROR_CODE err_code,
                                            uint32_t value,
                                            const std::string& key) override;

protected:
    Greenstack::UniqueMessagePtr recvMessage();

};
