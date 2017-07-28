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
#include <unordered_set>

#include "config.h"

#include "client_connection.h"
#include "client_mcbp_commands.h"

class BinprotConnectionError : public ConnectionError {
public:
    BinprotConnectionError(const std::string& prefix, uint16_t reason_);

    BinprotConnectionError(const std::string& prefix,
                           const BinprotResponse& response);

    uint16_t getReason() const override {
        return reason;
    }

    Protocol getProtocol() const override {
        return Protocol::Memcached;
    }

    bool isInvalidArguments() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    bool isAlreadyExists() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    }

    bool isNotFound() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
    }

    bool isNotMyVbucket() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    bool isNotStored() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_NOT_STORED;
    }

    bool isAccessDenied() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_EACCESS;
    }

    bool isDeltaBadval() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL;
    }

    bool isAuthError() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_AUTH_ERROR;
    }

    bool isNotSupported() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    bool isLocked() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_LOCKED;
    }

    bool isTemporaryFailure() const override {
        return reason == PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
    }

private:
    uint16_t reason;
};

class MemcachedBinprotConnection : public MemcachedConnection {
public:
    MemcachedBinprotConnection(const std::string& host, in_port_t port,
                               sa_family_t family, bool ssl)
        : MemcachedConnection(host, port, family, ssl, Protocol::Memcached) {
    }

    std::unique_ptr<MemcachedConnection> clone() override;

    std::string to_string() override;

    void authenticate(const std::string& username,
                      const std::string& password,
                      const std::string& mech) override;

    void createBucket(const std::string& name,
                      const std::string& config,
                      const BucketType type) override;

    void deleteBucket(const std::string& name) override;

    void selectBucket(const std::string& name) override;

    std::vector<std::string> listBuckets() override;

    Document get(const std::string& id, uint16_t vbucket) override;

    Document get_and_lock(const std::string& id, uint16_t vbucket,
                          uint32_t lock_timeout) override;

    void unlock(const std::string& id, uint16_t vbucket, uint64_t cas) override;

    void dropPrivilege(cb::rbac::Privilege privilege) override;

    Frame encodeCmdGet(const std::string& id, uint16_t vbucket) override;

    MutationInfo mutate(const DocumentInfo& doc,
                        uint16_t vbucket,
                        cb::const_byte_buffer value,
                        MutationType type) override;

    using MemcachedConnection::mutate;

    std::map<std::string, std::string>statsMap(
            const std::string& subcommand) override;

    unique_cJSON_ptr timings(uint8_t opcode, const std::string& bucket);

    void reloadAuditConfiguration() override;

    void sendFrame(const Frame& frame) override;

    void recvFrame(Frame& frame) override;

    void sendCommand(const BinprotCommand& command);

    void recvResponse(BinprotResponse& response);

    void executeCommand(const BinprotCommand& command, BinprotResponse& response) {
        sendCommand(command);
        recvResponse(response);
    }

    void hello(const std::string& userAgent,
               const std::string& userAgentVersion,
               const std::string& comment) override;

    void setDatatypeJson(bool enable) {
        setFeature(mcbp::Feature::JSON, enable);
    }

    void setDatatypeCompressed(bool enable) {
        setFeature(mcbp::Feature::SNAPPY, enable);
    }

    void setMutationSeqnoSupport(bool enable) {
        setFeature(mcbp::Feature::MUTATION_SEQNO, enable);
    }

    void setXattrSupport(bool enable) {
        setFeature(mcbp::Feature::XATTR, enable);
    }

    void setXerrorSupport(bool enable) {
        setFeature(mcbp::Feature::XERROR, enable);
    }

    std::string ioctl_get(const std::string& key) override;

    void ioctl_set(const std::string& key,
                   const std::string& value) override;

    uint64_t increment(const std::string& key,
                       uint64_t delta,
                       uint64_t initial,
                       rel_time_t exptime,
                       MutationInfo* info) override;

    uint64_t decrement(const std::string& key,
                       uint64_t delta,
                       uint64_t initial,
                       rel_time_t exptime,
                       MutationInfo* info) override;

    MutationInfo remove(const std::string& key,
                        uint16_t vbucket,
                        uint64_t cas) override;

    void configureEwouldBlockEngine(const EWBEngineMode& mode,
                                    ENGINE_ERROR_CODE err_code = ENGINE_EWOULDBLOCK,
                                    uint32_t value = 0,
                                    const std::string& key = "") override;

    bool hasFeature(mcbp::Feature feature) const {
        return effective_features.find(uint16_t(feature))
                != effective_features.end();
    }

    MutationInfo mutateWithMeta(Document& doc,
                                uint16_t vbucket,
                                uint64_t cas,
                                uint64_t seqno,
                                uint32_t metaOption,
                                std::vector<uint8_t> metaExtras = {}) override;

    GetMetaResponse getMeta(const std::string& key,
                            uint16_t vbucket,
                            uint64_t cas) override;

protected:
    typedef std::unordered_set<uint16_t> Featureset;

    uint64_t incr_decr(protocol_binary_command opcode,
                       const std::string& key,
                       uint64_t delta,
                       uint64_t initial,
                       rel_time_t exptime,
                       MutationInfo* info);

    /**
     * Set the features on the server by using the MCBP hello command
     *
     * The internal `features` array is updated with the result sent back
     * from the server.
     *
     * @param agent the agent name provided by the client
     * @param feat the featureset to enable.
     */
    void applyFeatures(const std::string& agent, const Featureset& features);

    /**
     * Attempts to enable or disable a feature
     * @param feature Feature to enable or disable
     * @param enabled whether to enable or disable
     */
    void setFeature(mcbp::Feature feature, bool enabled);

    /**
     * Erases the per-connection features from Hello
     */
    void close() override;

    Featureset effective_features;
};
