/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <unordered_set>
#include <platform/sized_buffer.h>
#include "client_connection.h"

/**
 * This is the base class used for binary protocol commands. You probably
 * want to use one of the subclasses. Do not subclass this class directly,
 * rather, instantiate/derive from BinprotCommandT or BinprotGenericCommand
 */
class BinprotCommand {
public:
    BinprotCommand(const BinprotCommand&) = delete;

    BinprotCommand() {
    }
    virtual ~BinprotCommand() {
    }

    protocol_binary_command getOp() const {
        return opcode;
    }
    const std::string& getKey() const {
        return key;
    }
    uint64_t getCas() const {
        return cas;
    }

    void clear() {
        opcode = PROTOCOL_BINARY_CMD_INVALID;
        key.clear();
        cas = 0;
        vbucket = 0;
    }

    /**
     * Encode the command to a buffer.
     * @param buf The buffer
     * @note the buffer's contents are _not_ reset, and the encoded command
     *       is simply appended to it.
     *
     * The default implementation is to encode the standard header fields.
     * The key itself is not added to the buffer.
     */
    virtual void encode(std::vector<uint8_t>& buf) const;

protected:
    /**
     * This class exposes a tri-state expiry object, to allow for a 0-value
     * expiry. This is not used directly by this class, but is used a bit in
     * subclasses
     */
    class ExpiryValue {
    public:
        void assign(uint32_t value_) {
            value = value_;
            set = true;
        }
        void clear() {
            set = false;
        }
        bool isSet() const {
            return set;
        }
        uint32_t getValue() const {
            return value;
        }

    private:
        bool set = false;
        uint32_t value = 0;
    };

    /**
     * These functions are internal wrappers for use by the CRTP-style
     * BinprotCommandT class, which has setKey, setCas, etc. which just calls
     * the *Priv variants and returns this, thus e.g.
     *
     * @code{c}
     * T& setKey(const std::string& key) {
     *    return static_cast<T&>(setKeyPriv(key));
     * }
     * @endcode
     */
    BinprotCommand& setKeyPriv(const std::string& key_) {
        key = key_;
        return *this;
    }

    BinprotCommand& setCasPriv(uint64_t cas_) {
        cas = cas_;
        return *this;
    }

    BinprotCommand& setOpPriv(protocol_binary_command cmd_) {
        opcode = cmd_;
        return *this;
    }

    BinprotCommand& setVBucketPriv(uint16_t vbid) {
        vbucket = vbid;
        return *this;
    }

    /**
     * Fills the header with the current fields.
     *
     * @param[out] header header to write to
     * @param payload_len length of the "value" of the payload
     * @param extlen extras length.
     */
    void fillHeader(protocol_binary_request_header& header,
                    size_t payload_len = 0,
                    size_t extlen = 0) const;

    /**
     * Writes the header to the buffer
     * @param buf Buffer to write to
     * @param payload_len Payload length (excluding keylen and extlen)
     * @param extlen Length of extras
     */
    void writeHeader(std::vector<uint8_t>& buf,
                     size_t payload_len = 0,
                     size_t extlen = 0) const;

    protocol_binary_command opcode = PROTOCOL_BINARY_CMD_INVALID;
    std::string key;
    uint64_t cas = 0;
    uint16_t vbucket = 0;
};

/**
 * For use with subclasses of @class MemcachedCommand. This installs setter
 * methods which
 * return the actual class rather than TestCmd.
 *
 * @code{.c++}
 * class SomeCmd : public MemcachedCommandT<SomeCmd> {
 *   // ...
 * };
 * @endcode
 *
 * And therefore allows subclasses to be used like so:
 *
 * @code{.c++}
 * MyCommand cmd;
 * cmd.setKey("foo").
 *     setExpiry(300).
 *     setCas(0xdeadbeef);
 *
 * @endcode
 */
template <typename T,
          protocol_binary_command OpCode = PROTOCOL_BINARY_CMD_INVALID>
class BinprotCommandT : public BinprotCommand {
public:
    T& setKey(const std::string& key_) {
        return static_cast<T&>(setKeyPriv(key_));
    }
    T& setCas(uint64_t cas_) {
        return static_cast<T&>(setCasPriv(cas_));
    }
    T& setOp(protocol_binary_command cmd_) {
        return static_cast<T&>(setOpPriv(cmd_));
    }
    T& setVBucket(uint16_t vbid) {
        return static_cast<T&>(setVBucketPriv(vbid));
    }
    BinprotCommandT() {
        setOp(OpCode);
    }
};

/**
 * Convenience class for constructing ad-hoc commands with no special semantics.
 * Ideally, you should use another class which provides nicer wrapper functions.
 */
class BinprotGenericCommand : public BinprotCommandT<BinprotGenericCommand> {
public:
    BinprotGenericCommand(protocol_binary_command opcode,
                          const std::string& key_,
                          const std::string& value_)
        : BinprotCommandT() {
        setOp(opcode);
        setKey(key_);
        setValue(value_);
    }
    BinprotGenericCommand(protocol_binary_command opcode,
                          const std::string& key_)
        : BinprotCommandT() {
        setOp(opcode);
        setKey(key_);
    }
    BinprotGenericCommand(protocol_binary_command opcode) : BinprotCommandT() {
        setOp(opcode);
    }
    BinprotGenericCommand() : BinprotCommandT() {
    }
    BinprotGenericCommand& setValue(const std::string& value_) {
        value = value_;
        return *this;
    }
    BinprotGenericCommand& setExtras(const std::vector<uint8_t>& buf) {
        extras.assign(buf.begin(), buf.end());
        return *this;
    }

    // Use for setting a simple value as an extras
    template <typename T>
    BinprotGenericCommand& setExtrasValue(T value) {
        std::vector<uint8_t> buf;
        buf.resize(sizeof(T));
        memcpy(buf.data(), &value, sizeof(T));
        return setExtras(buf);
    }

    void clear() {
        BinprotCommand::clear();
        value.clear();
        extras.clear();
    }

    virtual void encode(std::vector<uint8_t>& buf) const;

protected:
    std::string value;
    std::vector<uint8_t> extras;
};

class BinprotResponse {
public:
    bool isSuccess() const {
        return getStatus() == PROTOCOL_BINARY_RESPONSE_SUCCESS;
    }

    /** Get the status code for the response */
    protocol_binary_response_status getStatus() const {
        return protocol_binary_response_status(getHeader().response.status);
    }

    size_t getExtlen() const {
        return getHeader().response.extlen;
    }

    /** Get the length of packet (minus the header) */
    size_t getBodylen() const {
        return getHeader().response.bodylen;
    }

    /**
     * Get the length of the header. This is a static function as it is
     * always 24
     */
    static size_t getHeaderLen() {
        static const protocol_binary_request_header header{};
        return sizeof(header.bytes);
    }
    uint64_t getCas() const {
        return getHeader().response.cas;
    }
    protocol_binary_datatype_t getDatatype() const {
        return getHeader().response.datatype;
    }

    /**
     * Get a pointer to the payload of the response. This begins immediately
     * after the 24 byte memcached header
     */
    const uint8_t* getPayload() const {
        return payload.data() + getHeaderLen();
    }

    /**
     * Get a pointer to the key returned in the packet, if a key is present.
     * Use #getKeyLen() to determine this.
     */
    cb::const_char_buffer getKey() const {
        return cb::const_char_buffer(reinterpret_cast<const char*>(getPayload()) + getExtlen(),
                                     getHeader().response.keylen);
    }

    std::string getKeyString() const {
        return std::string(getKey().data(), getKey().size());
    }

    /**
     * Get a pointer to the "data" or "value" part of the response. This is
     * any payload content _after_ the key and extras (if present
     */
    cb::const_byte_buffer getData() const {
        // Clarify
        const uint8_t* buf = payload.data() + getHeaderLen() + getKey().size() + getExtlen();
        size_t len = getBodylen() - (getExtlen() + getKey().size());
        return cb::const_byte_buffer(buf, len);
    }

    std::string getDataString() const {
        return std::string(reinterpret_cast<const char*>(getData().data()),
                           getData().size());
    }

    /**
     * Get the entire packet, beginning at the header
     * Note that all fields in the header is in the host local byte order
     */
    const std::vector<uint8_t>& getRawPacket() const {
        return payload;
    }

    const protocol_binary_response_header& getHeader() const {
        return *reinterpret_cast<const protocol_binary_response_header*>(
                payload.data());
    }

    /**
     * Populate this response from a response
     * @param srcbuf The buffer containing the response.
     *
     * The input parameter here is forced to be an rvalue reference because
     * we don't want careless copying of potentially large payloads.
     */
    virtual void assign(std::vector<uint8_t>&& srcbuf);

    virtual void clear() {
        payload.clear();
    }

    virtual ~BinprotResponse() {
    }

protected:
    protocol_binary_response_header& getHeader() {
        return *reinterpret_cast<protocol_binary_response_header*>(
                payload.data());
    }
    std::vector<uint8_t> payload;
};

class BinprotSubdocCommand : public BinprotCommandT<BinprotSubdocCommand> {
public:
    BinprotSubdocCommand() : BinprotCommandT() {
    }

    BinprotSubdocCommand(protocol_binary_command cmd_) {
        setOp(cmd_);
    }

    // Old-style constructors. These are all used by testapp_subdoc.
    BinprotSubdocCommand(protocol_binary_command cmd_,
                         const std::string& key_,
                         const std::string& path_)
        : BinprotSubdocCommand(cmd_, key_, path_, "", SUBDOC_FLAG_NONE, 0) {
    }

    BinprotSubdocCommand(protocol_binary_command cmd,
                         const std::string& key,
                         const std::string& path,
                         const std::string& value,
                         protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE,
                         uint64_t cas = 0);

    BinprotSubdocCommand& setPath(const std::string& path_);
    BinprotSubdocCommand& setValue(const std::string& value_) {
        value = value_;
        return *this;
    }
    BinprotSubdocCommand& setFlags(protocol_binary_subdoc_flag flags_) {
        flags = flags_;
        return *this;
    }
    BinprotSubdocCommand& setExpiry(uint32_t value_) {
        expiry.assign(value_);
        return *this;
    }
    const std::string& getPath() const {
        return path;
    }
    const std::string& getValue() const {
        return value;
    }
    protocol_binary_subdoc_flag getFlags() const {
        return flags;
    }

    virtual void encode(std::vector<uint8_t>& buf) const override;

private:
    std::string path;
    std::string value;
    BinprotCommand::ExpiryValue expiry;
    protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE;
};

class BinprotSubdocResponse : public BinprotResponse {
public:
    const std::string& getValue() {
        return value;
    }
    virtual void clear() override {
        BinprotResponse::clear();
        value.clear();
    }

    virtual void assign(std::vector<uint8_t>&& srcbuf) override;

private:
    std::string value;
};

class BinprotSubdocMultiMutationCommand :
    public BinprotCommandT<BinprotSubdocMultiMutationCommand,
        PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION> {
public:
    struct MutationSpecifier {
        protocol_binary_command opcode;
        protocol_binary_subdoc_flag flags;
        std::string path;
        std::string value;
    };

    void encode(std::vector<uint8_t>& buf) const override;

    void add_mutation(const MutationSpecifier& spec) {
        specs.push_back(spec);
    }

    void add_mutation(protocol_binary_command opcode,
                      protocol_binary_subdoc_flag flags,
                      const std::string& path,
                      const std::string& value) {
        specs.emplace_back(MutationSpecifier{opcode, flags, path, value});
    }

protected:
    std::vector<MutationSpecifier> specs;
};

class BinprotSaslAuthCommand
        : public BinprotCommandT<BinprotSaslAuthCommand,
                                 PROTOCOL_BINARY_CMD_SASL_AUTH> {
public:
    void setMechanism(const char* mech_) {
        setKey(mech_);
    }

    void setChallenge(const char* data, size_t n) {
        challenge.assign(data, n);
    }

    void encode(std::vector<uint8_t>&) const override;

private:
    std::string challenge;
};

class BinprotSaslStepCommand
        : public BinprotCommandT<BinprotSaslStepCommand,
                                 PROTOCOL_BINARY_CMD_SASL_STEP> {
public:
    void setMechanism(const char* mech) {
        setKey(mech);
    }
    void setChallengeResponse(const char* resp, size_t len) {
        challenge_response.assign(resp, len);
    }
    void encode(std::vector<uint8_t>&) const override;

private:
    std::string challenge_response;
};

class BinprotHelloCommand : public BinprotCommandT<BinprotHelloCommand,
                                                   PROTOCOL_BINARY_CMD_HELLO> {
public:
    BinprotHelloCommand(const std::string& client_id) : BinprotCommandT() {
        setKey(client_id);
    }
    BinprotHelloCommand& enableFeature(mcbp::Feature feature,
                                       bool enabled = true) {
        if (enabled) {
            features.insert(static_cast<uint8_t>(feature));
        } else {
            features.erase(static_cast<uint8_t>(feature));
        }
        return *this;
    }

    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::unordered_set<uint16_t> features;
};

class BinprotHelloResponse : public BinprotResponse {
public:
    void assign(std::vector<uint8_t>&& buf) override;
    const std::vector<mcbp::Feature>& getFeatures() const {
        return features;
    }

private:
    std::vector<mcbp::Feature> features;
};

class BinprotCreateBucketCommand
        : public BinprotCommandT<BinprotCreateBucketCommand,
                                 PROTOCOL_BINARY_CMD_CREATE_BUCKET> {
public:
    BinprotCreateBucketCommand(const char* name) {
        setKey(name);
    }

    void setConfig(const std::string& module, const std::string& config);
    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::vector<uint8_t> module_config;
};

class BinprotGetCommand
        : public BinprotCommandT<BinprotGetCommand, PROTOCOL_BINARY_CMD_GET> {
public:
    void encode(std::vector<uint8_t>& buf) const override;
};

class BinprotGetAndLockCommand
    : public BinprotCommandT<BinprotGetAndLockCommand, PROTOCOL_BINARY_CMD_GET_LOCKED> {
public:
    BinprotGetAndLockCommand() : BinprotCommandT(), lock_timeout(0) {}

    void encode(std::vector<uint8_t>& buf) const override;

    BinprotGetAndLockCommand& setLockTimeout(uint32_t timeout) {
        lock_timeout = timeout;
        return *this;
    }

protected:
    uint32_t lock_timeout;
};

class BinprotGetResponse : public BinprotResponse {
public:
    uint32_t getDocumentFlags() const;
};

using BinprotGetAndLockResponse = BinprotGetResponse;

class BinprotUnlockCommand
    : public BinprotCommandT<BinprotGetCommand, PROTOCOL_BINARY_CMD_UNLOCK_KEY> {
public:
    void encode(std::vector<uint8_t>& buf) const override;
};

using BinprotUnlockResponse = BinprotResponse;

class BinprotGetCmdTimerCommand
    : public BinprotCommandT<BinprotGetCmdTimerCommand, PROTOCOL_BINARY_CMD_GET_CMD_TIMER> {
public:
    void encode(std::vector<uint8_t>& buf) const override;

    void setOpcode(uint8_t opcode) {
        BinprotGetCmdTimerCommand::opcode = opcode;
    }

    void setBucket(const std::string& bucket) {
        setKey(bucket);
    }

protected:
    uint8_t opcode;
};

class BinprotGetCmdTimerResponse : public BinprotResponse {
public:
    virtual void assign(std::vector<uint8_t>&& buf) override;

    cJSON* getTimings() const {
        return timings.get();
    }

private:
    unique_cJSON_ptr timings;
};

class BinprotVerbosityCommand
    : public BinprotCommandT<BinprotVerbosityCommand, PROTOCOL_BINARY_CMD_VERBOSITY> {
public:
    void encode(std::vector<uint8_t>& buf) const override;

    void setLevel(int level) {
        BinprotVerbosityCommand::level = level;
    }

protected:
    int level;
};

using BinprotVerbosityResponse = BinprotResponse;

class BinprotIsaslRefreshCommand
    : public BinprotCommandT<BinprotIsaslRefreshCommand,
        PROTOCOL_BINARY_CMD_ISASL_REFRESH> {
};

using BinprotIsaslRefreshResponse = BinprotResponse;

class BinprotMutationCommand : public BinprotCommandT<BinprotMutationCommand> {
public:
    BinprotMutationCommand& setMutationType(const Greenstack::mutation_type_t);
    BinprotMutationCommand& setDocumentInfo(const DocumentInfo& info);

    BinprotMutationCommand& setValue(std::vector<uint8_t>&& value_) {
        value = std::move(value_);
        return *this;
    }

    template <typename T>
    BinprotMutationCommand& setValue(const T& value_) {
        value.assign(value_.begin(), value_.end());
        return *this;
    }
    BinprotMutationCommand& setDatatype(uint8_t datatype_) {
        datatype = datatype_;
        return *this;
    }
    BinprotMutationCommand& setDatatype(Greenstack::Datatype datatype_) {
        return setDatatype(uint8_t(datatype_));
    }
    BinprotMutationCommand& setDocumentFlags(uint32_t flags_) {
        flags = flags_;
        return *this;
    }
    BinprotMutationCommand& setExpiry(uint32_t expiry_) {
        expiry.assign(expiry_);
        return *this;
    }

    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::vector<uint8_t> value;
    BinprotCommand::ExpiryValue expiry;
    uint32_t flags = 0;
    uint8_t datatype = 0;
};

class BinprotMutationResponse : public BinprotResponse {
public:
    virtual void assign(std::vector<uint8_t>&& buf) override;

    const MutationInfo& getMutationInfo() const {
        return mutation_info;
    }

private:
    MutationInfo mutation_info;
};

class BinprotIncrDecrCommand : public BinprotCommandT<BinprotIncrDecrCommand> {
public:
    BinprotIncrDecrCommand& setDelta(uint64_t delta_) {
        delta = delta_;
        return *this;
    }

    BinprotIncrDecrCommand& setInitialValue(uint64_t initial_) {
        initial = initial_;
        return *this;
    }

    BinprotIncrDecrCommand& setExpiry(uint32_t expiry_) {
        expiry.assign(expiry_);
        return *this;
    }

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint64_t delta = 0;
    uint64_t initial = 0;
    BinprotCommand::ExpiryValue expiry;
};

class BinprotIncrDecrResponse : public BinprotMutationResponse {
public:
    uint64_t getValue() const {
        return value;
    }

    virtual void assign(std::vector<uint8_t>&& buf) override;

private:
    uint64_t value = 0;
};

class BinprotRemoveCommand
    : public BinprotCommandT<BinprotRemoveCommand, PROTOCOL_BINARY_CMD_DELETE> {
public:
    void encode(std::vector<uint8_t>& buf) const override;
};

using BinprotRemoveResponse = BinprotMutationResponse;


class BinprotGetErrorMapCommand
    : public BinprotCommandT<BinprotGetErrorMapCommand,
                             PROTOCOL_BINARY_CMD_GET_ERROR_MAP> {
public:
    void setVersion(uint16_t version_) {
        version = version_;
    }

    void encode(std::vector<uint8_t>& buf) const override;
private:
    uint16_t version = 0;
};

using BinprotGetErrorMapResponse = BinprotResponse;

class BinprotDcpOpenCommand : public BinprotGenericCommand {
public:
    /**
     * DCP Open
     *
     * @param name the name of the DCP stream to create
     * @param seqno_ the sequence number for the stream
     * @param flags_ the open flags
     */
    BinprotDcpOpenCommand(const std::string& name,
                          uint32_t seqno_ = 0,
                          uint32_t flags_ = 0)
        : BinprotGenericCommand(PROTOCOL_BINARY_CMD_DCP_OPEN, name, {}),
          seqno(seqno_),
          flags(flags_) {
    }

    /**
     * Make this a producer stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeProducer() {
        if (flags & DCP_OPEN_NOTIFIER) {
            throw std::invalid_argument(
                "BinprotDcpOpenCommand::makeProducer: a stream can't be both a consumer and producer");
        }
        flags |= DCP_OPEN_PRODUCER;
        return *this;
    }

    /**
     * Make this a consumer stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeConsumer() {
        if (flags & DCP_OPEN_PRODUCER) {
            throw std::invalid_argument(
                "BinprotDcpOpenCommand::makeConsumer: a stream can't be both a consumer and producer");
        }
        flags |= DCP_OPEN_NOTIFIER;
        return *this;
    }

    /**
     * Let the stream include xattrs (if any)
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeIncludeXattr() {
        flags |= DCP_OPEN_INCLUDE_XATTRS;
        return *this;
    }

    /**
     * Don't add any values into the stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeNoValue() {
        flags |= DCP_OPEN_NO_VALUE;
        return *this;
    }

    /**
     * Set an arbitrary flag value. This may be used in order to test
     * the sanity checks on the server
     *
     * @param flags the raw 32 bit flag section to inject
     * @return this
     */
    BinprotDcpOpenCommand& setFlags(uint32_t flags) {
        BinprotDcpOpenCommand::flags = flags;
        return *this;
    }

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint32_t seqno;
    uint32_t flags;
};

class BinprotDcpStreamRequestCommand : public BinprotGenericCommand {
public:
    BinprotDcpStreamRequestCommand()
        :
        BinprotGenericCommand(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ, {}, {}),
        flags(0),
        reserved(0),
        start_seqno(htonll(std::numeric_limits<uint64_t>::min())),
        end_seqno(htonll(std::numeric_limits<uint64_t>::max())),
        vbucket_uuid(0),
        snap_start_seqno(htonll(std::numeric_limits<uint64_t>::min())),
        snap_end_seqno(htonll(std::numeric_limits<uint64_t>::max())) {

    }

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint32_t flags;
    uint32_t reserved;
    uint64_t start_seqno;
    uint64_t end_seqno;
    uint64_t vbucket_uuid;
    uint64_t snap_start_seqno;
    uint64_t snap_end_seqno;
};


class BinprotDcpMutationCommand : public BinprotGenericCommand {
public:
    BinprotDcpMutationCommand()
        : BinprotGenericCommand(PROTOCOL_BINARY_CMD_DCP_MUTATION, {}, {}),
          by_seqno(0),
          rev_seqno(0),
          flags(0),
          expiration(0),
          lock_time(0),
          nmeta(0),
          nru(0) {

    }

    void reset(const std::vector<uint8_t>& packet);

    const std::string& getValue() const {
        return value;
    }

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint64_t by_seqno;
    uint64_t rev_seqno;
    uint32_t flags;
    uint32_t expiration;
    uint32_t lock_time;
    uint16_t nmeta;
    uint8_t nru;
};
