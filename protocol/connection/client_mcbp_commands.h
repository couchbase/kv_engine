/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once
#include "client_connection.h"
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/response.h>
#include <memcached/durability_spec.h>
#include <memcached/range_scan_id.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <unordered_set>

namespace cb::mcbp::request {
class FrameInfo;
}

namespace cb::dek {
enum class Entity;
}

/**
 * This is the base class used for binary protocol commands. You probably
 * want to use one of the subclasses. Do not subclass this class directly,
 * rather, instantiate/derive from BinprotGenericCommand
 */
class BinprotCommand {
public:
    virtual ~BinprotCommand() = default;

    cb::mcbp::ClientOpcode getOp() const;

    const std::string& getKey() const;

    uint64_t getCas() const;

    [[nodiscard]] bool hasFrameInfos() const {
        return !frame_info.empty();
    }

    virtual void clear();

    void setKey(std::string key_);

    void setCas(uint64_t cas_);

    void setOp(cb::mcbp::ClientOpcode cmd_);

    void setVBucket(Vbid vbid);

    void setOpaque(uint32_t opaq);

    void setDatatype(uint8_t datatype_);

    void setDatatype(cb::mcbp::Datatype datatype_);

    /// Add a frame info object to the stream
    void addFrameInfo(const cb::mcbp::request::FrameInfo& fi);

    /// Add something you want to put into the frame info section of the
    /// packet (in the case you want to create illegal frame encodings
    /// to make sure that the server handle them correctly)
    void addFrameInfo(cb::const_byte_buffer section);

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

    struct Encoded {
        /**
         * 'scratch' space for data which isn't owned by anything and is
         * generated on demand. Any data here is sent before the data in the
         * buffers.
         */
        std::vector<uint8_t> header;

        /** The actual buffers to be sent */
        std::vector<cb::const_byte_buffer> bufs;
    };

    /**
     * Encode data into an 'Encoded' object.
     * @return an `Encoded` object which may be sent on the wire.
     *
     * Note that unlike the vector<uint8_t> variant, the actual buffers
     * are not copied into the new structure, so ensure the command object
     * (which owns the buffers), or the original buffers (if the command object
     * doesn't own the buffers either; see e.g.
     * BinprotMutationCommand::setValueBuffers()) remain in tact between this
     * call and actually sending it.
     *
     * The default implementation simply copies what encode(vector<uint8_t>)
     * does into Encoded::header, and Encoded::bufs contains a single
     * element.
     */
    virtual Encoded encode() const;

protected:
    // Private constructor to avoid using the class directly
    BinprotCommand() = default;

    /**
     * This class exposes a tri-state expiry object, to allow for a 0-value
     * expiry. This is not used directly by this class, but is used a bit in
     * subclasses
     */
    class ExpiryValue {
    public:
        void assign(uint32_t value_);
        void clear();
        bool isSet() const;
        uint32_t getValue() const;

    private:
        bool set = false;
        uint32_t value = 0;
    };

    /**
     * Writes the header to the buffer
     * @param buf Buffer to write to
     * @param payload_len Payload length (excluding keylen and extlen)
     * @param extlen Length of extras
     */
    void writeHeader(std::vector<uint8_t>& buf,
                     size_t payload_len = 0,
                     size_t extlen = 0) const;

    cb::mcbp::ClientOpcode opcode = cb::mcbp::ClientOpcode::Invalid;
    std::string key;
    uint64_t cas = 0;
    Vbid vbucket = Vbid(0);
    uint32_t opaque{0xdeadbeef};
    uint8_t datatype{PROTOCOL_BINARY_RAW_BYTES};

    /// The frame info sections to inject into the packet
    std::vector<uint8_t> frame_info;

private:
    /**
     * Fills the header with the current fields.
     *
     * @param[out] header header to write to
     * @param payload_len length of the "value" of the payload
     * @param extlen extras length.
     */
    void fillHeader(cb::mcbp::Request& header,
                    size_t payload_len = 0,
                    size_t extlen = 0) const;
};

/**
 * Convenience class for constructing ad-hoc commands with no special semantics.
 * Ideally, you should use another class which provides nicer wrapper functions.
 */
class BinprotGenericCommand : public BinprotCommand {
public:
    /// It shouldn't be possible to create a command without an opcode
    BinprotGenericCommand() = delete;

    explicit BinprotGenericCommand(cb::mcbp::ClientOpcode opcode)
        : BinprotGenericCommand(opcode, {}) {
    }
    BinprotGenericCommand(cb::mcbp::ClientOpcode opcode, std::string key_)
        : BinprotGenericCommand(opcode, std::move(key_), {}) {
    }
    BinprotGenericCommand(cb::mcbp::ClientOpcode opcode,
                          std::string key_,
                          std::string value_);

    void setValue(std::string value_);
    void setExtras(const std::vector<uint8_t>& buf);
    void setExtras(std::string_view buf);

    // Use for setting a simple value as an extras
    template <typename T>
    void setExtrasValue(T ext) {
        std::vector<uint8_t> buf;
        buf.resize(sizeof(T));
        memcpy(buf.data(), &ext, sizeof(T));
        setExtras(buf);
    }

    void clear() override;

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    std::string value;
    std::vector<uint8_t> extras;
};

/**
 * Simple response based on command allowing client to initiate a response to
 * server.
 */
class BinprotCommandResponse : public BinprotGenericCommand {
public:
    BinprotCommandResponse(cb::mcbp::ClientOpcode opcode,
                           uint32_t opaque,
                           cb::mcbp::Status status = cb::mcbp::Status::Success);
    BinprotCommandResponse& setStatus(cb::mcbp::Status);
    void encode(std::vector<uint8_t>& buf) const override;

private:
    cb::mcbp::Status status{cb::mcbp::Status::Success};
};

class BinprotResponse {
public:
    bool isSuccess() const;

    /** Get the opcode for the response */
    cb::mcbp::ClientOpcode getOp() const;

    /** Get the status code for the response */
    cb::mcbp::Status getStatus() const;

    uint64_t getCas() const;
    protocol_binary_datatype_t getDatatype() const;

    /// Get a view containing the key in the packet.
    std::string_view getKey() const;

    /**
     * Get a pointer to the "data" or "value" part of the response. This is
     * any payload content _after_ the key and extras (if present
     */
    std::string_view getDataView() const;

    /// Parse the payload as JSON and return the parsed payload
    /// @throws exception if a parse error occurs (not json for instance)
    nlohmann::json getDataJson() const;

    std::string_view getExtrasView() const;

    /// For error response return the context string. Note minimal validation
    /// occurs and should only be used when a error JSON blob is expected.
    /// @return "error"."context" value from the JSON payload
    std::string getErrorContext() const;

    /// @throws std::logic_exception if the object is invalid
    const cb::mcbp::Response& getResponse() const;

    /// Retrieve the approximate time spent on the server
    std::optional<std::chrono::microseconds> getTracingData() const;

    /// Get the RU count in the response (if present). We could of course
    /// return 0 if not set, but by using std::optional we can write more
    /// unit tests to verify if it is sent or not
    std::optional<size_t> getReadUnits() const;

    /// Get the WU count in the response (if present). We could of course
    /// return 0 if not set, but by using std::optional we can write more
    /// unit tests to verify if it is sent or not
    std::optional<size_t> getWriteUnits() const;

    /// Get the number of microseconds the command was throttled
    /// on the server. We could of course return 0 if not set, but by
    /// using std::optional we can write more unit tests to verify if it
    /// is sent or not
    std::optional<std::chrono::microseconds> getThrottledDuration() const;

    /**
     * Populate this response from a response
     * @param srcbuf The buffer containing the response.
     *
     * The input parameter here is forced to be an rvalue reference because
     * we don't want careless copying of potentially large payloads.
     */
    void assign(std::vector<uint8_t>&& srcbuf);

    virtual ~BinprotResponse() = default;

protected:
    const cb::mcbp::Header& getHeader() const;
    std::vector<uint8_t> payload;
};

class BinprotSubdocCommand : public BinprotCommand {
public:
    BinprotSubdocCommand();

    explicit BinprotSubdocCommand(cb::mcbp::ClientOpcode cmd_);

    // Old-style constructors. These are all used by testapp_subdoc.
    BinprotSubdocCommand(cb::mcbp::ClientOpcode cmd_,
                         std::string key_,
                         const std::string& path_);

    BinprotSubdocCommand(cb::mcbp::ClientOpcode cmd,
                         std::string key,
                         const std::string& path,
                         const std::string& value,
                         cb::mcbp::subdoc::PathFlag flags = {},
                         cb::mcbp::subdoc::DocFlag docFlags =
                                 cb::mcbp::subdoc::DocFlag::None,
                         uint64_t cas = 0);

    BinprotSubdocCommand& setPath(std::string path_);
    BinprotSubdocCommand& setValue(std::string value_);
    BinprotSubdocCommand& addPathFlags(cb::mcbp::subdoc::PathFlag flags_);
    BinprotSubdocCommand& addDocFlags(cb::mcbp::subdoc::DocFlag flags_);
    BinprotSubdocCommand& setExpiry(uint32_t value_);
    BinprotSubdocCommand& setUserFlags(uint32_t flags);

    const std::string& getPath() const;
    const std::string& getValue() const;
    cb::mcbp::subdoc::PathFlag getFlags() const;

    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::string path;
    std::string value;
    BinprotCommand::ExpiryValue expiry;
    cb::mcbp::subdoc::PathFlag flags = cb::mcbp::subdoc::PathFlag::None;
    cb::mcbp::subdoc::DocFlag doc_flags = cb::mcbp::subdoc::DocFlag::None;
    std::optional<uint32_t> userFlags;
};

using BinprotSubdocResponse = BinprotResponse;

class BinprotSubdocMultiMutationCommand : public BinprotCommand {
public:
    BinprotSubdocMultiMutationCommand();

    struct MutationSpecifier {
        cb::mcbp::ClientOpcode opcode;
        cb::mcbp::subdoc::PathFlag flags;
        std::string path;
        std::string value;
    };

    BinprotSubdocMultiMutationCommand(
            std::string key,
            std::vector<MutationSpecifier> specs,
            cb::mcbp::subdoc::DocFlag docFlags,
            const std::optional<cb::durability::Requirements>& durReqs = {});

    void encode(std::vector<uint8_t>& buf) const override;

    BinprotSubdocMultiMutationCommand& addDocFlag(
            cb::mcbp::subdoc::DocFlag docFlag);

    BinprotSubdocMultiMutationCommand& addMutation(
            const MutationSpecifier& spec);

    BinprotSubdocMultiMutationCommand& addMutation(
            cb::mcbp::ClientOpcode opcode,
            cb::mcbp::subdoc::PathFlag flags,
            std::string path,
            std::string value);

    BinprotSubdocMultiMutationCommand& setExpiry(uint32_t expiry_);

    BinprotSubdocMultiMutationCommand& setDurabilityReqs(
            const cb::durability::Requirements& durReqs);

    BinprotSubdocMultiMutationCommand& setUserFlags(uint32_t flags);

    MutationSpecifier& at(size_t index);

    MutationSpecifier& operator[](size_t index);

    bool empty() const;

    size_t size() const;

    void clearMutations();

    void clearDocFlags();

protected:
    std::vector<MutationSpecifier> specs;
    ExpiryValue expiry;
    cb::mcbp::subdoc::DocFlag docFlags;
    std::optional<uint32_t> userFlags;
};

class BinprotSubdocMultiMutationResponse : public BinprotResponse {
public:
    BinprotSubdocMultiMutationResponse() = default;
    explicit BinprotSubdocMultiMutationResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }

    struct MutationResult {
        uint8_t index;
        cb::mcbp::Status status;
        std::string value;
    };

    [[nodiscard]] std::vector<MutationResult> getResults() const;
};

class BinprotSubdocMultiLookupCommand : public BinprotCommand {
public:
    BinprotSubdocMultiLookupCommand();

    struct LookupSpecifier {
        cb::mcbp::ClientOpcode opcode;
        cb::mcbp::subdoc::PathFlag flags;
        std::string path;
    };

    BinprotSubdocMultiLookupCommand(std::string key,
                                    std::vector<LookupSpecifier> specs,
                                    cb::mcbp::subdoc::DocFlag docFlags);

    void encode(std::vector<uint8_t>& buf) const override;

    BinprotSubdocMultiLookupCommand& addLookup(const LookupSpecifier& spec);

    BinprotSubdocMultiLookupCommand& addLookup(
            const std::string& path,
            cb::mcbp::ClientOpcode opcode = cb::mcbp::ClientOpcode::SubdocGet,
            cb::mcbp::subdoc::PathFlag flags = {});

    BinprotSubdocMultiLookupCommand& addGet(
            const std::string& path, cb::mcbp::subdoc::PathFlag flags = {});
    BinprotSubdocMultiLookupCommand& addExists(
            const std::string& path, cb::mcbp::subdoc::PathFlag flags = {});
    BinprotSubdocMultiLookupCommand& addGetCount(
            const std::string& path, cb::mcbp::subdoc::PathFlag flags = {});
    BinprotSubdocMultiLookupCommand& addDocFlags(
            cb::mcbp::subdoc::DocFlag docFlag);

    void clearLookups();

    LookupSpecifier& at(size_t index);

    LookupSpecifier& operator[](size_t index);

    bool empty() const;

    size_t size() const;

    void clearDocFlags();

protected:
    std::vector<LookupSpecifier> specs;
    ExpiryValue expiry;
    cb::mcbp::subdoc::DocFlag docFlags;
};

class BinprotSubdocMultiLookupResponse : public BinprotResponse {
public:
    BinprotSubdocMultiLookupResponse() = default;
    explicit BinprotSubdocMultiLookupResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }

    struct LookupResult {
        cb::mcbp::Status status;
        std::string value;
    };

    [[nodiscard]] std::vector<LookupResult> getResults() const;
};

class BinprotSaslAuthCommand : public BinprotGenericCommand {
public:
    BinprotSaslAuthCommand()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::SaslAuth) {
    }
    void setMechanism(const std::string& mech_);

    void setChallenge(std::string_view data);

    void encode(std::vector<uint8_t>&) const override;

private:
    std::string challenge;
};

class BinprotSaslStepCommand : public BinprotGenericCommand {
public:
    BinprotSaslStepCommand()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::SaslStep) {
    }

    void setMechanism(const std::string& mech);

    void setChallenge(std::string_view data);

    void encode(std::vector<uint8_t>&) const override;

private:
    std::string challenge;
};

class BinprotHelloCommand : public BinprotGenericCommand {
public:
    explicit BinprotHelloCommand(std::string client_id);
    BinprotHelloCommand& enableFeature(cb::mcbp::Feature feature,
                                       bool enabled = true);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::unordered_set<uint16_t> features;
};

class BinprotHelloResponse : public BinprotResponse {
public:
    BinprotHelloResponse() = default;
    explicit BinprotHelloResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }
    std::vector<cb::mcbp::Feature> getFeatures() const;
};

class BinprotCreateBucketCommand : public BinprotGenericCommand {
public:
    explicit BinprotCreateBucketCommand(std::string name,
                                        const std::string& module,
                                        const std::string& config);
    void encode(std::vector<uint8_t>& buf) const override;

private:
    std::vector<uint8_t> module_config;
};

class BinprotPauseBucketCommand : public BinprotGenericCommand {
public:
    BinprotPauseBucketCommand(std::string name, uint64_t session_token = 0);
};

class BinprotResumeBucketCommand : public BinprotGenericCommand {
public:
    BinprotResumeBucketCommand(std::string name, uint64_t session_token = 0);
};

class BinprotGetCommand : public BinprotGenericCommand {
public:
    BinprotGetCommand(std::string key, Vbid vbid = Vbid{0})
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::Get, std::move(key)) {
        setVBucket(vbid);
    }

    void encode(std::vector<uint8_t>& buf) const override;
};

class BinprotGetAndLockCommand : public BinprotGenericCommand {
public:
    BinprotGetAndLockCommand(std::string key,
                             Vbid vbid = Vbid{0},
                             uint32_t timeout = 0);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    uint32_t lock_timeout;
};

class BinprotGetAndTouchCommand : public BinprotGenericCommand {
public:
    BinprotGetAndTouchCommand(std::string key, Vbid vb, uint32_t exp);

    void encode(std::vector<uint8_t>& buf) const override;

    bool isQuiet() const;

    void setQuiet(bool quiet = true);

    void setExpirytime(uint32_t timeout);

protected:
    uint32_t expirytime;
};

class BinprotGetResponse : public BinprotResponse {
public:
    BinprotGetResponse() = default;
    explicit BinprotGetResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }
    uint32_t getDocumentFlags() const;
};

using BinprotGetAndLockResponse = BinprotGetResponse;
using BinprotGetAndTouchResponse = BinprotGetResponse;

class BinprotUnlockCommand : public BinprotGenericCommand {
public:
    BinprotUnlockCommand(std::string key, Vbid vb, uint64_t cas)
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::UnlockKey,
                                std::move(key)) {
        setVBucket(vb);
        setCas(cas);
    }
    void encode(std::vector<uint8_t>& buf) const override;
};

class BinprotTouchCommand : public BinprotCommand {
public:
    explicit BinprotTouchCommand(std::string key = {}, uint32_t exp = 0);
    void encode(std::vector<uint8_t>& buf) const override;

    BinprotTouchCommand& setExpirytime(uint32_t timeout);

protected:
    uint32_t expirytime = 0;
};

using BinprotTouchResponse = BinprotResponse;

class BinprotGetCmdTimerCommand : public BinprotGenericCommand {
public:
    BinprotGetCmdTimerCommand(std::string bucket,
                              cb::mcbp::ClientOpcode opcode);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    cb::mcbp::ClientOpcode opcode = cb::mcbp::ClientOpcode::Invalid;
};

class BinprotGetCmdTimerResponse : public BinprotResponse {
public:
    BinprotGetCmdTimerResponse() = default;
    explicit BinprotGetCmdTimerResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }

    nlohmann::json getTimings() const;
};

class BinprotVerbosityCommand : public BinprotGenericCommand {
public:
    BinprotVerbosityCommand(int level)
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::Verbosity),
          level(level) {
    }
    void encode(std::vector<uint8_t>& buf) const override;

protected:
    int level;
};

using BinprotVerbosityResponse = BinprotResponse;

class BinprotIsaslRefreshCommand : public BinprotGenericCommand {
public:
    BinprotIsaslRefreshCommand()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::IsaslRefresh) {
    }
};

class BinprotMutationCommand : public BinprotCommand {
public:
    BinprotMutationCommand& setMutationType(MutationType);
    BinprotMutationCommand& setDocumentInfo(const DocumentInfo& info);

    BinprotMutationCommand& setValue(std::vector<uint8_t>&& value_);

    template <typename T>
    BinprotMutationCommand& setValue(const T& value_);

    BinprotMutationCommand& addValueBuffer(cb::const_byte_buffer buf);
    BinprotMutationCommand& addValueBuffer(std::string_view buf);

    BinprotMutationCommand& setDocumentFlags(uint32_t flags_);
    BinprotMutationCommand& setExpiry(uint32_t expiry_);

    void encode(std::vector<uint8_t>& buf) const override;
    Encoded encode() const override;

protected:
    void encodeHeader(std::vector<uint8_t>& buf) const;

    /** This contains our copied value (i.e. setValue) */
    std::vector<uint8_t> value;
    /** This contains value references (e.g. addValueBuffer/setValueBuffers) */
    std::vector<cb::const_byte_buffer> value_refs;

    BinprotCommand::ExpiryValue expiry;
    uint32_t flags = 0;
};

class BinprotMutationResponse : public BinprotResponse {
public:
    BinprotMutationResponse() = default;
    explicit BinprotMutationResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }
    MutationInfo getMutationInfo() const;
};

class BinprotIncrDecrCommand : public BinprotGenericCommand {
public:
    BinprotIncrDecrCommand(cb::mcbp::ClientOpcode opcode,
                           std::string key,
                           Vbid vb,
                           uint64_t delta,
                           uint64_t initial,
                           uint32_t expiry)
        : BinprotGenericCommand(opcode, std::move(key)) {
        payload.setDelta(delta);
        payload.setInitial(initial);
        payload.setExpiration(expiry);
    }

    void encode(std::vector<uint8_t>& buf) const override;

private:
    cb::mcbp::request::ArithmeticPayload payload;
};

class BinprotIncrDecrResponse : public BinprotMutationResponse {
public:
    BinprotIncrDecrResponse() = default;
    explicit BinprotIncrDecrResponse(BinprotResponse&& other)
        : BinprotMutationResponse(std::move(other)){};
    uint64_t getValue() const;
};

class BinprotRemoveCommand : public BinprotGenericCommand {
public:
    explicit BinprotRemoveCommand(std::string key,
                                  Vbid vb = Vbid{0},
                                  uint64_t cas = cb::mcbp::cas::Wildcard)
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::Delete,
                                std::move(key)) {
        setVBucket(vb);
        setCas(cas);
    }
    void encode(std::vector<uint8_t>& buf) const override;
};

using BinprotRemoveResponse = BinprotMutationResponse;

class BinprotGetErrorMapCommand : public BinprotGenericCommand {
public:
    explicit BinprotGetErrorMapCommand(uint16_t ver = 2)
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::GetErrorMap),
          version(ver) {
    }
    void setVersion(uint16_t version_);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint16_t version = 0;
};

class BinprotDcpOpenCommand : public BinprotGenericCommand {
public:
    /**
     * DCP Open
     *
     * @param name the name of the DCP stream to create
     * @param flags the open flags
     */
    explicit BinprotDcpOpenCommand(std::string name,
                                   cb::mcbp::DcpOpenFlag flags = {});

    void setConsumerName(std::string name);

    /**
     * Make this a producer stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeProducer();

    /**
     * Make this a consumer stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeConsumer();

    /**
     * Let the stream include xattrs (if any)
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeIncludeXattr();

    /**
     * Don't add any values into the stream
     *
     * @return this
     */
    BinprotDcpOpenCommand& makeNoValue();

    /**
     * Set an arbitrary flag value. This may be used in order to test
     * the sanity checks on the server
     *
     * @param flag the flag section to inject
     * @return this
     */
    BinprotDcpOpenCommand& setFlags(cb::mcbp::DcpOpenFlag flag);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    cb::mcbp::DcpOpenFlag flags;
    nlohmann::json payload;
};

class BinprotDcpStreamRequestCommand : public BinprotGenericCommand {
public:
    BinprotDcpStreamRequestCommand();
    BinprotDcpStreamRequestCommand(Vbid vbid,
                                   cb::mcbp::DcpAddStreamFlag flags,
                                   uint64_t startSeq,
                                   uint64_t endSeq,
                                   uint64_t vbUuid,
                                   uint64_t snapStart,
                                   uint64_t snapEnd);
    BinprotDcpStreamRequestCommand(Vbid vbid,
                                   cb::mcbp::DcpAddStreamFlag flags,
                                   uint64_t startSeq,
                                   uint64_t endSeq,
                                   uint64_t vbUuid,
                                   uint64_t snapStart,
                                   uint64_t snapEnd,
                                   const nlohmann::json& value);

    BinprotDcpStreamRequestCommand& setDcpFlags(
            cb::mcbp::DcpAddStreamFlag value);

    BinprotDcpStreamRequestCommand& setDcpReserved(uint32_t value);

    BinprotDcpStreamRequestCommand& setDcpStartSeqno(uint64_t value);

    BinprotDcpStreamRequestCommand& setDcpEndSeqno(uint64_t value);

    BinprotDcpStreamRequestCommand& setDcpVbucketUuid(uint64_t value);

    BinprotDcpStreamRequestCommand& setDcpSnapStartSeqno(uint64_t value);

    BinprotDcpStreamRequestCommand& setDcpSnapEndSeqno(uint64_t value);

    BinprotDcpStreamRequestCommand& setValue(const nlohmann::json& value);

    BinprotDcpStreamRequestCommand& setValue(std::string_view value);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    // The byteorder is fixed when we append the members to the packet
    cb::mcbp::request::DcpStreamReqPayload meta;
};

class BinprotDcpAddStreamCommand : public BinprotGenericCommand {
public:
    explicit BinprotDcpAddStreamCommand(cb::mcbp::DcpAddStreamFlag flags,
                                        Vbid vb = Vbid{0});
    void encode(std::vector<uint8_t>& buf) const override;

protected:
    cb::mcbp::request::DcpAddStreamPayload meta;
};

class BinprotDcpControlCommand : public BinprotGenericCommand {
public:
    BinprotDcpControlCommand();
};

class BinprotDcpMutationCommand : public BinprotMutationCommand {
public:
    BinprotDcpMutationCommand(std::string key,
                              std::string_view value,
                              uint32_t opaque,
                              uint8_t datatype,
                              uint32_t expiry,
                              uint64_t cas,
                              uint64_t seqno,
                              uint64_t revSeqno,
                              uint32_t flags,
                              uint32_t lockTime,
                              uint8_t nru);

    BinprotDcpMutationCommand& setBySeqno(uint64_t);
    BinprotDcpMutationCommand& setRevSeqno(uint64_t);
    BinprotDcpMutationCommand& setNru(uint8_t);
    BinprotDcpMutationCommand& setLockTime(uint32_t);

    void encode(std::vector<uint8_t>& buf) const override;
    Encoded encode() const override;

private:
    void encodeHeader(std::vector<uint8_t>& buf) const;
    uint64_t bySeqno = 0;
    uint64_t revSeqno = 0;
    uint32_t lockTime = 0;
    uint8_t nru = 0;
};

class BinprotDcpDeletionV2Command : public BinprotMutationCommand {
public:
    BinprotDcpDeletionV2Command(std::string key,
                                std::string_view value,
                                uint32_t opaque,
                                uint8_t datatype,
                                uint64_t cas,
                                uint64_t seqno,
                                uint64_t revSeqno,
                                uint32_t deleteTime);

    BinprotDcpDeletionV2Command& setBySeqno(uint64_t);
    BinprotDcpDeletionV2Command& setRevSeqno(uint64_t);
    BinprotDcpDeletionV2Command& setDeleteTime(uint32_t);

    void encode(std::vector<uint8_t>& buf) const override;
    Encoded encode() const override;

private:
    void encodeHeader(std::vector<uint8_t>& buf) const;
    uint64_t bySeqno = 0;
    uint64_t revSeqno = 0;
    uint32_t deleteTime = 0;
};

class BinprotGetFailoverLogCommand : public BinprotGenericCommand {
public:
    BinprotGetFailoverLogCommand()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::GetFailoverLog){};
};

class BinprotSetParamCommand : public BinprotGenericCommand {
public:
    BinprotSetParamCommand(cb::mcbp::request::SetParamPayload::Type type_,
                           const std::string& key_,
                           std::string value_);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const cb::mcbp::request::SetParamPayload::Type type;
    const std::string value;
};

class BinprotSetWithMetaCommand : public BinprotGenericCommand {
public:
    BinprotSetWithMetaCommand(const Document& doc,
                              Vbid vbucket,
                              uint64_t operationCas,
                              uint64_t seqno,
                              uint32_t options,
                              std::vector<uint8_t> meta);

    BinprotSetWithMetaCommand& setQuiet(bool quiet);

    uint32_t getFlags() const;

    BinprotSetWithMetaCommand& setFlags(uint32_t flags);

    uint32_t getExptime() const;

    BinprotSetWithMetaCommand& setExptime(uint32_t exptime);

    uint64_t getSeqno() const;

    BinprotSetWithMetaCommand& setSeqno(uint64_t seqno);

    uint64_t getMetaCas() const;

    BinprotSetWithMetaCommand& setMetaCas(uint64_t cas);

    const std::vector<uint8_t>& getMeta();

    BinprotSetWithMetaCommand& setMeta(const std::vector<uint8_t>& meta);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    Document doc;
    uint64_t seqno;
    uint64_t operationCas;
    uint32_t options;
    std::vector<uint8_t> meta;
};

class BinprotDelWithMetaCommand : public BinprotGenericCommand {
public:
    BinprotDelWithMetaCommand(Document doc,
                              Vbid vbucket,
                              uint32_t flags,
                              uint32_t delete_time,
                              uint64_t seqno,
                              uint64_t operationCas,
                              bool quiet = false);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const Document doc;
    cb::mcbp::request::DelWithMetaPayload extras;
};

class BinprotReturnMetaCommand : public BinprotGenericCommand {
public:
    BinprotReturnMetaCommand(cb::mcbp::request::ReturnMetaType type,
                             Document doc);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    Document doc;
    cb::mcbp::request::ReturnMetaPayload extras;
};

class BinprotSetControlTokenCommand : public BinprotGenericCommand {
public:
    BinprotSetControlTokenCommand(uint64_t token_, uint64_t oldtoken);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const uint64_t token;
};

class BinprotSetClusterConfigCommand : public BinprotGenericCommand {
public:
    BinprotSetClusterConfigCommand(std::string config,
                                   int64_t epoch,
                                   int64_t revision,
                                   std::string bucket);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const std::string config;
    int64_t epoch;
    int64_t revision;
};

class BinprotGetClusterConfigCommand : public BinprotGenericCommand {
public:
    BinprotGetClusterConfigCommand()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::GetClusterConfig) {
    }
    BinprotGetClusterConfigCommand(int64_t epoch, int64_t revision)
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::GetClusterConfig) {
        version = {epoch, revision};
    }

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    std::optional<std::pair<int64_t, int64_t>> version;
};

class BinprotObserveSeqnoCommand : public BinprotGenericCommand {
public:
    BinprotObserveSeqnoCommand(Vbid vbid, uint64_t uuid);

    void encode(std::vector<uint8_t>& buf) const override;

private:
    uint64_t uuid;
};

class BinprotObserveSeqnoResponse : public BinprotResponse {
public:
    BinprotObserveSeqnoResponse() = default;
    explicit BinprotObserveSeqnoResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }
    ObserveInfo getInfo() const;
};

class BinprotObserveCommand : public BinprotGenericCommand {
public:
    explicit BinprotObserveCommand(
            std::vector<std::pair<Vbid, std::string>> keys)
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::Observe),
          keys(std::move(keys)) {
    }

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    std::vector<std::pair<Vbid, std::string>> keys;
};

class BinprotObserveResponse : public BinprotResponse {
public:
    BinprotObserveResponse() = default;
    explicit BinprotObserveResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }

    struct Result {
        Vbid vbid = {};
        ObserveKeyState status{};
        std::string key;
        uint64_t cas{};
    };

    std::vector<Result> getResults();
};

class BinprotUpdateUserPermissionsCommand : public BinprotGenericCommand {
public:
    explicit BinprotUpdateUserPermissionsCommand(std::string payload);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const std::string payload;
};

class BinprotAuthProviderCommand : public BinprotGenericCommand {
public:
    BinprotAuthProviderCommand()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::AuthProvider) {
    }
};

class BinprotRbacRefreshCommand : public BinprotGenericCommand {
public:
    BinprotRbacRefreshCommand()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::RbacRefresh) {
    }
};

class BinprotAuditPutCommand : public BinprotGenericCommand {
public:
    BinprotAuditPutCommand(uint32_t id, std::string payload);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const uint32_t id;
    const std::string payload;
};

class BinprotSetVbucketCommand : public BinprotGenericCommand {
public:
    BinprotSetVbucketCommand(Vbid vbid,
                             vbucket_state_t state,
                             nlohmann::json payload);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    const vbucket_state_t state;
    const nlohmann::json payload;
};

class BinprotEWBCommand : public BinprotGenericCommand {
public:
    BinprotEWBCommand(EWBEngineMode mode,
                      cb::engine_errc err_code,
                      uint32_t value,
                      const std::string& key);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    cb::mcbp::request::EWB_Payload extras;
};

class BinprotCompactDbCommand : public BinprotGenericCommand {
public:
    BinprotCompactDbCommand();

    void encode(std::vector<uint8_t>& buf) const override;

    cb::mcbp::request::CompactDbPayload extras;
};

class BinprotGetAllVbucketSequenceNumbers : public BinprotGenericCommand {
public:
    BinprotGetAllVbucketSequenceNumbers()
        : BinprotGenericCommand(cb::mcbp::ClientOpcode::GetAllVbSeqnos) {
    }
    BinprotGetAllVbucketSequenceNumbers(uint32_t state,
                                        CollectionID collection);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    std::optional<uint32_t> state;
    std::optional<CollectionID> collection;
};

class BinprotGetAllVbucketSequenceNumbersResponse : public BinprotResponse {
public:
    BinprotGetAllVbucketSequenceNumbersResponse() = default;
    explicit BinprotGetAllVbucketSequenceNumbersResponse(
            BinprotResponse&& other)
        : BinprotResponse(other) {
    }
    std::unordered_map<Vbid, uint64_t> getVbucketSeqnos() const;
};

class SetBucketThrottlePropertiesCommand : public BinprotGenericCommand {
public:
    SetBucketThrottlePropertiesCommand(std::string key_, nlohmann::json json);
    void encode(std::vector<uint8_t>& buf) const override;

protected:
    nlohmann::json document;
};

class SetNodeThrottlePropertiesCommand : public BinprotGenericCommand {
public:
    SetNodeThrottlePropertiesCommand(nlohmann::json json);
    void encode(std::vector<uint8_t>& buf) const override;

protected:
    nlohmann::json document;
};

class SetBucketDataLimitExceededCommand : public BinprotGenericCommand {
public:
    SetBucketDataLimitExceededCommand(std::string key_,
                                      cb::mcbp::Status status);

    void encode(std::vector<uint8_t>& buf) const override;

protected:
    cb::mcbp::request::SetBucketDataLimitExceededPayload extras;
};

class BinprotRangeScanCreate : public BinprotGenericCommand {
public:
    BinprotRangeScanCreate(Vbid vbid, const nlohmann::json& config);
};

class BinprotRangeScanContinue : public BinprotGenericCommand {
public:
    BinprotRangeScanContinue(Vbid vbid,
                             cb::rangescan::Id id,
                             size_t itemLimit,
                             std::chrono::milliseconds timeLimit,
                             size_t byteLimit);
    void encode(std::vector<uint8_t>& buf) const override;

protected:
    cb::mcbp::request::RangeScanContinuePayload extras;
};

class BinprotRangeScanCancel : public BinprotGenericCommand {
public:
    BinprotRangeScanCancel(Vbid vbid, cb::rangescan::Id id);
};

class BinprotGetKeysCommand : public BinprotGenericCommand {
public:
    BinprotGetKeysCommand(std::string start,
                          std::optional<uint32_t> nkeys = {});
    void encode(std::vector<uint8_t>& buf) const override;

    void setKeyLimit(uint32_t limit) {
        nkeys = limit;
    }

protected:
    std::optional<uint32_t> nkeys;
};

class BinprotGetKeysResponse : public BinprotResponse {
public:
    BinprotGetKeysResponse() = default;
    explicit BinprotGetKeysResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }
    std::vector<std::string> getKeys() const;
};

class BinprotSetActiveEncryptionKeysCommand : public BinprotGenericCommand {
public:
    BinprotSetActiveEncryptionKeysCommand() = delete;
    BinprotSetActiveEncryptionKeysCommand(
            std::string entity,
            const nlohmann::json& keystore,
            const std::vector<std::string>& unavailable = {});
    BinprotSetActiveEncryptionKeysCommand(
            cb::dek::Entity entity,
            const nlohmann::json& keystore,
            const std::vector<std::string>& unavailable = {});
};

class BinprotGetMetaCommand : public BinprotGenericCommand {
public:
    BinprotGetMetaCommand() = delete;
    BinprotGetMetaCommand(std::string key,
                          Vbid vbucket,
                          GetMetaVersion version);
};

class BinprotGetMetaResponse : public BinprotResponse {
public:
    BinprotGetMetaResponse() = default;
    explicit BinprotGetMetaResponse(BinprotResponse&& other)
        : BinprotResponse(std::move(other)) {
    }
    GetMetaPayload getMetaPayload() const;
};
