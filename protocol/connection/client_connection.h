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

#include <engines/ewouldblock_engine/ewouldblock_engine.h>
#include <memcached/engine_error.h>
#include <memcached/openssl.h>
#include <memcached/protocol_binary.h>
#include <memcached/rbac.h>
#include <memcached/types.h>
#include <platform/dynamic.h>
#include <platform/socket.h>
#include <optional>

#include <nlohmann/json_fwd.hpp>

#include <chrono>
#include <cstdlib>
#include <functional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

class FrameInfo;

using FrameInfoVector = std::vector<std::unique_ptr<FrameInfo>>;
using GetFrameInfoFunction = std::function<FrameInfoVector()>;

/**
 * The Frame class is used to represent all of the data included in the
 * protocol unit going over the wire.
 */
class Frame {
public:
    void reset() {
        payload.resize(0);
    }

    cb::mcbp::Magic getMagic() const {
        const auto magic(cb::mcbp::Magic(payload.at(0)));
        if (!cb::mcbp::is_legal(magic)) {
            throw std::invalid_argument(
                    "Frame::getMagic: invalid magic provided in buffer");
        }

        return magic;
    }

    const cb::mcbp::Request* getRequest() const {
        return reinterpret_cast<const cb::mcbp::Request*>(payload.data());
    }

    const cb::mcbp::Response* getResponse() const {
        return reinterpret_cast<const cb::mcbp::Response*>(payload.data());
    }

    std::vector<uint8_t> payload;
    typedef std::vector<uint8_t>::size_type size_type;
};

class DocumentInfo {
public:
    std::string id;
    uint32_t flags;
    uint32_t expiration;
    cb::mcbp::Datatype datatype;
    uint64_t cas;

    DocumentInfo()
        : id(""),
          flags(0),
          expiration(0),
          datatype(cb::mcbp::Datatype::Raw),
          cas(0) {
    }

    bool operator==(const DocumentInfo& rhs) const {
        return (id == rhs.id) && (flags == rhs.flags) &&
               (expiration == rhs.expiration) && (datatype == rhs.datatype) &&
               (cas == rhs.cas);
    }
};

::std::ostream& operator<<(::std::ostream& os, const DocumentInfo& info);

class Document {
public:
    bool operator==(const Document& rhs) const {
        return (info == rhs.info) && (value == rhs.value);
    }

    /**
     * Compress this document using Snappy. Replaces the value with a compressed
     * version, adds Snappy to the set of datatypes.
     */
    void compress();

    DocumentInfo info;
    std::string value;
};

::std::ostream& operator<<(::std::ostream& os, const Document& doc);

class MutationInfo {
public:
    uint64_t cas;
    size_t size;
    uint64_t seqno;
    uint64_t vbucketuuid;
};

struct ObserveInfo {
    uint8_t formatType;
    Vbid vbId;
    uint64_t uuid;
    uint64_t lastPersistedSeqno;
    uint64_t currentSeqno;
    uint64_t failoverUUID;
    uint64_t failoverSeqno;
};

enum class BucketType : uint8_t {
    Invalid = 0,
    Memcached = 1,
    Couchbase = 2,
    EWouldBlock = 3
};

enum class MutationType {
    Add, Set, Replace, Append, Prepend
};

std::string to_string(MutationType type);

class BinprotResponse;
class BinprotCommand;

class ConnectionError : public std::runtime_error {
public:
    ConnectionError(const std::string& prefix, cb::mcbp::Status reason_);

    ConnectionError(const std::string& prefix, const BinprotResponse& response);

    cb::mcbp::Status getReason() const {
        return reason;
    }

    bool isInvalidArguments() const {
        return reason == cb::mcbp::Status::Einval;
    }

    bool isAlreadyExists() const {
        return reason == cb::mcbp::Status::KeyEexists;
    }

    bool isNotFound() const {
        return reason == cb::mcbp::Status::KeyEnoent;
    }

    bool isNotMyVbucket() const {
        return reason == cb::mcbp::Status::NotMyVbucket;
    }

    bool isNotStored() const {
        return reason == cb::mcbp::Status::NotStored;
    }

    bool isAccessDenied() const {
        return reason == cb::mcbp::Status::Eaccess;
    }

    bool isDeltaBadval() const {
        return reason == cb::mcbp::Status::DeltaBadval;
    }

    bool isAuthError() const {
        return reason == cb::mcbp::Status::AuthError;
    }

    bool isNotSupported() const {
        return reason == cb::mcbp::Status::NotSupported;
    }

    bool isLocked() const {
        return reason == cb::mcbp::Status::Locked;
    }

    bool isTemporaryFailure() const {
        return reason == cb::mcbp::Status::Etmpfail;
    }

    bool isTooBig() const {
        return reason == cb::mcbp::Status::E2big;
    }

    bool isUnknownCollection() const {
        return reason == cb::mcbp::Status::UnknownCollection;
    }

    bool isUnknownScope() const {
        return reason == cb::mcbp::Status::UnknownScope;
    }

    std::string getErrorContext() const;

    nlohmann::json getErrorJsonContext() const;

private:
    const cb::mcbp::Status reason;
    const std::string payload;
};

/**
 * Exception thrown when the received response deosn't match our expections.
 */
struct ValidationError : public std::runtime_error {
    explicit ValidationError(const std::string& msg) : std::runtime_error(msg) {
    }
};

/**
 * The execution mode represents the mode the server executes commands
 * retrieved over the network. In an Ordered mode (that's the default mode
 * and how things was defined in the initial implementation of the binary
 * protocol) the server must not start executing the next command before
 * the execution of the current command is completed. In Unordered mode
 * the server may start executing (and report the result back to the client)
 * whenever it feels like.
 */
enum class ExecutionMode { Ordered, Unordered };

/**
 * The MemcachedConnection class is an abstract class representing a
 * connection to memcached. The concrete implementations of the class
 * implements the Memcached binary protocol and Greenstack.
 *
 * By default a connection is set into a synchronous mode.
 *
 * All methods is expeted to work, and all failures is reported through
 * exceptions. Unexpected packets / responses etc will use the ConnectionError,
 * and other problems (like network error etc) use std::runtime_error.
 *
 */
class MemcachedConnection {
public:
    MemcachedConnection() = delete;

    MemcachedConnection(const MemcachedConnection&) = delete;

    /**
     * Create a new instance of the MemcachedConnection
     *
     * @param host the hostname to connect to (empty == localhost)
     * @param port the port number to connect to
     * @param family the socket family to connect as (AF_INET, AF_INET6
     *               or use AF_UNSPEC to just pick one)
     * @param ssl connect over SSL or not
     */
    MemcachedConnection(std::string host,
                        in_port_t port,
                        sa_family_t family,
                        bool ssl);

    ~MemcachedConnection();

    /**
     * Release the socket from this instance. The caller is required
     * to close the socket when it is no longer in use!
     *
     * @return the underlying socket
     */
    SOCKET releaseSocket();

    // Set a tag / label on this connection
    void setTag(std::string tag) {
        MemcachedConnection::tag = std::move(tag);
    }

    const std::string& getTag() const {
        return tag;
    }

    // Creates clone (copy) of the given connection - i.e. a second independent
    // channel to memcached. Used for multi-connection testing.
    std::unique_ptr<MemcachedConnection> clone();

    in_port_t getPort() const {
        return port;
    }

    sa_family_t getFamily() const {
        return family;
    }

    bool isSsl() const {
        return ssl;
    }

    /**
     * Set the SSL Certificate file to use
     *
     * @throws std::system_error if the file doesn't exist
     */
    void setSslCertFile(const std::string& file);

    /**
     * Set the SSL private key file to use
     *
     * @throws std::system_error if the file doesn't exist
     */
    void setSslKeyFile(const std::string& file);

    /// Set the TLS version to use
    void setTlsProtocol(std::string protocol);
    /// Set the ciphers to use for TLS < 1.3
    void setTls12Ciphers(std::string ciphers);
    /// Set the ciphers to use for TLS >= 1.3
    void setTls13Ciphers(std::string ciphers);

    /**
     * Try to establish a connection to the server.
     *
     * @thows std::exception if an error occurs
     */
    void connect();

    /**
     * Close the connection to the server
     */
    void close();

    /**
     * Drop the current connection to the server and re-establish the
     * connection.
     */
    void reconnect() {
        close();
        connect();
    }

    /**
     * Perform a SASL authentication to memcached
     *
     * @param username the username to use in authentication
     * @param password the password to use in authentication
     * @param mech the SASL mech to use
     */
    void authenticate(const std::string& username,
                      const std::string& password,
                      const std::string& mech = "PLAIN");

    /**
     * Create a bucket
     *
     * @param name the name of the bucket
     * @param config the buckets configuration attributes
     * @param type the kind of bucket to create
     */
    void createBucket(const std::string& name,
                      const std::string& config,
                      BucketType type);

    /**
     * Delete the named bucket
     *
     * @param name the name of the bucket
     */
    void deleteBucket(const std::string& name);

    /**
     * Select the named bucket
     *
     * @param name the name of the bucket to select
     */
    void selectBucket(const std::string& name);

    /**
     * List all of the buckets on the server
     *
     * @return a vector containing all of the buckets
     */
    std::vector<std::string> listBuckets(
            GetFrameInfoFunction getFrameInfo = {});

    /**
     * Fetch a document from the server
     *
     * @param id the name of the document
     * @param vbucket the vbucket the document resides in
     * @return a document object containg the information about the
     *         document.
     */
    Document get(const std::string& id,
                 Vbid vbucket,
                 GetFrameInfoFunction getFrameInfo = {});

    /**
     * Fetch multiple documents
     *
     * Send a pipeline of (quiet) get commands to the server and fire
     * the documentCallback with the documents found in the server.
     *
     * If the server returns with an error the provided error callback
     * will be called. (note that you won't receive a callback for
     * documents that don't exist on the server as we're using the
     * quiet commands.
     *
     * Use the getFrameInfo method if you'd like the server to perform
     * out of order requests (note: the connection must be set to
     * allow unordered execution).
     *
     * @param id The key and the vbucket the document resides in
     * @param documentCallback the callback with the document for an
     *                         operation
     * @param errorCallback the callback if the server returns an error
     * @param getFrameInfo Optional FrameInfo to inject to the commands
     * @return A vector containing all of the found documents
     */
    void mget(const std::vector<std::pair<const std::string, Vbid>>& id,
              std::function<void(std::unique_ptr<Document>&)> documentCallback,
              std::function<void(const std::string&, const cb::mcbp::Response&)>
                      errorCallback = {},
              GetFrameInfoFunction getFrameInfo = {});

    /**
     * Fetch and lock a document from the server
     *
     * @param id the name of the document
     * @param vbucket the vbucket the document resides in
     * @param lock_timeout the timeout (in sec) for the lock. 0 means
     *                     use the default lock timeout from the server
     * @return a document object containing the information about the
     *         document.
     */
    Document get_and_lock(const std::string& id,
                          Vbid vbucket,
                          uint32_t lock_timeout,
                          GetFrameInfoFunction getFrameInfo = {});

    /**
     * Get the Failover Log for a given VBucket
     *
     * @param vbucket
     * @return the raw BinprotResponse
     */
    BinprotResponse getFailoverLog(Vbid vbucket,
                                   GetFrameInfoFunction getFrameInfo = {});

    /**
     * Unlock a locked document
     *
     * @param id the name of the document
     * @param vbucket the vbucket the document resides in
     * @param cas the cas identifier of the locked document
     */
    void unlock(const std::string& id,
                Vbid vbucket,
                uint64_t cas,
                GetFrameInfoFunction getFrameInfo = {});

    void dropPrivilege(cb::rbac::Privilege privilege,
                       GetFrameInfoFunction getFrameInfo = {});

    /*
     * Form a Frame representing a CMD_GET
     */
    static Frame encodeCmdGet(const std::string& id, Vbid vbucket);

    MutationInfo mutate(const Document& doc,
                        Vbid vbucket,
                        MutationType type,
                        GetFrameInfoFunction getFrameInfo = {}) {
        return mutate(doc.info,
                      vbucket,
                      cb::const_byte_buffer(reinterpret_cast<const uint8_t*>(
                                                    doc.value.data()),
                                            doc.value.size()),
                      type,
                      getFrameInfo);
    }

    /**
     * Perform the mutation on the attached document.
     *
     * The method throws an exception upon errors
     *
     * @param info Document metadata
     * @param vbucket the vbucket to operate on
     * @param value new value for the document
     * @param type the type of mutation to perform
     * @return the new cas value for success
     */
    MutationInfo mutate(const DocumentInfo& info,
                        Vbid vbucket,
                        cb::const_byte_buffer value,
                        MutationType type,
                        GetFrameInfoFunction getFrameInfo = {});

    /**
     * Convenience method to store (aka "upsert") an item.
     * @param id The item's ID
     * @param vbucket vBucket
     * @param value Value of the item.
     * @return The mutation result.
     */
    MutationInfo store(const std::string& id,
                       Vbid vbucket,
                       std::string value,
                       cb::mcbp::Datatype datatype = cb::mcbp::Datatype::Raw,
                       GetFrameInfoFunction getFrameInfo = {});

    /**
     * Get statistics from the server, and fire a callback with the key and
     * value of each reported stat
     *
     * @param callback the callback to call for each stat
     * @param group the stats group to request
     */
    void
    stats(std::function<void(const std::string&, const std::string&)> callback,
          const std::string& group = std::string{},
          GetFrameInfoFunction getFrameInfo = {});

    /**
     * Get stats as a map
     * @param subcommand
     * @return
     */
    std::map<std::string, std::string> statsMap(
            const std::string& subcommand,
            GetFrameInfoFunction getFrameInfo = {});

    nlohmann::json stats(const std::string& subcommand,
                         GetFrameInfoFunction getFrameInfo = {});

    /**
     * Instruct the audit daemon to reload the configuration
     */
    void reloadAuditConfiguration(GetFrameInfoFunction getFrameInfo = {});

    /**
     * Sent the given frame over this connection
     *
     * @param frame the frame to send to the server
     */
    void sendFrame(const Frame& frame);

    /** Send part of the given frame over this connection. Upon success,
     * the frame's payload will be modified such that the sent bytes are
     * deleted - i.e. after a successful call the frame object will only have
     * the remaining, unsent bytes left.
     *
     * @param frame The frame to partially send.
     * @param length The number of bytes to transmit. Must be less than or
     *               equal to the size of the frame.
     */
    void sendPartialFrame(Frame& frame, Frame::size_type length);

    /**
     * Receive the next frame on the connection
     *
     * @param frame the frame object to populate with the next frame
     */
    void recvFrame(Frame& frame);

    void sendCommand(const BinprotCommand& command);

    void recvResponse(BinprotResponse& response);

    /**
     * Execute a command on the server and return the raw response packet.
     */
    BinprotResponse execute(const BinprotCommand& command);

    /// Execute a command on the server and return the response
    Frame execute(const Frame& frame);
    /**
     * Get a textual representation of this connection
     *
     * @return a textual representation of the connection including the
     *         protocol and any special attributes
     */
    std::string to_string();

    /**
     * Try to configure the ewouldblock engine
     *
     * See the header /engines/ewouldblock_engine/ewouldblock_engine.h
     * for a full description on the parameters.
     */
    void configureEwouldBlockEngine(
            const EWBEngineMode& mode,
            ENGINE_ERROR_CODE err_code = ENGINE_EWOULDBLOCK,
            uint32_t value = 0,
            const std::string& key = "");

    /**
     * Disable the ewouldblock engine entirely.
     */
    void disableEwouldBlockEngine() {
        // We disable the engine by telling it to inject the given error
        // the next 0 times
        configureEwouldBlockEngine(EWBEngineMode::Next_N, ENGINE_SUCCESS, 0);
    }

    /**
     * Identify ourself to the server.
     *
     * @throws std::runtime_error if an error occurs
     */
    void hello(const std::string& userAgent,
               const std::string& userAgentVersion,
               const std::string& comment);

    /**
     * Get the servers SASL mechanisms.
     *
     * @throws std::runtime_error if an error occurs
     */
    std::string getSaslMechanisms();

    /**
     * Request the IOCTL value from the server
     *
     * @param key the IOCTL to request
     * @return A textual representation of the key
     */
    std::string ioctl_get(const std::string& key,
                          GetFrameInfoFunction getFrameInfo = {});

    /**
     * Perform an IOCTL on the server
     *
     * @param key the IOCTL to set
     * @param value the value to specify for the given key
     */
    void ioctl_set(const std::string& key,
                   const std::string& value,
                   GetFrameInfoFunction getFrameInfo = {});

    /**
     * Perform an arithmetic operation on a document (increment or decrement)
     *
     * You may use this method when operating on "small" delta values which
     * fit into a signed 64 bit integer. If you for some reason need to
     * incr / decr values above that you must use increment and decrement
     * directly.
     *
     * @param key the document to operate on
     * @param delta The value to increment / decrement
     * @param initial Create with the initial value (exptime must be set to
     *                != 0xffffffff)
     * @param exptime The expiry time for the document
     * @param info Where to store the mutation info.
     * @return The new value for the counter
     */
    uint64_t arithmetic(const std::string& key,
                        int64_t delta,
                        uint64_t initial = 0,
                        rel_time_t exptime = 0,
                        MutationInfo* info = nullptr,
                        GetFrameInfoFunction getFrameInfo = {}) {
        if (delta < 0) {
            return decrement(key,
                             uint64_t(std::abs(delta)),
                             initial,
                             exptime,
                             info,
                             getFrameInfo);
        } else {
            return increment(
                    key, uint64_t(delta), initial, exptime, info, getFrameInfo);
        }
    }

    /**
     * Perform an increment operation on a document
     *
     * This method only exists in order to test the situations where you want
     * to increment a value that wouldn't fit into a signed 64 bit integer.
     *
     * @param key the document to operate on
     * @param delta The value to increment
     * @param initial Create with the initial value (exptime must be set to
     *                != 0xffffffff)
     * @param exptime The expiry time for the document
     * @param info Where to store the mutation info.
     * @return The new value for the counter
     */
    uint64_t increment(const std::string& key,
                       uint64_t delta,
                       uint64_t initial = 0,
                       rel_time_t exptime = 0,
                       MutationInfo* info = nullptr,
                       GetFrameInfoFunction getFrameInfo = {});

    /**
     * Perform an decrement operation on a document
     *
     * @param key the document to operate on
     * @param delta The value to increment / decrement
     * @param initial Create with the initial value (exptime must be set to
     *                != 0xffffffff)
     * @param exptime The expiry time for the document
     * @param info Where to store the mutation info.
     * @return The new value for the counter
     */
    uint64_t decrement(const std::string& key,
                       uint64_t delta,
                       uint64_t initial = 0,
                       rel_time_t exptime = 0,
                       MutationInfo* info = nullptr,
                       GetFrameInfoFunction getFrameInfo = {});

    /**
     * Remove the named document
     *
     * @param key the document to remove
     * @param vbucket the vbucket the document is stored in
     * @param cas the specific version of the document or 0 for "any"
     * @return Details about the detion
     */
    MutationInfo remove(const std::string& key,
                        Vbid vbucket,
                        uint64_t cas = 0,
                        GetFrameInfoFunction getFrameInfo = {});

    /**
     * Mutate with meta - stores doc into the bucket using all the metadata
     * from doc, e.g. doc.cas will become the stored cas (on success).
     *
     * @param doc The document to set
     * @param vbucket The vbucket the document is stored in
     * @param cas The cas used for the setWithMeta (note this cas is not stored
     *            on success)
     * @param seqno The seqno to store the document as
     * @param metaOption MCBP options that can be sent with the command
     * @param metaExtras Optional - see ep/src/ext_meta_parser.h for the details
     *                   of this.
     */
    MutationInfo mutateWithMeta(Document& doc,
                                Vbid vbucket,
                                uint64_t cas,
                                uint64_t seqno,
                                uint32_t metaOption,
                                std::vector<uint8_t> metaExtras = {},
                                GetFrameInfoFunction getFrameInfo = {});

    std::pair<cb::mcbp::Status, GetMetaResponse> getMeta(
            const std::string& key,
            Vbid vbucket,
            GetMetaVersion version,
            GetFrameInfoFunction getFrameInfo = {});

    /**
     * Evict the provided key
     *
     * @param key The key to evict
     * @param vbucket The vbucket the key belongs to
     * @param getFrameInfo  Optional frame ids
     */
    void evict(const std::string& key,
               Vbid vbucket,
               GetFrameInfoFunction getFrameInfo = {});

    /**
     * Observe Seqno command - retrieve the persistence status of the given
     * vBucket and UUID.
     */
    ObserveInfo observeSeqno(Vbid vbid,
                             uint64_t uuid,
                             GetFrameInfoFunction getFrameInfo = {});

    /// Enable persistence for the connected bucket.
    void enablePersistence(GetFrameInfoFunction getFrameInfo = {});

    /// Disable persistence for the connected bucket.
    void disablePersistence(GetFrameInfoFunction getFrameInfo = {});

    bool hasFeature(cb::mcbp::Feature feature) const {
        return effective_features.find(uint16_t(feature)) !=
               effective_features.end();
    }

    void setDatatypeJson(bool enable) {
        setFeature(cb::mcbp::Feature::JSON, enable);
    }

    void setMutationSeqnoSupport(bool enable) {
        setFeature(cb::mcbp::Feature::MUTATION_SEQNO, enable);
    }

    void setXattrSupport(bool enable) {
        setFeature(cb::mcbp::Feature::XATTR, enable);
    }

    void setXerrorSupport(bool enable) {
        setFeature(cb::mcbp::Feature::XERROR, enable);
    }

    void setDuplexSupport(bool enable) {
        setFeature(cb::mcbp::Feature::Duplex, enable);
    }

    void setClustermapChangeNotification(bool enable) {
        setFeature(cb::mcbp::Feature::ClustermapChangeNotification, enable);
    }

    void setUnorderedExecutionMode(ExecutionMode mode);

    /**
     * Attempts to enable or disable a feature
     * @param feature Feature to enable or disable
     * @param enabled whether to enable or disable
     */
    void setFeature(cb::mcbp::Feature feature, bool enabled);

    std::optional<std::chrono::microseconds> getTraceData() const {
        return traceData;
    }

    /**
     * Set the connection features to use
     *
     * @param agent the agent name to report to the server
     * @param features a vector containing all of the features to try
     *                 to enable on the server
     */
    void setFeatures(const std::string& agent,
                     const std::vector<cb::mcbp::Feature>& features);

    void setVbucket(Vbid vbid,
                    vbucket_state_t state,
                    const nlohmann::json& payload,
                    GetFrameInfoFunction getFrameInfo = {});

    /// should the client automatically retry operations which fail
    /// with a tmpfail or not (note that this is only possible when
    /// the client object have the command frame available
    void setAutoRetryTmpfail(bool value) {
        auto_retry_tmpfail = value;
    }

    bool getAutoRetryTmpfail() const {
        return auto_retry_tmpfail;
    }

    Document getRandomKey(Vbid vbid);

    void dcpOpenProducer(std::string_view value);
    void dcpControl(std::string_view key, std::string_view value);
    void dcpStreamRequest(Vbid vbid,
                          uint32_t flags,
                          uint64_t startSeq,
                          uint64_t endSeq,
                          uint64_t vbUuid,
                          uint64_t snapStart,
                          uint64_t snapEnd);
    void dcpStreamRequest(Vbid vbid,
                          uint32_t flags,
                          uint64_t startSeq,
                          uint64_t endSeq,
                          uint64_t vbUuid,
                          uint64_t snapStart,
                          uint64_t snapEnd,
                          const nlohmann::json& value);

    cb::mcbp::request::GetCollectionIDPayload getCollectionId(
            std::string_view path);
    cb::mcbp::request::GetScopeIDPayload getScopeId(std::string_view path);

    nlohmann::json getCollectionsManifest();

protected:
    void read(Frame& frame, size_t bytes);

    void readPlain(Frame& frame, size_t bytes);

    void readSsl(Frame& frame, size_t bytes);

    void sendBuffer(const std::vector<iovec>& buf);
    void sendBuffer(cb::const_byte_buffer buf);

    void sendBufferPlain(cb::const_byte_buffer buf);
    void sendBufferPlain(const std::vector<iovec>& list);

    void sendBufferSsl(cb::const_byte_buffer buf);
    void sendBufferSsl(const std::vector<iovec>& list);

    void applyFrameInfos(BinprotCommand& command, GetFrameInfoFunction& fi);

    /**
     * Keep on calling the executor function until it returns true.
     *
     * Every time the function returns false the thread sleeps for the
     * provided number of milliseconds. If the loop takes longer than
     * the provided number of seconds the method throws an exception.
     *
     * @param executor The function to call until it returns true
     * @param backoff The number of milliseconds to back off
     * @param timeout The number of seconds until an exception is thrown
     * @throws std::runtime_error for timeouts
     */
    void backoff_execute(
            std::function<bool()> executor,
            std::chrono::milliseconds backoff = std::chrono::milliseconds(10),
            std::chrono::seconds timeout = std::chrono::seconds(30));

    std::string host;
    in_port_t port;
    sa_family_t family;
    bool auto_retry_tmpfail = false;
    bool ssl;
    std::string tls_protocol;
    std::string tls12_ciphers{"HIGH"};
    std::string tls13_ciphers{
            "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_"
            "GCM_SHA256"};
    std::string ssl_cert_file;
    std::string ssl_key_file;
    SSL_CTX* context = nullptr;
    BIO* bio = nullptr;
    SOCKET sock = INVALID_SOCKET;
    std::string tag;
    std::optional<std::chrono::microseconds> traceData;

    typedef std::unordered_set<uint16_t> Featureset;

    uint64_t incr_decr(cb::mcbp::ClientOpcode opcode,
                       const std::string& key,
                       uint64_t delta,
                       uint64_t initial,
                       rel_time_t exptime,
                       MutationInfo* info,
                       GetFrameInfoFunction getFrameInfo = {});

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

    Featureset effective_features;
};

namespace cb {
namespace net {
/**
 * Create a new socket and connect it to the given host
 *
 * @param host The name of the host to try to connect to. If
 *             empty (or set to localhost) it'll be replaced
 *             with "127.0.0.1" or "::1" depending on the value
 *             of family
 * @param port The port number to connect to
 * @param family The socket family to create (AF_INET/AF_INET6/AF_UNSPEC)
 * @return The connected socket or INVALID_SOCKET if we failed to connect
 *         to the socket
 *
 */
SOCKET new_socket(const std::string& host, in_port_t port, sa_family_t family);

/**
 * Create a new socket and connect it to the given host
 *
 * @param host The name of the host to try to connect to. If
 *             empty (or set to localhost) it'll be replaced
 *             with "127.0.0.1" or "::1" depending on the value
 *             of family
 * @param port The port number to connect to
 * @param family The socket family to create (AF_INET/AF_INET6/AF_UNSPEC)
 * @param setup_ssl_ctx callback to configure the SSL context
 * @return Tuple with:
 *             SOCKET The connected socket or INVALID_SOCKET if we failed
 *                    to connect to the socket
 *             SSL_CTX The ssl context in use
 *             BIO The BIO to use.
 *
 * The caller takes ownership of the socket, ssl_ctx and bio and must
 * release the resources when done using them.
 *
 * @throws std::exception for SSL related problems
 */
std::tuple<SOCKET, SSL_CTX*, BIO*> new_ssl_socket(
        const std::string& host,
        in_port_t port,
        sa_family_t family,
        std::function<void(SSL_CTX*)> setup_ssl_ctx);

} // namespace net
} // namespace cb
