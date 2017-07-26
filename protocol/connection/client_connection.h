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

#include <cJSON.h>
#include <cJSON_utils.h>
#include <cstdlib>
#include <daemon/settings.h>
#include <engines/ewouldblock_engine/ewouldblock_engine.h>
#include <memcached/openssl.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include <platform/dynamic.h>
#include <platform/sized_buffer.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <utilities/protocol2text.h>

/**
 * The Frame class is used to represent all of the data included in the
 * protocol unit going over the wire. For the memcached binary protocol
 * this is either the full request or response as defined in
 * memcached/protocol_binary.h, and for greenstack this is the greenstack
 * frame as defined in libreenstack/Frame.h
 */
class Frame {
public:
    void reset() {
        payload.resize(0);
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
};

class Document {
public:
    DocumentInfo info;
    std::vector<uint8_t> value;
};

class MutationInfo {
public:
    uint64_t cas;
    size_t size;
    uint64_t seqno;
    uint64_t vbucketuuid;
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

class ConnectionError : public std::runtime_error {
public:
    explicit ConnectionError(const char* what_arg)
        : std::runtime_error(what_arg) {
        // Empty
    }

    virtual uint16_t getReason() const = 0;

    virtual Protocol getProtocol() const = 0;

    virtual bool isInvalidArguments() const = 0;

    virtual bool isAlreadyExists() const = 0;

    virtual bool isNotMyVbucket() const = 0;

    virtual bool isNotFound() const = 0;

    virtual bool isNotStored() const = 0;

    virtual bool isAccessDenied() const = 0;

    virtual bool isDeltaBadval() const = 0;

    virtual bool isAuthError() const = 0;

    virtual bool isNotSupported() const = 0;

    virtual bool isLocked() const = 0;

    virtual bool isTemporaryFailure() const = 0;
};

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

    virtual ~MemcachedConnection();

    // Creates clone (copy) of the given connection - i.e. a second independent
    // channel to memcached. Used for multi-connection testing.
    virtual std::unique_ptr<MemcachedConnection> clone() = 0;

    in_port_t getPort() const {
        return port;
    }

    sa_family_t getFamily() const {
        return family;
    }

    bool isSsl() const {
        return ssl;
    }

    const Protocol& getProtocol() const {
        return protocol;
    }

    bool isSynchronous() const {
        return synchronous;
    }

    virtual void setSynchronous(bool enable) {
        if (!enable) {
            std::runtime_error("MemcachedConnection::setSynchronous: Not implemented");
        }
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

    /**
     * Try to establish a connection to the server.
     *
     * @thows std::exception if an error occurs
     */
    void connect();

    bool isConnected() const {
        return sock != INVALID_SOCKET;
    }

    /**
     * Close the connection to the server
     */
    virtual void close();

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
    virtual void authenticate(const std::string& username,
                              const std::string& password,
                              const std::string& mech) = 0;

    /**
     * Create a bucket
     *
     * @param name the name of the bucket
     * @param config the buckets configuration attributes
     * @param type the kind of bucket to create
     */
    virtual void createBucket(const std::string& name,
                              const std::string& config,
                              const BucketType type) = 0;

    /**
     * Delete the named bucket
     *
     * @param name the name of the bucket
     */
    virtual void deleteBucket(const std::string& name) = 0;

    /**
     * Select the named bucket
     *
     * @param name the name of the bucket to select
     */
    virtual void selectBucket(const std::string& name) = 0;

    /**
     * List all of the buckets on the server
     *
     * @return a vector containing all of the buckets
     */
    virtual std::vector<std::string> listBuckets() = 0;

    /**
     * Fetch a document from the server
     *
     * @param id the name of the document
     * @param vbucket the vbucket the document resides in
     * @return a document object containg the information about the
     *         document.
     */
    virtual Document get(const std::string& id, uint16_t vbucket) = 0;

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
    virtual Document get_and_lock(const std::string& id,
                                  uint16_t vbucket,
                                  uint32_t lock_timeout) {
        // We don't want to implement this for greenstack at this time
        throw std::invalid_argument("Not implemented");
    }


    /**
     * Unlock a locked document
     *
     * @param id the name of the document
     * @param vbucket the vbucket the document resides in
     * @param cas the cas identifier of the locked document
     */
    virtual void unlock(const std::string& id,
                        uint16_t vbucket,
                        uint64_t cas) {
        // We don't want to implement this for greenstack at this time
        throw std::invalid_argument("Not implemented");
    }

    virtual void dropPrivilege(cb::rbac::Privilege privilege) = 0;

    /*
     * Form a Frame representing a CMD_GET
     */
    virtual Frame encodeCmdGet(const std::string& id, uint16_t vbucket) = 0;

    MutationInfo mutate(const Document& doc,
                        uint16_t vbucket,
                        MutationType type) {
        return mutate(doc.info,
                      vbucket,
                      cb::const_byte_buffer(doc.value.data(), doc.value.size()),
                      type);
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
    virtual MutationInfo mutate(const DocumentInfo& info,
                                uint16_t vbucket,
                                cb::const_byte_buffer value,
                                MutationType type) = 0;

    /**
     * Convenience method to store (aka "upsert") an item.
     * @param id The item's ID
     * @param vbucket vBucket
     * @param value Value of the item. Should have a begin/end function
     * @return The mutation result.
     */
    template <typename T>
    MutationInfo store(
            const std::string& id,
            uint16_t vbucket,
            const T& value,
            cb::mcbp::Datatype datatype = cb::mcbp::Datatype::Raw) {
        Document doc{};
        doc.value.assign(value.begin(), value.end());
        doc.info.id = id;
        doc.info.datatype = datatype;
        return mutate(doc, vbucket, MutationType::Set);
    }
    MutationInfo store(const std::string& id, uint16_t vbucket,
                       const char *value, size_t len) {
        cb::const_char_buffer buf(value, len);
        return store(id, vbucket, buf);
    }

    /**
     * Get stats as a map
     * @param subcommand
     * @return
     */
    virtual std::map<std::string,std::string> statsMap(
            const std::string& subcommand) = 0;

    unique_cJSON_ptr stats(const std::string& subcommand);

    /**
     * Instruct the audit daemon to reload the configuration
     */
    virtual void reloadAuditConfiguration() = 0;

    /**
     * Sent the given frame over this connection
     *
     * @param frame the frame to send to the server
     */
    virtual void sendFrame(const Frame& frame);

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
    virtual void recvFrame(Frame& frame) = 0;

    /**
     * Get a textual representation of this connection
     *
     * @return a textual representation of the connection including the
     *         protocol and any special attributes
     */
    virtual std::string to_string() = 0;

    /**
     * Try to configure the ewouldblock engine
     *
     * See the header /engines/ewouldblock_engine/ewouldblock_engine.h
     * for a full description on the parameters.
     */
    virtual void configureEwouldBlockEngine(const EWBEngineMode& mode,
                                            ENGINE_ERROR_CODE err_code = ENGINE_EWOULDBLOCK,
                                            uint32_t value = 0,
                                            const std::string& key = "") = 0;

    /**
     * Disable the ewouldblock engine entirely.
     */
    void disableEwouldBlockEngine() {
        // We disable the engine by telling it to inject the given error
        // the next 0 times
        configureEwouldBlockEngine(EWBEngineMode::Next_N, ENGINE_SUCCESS, 0);
    }


    /**
     * Identify ourself to the server and fetch the available SASL mechanisms
     * (available by calling `getSaslMechanisms()`
     *
     * @throws std::runtime_error if an error occurs
     */
    virtual void hello(const std::string& userAgent,
                       const std::string& userAgentVersion,
                       const std::string& comment) = 0;


    /**
     * Get the servers SASL mechanisms. This is only valid after running a
     * successful `hello()`
     */
    const std::string& getSaslMechanisms() const {
        return saslMechanisms;
    }

    /**
     * Request the IOCTL value from the server
     *
     * @param key the IOCTL to request
     * @return A textual representation of the key
     */
    virtual std::string ioctl_get(const std::string& key) {
        throw std::invalid_argument("Not implemented");
    }

    /**
     * Perform an IOCTL on the server
     *
     * @param key the IOCTL to set
     * @param value the value to specify for the given key
     */
    virtual void ioctl_set(const std::string& key,
                           const std::string& value) {
        throw std::invalid_argument("Not implemented");
    }

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
    virtual uint64_t arithmetic(const std::string& key,
                                int64_t delta,
                                uint64_t initial = 0,
                                rel_time_t exptime = 0,
                                MutationInfo* info = nullptr) {
        if (delta < 0) {
            return decrement(key, uint64_t(std::abs(delta)), initial,
                             exptime, info);
        } else {
            return increment(key, uint64_t(delta), initial, exptime, info);
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
    virtual uint64_t increment(const std::string& key,
                               uint64_t delta,
                               uint64_t initial = 0,
                               rel_time_t exptime = 0,
                               MutationInfo* info = nullptr) {
        throw std::invalid_argument("Not implemented");
    }

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
    virtual uint64_t decrement(const std::string& key,
                               uint64_t delta,
                               uint64_t initial = 0,
                               rel_time_t exptime = 0,
                               MutationInfo* info = nullptr) {
        throw std::invalid_argument("Not implemented");
    }

    /**
     * Remove the named document
     *
     * @param key the document to remove
     * @param vbucket the vbucket the document is stored in
     * @param cas the specific version of the document or 0 for "any"
     * @return Details about the detion
     */
    virtual MutationInfo remove(const std::string& key,
                                uint16_t vbucket,
                                uint64_t cas = 0) {
        // Don't bother implementing it for Greenstack at this moment
        throw std::invalid_argument("Not implemented");
    }

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
    virtual MutationInfo mutateWithMeta(
            Document& doc,
            uint16_t vbucket,
            uint64_t cas,
            uint64_t seqno,
            uint32_t metaOption,
            std::vector<uint8_t> metaExtras = {}) = 0;

    virtual GetMetaResponse getMeta(const std::string& key,
                                    uint16_t vbucket,
                                    uint64_t cas) = 0;

protected:
    /**
     * Create a new instance of the MemcachedConnection
     *
     * @param host the hostname to connect to (empty == localhost)
     * @param port the port number to connect to
     * @param family the socket family to connect as (AF_INET, AF_INET6
     *               or use AF_UNSPEC to just pick one)
     * @param ssl connect over SSL or not
     * @param protocol the protocol the implementation is using
     * @return
     */
    MemcachedConnection(const std::string& host, in_port_t port,
                        sa_family_t family, bool ssl,
                        const Protocol& protocol);

    void read(Frame& frame, size_t bytes);

    void readPlain(Frame& frame, size_t bytes);

    void readSsl(Frame& frame, size_t bytes);

    void sendFramePlain(const Frame& frame) {
        sendBufferPlain(cb::const_byte_buffer(frame.payload.data(),
                                              frame.payload.size()));
    }

    void sendFrameSsl(const Frame& frame) {
        sendBufferSsl(cb::const_byte_buffer(frame.payload.data(),
                                             frame.payload.size()));
    }

    void sendBuffer(cb::const_byte_buffer& buf);

    void sendBufferPlain(cb::const_byte_buffer buf);

    void sendBufferSsl(cb::const_byte_buffer buf);

    std::string host;
    in_port_t port;
    sa_family_t family;
    bool ssl;
    std::string ssl_cert_file;
    std::string ssl_key_file;
    Protocol protocol;
    SSL_CTX* context;
    BIO* bio;
    SOCKET sock;
    bool synchronous;
    std::string saslMechanisms;
};

class ConnectionMap {
public:
    /**
     * Initialize the connection map with connections matching the ports
     * opened from Memcached
     */
    void initialize(cJSON* ports);

    /**
     * Invalidate all of the connections
     */
    void invalidate();

    /**
     * Get a connection object matching the given attributes
     *
     * @param protocol The requested protocol (Greenstack / Memcached)
     * @param ssl If ssl should be enabled or not
     * @param family the network family (IPv4 / IPv6)
     * @param port (optional) The specific port number to use..
     * @return A connection object to use
     * @throws std::runtime_error if the request can't be served
     */
    MemcachedConnection& getConnection(const Protocol& protocol,
                                       bool ssl,
                                       sa_family_t family = AF_INET,
                                       in_port_t port = 0);

    /**
     * Just get a connection to the server (protocol / ssl etc
     * doesn't matter)
     *
     * @return A connection to the server
     */
    MemcachedConnection& getConnection() {
        return *connections.front().get();
    }

    /**
     * Do we have a connection matching the requested attributes
     */
    bool contains(const Protocol& protocol, bool ssl, sa_family_t family);

private:
    std::vector<std::unique_ptr<MemcachedConnection>> connections;
};
