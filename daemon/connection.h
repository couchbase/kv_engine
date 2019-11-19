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

#include "datatype.h"
#include "statemachine.h"
#include "stats.h"
#include "task.h"

#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <daemon/protocol/mcbp/command_context.h>
#include <event.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dcp.h>
#include <memcached/openssl.h>
#include <memcached/rbac.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/pipe.h>
#include <platform/sized_buffer.h>
#include <platform/socket.h>

#include <array>
#include <chrono>
#include <memory>
#include <queue>
#include <string>
#include <vector>

class Bucket;
class Cookie;
class ListeningPort;
class ServerEvent;
struct EngineIface;
struct FrontEndThread;
class SendBuffer;

namespace cb {
namespace mcbp {
class Header;
} // namespace mcbp
} // namespace cb

/**
 * The maximum number of character the core preserves for the
 * agent name for each connection
 */
const size_t MaxSavedAgentName = 33;

/**
 * The maximum number of character the core preserves for the
 * connection identifier for each connection
 */
const size_t MaxSavedConnectionId = 34;

/**
 * The structure representing a connection in memcached.
 */
class Connection : public dcp_message_producers {
public:
    enum class Priority : uint8_t {
        High,
        Medium,
        Low
    };

    Connection(const Connection&) = delete;

    Connection(SOCKET sfd,
               event_base* b,
               const ListeningPort& ifc,
               FrontEndThread& thr);

    ~Connection() override;

    /**
     * Return an identifier for this connection. To be backwards compatible
     * this is the socket filedescriptor (or the socket handle casted to an
     * unsigned integer on windows).
     */
    uint32_t getId() const {
        return uint32_t(socketDescriptor);
    }

    /**
     *  Get the socket descriptor used by this connection.
     */
    SOCKET getSocketDescriptor() const {
        return socketDescriptor;
    }

    /**
     * Set the socket descriptor used by this connection
     */
    void setSocketDescriptor(SOCKET sfd) {
        Connection::socketDescriptor = sfd;
    }

    bool isSocketClosed() const {
        return socketDescriptor == INVALID_SOCKET;
    }

    const std::string& getPeername() const {
        return peername;
    }

    const std::string& getSockname() const {
        return sockname;
    }

    /**
     * Returns a descriptive name for the connection, of the form:
     *   "[peer_name - local_name ]"
     * (system) is appended to the string for system connections.
     */
    const std::string& getDescription() const {
        return description;
    }

    /**
     * Signal a connection if it's idle
     *
     * The connections thread lock must be held when calling the method
     *
     * @return true if the connection was idle, false otherwise
     */
    bool signalIfIdle();

    /**
     * Is the connection representing a system internal user
     */
    bool isInternal() const {
        return internal;
    }

    /**
     * Specify if this connection is representing an internal user.
     * An internal user is a user which is used by one of the components
     * in Couchbase (like ns_server, indexer etc).
     */
    void setInternal(bool internal);

    /**
     * Update the username to reflect what the user used from the SASL
     * authentication.
     */
    void resetUsernameCache();


    bool isAuthenticated() const {
        return authenticated;
    }

    void setAuthenticated(bool authenticated);

    Priority getPriority() const {
        return priority.load();
    }

    void setPriority(Priority priority);

    /**
     * Create a JSON representation of the members of the connection
     */
    nlohmann::json toJSON() const;

    /**
     * Enable or disable TCP NoDelay on the underlying socket
     *
     * @return true on success, false otherwise
     */
    bool setTcpNoDelay(bool enable);

    /**
     * Get the username this connection is authenticated as
     */
    const std::string& getUsername() const {
        return username;
    }

    /**
     * Get the domain where the user is defined (builtin or saslauthd)
     */
    cb::sasl::Domain getDomain() const {
        return domain;
    }

    cb::sasl::server::ServerContext& getSaslConn() {
        return sasl_conn;
    }

    /**
     * Get the current reference count
     */
    uint8_t getRefcount() const {
        return refcount;
    }

    void incrementRefcount() {
        ++refcount;
    }

    void decrementRefcount() {
        --refcount;
    }

    FrontEndThread& getThread() const {
        return thread;
    }

    in_port_t getParentPort() const {
        return parent_port;
    }

    /**
     * Check if this connection is in posession of the requested privilege
     *
     * @param privilege the privilege to check for
     * @return Ok - the connection holds the privilege
     *         Fail - the connection is missing the privilege
     *         Stale - the authentication context is stale
     */
    cb::rbac::PrivilegeAccess checkPrivilege(cb::rbac::Privilege privilege,
                                             Cookie& cookie);

    /**
     * Try to drop the specified privilege from the current context
     *
     * @param privilege the privilege to drop
     * @return The appropriate error code to return back to the client
     */
    cb::engine_errc dropPrivilege(cb::rbac::Privilege privilege);

    int getBucketIndex() const {
        return bucketIndex.load(std::memory_order_relaxed);
    }

    void setBucketIndex(int bucketIndex);

    Bucket& getBucket() const;

    EngineIface* getBucketEngine() const;

    int getClustermapRevno() const {
        return clustermap_revno;
    }

    void setClustermapRevno(int revno) {
        clustermap_revno = revno;
    }

    /**
     * Restart the authentication (this clears all of the authentication
     * data...)
     */
    void restartAuthentication();

    bool isXerrorSupport() const {
        return xerror_support;
    }

    void setXerrorSupport(bool enable) {
        xerror_support = enable;
    }

    bool isCollectionsSupported() const {
        return collections_support;
    }

    void setCollectionsSupported(bool enable) {
        collections_support = enable;
    }

    DocKey makeDocKey(cb::const_byte_buffer key) {
        return DocKey{key.data(),
                      key.size(),
                      isCollectionsSupported() ? DocKeyEncodesCollectionId::Yes
                                               : DocKeyEncodesCollectionId::No};
    }

    bool isDuplexSupported() const {
        return duplex_support;
    }

    void setDuplexSupported(bool enable) {
        duplex_support = enable;
    }

    bool isClustermapChangeNotificationSupported() const {
        return cccp.load(std::memory_order_acquire);
    }

    void setClustermapChangeNotificationSupported(bool enable) {
        cccp.store(enable, std::memory_order_release);
    }

    bool allowUnorderedExecution() const {
        return allow_unordered_execution;
    }

    void setAllowUnorderedExecution(bool enable) {
        allow_unordered_execution = enable;
    }

    /**
     * Remap the current error code
     *
     * The method modifies the input code and returns the mapped value
     * (to make the function a bit easier to use).
     *
     * Depending on which features the client have enabled the method
     * may either just return the input value, map it to a different value
     * (like ENGINE_DISCONNECT if the client hasn't enabled the extened
     * error codes).
     *
     * @param code The code to map (will be changed on return)
     * @return the mapped value.
     */
    ENGINE_ERROR_CODE remapErrorCode(ENGINE_ERROR_CODE code) const;

    /// convenience wrapper when working with the newer enum cb::engine_errc
    cb::engine_errc remapErrorCode(cb::engine_errc code) const {
        return cb::engine_errc(remapErrorCode(ENGINE_ERROR_CODE(code)));
    }

    /**
     * Add the specified number of ns to the amount of CPU time this
     * connection have used on the CPU (We could alternatively have
     * separate "ON_CPU" and "OFF_CPU" events and record all of this
     * within the connection object instead, but it seemed easier to
     * just wrap it from the method driving the event loop (as we
     * also want to record the delta to the thread scheduler histogram
     *
     * @param ns The number of nanoseconds spent in this iteration.
     */
    void addCpuTime(std::chrono::nanoseconds ns);

    /**
     * Enqueue a new server event
     *
     * @param event
     */
    void enqueueServerEvent(std::unique_ptr<ServerEvent> event);

    /**
     * Close the connection. If there is any references to the connection
     * or the cookies we'll enter the "pending close" state to wait for
     * these operations to complete before changing state to immediate
     * close.
     *
     * @return true if the state machinery could be continued, false if
     *              we'd need external input in order to continue to drive
     *              the state machinery
     */
    bool close();

    /**
     * fire ON_DISCONNECT for all of the cookie objects (in case the
     * underlying engine keeps track of any of them)
     */
    void propagateDisconnect() const;

    void setState(StateMachine::State next_state);

    StateMachine::State getState() const {
        return stateMachine.getCurrentState();
    }

    const char* getStateName() const {
        return stateMachine.getCurrentStateName();
    }

    bool isDCP() const {
        return dcp;
    }

    void setDCP(bool enable);

    bool isDcpXattrAware() const {
        return dcpXattrAware;
    }

    void setDcpXattrAware(bool enable) {
        dcpXattrAware = enable;
    }

    void setDcpDeleteTimeEnabled(bool enable) {
        dcpDeleteTimeEnabled = enable;
    }

    bool isDcpDeleteTimeEnabled() const {
        return dcpDeleteTimeEnabled;
    }

    /// returns true if either collections or delete_time is enabled
    bool isDcpDeleteV2() const {
        return isCollectionsSupported() || isDcpDeleteTimeEnabled();
    }


    bool isDcpNoValue() const {
        return dcpNoValue;
    }

    void setDcpNoValue(bool enable) {
        dcpNoValue = enable;
    }

    /**
     * Disable read event for this connection (we won't get notified if
     * more data arrives on the socket).
     */
    void disableReadEvent();

    /**
     * Enable read event for this connection (cause the read callback to
     * be triggered once there is data on the socket).
     */
    void enableReadEvent();

    /**
     * Copy the provided data to the end of the output stream
     *
     * @param data the data to send
     * @throws std::bad_alloc if we failed to insert the data into the output
     *                        stream.
     */
    void copyToOutputStream(cb::const_char_buffer data);

    /// Wrapper function to deal with byte buffers during the transition over
    /// to only use char buffers
    void copyToOutputStream(cb::const_byte_buffer data) {
        copyToOutputStream(
                {reinterpret_cast<const char*>(data.data()), data.size()});
    }

    /**
     * Add a reference to the data to the output stream.
     *
     * @param data The data to send
     * @param cleanupfn The callback function to call when we're done with the
     *                  data.
     * @param cleanupfn_arg The argument to provide to the cleanup function
     * @throws std::bad_alloc if we failed to insert the data into the output
     *                        stream.
     */
    void chainDataToOutputStream(cb::const_char_buffer data,
                                 evbuffer_ref_cleanup_cb cleanupfn,
                                 void* cleanupfn_arg);

    /**
     * Add a reference to the data to the output stream.
     *
     * @param buffer the send buffer to send
     * @throws std::bad_alloc if we failed to insert the data into the output
     *                        stream.
     */
    void chainDataToOutputStream(std::unique_ptr<SendBuffer> buffer);

    /**
     * Enable the datatype which corresponds to the feature
     *
     * @param feature mcbp::Feature::JSON|XATTR|SNAPPY
     * @throws if feature does not correspond to a datatype
     */
    void enableDatatype(cb::mcbp::Feature feature) {
        datatype.enable(feature);
    }

    /**
     * Disable all the datatypes
     */
    void disableAllDatatypes() {
        datatype.disableAll();
    }

    /**
     * Given the input datatype, return only those which are enabled for the
     * connection.
     *
     * @param dtype the set to intersect against the enabled set
     * @returns the intersection of the enabled bits and dtype
     */
    protocol_binary_datatype_t getEnabledDatatypes(
            protocol_binary_datatype_t dtype) const {
        return datatype.getIntersection(dtype);
    }

    /**
     * @return true if the all of the dtype datatypes are all enabled
     */
    bool isDatatypeEnabled(protocol_binary_datatype_t dtype) const {
        bool rv = datatype.isEnabled(dtype);

        // If the bucket has disabled xattr, then we must reflect that in the
        // returned value
        if (rv && mcbp::datatype::is_xattr(dtype) &&
            !selectedBucketIsXattrEnabled()) {
            rv = false;
        }
        return rv;
    }

    /**
     * @return true if compression datatype is enabled
     */
    bool isSnappyEnabled() const {
        return datatype.isSnappyEnabled();
    }

    /**
     * @return true if the XATTR datatype is enabled
     */
    bool isXattrEnabled() const {
        return datatype.isXattrEnabled();
    }

    bool isSupportsMutationExtras() const {
        return supports_mutation_extras;
    }

    void setSupportsMutationExtras(bool enable) {
        supports_mutation_extras = enable;
    }

    bool isTracingEnabled() const {
        return tracingEnabled;
    }

    void setTracingEnabled(bool enable) {
        tracingEnabled = enable;
    }

    /**
     * Is SSL enabled for this connection or not?
     *
     * @return true if the connection is running over SSL, false otherwise
     */
    bool isSslEnabled() const {
        return server_ctx != nullptr;
    }

    /**
     * Try to find RBAC user from the client ssl cert
     *
     * @return true if username has been linked to RBAC or ssl cert was not
     * presented by the client.
     */
    bool tryAuthFromSslCert(const std::string& userName);

    bool shouldDelete();

    void runEventLoop();

    Cookie& getCookieObject() {
        return *cookies.front();
    }

    /**
     * Get the number of cookies currently bound to this connection
     */
    size_t getNumberOfCookies() const;

    /**
     * Check to see if the next packet to process is completely received
     * and available in the input pipe.
     *
     * @return true if we've got the entire packet, false otherwise
     */
    bool isPacketAvailable() const;

    /**
     * Get the next packet available in the stream.
     *
     * The returned pointer is a pointer directly into the input buffer (and
     * not allocated, so the user should NOT keep the pointer around or try
     * to free it.
     *
     * @return the next packet
     * @throws std::runtime_error if the packet isn't available
     */
    const cb::mcbp::Header& getPacket() const;

    /**
     * Get all of the available bytes (up to a maximum bumber of bytes) in
     * the input stream in a continuous byte buffer.
     *
     * NOTE: THIS MIGHT CAUSE REALLOCATION of the input stream so it should
     * NOT be used unless strictly needed
     */
    cb::const_byte_buffer getAvailableBytes(size_t max = 1024) const;

    /**
     * Is SASL disabled for this connection or not? (connection authenticated
     * with SSL certificates will disable the possibility re-authenticate over
     * SASL)
     */
    bool isSaslAuthEnabled() const {
        return saslAuthEnabled;
    }

    /**
     * Disable the ability for the connected client to perform SASL AUTH
     */
    void disableSaslAuth() {
        saslAuthEnabled = false;
    }

    bool selectedBucketIsXattrEnabled() const;

    /// Initiate shutdown of the connection
    void shutdown() {
        setState(StateMachine::State::closing);
    }

    /**
     * Try to process some of the server events. This may _ONLY_ be performed
     * after we've completely transferred the response for one command, and
     * before we start executing the next one.
     *
     * @return true if processing server events set changed the path in the
     *              state machine (and the current task should be
     *              terminated immediately)
     */
    bool processServerEvents();

    /**
     * Set the name of the connected agent
     */
    void setAgentName(cb::const_char_buffer name);

    /**
     * Get the Identifier specified for this connection.
     */
    const std::array<char, MaxSavedConnectionId>& getConnectionId() {
        return connectionId;
    }

    /**
     * Set the identifier for this connection. By default the
     * identifier is set to the peername, but the client
     * may set it to whatever it likes (truncated at 33
     * characters)
     *
     * @param uuid the uuid to use
     */
    void setConnectionId(cb::const_char_buffer uuid);

    /**
     * Check to see if it is time to back off the CPU to let other
     * connections perform operations. If it is time to back off the
     * CPU reschedule execution by setting a callback in libevent
     *
     * @return true if it is time yield, false otherwise
     */
    bool maybeYield();

    /**
     * Add a header, extras and key to the output socket
     *
     * @param cookie the command context to add the header for
     * @param status The error code to use
     * @param extras The data to put in the extras field
     * @param key The data to put in the data field
     * @param value_len The length of the value field (without extras and key)
     * @param datatype The datatype to inject into the header
     * @throws std::bad_alloc
     */
    void sendResponseHeaders(Cookie& cookie,
                             cb::mcbp::Status status,
                             cb::const_char_buffer extras,
                             cb::const_char_buffer key,
                             std::size_t value_length,
                             uint8_t datatype);

    /**
     * Format and put a response into the send buffer
     *
     * @param cookie The command we're sending the response for
     * @param status The status code for the response
     * @param extras The extras section to insert into the response
     * @param key The key to insert into the response
     * @param value The value to insert into the response
     * @param datatype The value to specify as the datatype of the response
     * @param sendbuffer An optional send buffer to chain into the response
     *                   if present (instead of copying the content).
     * @throws std::runtime_error if the sendbuffers payload don't match the
     *                   provided value.
     */
    void sendResponse(Cookie& cookie,
                      cb::mcbp::Status status,
                      cb::const_char_buffer extras,
                      cb::const_char_buffer key,
                      cb::const_char_buffer value,
                      uint8_t datatype,
                      std::unique_ptr<SendBuffer> sendbuffer);

    /// Check if DCP should use the write buffer for the message or if it
    /// should use an IOVector to do so
    bool dcpUseWriteBuffer(size_t total) const;

    // Implementation of dcp_message_producers interface //////////////////////

    ENGINE_ERROR_CODE get_failover_log(uint32_t opaque, Vbid vbucket) override;

    ENGINE_ERROR_CODE stream_req(uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags,
                                 uint64_t start_seqno,
                                 uint64_t end_seqno,
                                 uint64_t vbucket_uuid,
                                 uint64_t snap_start_seqno,
                                 uint64_t snap_end_seqno,
                                 const std::string& request_value) override;

    ENGINE_ERROR_CODE add_stream_rsp(uint32_t opaque,
                                     uint32_t stream_opaque,
                                     cb::mcbp::Status status) override;

    ENGINE_ERROR_CODE marker_rsp(uint32_t opaque,
                                 cb::mcbp::Status status) override;

    ENGINE_ERROR_CODE set_vbucket_state_rsp(uint32_t opaque,
                                            cb::mcbp::Status status) override;

    ENGINE_ERROR_CODE stream_end(uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags,
                                 cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE marker(uint32_t opaque,
                             Vbid vbucket,
                             uint64_t start_seqno,
                             uint64_t end_seqno,
                             uint32_t flags,
                             boost::optional<uint64_t> high_completed_seqno,
                             cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE mutation(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t lock_time,
                               uint8_t nru,
                               cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE deletion(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE deletion_v2(uint32_t opaque,
                                  cb::unique_item_ptr itm,
                                  Vbid vbucket,
                                  uint64_t by_seqno,
                                  uint64_t rev_seqno,
                                  uint32_t delete_time,
                                  cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE expiration(uint32_t opaque,
                                 cb::unique_item_ptr itm,
                                 Vbid vbucket,
                                 uint64_t by_seqno,
                                 uint64_t rev_seqno,
                                 uint32_t delete_time,
                                 cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE set_vbucket_state(uint32_t opaque,
                                        Vbid vbucket,
                                        vbucket_state_t state) override;
    ENGINE_ERROR_CODE noop(uint32_t opaque) override;

    ENGINE_ERROR_CODE buffer_acknowledgement(uint32_t opaque,
                                             Vbid vbucket,
                                             uint32_t buffer_bytes) override;
    ENGINE_ERROR_CODE control(uint32_t opaque,
                              cb::const_char_buffer key,
                              cb::const_char_buffer value) override;

    ENGINE_ERROR_CODE get_error_map(uint32_t opaque, uint16_t version) override;

    ENGINE_ERROR_CODE system_event(uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   mcbp::systemevent::version version,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData,
                                   cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE prepare(uint32_t opaque,
                              cb::unique_item_ptr itm,
                              Vbid vbucket,
                              uint64_t by_seqno,
                              uint64_t rev_seqno,
                              uint32_t lock_time,
                              uint8_t nru,
                              DocumentState document_state,
                              cb::durability::Level level) override;

    ENGINE_ERROR_CODE seqno_acknowledged(uint32_t opaque,
                                         Vbid vbucket,
                                         uint64_t prepared_seqno) override;

    ENGINE_ERROR_CODE commit(uint32_t opaque,
                             Vbid vbucket,
                             const DocKey& key,
                             uint64_t prepare_seqno,
                             uint64_t commit_seqno) override;

    ENGINE_ERROR_CODE abort(uint32_t opaque,
                            Vbid vbucket,
                            const DocKey& key,
                            uint64_t prepared_seqno,
                            uint64_t abort_seqno) override;

protected:
    /**
     * Protected constructor so that it may only be used by MockSubclasses
     */
    explicit Connection(FrontEndThread& thr);

    /**
     * Update the description string for the connection. This
     * method should be called every time the authentication data
     * (or the sockname/peername) changes
     */
    void updateDescription();

    void runStateMachinery();

    // Shared DCP_DELETION write function for the v1/v2 commands.
    ENGINE_ERROR_CODE deletionInner(const item_info& info,
                                    cb::const_byte_buffer packet,
                                    const DocKey& key);

    /**
     * Add the provided packet to the send pipe for the connection
     */
    ENGINE_ERROR_CODE add_packet_to_send_pipe(cb::const_byte_buffer packet);

    /**
     * The actual socket descriptor used by this connection
     */
    SOCKET socketDescriptor;

    const bool connectedToSystemPort;

    // The number of times we've been backing off and yielding
    // to allow other threads to run
    cb::RelaxedAtomic<uint64_t> yields;

    /**
     * The event base this connection is bound to
     */
    event_base *base;

    /**
     * The current privilege context
     */
    cb::rbac::PrivilegeContext privilegeContext{cb::sasl::Domain::Local};

    /**
     * The SASL object used to do sasl authentication
     */
    cb::sasl::server::ServerContext sasl_conn;

    /** Is this a system internal connection */
    bool internal{false};

    /** Is the connection authenticated or not */
    bool authenticated{false};

    /** The username authenticated as */
    std::string username{"unknown"};

    /** The domain where the user is defined */
    cb::sasl::Domain domain{cb::sasl::Domain::Local};

    /** The description of the connection */
    std::string description;

    /** Is tcp nodelay enabled or not? */
    bool nodelay{false};

    /** number of references to the object */
    uint8_t refcount{0};

    /** Pointer to the thread object serving this connection */
    FrontEndThread& thread;

    /** Listening port that creates this connection instance */
    const in_port_t parent_port{0};

    /**
     * The index of the connected bucket
     */
    std::atomic_int bucketIndex{0};

    /** Name of the peer if known */
    const std::string peername;

    /** Name of the local socket if known */
    const std::string sockname;

    /**
     * The connections' priority.
     * atomic to allow read (from DCP stats) without acquiring any
     * additional locks (priority should rarely change).
     */
    std::atomic<Priority> priority{Priority::Medium};

    /** The cluster map revision used by this client */
    int clustermap_revno{-2};

    /**
     * Is XERROR supported for this connection or not (or should we just
     * silently disconnect the client)
     */
    bool xerror_support{false};

    /**
     * Is COLLECTIONS supported for this connection or not. Collection aware
     * clients are allowed to encode operations to occur against their defined
     * collections or the legacy default collection (and receive new errors).
     * Collection aware clients also see mutations/deletions for all collection
     * if they are subscribed to DCP.
     * Collections unaware clients can only target operations at the legacy
     * default collection and receive no new errors. They also only ever see
     * default collection mutations/deletions etc... when subscribed to DCP.
     */
    bool collections_support{false};

    /**
     * Is duplex mode supported by this client? (do the server allow sending
     * commands)
     */
    bool duplex_support{false};

    std::atomic_bool cccp{false};

    bool allow_unordered_execution{false};

    std::queue<std::unique_ptr<ServerEvent>> server_events;

    /**
     * The total time this connection been on the CPU
     */
    std::chrono::nanoseconds total_cpu_time = std::chrono::nanoseconds::zero();
    /**
     * The shortest time this connection was occupying the thread
     */
    std::chrono::nanoseconds min_sched_time = std::chrono::nanoseconds::max();
    /**
     * The longest time this connection was occupying the thread
     */
    std::chrono::nanoseconds max_sched_time = std::chrono::nanoseconds::zero();

    /**
     * The name of the client provided to us by hello
     */
    std::array<char, MaxSavedAgentName> agentName{};

    /**
     * The connection id as specified by the client.
     *
     * The connection UUID is defined to be a string of 33 characters
     * (two 8 byte integers separated with a /). To ease the printout
     * of the string we allocate room for the termination character.
     */
    std::array<char, MaxSavedConnectionId> connectionId{};

    /**
     * The state machine we're currently using
     */
    StateMachine stateMachine;

    /** Is this connection used by a DCP connection? */
    bool dcp = false;

    /** Is this DCP channel XAttrAware */
    bool dcpXattrAware = false;

    /** Shuld values be stripped off? */
    bool dcpNoValue = false;

    /** Is Tracing enabled for this connection? */
    bool tracingEnabled = false;

    /** Should DCP replicate the time a delete was created? */
    bool dcpDeleteTimeEnabled = false;

    /** The maximum requests we can process in a worker thread timeslice */
    int max_reqs_per_event;

    /**
     * number of events this connection can process in a single worker
     * thread timeslice
     */
    int numEvents = 0;

    // Members related to libevent
    struct EventDeleter {
        void operator()(bufferevent* ev);
    };

public:
    /**
     * The bufferevent is currently public while we're figuring out the need
     * for it. It'll be refactored to be made private in one of the following
     * commits
     *
     * @todo make sure this is protected
     */
    std::unique_ptr<bufferevent, EventDeleter> bev;

protected:
    /**
     * If the client enabled the mutation seqno feature each mutation
     * command will return the vbucket UUID and sequence number for the
     * mutation.
     */
    bool supports_mutation_extras = false;

    // Total number of bytes received on the network
    size_t totalRecv = 0;
    // Total number of bytes sent to the network
    size_t totalSend = 0;

    /**
     * The list of commands currently being processed. Currently we
     * only use a single entry in this vector (and always reuse that
     * object for all commands), but when the client tries to
     * enable unordered execution we may operate with multiple
     * commands at the same time and they're all stored in this
     * vector)
     */
    std::vector<std::unique_ptr<Cookie>> cookies;

    Datatype datatype;

    /**
     * It is possible to disable the SASL authentication for some
     * connections after they've been established.
     */
    bool saslAuthEnabled = true;

    /**
     * The SSL context object used to create the ssl instance for this
     * connection (contains the certificates and attributes etc)
     */
    SSL_CTX* server_ctx = nullptr;

    /**
     * The actual SSL handle used by libevent for the SSL communication
     */
    SSL* client_ctx = nullptr;

    struct SendQueueInfo {
        std::chrono::steady_clock::time_point last{};
        size_t size{};
        bool term{false};
    } sendQueueInfo;

public:
    /**
     * Given that we "ack" the writing once we drain the write buffer in
     * memcached we need an extra state variable to make sure that we don't
     * kill the connection object before the data is sent over the wire
     * (in the case where we want to send an error and shut down the connection)
     *
     * @todo This state variable is temporary and will be eliminated in
     * the following patch series when we stop using our internal read
     * and write buffer.
     */
    bool havePendingData() const;

    /**
     * Get the number of bytes stuck in the send queue
     *
     * @return
     */
    size_t getSendQueueSize() const;

protected:
    static void read_callback(bufferevent*, void* ctx);
    static void write_callback(bufferevent*, void* ctx);
    static void event_callback(bufferevent*, short event, void* ctx);
    /**
     * The initial read callback for SSL connections and perform
     * client certificate verification, authentication and authorization
     * if configured. When the action is performed we'll switch over to
     * the standard read callback.
     */
    static void ssl_read_callback(bufferevent*, void* ctx);
};

/**
 * Convert a priority to a textual representation
 */
std::string to_string(Connection::Priority priority);
