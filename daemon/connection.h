/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "cluster_config.h"
#include "datatype_filter.h"
#include "sendbuffer.h"
#include "ssl_utils.h"
#include "stats.h"

#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <daemon/protocol/mcbp/command_context.h>
#include <libevent/utilities.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dcp.h>
#include <memcached/openssl.h>
#include <memcached/rbac.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/socket.h>

#include <array>
#include <chrono>
#include <deque>
#include <memory>
#include <queue>
#include <string>

class Bucket;
class Cookie;
class ListeningPort;
struct EngineIface;
struct FrontEndThread;
class SendBuffer;

namespace cb::mcbp {
class Header;
} // namespace cb::mcbp

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
class Connection : public DcpMessageProducersIface {
public:
    enum class Type : uint8_t { Normal, Producer, Consumer };
    Connection(const Connection&) = delete;

    Connection(SOCKET sfd,
               FrontEndThread& thr,
               std::shared_ptr<ListeningPort> descr,
               uniqueSslPtr sslStructure);

    ~Connection() override;

    /**
     * Return an identifier for this connection. To be backwards compatible
     * this is the socket filedescriptor (or the socket handle casted to an
     * unsigned integer on windows).
     */
    uint32_t getId() const {
        return uint32_t(socketDescriptor);
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

    bool isAuthenticated() const {
        return authenticated;
    }

    void setAuthenticated(bool authenticated,
                          bool internal = false,
                          cb::rbac::UserIdent ui = {"unknown",
                                                    cb::sasl::Domain::Local});

    ConnectionPriority getPriority() const {
        return priority.load();
    }

    void setPriority(ConnectionPriority priority);

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
    const cb::rbac::UserIdent& getUser() const {
        return user;
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

    in_port_t getParentPort() const;

    /**
     * Get the command context to use for checking access.
     *
     * @return The privilege context object to use, or {} if no access
     *         exists for the user at all.
     */
    cb::rbac::PrivilegeContext getPrivilegeContext();

    /**
     * Try to drop the specified privilege from the current context
     *
     * @param privilege the privilege to drop
     * @return The appropriate error code to return back to the client
     */
    cb::engine_errc dropPrivilege(cb::rbac::Privilege privilege);

    unsigned int getBucketIndex() const {
        return bucketIndex.load(std::memory_order_relaxed);
    }

    void setBucketIndex(int index, Cookie* cookie = nullptr);

    Bucket& getBucket() const;

    EngineIface& getBucketEngine() const;

    ClustermapVersion getPushedClustermapRevno() const {
        return pushed_clustermap;
    }

    void setPushedClustermapRevno(ClustermapVersion version) {
        pushed_clustermap = version;
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

    void setReportComputeUnitUsage(bool enable) {
        report_compute_unit_usage = enable;
    }

    bool isReportComputeUnitUsage() const {
        return report_compute_unit_usage;
    }

    /**
     * Remap the current error code
     *
     * The method modifies the input code and returns the mapped value
     * (to make the function a bit easier to use).
     *
     * Depending on which features the client have enabled the method
     * may either just return the input value, map it to a different value
     * (like cb::engine_errc::disconnect if the client hasn't enabled the
     * extened error codes).
     *
     * @param code The code to map (will be changed on return)
     * @return the mapped value.
     */
    cb::engine_errc remapErrorCode(cb::engine_errc code);

    /// Revaluate if the parent port is still valid or not (and if
    /// we should shut down the connection or not).
    void reEvaluateParentPort();

    /// Iterate over all cookies and try to reevaluate the throttled
    /// commands
    bool reEvaluateThrottledCookies();

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

    const std::string& getTerminationReason() const {
        return terminationReason;
    }

    /// Set the reason for why the connection is being shut down
    void setTerminationReason(std::string reason);

    bool isDCP() const {
        return dcpConnHandlerIface.load() != nullptr;
    }

    bool isDcpXattrAware() const {
        return dcpXattrAware;
    }

    void setDcpXattrAware(bool enable) {
        dcpXattrAware = enable;
    }

    bool isDcpDeletedUserXattr() const {
        return dcpDeletedUserXattr;
    }

    void setDcpDeletedUserXattr(bool value) {
        dcpDeletedUserXattr = value;
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

    /// Set the size of the flow control buffer used by this producer
    void setDcpFlowControlBufferSize(std::size_t size);

    /**
     * Copy the provided data to the end of the output stream.
     *
     * @param data The data to send
     * @throws std::bad_alloc if we failed to insert the data into the output
     *                        stream.
     */
    void copyToOutputStream(std::string_view data1);

    /**
     * Copy the provided data to the end of the output stream.
     *
     * @param data The data to send
     * @throws std::bad_alloc if we failed to insert the data into the output
     *                        stream.
     */
    void copyToOutputStream(gsl::span<std::string_view> data);

    /**
     * Copy the provided data to the end of the output stream.
     *
     * This is a convenience function around the span one to avoid having
     * to build up the array everywhere we want to use the function. See
     * https://stackoverflow.com/questions/33874772/why-cant-i-construct-a-gslspan-with-a-brace-enclosed-initializer-list
     * for related problems.
     *
     * @param data[1,5] the data to send
     * @throws std::bad_alloc if we failed to insert the data into the output
     *                        stream.
     */
    void copyToOutputStream(std::string_view data1, std::string_view data2) {
        std::array<std::string_view, 2> data{{data1, data2}};
        copyToOutputStream(data);
    }

    void copyToOutputStream(std::string_view data1,
                            std::string_view data2,
                            std::string_view data3) {
        std::array<std::string_view, 3> data{{data1, data2, data3}};
        copyToOutputStream(data);
    }

    void copyToOutputStream(std::string_view data1,
                            std::string_view data2,
                            std::string_view data3,
                            std::string_view data4) {
        std::array<std::string_view, 4> data{{data1, data2, data3, data4}};
        copyToOutputStream(data);
    }

    void copyToOutputStream(std::string_view data1,
                            std::string_view data2,
                            std::string_view data3,
                            std::string_view data4,
                            std::string_view data5) {
        std::array<std::string_view, 5> data{
                {data1, data2, data3, data4, data5}};
        copyToOutputStream(data);
    }

    /// Wrapper function to deal with byte buffers during the transition over
    /// to only use char buffers
    void copyToOutputStream(cb::const_byte_buffer data) {
        copyToOutputStream(std::string_view{
                reinterpret_cast<const char*>(data.data()), data.size()});
    }

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
        datatypeFilter.enable(feature);
    }

    /**
     * Disable all the datatypes
     */
    void disableAllDatatypes() {
        datatypeFilter.disableAll();
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
        return datatypeFilter.getIntersection(dtype);
    }

    /**
     * @return true if the all of the dtype datatypes are all enabled
     */
    bool isDatatypeEnabled(protocol_binary_datatype_t dtype) const {
        bool rv = datatypeFilter.isEnabled(dtype);

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
        return datatypeFilter.isSnappyEnabled();
    }

    /**
     * @return true if the XATTR datatype is enabled
     */
    bool isXattrEnabled() const {
        return datatypeFilter.isXattrEnabled();
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
        return ssl;
    }

    /**
     * Try to find RBAC user from the client ssl cert
     *
     * @return true if username has been linked to RBAC or ssl cert was not
     * presented by the client.
     */
    bool tryAuthFromSslCert(const std::string& userName,
                            std::string_view cipherName);

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
    void shutdown();

    DcpConnHandlerIface* getDcpConnHandlerIface() const {
        return dcpConnHandlerIface.load(std::memory_order_acquire);
    }

    void setDcpConnHandlerIface(DcpConnHandlerIface* handler) {
        dcpConnHandlerIface.store(handler, std::memory_order_release);
    }

    /**
     * Set the name of the connected agent
     */
    void setAgentName(std::string_view name);

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
    void setConnectionId(std::string_view uuid);

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
                             std::string_view extras,
                             std::string_view key,
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
                      std::string_view extras,
                      std::string_view key,
                      std::string_view value,
                      uint8_t datatype,
                      std::unique_ptr<SendBuffer> sendbuffer);

    /**
     * Trigger a callback from libevent for the connection at some time
     * in the future (as part of the event dispatch loop) so that the
     * connection may continue its command execution.
     */
    void triggerCallback();

    /// Check if DCP should use the write buffer for the message or if it
    /// should use an IOVector to do so
    bool dcpUseWriteBuffer(size_t total) const;

    // Implementation of DcpMessageProducersIface interface
    // //////////////////////

    cb::engine_errc get_failover_log(uint32_t opaque, Vbid vbucket) override;

    cb::engine_errc stream_req(uint32_t opaque,
                               Vbid vbucket,
                               uint32_t flags,
                               uint64_t start_seqno,
                               uint64_t end_seqno,
                               uint64_t vbucket_uuid,
                               uint64_t snap_start_seqno,
                               uint64_t snap_end_seqno,
                               const std::string& request_value) override;

    cb::engine_errc add_stream_rsp(uint32_t opaque,
                                   uint32_t stream_opaque,
                                   cb::mcbp::Status status) override;

    cb::engine_errc marker_rsp(uint32_t opaque,
                               cb::mcbp::Status status) override;

    cb::engine_errc set_vbucket_state_rsp(uint32_t opaque,
                                          cb::mcbp::Status status) override;

    cb::engine_errc stream_end(uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpStreamEndStatus status,
                               cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc marker(uint32_t opaque,
                           Vbid vbucket,
                           uint64_t start_seqno,
                           uint64_t end_seqno,
                           uint32_t flags,
                           std::optional<uint64_t> high_completed_seqno,
                           std::optional<uint64_t> max_visible_seqno,
                           std::optional<uint64_t> timestamp,
                           cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc mutation(uint32_t opaque,
                             cb::unique_item_ptr itm,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             uint32_t lock_time,
                             uint8_t nru,
                             cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc deletion(uint32_t opaque,
                             cb::unique_item_ptr itm,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc deletion_v2(uint32_t opaque,
                                cb::unique_item_ptr itm,
                                Vbid vbucket,
                                uint64_t by_seqno,
                                uint64_t rev_seqno,
                                uint32_t delete_time,
                                cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc expiration(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t delete_time,
                               cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc set_vbucket_state(uint32_t opaque,
                                      Vbid vbucket,
                                      vbucket_state_t state) override;
    cb::engine_errc noop(uint32_t opaque) override;

    cb::engine_errc buffer_acknowledgement(uint32_t opaque,
                                           Vbid vbucket,
                                           uint32_t buffer_bytes) override;
    cb::engine_errc control(uint32_t opaque,
                            std::string_view key,
                            std::string_view value) override;

    cb::engine_errc get_error_map(uint32_t opaque, uint16_t version) override;

    cb::engine_errc system_event(uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData,
                                 cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc prepare(uint32_t opaque,
                            cb::unique_item_ptr itm,
                            Vbid vbucket,
                            uint64_t by_seqno,
                            uint64_t rev_seqno,
                            uint32_t lock_time,
                            uint8_t nru,
                            DocumentState document_state,
                            cb::durability::Level level) override;

    cb::engine_errc seqno_acknowledged(uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t prepared_seqno) override;

    cb::engine_errc commit(uint32_t opaque,
                           Vbid vbucket,
                           const DocKey& key,
                           uint64_t prepare_seqno,
                           uint64_t commit_seqno) override;

    cb::engine_errc abort(uint32_t opaque,
                          Vbid vbucket,
                          const DocKey& key,
                          uint64_t prepared_seqno,
                          uint64_t abort_seqno) override;

    cb::engine_errc oso_snapshot(uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags,
                                 cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc seqno_advanced(uint32_t opaque,
                                   Vbid vbucket,
                                   uint64_t seqno,
                                   cb::mcbp::DcpStreamId sid) override;

    /// Create the SASL server context object to use for SASL authentication
    void createSaslServerContext() {
        saslServerContext = std::make_unique<cb::sasl::server::ServerContext>();
    }

    /// Get the a handle to the SASL server context
    cb::sasl::server::ServerContext* getSaslServerContext() const {
        return saslServerContext.get();
    }

    /// Release the allocate SASL server context.
    void releaseSaslServerContext() {
        saslServerContext.reset();
    }

    void setType(Type value) {
        type = value;
    }

    Type getType() const {
        return type;
    }

    /**
     * Process a blocked cookie
     *
     * @param cookie The cookie to resume execution of (must be blocked)
     * @param status The status the background thread set for the cookie
     */
    void processNotifiedCookie(Cookie& cookie, cb::engine_errc status);

    /**
     * Process a blocked cookie but in this case the caller is indicating the
     * execution is complete, there is no re-entry into the engine
     *
     * @param cookie The cookie that completed execution (must be blocked)
     */
    void processCompletedCookie(Cookie& cookie);

    /// Notify that a command was executed (needed for command rate limiting)
    void commandExecuted(Cookie& cookie);

protected:
    /**
     * Protected constructor so that it may only be used by MockSubclasses
     */
    explicit Connection(FrontEndThread& thr);

    /**
     * Close the connection. If there is any references to the connection
     * or the cookies we'll enter the "pending close" state to wait for
     * these operations to complete before changing state to immediate
     * close.
     */
    void close();

    /**
     * fire ON_DISCONNECT for all of the cookie objects (in case the
     * underlying engine keeps track of any of them)
     */
    void propagateDisconnect() const;

    /**
     * Update the description string for the connection. This
     * method should be called every time the authentication data
     * (or the sockname/peername) changes
     */
    void updateDescription();

    // Shared DCP_DELETION write function for the v1/v2 commands.
    cb::engine_errc deletionInner(const ItemIface& item,
                                  cb::const_byte_buffer packet,
                                  const DocKey& key);

    /**
     * Add the provided packet to the send pipe for the connection
     */
    cb::engine_errc add_packet_to_send_pipe(cb::const_byte_buffer packet);

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
     * Format the response header into the provided buffer.
     *
     * @param cookie The command to create a response for
     * @param dest The destination buffer
     * @param status The status code
     * @param extras_len The length of the extras section
     * @param key_len The length of the key
     * @param value_len The length of the value
     * @param datatype The datatype for the response
     * @return A view into the destination buffer containing the header
     */
    std::string_view formatResponseHeaders(Cookie& cookie,
                                           cb::char_buffer dest,
                                           cb::mcbp::Status status,
                                           std::size_t extras_len,
                                           std::size_t key_len,
                                           std::size_t value_len,
                                           uint8_t datatype);

    void updateSendBytes(size_t nbytes);
    void updateRecvBytes(size_t nbytes);

    /**
     * The "list" of commands currently being processed. We ALWAYS keep the
     * the first entry in the list (and try to reuse that) due to how DCP
     * works. The engine keeps a reference to the DCP consumer based on
     * the address of the provided cookie. Connections registered for
     * DCP will _ONLY_ use the first entry in the list (and never use OoO)
     *
     * Given that we want to be able to put them in an ordered sequence
     * (we may receive a command we cannot reorder) we want to use a
     * datastructure where we can easily remove an item in the middle
     * (without having to move the rest of the elements as it could be
     * that we want to get rid of more elements as we traverse).
     */
    std::deque<std::unique_ptr<Cookie>> cookies;

    /// The current privilege context
    cb::rbac::PrivilegeContext privilegeContext{cb::sasl::Domain::Local};

    /// The authenticated user
    cb::rbac::UserIdent user{std::string("unknown"), cb::sasl::Domain::Local};

    /// The description of the connection
    std::string description;

    /// Name of the peer if known
    const std::string peername;

    /// Name of the local socket if known
    const std::string sockname;

    /// The reason why the session was terminated
    std::string terminationReason;

    struct SendQueueInfo {
        std::chrono::steady_clock::time_point last{};
        size_t size{};
        bool term{false};
    } sendQueueInfo;

    /// Pointer to the thread object serving this connection
    FrontEndThread& thread;

    /// The description of the listening port which accepted the client
    /// (needed in order to shut down the connection if the administrator
    /// disables the port)
    std::shared_ptr<ListeningPort> listening_port;

    /// The SASL object used to do sasl authentication. It should be created
    /// as part of SASL START, and released UNLESS the underlying mechanism
    /// returns CONTINUE (if it does it should be used for the next call
    /// to STEP).
    std::unique_ptr<cb::sasl::server::ServerContext> saslServerContext;

    /// The number of times we've been backing off and yielding
    /// to allow other threads to run
    cb::RelaxedAtomic<uint64_t> yields;

    /// The total time this connection been on the CPU
    std::chrono::nanoseconds total_cpu_time = std::chrono::nanoseconds::zero();
    /// The shortest time this connection was occupying the thread
    std::chrono::nanoseconds min_sched_time = std::chrono::nanoseconds::max();
    /// The longest time this connection was occupying the thread
    std::chrono::nanoseconds max_sched_time = std::chrono::nanoseconds::zero();

    /// The stored DCP Connection Interface
    std::atomic<DcpConnHandlerIface*> dcpConnHandlerIface{nullptr};

    /// The bufferevent structure for the object
    cb::libevent::unique_bufferevent_ptr bev;
    /// Total number of bytes received on the network
    size_t totalRecv = 0;
    /// Total number of bytes sent to the network
    size_t totalSend = 0;

    /// The maximum requests we can process in a worker thread timeslice
    int max_reqs_per_event;

    /// number of events this connection can process in a single worker
    /// thread timeslice
    int numEvents = 0;

    /// The actual socket descriptor used by this connection
    const SOCKET socketDescriptor;

    /// The index of the connected bucket
    std::atomic<unsigned int> bucketIndex{0};

    /// The cluster map revision used by this client
    ClustermapVersion pushed_clustermap;

    /// Listening port that creates this connection instance
    const in_port_t parent_port{0};

    const bool connectedToSystemPort;

    /// Is this a system internal connection
    bool internal{false};

    /// Is the connection authenticated or not
    bool authenticated{false};

    /// Is tcp nodelay enabled or not?
    bool nodelay{false};

    /// number of references to the object (set to 1 during creation as the
    /// creator has a reference)
    uint8_t refcount{1};

    /**
     * The connections' priority.
     * atomic to allow read (from DCP stats) without acquiring any
     * additional locks (priority should rarely change).
     */
    std::atomic<ConnectionPriority> priority{ConnectionPriority::Medium};


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

    bool report_compute_unit_usage{false};

    /// The name of the client provided to us by hello
    std::array<char, MaxSavedAgentName> agentName{};

    /**
     * The connection id as specified by the client.
     *
     * The connection UUID is defined to be a string of 33 characters
     * (two 8 byte integers separated with a /). To ease the printout
     * of the string we allocate room for the termination character.
     */
    std::array<char, MaxSavedConnectionId> connectionId{};

    /// A class representing the states the connection may be in
    enum class State : int8_t {
        /// The client is running and may accept new commands
        running,
        /// Initiating shutdown of the connection. Depending of the number
        /// of external references to the connection we may go to pending_close
        /// or immediate close
        closing,
        /// Waiting for all of the references to the connections to be released
        pending_close,
        /// NO external references to the connection; may go ahead and kill it
        immediate_close
    };

    /// The current state we're in
    State state{State::running};

    /// Is this DCP channel XAttrAware
    bool dcpXattrAware = false;

    /// Is this DCP channel aware of DeletedUserXattr
    bool dcpDeletedUserXattr = false;

    /// Shuld values be stripped off?
    bool dcpNoValue = false;

    /// Is Tracing enabled for this connection?
    bool tracingEnabled = false;

    /// Should DCP replicate the time a delete was created?
    bool dcpDeleteTimeEnabled = false;

    /**
     * If the client enabled the mutation seqno feature each mutation
     * command will return the vbucket UUID and sequence number for the
     * mutation.
     */
    bool supports_mutation_extras = false;

    /// Filter containing the data types available for the connection
    DatatypeFilter datatypeFilter;

    /**
     * It is possible to disable the SASL authentication for some
     * connections after they've been established.
     */
    bool saslAuthEnabled = true;

    /// The type of connection this is
    Type type = Type::Normal;

    /// The size of the current Dcp flow control buffer (0 = unlimited)
    std::size_t dcpFlowControlBufferSize = 0;

    /**
     * Is this connection over SSL or not
     */
    const bool ssl;

    /**
     * Given that we "ack" the writing once we drain the write buffer in
     * memcached we need an extra state variable to make sure that we don't
     * kill the connection object before the data is sent over the wire
     * (in the case where we want to send an error and shut down the connection)
     *
     * @return true if there is more data to be sent over the wire
     */
    bool havePendingData() const;

    /// Get the number of bytes stuck in the send queue
    size_t getSendQueueSize() const;

    /**
     * Shutdown the connection if the send queue is stuck  (no data transmitted
     * drained from the send queue for a certain period of time).
     *
     * If the send queue is considered to be stuck we'll initiate shutdown
     * of the connection.
     *
     * @param now the current time (to avoid reading the clock)
     */
    void shutdownIfSendQueueStuck(std::chrono::steady_clock::time_point now);

    /**
     * Iterate over all of the existing cookies (commands) and try to call
     * execute() on all of the cookies which isn't blocked in the engine.
     *
     * @return true if there is any blocked commands, false otherwise
     */
    bool processAllReadyCookies();

    /**
     * Execute commands in the pipeline.
     *
     * Continue to execute the commands already started, and if we may start
     * the execution of the next command (out of order execution enabled by
     * the user, _AND_ the next commands allows for reordering) start the
     * execution
     */
    void executeCommandPipeline();

    /**
     * bufferevents calls rw_callback if there is a read or write event
     * for the connection, and event_callback for other events (error ect).
     *
     * The logic for our connection is however the same, so we can use the
     * same callback method for both of them (and make this a member
     * method on the instance rather a static function)
     *
     * @return true if the connection is still being used, false if it
     *              should be deleted
     */
    bool executeCommandsCallback();

    /**
     * Log the exception thrown by either executing the input command or
     * DCP step.
     *
     * @param where Where the exception was thrown (packet execution, DCP step)
     * @param e The exception to log
     */
    void logExecutionException(const std::string_view where,
                               const std::exception& e);

    std::string getOpenSSLErrors();

    /**
     * The callback method called from bufferevent when there is new data
     * available on the socket
     *
     * @param bev the bufferevent structure the event belongs to
     * @param ctx the context registered with the bufferevent (pointer to
     *            the connection object)
     */
    static void read_callback(bufferevent* bev, void* ctx);
    void read_callback();

    /**
     * The callback method called from bufferevent when we're below the write
     * threshold (or all data is sent)
     *
     * @param bev the bufferevent structure the event belongs to
     * @param ctx the context registered with the bufferevent (pointer to
     *            the connection object)
     */
    static void write_callback(bufferevent* bev, void* ctx);
    void write_callback();

    /**
     * The callback method called from bufferevent for "other" callbacks
     *
     * @param bev the bufferevent structure the event belongs to
     * @param event the event type
     * @param ctx the context registered with the bufferevent (pointer to
     *            the connection object)
     */
    static void event_callback(bufferevent* bev, short event, void* ctx);

    /**
     * The initial read callback for SSL connections and perform
     * client certificate verification, authentication and authorization
     * if configured. When the action is performed we'll switch over to
     * the standard read callback.
     */
    static void ssl_read_callback(bufferevent*, void* ctx);
};
