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
#include "resource_allocation_domain.h"
#include "sendbuffer.h"
#include "stats.h"

#include <cbsasl/server.h>
#include <daemon/protocol/mcbp/command_context.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/connection_iface.h>
#include <memcached/dcp.h>
#include <memcached/openssl.h>
#include <memcached/rbac.h>
#include <nlohmann/json.hpp>
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

enum class ClustermapChangeNotification : uint8_t { None, Brief, Full };

/**
 * The structure representing a connection in memcached.
 */
class Connection : public ConnectionIface, public DcpMessageProducersIface {
public:
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

    /// Factory method to create a new Connection structure
    static std::unique_ptr<Connection> create(
            SOCKET sfd,
            FrontEndThread& thr,
            std::shared_ptr<ListeningPort> descr);

    enum class Type : uint8_t { Normal, Producer, Consumer };
    Connection(const Connection&) = delete;

    ~Connection() override;

    /**
     * Return an identifier for this connection. To be backwards compatible
     * this is the socket filedescriptor (or the socket handle casted to an
     * unsigned integer on windows).
     */
    uint32_t getId() const {
        return uint32_t(socketDescriptor);
    }

    const nlohmann::json& getDescription() const override {
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

    /// Is this connection connected to the system port or not
    bool isConnectedToSystemPort() const;

    /**
     * Reset throttled cookies for the connection.
     *
     * Used during bucket deletion to get rid of any throttled operations we
     * haven't started processing yet.
     */
    void resetThrottledCookies();

    /// Is the connection representing a system internal user
    bool isInternal() const {
        return getUser().is_internal();
    }

    bool isAuthenticated() const {
        return user.has_value();
    }

    void setAuthenticated(cb::rbac::UserIdent ui);

    const cb::rbac::UserIdent& getUser() const override;

    ConnectionPriority getPriority() const override {
        return priority.load();
    }

    /**
     * Restart the authentication (this clears all of the authentication
     * data...)
     */
    void restartAuthentication();

    void setPriority(ConnectionPriority value) override;

    /**
     * Create a JSON representation of the members of the connection
     */
    nlohmann::json to_json() const;

    /**
     * Create a JSON representation of the members of the connection that
     * store TCP-related details of this connection
     */
    nlohmann::json to_json_tcp() const;

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

    /**
     * Get the command context to use for checking access.
     *
     * @return The privilege context object to use, or {} if no access
     *         exists for the user at all.
     */
    std::shared_ptr<cb::rbac::PrivilegeContext> getPrivilegeContext();

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

    DocKeyView makeDocKey(cb::const_byte_buffer key) const {
        return {key.data(),
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

    ClustermapChangeNotification getClustermapChangeNotification() const {
        return cccp;
    }

    void setClustermapChangeNotification(ClustermapChangeNotification type) {
        cccp = type;
    }

    bool allowUnorderedExecution() const {
        return allow_unordered_execution;
    }

    void setAllowUnorderedExecution(bool enable) {
        allow_unordered_execution = enable;
    }

    void setReportUnitUsage(bool enable) {
        report_unit_usage = enable;
    }

    bool isReportUnitUsage() const {
        return report_unit_usage;
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

    void setDcpFlowControlBufferSize(std::size_t size) override;

    /**
     * Copy the provided data to the end of the output stream.
     *
     * @param data The data to send
     * @throws std::bad_alloc if we failed to insert the data into the output
     *                        stream.
     */
    virtual void copyToOutputStream(std::string_view data) = 0;

    /**
     * Copy the provided data to the end of the output stream.
     *
     * @param data The data to send
     * @throws std::bad_alloc if we failed to insert the data into the output
     *                        stream.
     */
    virtual void copyToOutputStream(gsl::span<std::string_view> data) = 0;

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
    virtual void chainDataToOutputStream(
            std::unique_ptr<SendBuffer> buffer) = 0;

    /// Do the underlying implementation used by this backed support using
    /// sendfile(2) to transfer data between two file descriptors without
    /// copying the data via userspace. Given that it is only our bufferevent
    /// backend supporting this (and for !TLS) we save ourself some typing
    /// by not making it a pure virtual
    virtual bool isSendfileSupported() const {
        return false;
    }

    /// Send length bytes starting from the provided offset of the file
    /// descriptor fd to the other end. Given that it is only our bufferevent
    /// backend supporting this (and for !TLS) we save ourself some typing
    /// by not making it a pure virtual
    virtual cb::engine_errc sendFile(int fd, off_t offset, off_t length) {
        return cb::engine_errc::not_supported;
    }

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

    cb::mcbp::Datatype getEnabledDatatypes(cb::mcbp::Datatype dtype) const {
        return cb::mcbp::Datatype{
                datatypeFilter.getIntersection(uint8_t(dtype))};
    }

    /**
     * @return true if the all of the dtype datatypes are all enabled
     */
    bool isDatatypeEnabled(protocol_binary_datatype_t dtype) const {
        bool rv = datatypeFilter.isEnabled(dtype);

        // If the bucket has disabled xattr, then we must reflect that in the
        // returned value
        if (rv && cb::mcbp::datatype::is_xattr(dtype) &&
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
     * Try to authenticate the provided user found in an X.509 certificate
     * by trying to create a Couchbase user context for the provided
     * username. Normal users authenticated via an X.509 certificate can
     * no longer authenticate via SASL (to change their identity).
     * The Couchbase internal clients may however do so (they use the
     * internal user "unknown" initially (with no privileges) and
     * later SASL authenticate as their real users.
     *
     * @param userName the username found in the X.509 certificate
     * @param cipherName the cipher in use (for logging only)
     *
     * @return true if username has been linked to RBAC
     */
    bool tryAuthUserFromX509Cert(std::string_view userName,
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
    virtual bool isPacketAvailable() const = 0;

    /**
     * Get all of the available bytes (up to 1k) in the input stream in
     * a continuous byte buffer.
     */
    virtual cb::const_byte_buffer getAvailableBytes() const = 0;

    /**
     * Is SASL disabled for this connection or not? (connection authenticated
     * with SSL certificates will disable the possibility re-authenticate over
     * SASL)
     */
    bool isSaslAuthEnabled() const {
        return saslAuthEnabled;
    }

    /// Disable the ability for the connected client to perform SASL AUTH
    void disableSaslAuth() {
        saslAuthEnabled = false;
    }

    /// Get the list of SASL mechanisms this connection may use
    std::string getSaslMechanisms() const;

    bool selectedBucketIsXattrEnabled() const;

    /// Initiate shutdown of the connection
    void shutdown();

    /**
     * Set the name of the connected agent
     */
    void setAgentName(std::string_view name);
    std::string_view getAgentName() const;

    /**
     * Get the Identifier specified for this connection.
     */
    const std::array<char, MaxSavedConnectionId>& getConnectionId() const {
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
     * @return true send was successful, false if not (connection disconnect)
     * @throws std::runtime_error if the sendbuffers payload don't match the
     *                   provided value.
     */
    bool sendResponse(Cookie& cookie,
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
     * By default a new callback is only scheduled if there isn't data
     * in the send queue as that will generate a callback once the
     * data is in the send queue. In some cases we would want the
     * callback to be scheduled anyway (for instance when trying to shut
     * down a connection which isn't reading its socket). To do so
     * set force to true
     *
     * @param force force a new EV_READ callback to be triggered
     */
    virtual void triggerCallback(bool force = false) = 0;

    void scheduleDcpStep() override;

    // Implementation of DcpMessageProducersIface interface
    // //////////////////////

    cb::engine_errc get_failover_log(uint32_t opaque, Vbid vbucket) override;

    cb::engine_errc stream_req(uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpAddStreamFlag flags,
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
                           cb::mcbp::request::DcpSnapshotMarkerFlag flags,
                           std::optional<uint64_t> high_completed_seqno,
                           std::optional<uint64_t> high_prepared_seqno,
                           std::optional<uint64_t> max_visible_seqno,
                           std::optional<uint64_t> purge_seqno,
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
                           const DocKeyView& key,
                           uint64_t prepare_seqno,
                           uint64_t commit_seqno) override;

    cb::engine_errc abort(uint32_t opaque,
                          Vbid vbucket,
                          const DocKeyView& key,
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

    /// Get the handle to the SASL server context
    auto* getSaslServerContext() const {
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
     * @param scheduled The time point being added to the notification queue
     */
    void processNotifiedCookie(Cookie& cookie,
                               cb::engine_errc status,
                               std::chrono::steady_clock::time_point scheduled);

    /// Notify that a command was executed (needed for command rate limiting)
    void commandExecuted(Cookie& cookie);

    /// Is this connection not to be metered?
    bool isUnmetered() const {
        return !isSubjectToMetering();
    }

    bool isSubjectToMetering() const {
        return subject_to_metering.load(std::memory_order_acquire);
    }

    /// Is this connection not to be throttled?
    bool isUnthrottled() const {
        return !isSubjectToThrottling();
    }

    bool isSubjectToThrottling() const {
        return subject_to_throttling.load(std::memory_order_acquire);
    }

    bool isNodeSupervisor() const {
        return node_supervisor.load(std::memory_order_acquire);
    }

    /// Clear the DCP throttle for this connection to allow it to progress
    void resumeThrottledDcpStream();

    bool isNonBlockingThrottlingMode() const {
        return non_blocking_throttling_mode.load(std::memory_order_acquire);
    }

    void setNonBlockingThrottlingMode(bool enable) {
        non_blocking_throttling_mode.store(enable, std::memory_order_release);
    }

    /**
     * Try to initiate shutdown of the connection if it is idle
     *
     * @param reason the reason for disconnecting (used in logging)
     * @param log set to true to log the shutdown
     * @return true if shutdown was initiated
     */
    bool maybeInitiateShutdown(std::string_view reason, bool log);

    /// Log if this connection have been stuck in shutdown state
    void reportIfStuckInShutdown(
            const std::chrono::steady_clock::time_point& tp);

    /// Do the client deal with cluster map deduplication in NMVB
    bool dedupeNmvbMaps() const {
        return dedupe_nmvb_maps;
    }

    /// Set if the client deals with cluster map deduplication in NMVB
    void setDedupeNmvbMaps(bool val) {
        dedupe_nmvb_maps = val;
    }

    /// Do the client honor the Snappy flag on all kind of response
    /// payloads
    bool supportsSnappyEverywhere() const {
        return snappy_everywhere;
    }

    /// Set if the client honors the Snappy flag on all kinds of
    /// responses
    void setSupportsSnappyEverywhere(bool val) {
        snappy_everywhere = val;
    }

    /**
     *  Check to see if the authenticated user may access the named bucket
     *
     * @param bucket The name of the bucket to access
     * @return true if the user have (some) access to the bucket, false
     * otherwise
     */
    bool mayAccessBucket(std::string_view bucket) const;

    /**
     * Create a privilege context for the current user
     *
     * @param bucket the bucket (empty indicates "no bucket") to access
     * @return The privilege context to use
     */
    cb::rbac::PrivilegeContext createContext(std::string_view bucket) const;

    /// Get the time point for when the current timeslice ends (cookies
    /// should try to yield execution after this point)
    std::chrono::steady_clock::time_point getCurrentTimesliceEnd() const {
        return current_timeslice_end;
    }

    /// Set the lifetime for the auth context
    void setAuthContextLifetime(
            std::optional<std::chrono::system_clock::time_point> begin,
            std::optional<std::chrono::system_clock::time_point> end);

    /// Set the user entry provided by the token
    void setTokenProvidedUserEntry(std::unique_ptr<cb::rbac::UserEntry> entry) {
        tokenProvidedUserEntry = std::move(entry);
    }

protected:
    /// Protected constructor so that it may only be used from create();
    Connection(SOCKET sfd,
               FrontEndThread& thr,
               std::shared_ptr<ListeningPort> descr);

    /**
     * Protected constructor so that it may only be used by MockSubclasses
     */
    explicit Connection(FrontEndThread& thr);

    /// connected to a TLS enabled port or not
    bool isTlsEnabled() const;

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
     * Callback called right after the TLS handshake to allow us to look
     * at the provided certificate and possibly authenticate the connection
     *
     * @param ssl_st The SSL structure used by the underlying SSL library
     */
    void onTlsConnect(const SSL* ssl_st);

    /**
     * Get the next packet available in the stream.
     *
     * The returned reference goes directly into the underlying IO libraries
     * input buffer(s), so the caller should NOT keep the reference around.
     * It gets invalidated after a call to drainInputPipe (or a return
     * into the event framework)
     *
     * @return the next packet
     * @throws std::runtime_error if the packet isn't available
     */
    virtual const cb::mcbp::Header& getPacket() const = 0;

    /// Move to the next packet in the pipe
    virtual void nextPacket() = 0;

    /**
     * Update the description string for the connection. This
     * method should be called every time the authentication data
     * (or the sockname/peername) changes
     */
    void updateDescription();

    // Shared DCP_DELETION write function for the v1/v2 commands.
    cb::engine_errc deletionInner(const ItemIface& item,
                                  cb::const_byte_buffer packet,
                                  const DocKeyView& key);

    /**
     * Add the provided packet to the send pipe for the connection
     */
    cb::engine_errc add_packet_to_send_pipe(cb::const_byte_buffer packet);

    /**
     * Disable read event for this connection (we won't get notified if
     * more data arrives on the socket).
     */
    virtual void disableReadEvent() = 0;

    /**
     * Enable read event for this connection (cause the read callback to
     * be triggered once there is data on the socket).
     */
    virtual void enableReadEvent() = 0;

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

    /// Update the privilege context and drop all of the previously dropped
    /// privileges and update any cached variables
    void updatePrivilegeContext();

    // Accumulate duration the send queue has been blocked.
    void processBlockedSendQueue(
            const std::chrono::steady_clock::time_point& now);

    // Reset blocked send queue if it's no longer full.
    void updateBlockedSendQueue(
            const std::chrono::steady_clock::time_point& now);

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
    std::shared_ptr<cb::rbac::PrivilegeContext> privilegeContext;

    /// The current dropped privilege set
    cb::rbac::PrivilegeMask droppedPrivileges;

    /// The (authenticated) user for the connection.
    std::optional<cb::rbac::UserIdent> user;

    /// The authentication context may have restrictions on when it is valid
    struct AuthContextLifetime {
        std::optional<std::chrono::steady_clock::time_point> begin;
        std::optional<std::chrono::steady_clock::time_point> end;
        bool isStale(const std::chrono::steady_clock::time_point now) const {
            return (end.has_value() && *end < now) ||
                   (begin.has_value() && *begin > now);
        }
    } authContextLifetime;

    /// The description of the connection
    nlohmann::json description;

    /// The reason why the session was terminated
    std::string terminationReason;

    struct SendQueueInfo {
        std::chrono::steady_clock::time_point last{};
        size_t size{};
        bool term{false};
    } sendQueueInfo;

    /// Last usage of the connection
    std::chrono::steady_clock::time_point last_used_timestamp{};
    /// When shutdown was initiated
    std::optional<std::chrono::steady_clock::time_point> shutdown_initiated{};
    /// When the next time we should log for a pending close
    std::chrono::steady_clock::time_point pending_close_next_log{};

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

    /// A user entry provided in the token when using JWT
    std::unique_ptr<cb::rbac::UserEntry> tokenProvidedUserEntry;

    /// The number of times we've been backing off and yielding
    /// to allow other threads to run
    cb::RelaxedAtomic<uint64_t> yields;

    /// The end time of the current timeslice used for command
    std::chrono::steady_clock::time_point current_timeslice_end;

    /// The total time this connection been on the CPU
    std::chrono::nanoseconds total_cpu_time = std::chrono::nanoseconds::zero();
    /// The shortest time this connection was occupying the thread
    std::chrono::nanoseconds min_sched_time = std::chrono::nanoseconds::max();
    /// The longest time this connection was occupying the thread
    std::chrono::nanoseconds max_sched_time = std::chrono::nanoseconds::zero();

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

    /// number of references to the object (set to 1 during creation as the
    /// creator has a reference)
    uint8_t refcount{1};

    // Duration representing the total time the connection has spent blocked due
    // to a full send queue.
    std::chrono::nanoseconds blockedOnFullSendQueueDuration{};

    // Optional timestamp indicating the time a connection has been blocked due
    // to a full send queue.
    std::optional<std::chrono::steady_clock::time_point> blockedOnFullSendQueue;

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

    std::atomic<ClustermapChangeNotification> cccp{
            ClustermapChangeNotification::None};
    std::atomic_bool dedupe_nmvb_maps{false};
    std::atomic_bool snappy_everywhere{false};
    bool allow_unordered_execution{false};

    bool report_unit_usage{false};

    std::atomic_bool subject_to_throttling{true};
    std::atomic_bool subject_to_metering{true};
    std::atomic_bool node_supervisor{false};
    std::atomic_bool non_blocking_throttling_mode{false};

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

    /// is the sdk registered
    bool registeredSdk = false;

    /// The type of connection this is
    Type type = Type::Normal;

    /// Is this DCP Stream throttled or not
    bool dcpStreamThrottled = false;

    /// The size of the current Dcp flow control buffer (0 = unlimited)
    std::size_t dcpFlowControlBufferSize = 0;

    /// The memory domain allocations for DCP step was made from
    ResourceAllocationDomain dcpResourceAllocationDomain =
            ResourceAllocationDomain::Bucket;

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
    virtual size_t getSendQueueSize() const = 0;

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
     * Helper method for executeCommandPipeline to deal with the situation
     * where the cookie should be throttled.
     *
     * @param cookie The cookie to handle
     * @return true if we should stop accepting more commands
     */
    bool handleThrottleCommand(Cookie& cookie) const;
    /**
     * Helper method for executeCommandPipeline to deal with the situation
     * where the cookie should be rejected due to validation error or stale
     * authentication
     *
     * @param cookie The cookie to handle
     * @param status The status returned by the validator
     * @param auth_stale Set to true if the authentication is stale (currently
     *                   used in token auth)
     */
    static void handleRejectCommand(Cookie& cookie,
                                    cb::mcbp::Status status,
                                    bool auth_stale);

    /// Try to make any progress on a DCP stream by calling step()
    void tryToProgressDcpStream();

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
};

void to_json(nlohmann::json& json, const Connection& connection);
