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

#include "datatype.h"
#include "dynamic_buffer.h"
#include "log_macros.h"
#include "settings.h"
#include "ssl_context.h"
#include "statemachine.h"
#include "stats.h"
#include "task.h"

#include <cJSON.h>
#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <daemon/protocol/mcbp/command_context.h>
#include <event.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dcp.h>
#include <memcached/openssl.h>
#include <memcached/rbac.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/sized_buffer.h>
#include <platform/socket.h>

#include <array>
#include <chrono>
#include <memory>
#include <queue>
#include <string>
#include <vector>

struct FrontEndThread;
class Cookie;
class ListeningPort;
class Bucket;
class ServerEvent;

/**
 * Adjust a message header structure by "consuming" nbytes of data.
 *
 * The msghdr structure contains an io-vector of data to send, and
 * by consuming data, we "rebuild" the io-vector by moving the
 * base pointer to the io-vector past all of the fully transferred
 * elements, and move the last iov_base pointer the resulting bytes
 * forward (and reduce the last iov_len the same number of bytes)
 *
 * @param pipe The pipe structure where we may have stored data pointed
 *             to in the io-vector. We need to mark those as consumed
 *             when we skip them in the io-vector.
 * @param m The message header structure to update
 * @param nbytes The number of bytes to skip
 * @return The number of bytes left in the first element in the io-vector
 */
size_t adjust_msghdr(cb::Pipe& pipe, struct msghdr* m, ssize_t nbytes);

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

    Connection(SOCKET sfd, event_base* b, const ListeningPort& ifc);

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
     * @param logbusy set to true if you want to log the connection details
     *                if the connection isn't idle
     * @param workerthead the id of the workerthread (for logging purposes)
     */
    void signalIfIdle(bool logbusy, size_t workerthread);

    /**
     * Terminate the eventloop for the current event base. This method doesn't
     * really fit as a member for the class, but I don't want clients to access
     * the libevent details from outside the class (so I didn't want to make
     * a "getEventBase()" method.
     */
    void eventBaseLoopbreak() {
        event_base_loopbreak(base);
    }

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
    void setInternal(bool internal) {
        Connection::internal = internal;
    }

    /**
     * Update the username to reflect what the user used from the SASL
     * authentication.
     */
    void resetUsernameCache();


    bool isAuthenticated() const {
        return authenticated;
    }

    void setAuthenticated(bool authenticated) {
        Connection::authenticated = authenticated;
        if (authenticated) {
            updateDescription();
            privilegeContext = cb::rbac::createContext(username, "");
        } else {
            resetUsernameCache();
            privilegeContext = cb::rbac::PrivilegeContext{};
        }
    }

    Priority getPriority() const {
        return priority;
    }

    void setPriority(const Priority priority);

    /**
     * Create a cJSON representation of the members of the connection
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
     *
     * NOTE: the return value should not be returned by the client
     */
    const char* getUsername() const {
        return username.c_str();
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

    FrontEndThread* getThread() const {
        return thread.load(std::memory_order_relaxed);
    }

    void setThread(FrontEndThread* thread) {
        Connection::thread.store(thread,
                                 std::memory_order::memory_order_relaxed);
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

    void* getEngineStorage() const {
        return engine_storage;
    }

    void setEngineStorage(void* engine_storage) {
        Connection::engine_storage = engine_storage;
    }

    int getClustermapRevno() const {
        return clustermap_revno;
    }

    void setClustermapRevno(int clustermap_revno) {
        Connection::clustermap_revno = clustermap_revno;
    }

    /**
     * Restart the authentication (this clears all of the authentication
     * data...)
     */
    void restartAuthentication();

    bool isXerrorSupport() const {
        return xerror_support;
    }

    void setXerrorSupport(bool xerror_support) {
        Connection::xerror_support = xerror_support;
    }

    bool isCollectionsSupported() const {
        return collections_support;
    }

    void setCollectionsSupported(bool collections_support) {
        Connection::collections_support = collections_support;
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

    void setDuplexSupported(bool duplex_support) {
        Connection::duplex_support = duplex_support;
    }

    bool isClustermapChangeNotificationSupported() const {
        return cccp.load(std::memory_order_acquire);
    }

    void setClustermapChangeNotificationSupported(bool cccp) {
        Connection::cccp.store(cccp, std::memory_order_release);
    }

    bool allowUnorderedExecution() const {
        return allow_unordered_execution;
    }

    void setAllowUnorderedExecution(bool allow_unordered_execution) {
        Connection::allow_unordered_execution = allow_unordered_execution;
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

    void setDCP(bool dcp) {
        Connection::dcp = dcp;
    }

    bool isDcpXattrAware() const {
        return dcpXattrAware;
    }

    void setDcpXattrAware(bool dcpXattrAware) {
        Connection::dcpXattrAware = dcpXattrAware;
    }

    void setDcpDeleteTimeEnabled(bool dcpDeleteTimeEnabled) {
        Connection::dcpDeleteTimeEnabled = dcpDeleteTimeEnabled;
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

    void setDcpNoValue(bool dcpNoValue) {
        Connection::dcpNoValue = dcpNoValue;
    }

    /**
     * Decrement the number of events to process and return the new value
     */
    int decrementNumEvents() {
        return --numEvents;
    }

    /**
     * Set the number of events to process per timeslice of the worker
     * thread before yielding.
     */
    void setNumEvents(int nevents) {
        Connection::numEvents = nevents;
    }

    /**
     * Get the maximum number of events we should process per invocation
     * for a connection object (to avoid starvation of other connections)
     */
    int getMaxReqsPerEvent() const {
        return max_reqs_per_event;
    }

    /**
     * Update the settings in libevent for this connection
     *
     * @param mask the new event mask to get notified about
     */
    bool updateEvent(const short new_flags);

    /**
     * Reapply the event mask (in case of a timeout we might want to do
     * that)
     */
    bool reapplyEventmask();

    /**
     * Unregister the event structure from libevent
     * @return true if success, false otherwise
     */
    bool unregisterEvent();

    /**
     * Register the event structure in libevent
     * @return true if success, false otherwise
     */
    bool registerEvent();

    bool isRegisteredInLibevent() const {
        return registered_in_libevent;
    }

    void setCurrentEvent(short ev) {
        currentEvent = ev;
    }

    /** Is the current event a readevent? */
    bool isReadEvent() const {
        return currentEvent & EV_READ;
    }

    /** Is the current event a writeevent? */
    bool isWriteEvent() const {
        return currentEvent & EV_WRITE;
    }

    /**
     * Shrinks a connection's buffers if they're too big.  This prevents
     * periodic large "get" requests from permanently chewing lots of server
     * memory.
     *
     * This should only be called in between requests since it can wipe output
     * buffers!
     */
    void shrinkBuffers();

    /**
     * Receive data from the socket
     *
     * @param where to store the result
     * @param nbytes the size of the buffer
     *
     * @return the number of bytes read, or -1 for an error
     */
    int recv(char* dest, size_t nbytes);

    /**
     * Send data over the socket
     *
     * @param m the message header to send
     * @return the number of bytes sent, or -1 for an error
     */
    ssize_t sendmsg(struct msghdr* m);

    enum class TransmitResult {
        /** All done writing. */
        Complete,
        /** More data remaining to write. */
        Incomplete,
        /** Can't write any more right now. */
        SoftError,
        /** Can't write (c->state is set to conn_closing) */
        HardError
    };

    /**
     * Transmit the next chunk of data from our list of msgbuf structures.
     *
     * Returns:
     *   Complete   All done writing.
     *   Incomplete More data remaining to write.
     *   SoftError Can't write any more right now.
     *   HardError Can't write (c->state is set to conn_closing)
     */
    TransmitResult transmit();

    enum class TryReadResult {
        /** Data received on the socket and ready to parse */
        DataReceived,
        /** No data received on the socket */
        NoDataReceived,
        /** The client closed the connection */
        SocketClosed,
        /** An error occurred on the socket */
        SocketError,
        /** Failed to allocate more memory for the input buffer */
        MemoryError
    };

    /**
     * read from network as much as we can, handle buffer overflow and
     * connection close. Before reading, move the remaining incomplete fragment
     * of a command (if any) to the beginning of the buffer.
     *
     * @return enum try_read_result
     */
    TryReadResult tryReadNetwork();

    const StateMachine::State getWriteAndGo() const {
        return write_and_go;
    }

    void setWriteAndGo(StateMachine::State write_and_go) {
        Connection::write_and_go = write_and_go;
    }

    /**
     * Get the number of entries in use in the IO Vector
     */
    size_t getIovUsed() const {
        return iovused;
    }

    /**
     * Adds a message header to a connection.
     *
     * @param reset set to true to reset all message headers
     * @throws std::bad_alloc
     */
    void addMsgHdr(bool reset);

    /**
     * Add a chunk of memory to the the IO vector to send
     *
     * @param buf pointer to the data to send
     * @param len number of bytes to send
     * @throws std::bad_alloc
     */
    void addIov(const void* buf, size_t len);

    /**
     * Release all of the items we've saved a reference to
     */
    void releaseReservedItems();

    /**
     * Put an item on our list of reserved items (which we should release
     * at a later time through releaseReservedItems).
     *
     * @return true if success, false otherwise
     */
    bool reserveItem(void* item) {
        try {
            reservedItems.push_back(item);
            return true;
        } catch (const std::bad_alloc&) {
            return false;
        }
    }

    void releaseTempAlloc() {
        for (auto* ptr : temp_alloc) {
            cb_free(ptr);
        }
        temp_alloc.resize(0);
    }

    void pushTempAlloc(char* ptr) {
        temp_alloc.push_back(ptr);
    }

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
     * @return true if JSON datatype is enabled
     */
    bool isJsonEnabled() const {
        return datatype.isJsonEnabled();
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

    void setSupportsMutationExtras(bool supports_mutation_extras) {
        Connection::supports_mutation_extras = supports_mutation_extras;
    }

    const ENGINE_ERROR_CODE& getAiostat() const {
        return aiostat;
    }

    void setAiostat(const ENGINE_ERROR_CODE& aiostat) {
        Connection::aiostat = aiostat;
    }

    bool isTracingEnabled() const {
        return tracingEnabled;
    }

    void setTracingEnabled(bool enable) {
        tracingEnabled = enable;
    }

    bool isEwouldblock() const {
        return ewouldblock;
    }

    void setEwouldblock(bool ewouldblock) {
        Connection::ewouldblock = ewouldblock;
    }

    /**
     * Is SSL enabled for this connection or not?
     *
     * @return true if the connection is running over SSL, false otherwise
     */
    bool isSslEnabled() const {
        return ssl.isEnabled();
    }

    /**
     * Do we have any pending input data on this connection?
     */
    bool havePendingInputData() {
        return (!read->empty() || ssl.havePendingInputData());
    }

    /**
     * Try to find RBAC user from the client ssl cert
     *
     * @return true if username has been linked to RBAC or ssl cert was not
     * presented by the client.
     */
    bool tryAuthFromSslCert(const std::string& userName);

    bool shouldDelete();

    void runEventLoop(short which);

    /**
     * Input buffer containing the data we've read of the socket. It is
     * assigned to the connection when the connection is to be served, and
     * returned to the thread context if the pipe is empty when we're done
     * serving this connection.
     */
    std::unique_ptr<cb::Pipe> read;

    /** Write buffer */
    std::unique_ptr<cb::Pipe> write;

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
    bool isPacketAvailable() const {
        auto buffer = read->rdata();

        if (buffer.size() < sizeof(cb::mcbp::Request)) {
            // we don't have the header, so we can't even look at the body
            // length
            return false;
        }

        const auto* req =
                reinterpret_cast<const cb::mcbp::Request*>(buffer.data());
        return buffer.size() >= sizeof(cb::mcbp::Request) + req->getBodylen();
    }

    /**
     * Is SASL disabled for this connection or not? (connection authenticated
     * with SSL certificates will disable the possibility re-authenticate over
     * SASL)
     */
    bool isSaslAuthEnabled() const {
        return saslAuthEnabled;
    }

    bool selectedBucketIsXattrEnabled() const;

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

    const std::array<char, MaxSavedAgentName>& getAgentName() const {
        return agentName;
    }

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

    /// Notify that this connection is going to yield the CPU to allow
    /// other connections to perform operations
    void yield() {
        yields++;
        // Update the aggregated stat
        get_thread_stats(this)->conn_yields++;
    }

    // Implementation of dcp_message_producers interface //////////////////////

    ENGINE_ERROR_CODE get_failover_log(uint32_t opaque,
                                       uint16_t vbucket) override;

    ENGINE_ERROR_CODE stream_req(uint32_t opaque,
                                 uint16_t vbucket,
                                 uint32_t flags,
                                 uint64_t start_seqno,
                                 uint64_t end_seqno,
                                 uint64_t vbucket_uuid,
                                 uint64_t snap_start_seqno,
                                 uint64_t snap_end_seqno) override;

    ENGINE_ERROR_CODE add_stream_rsp(uint32_t opaque,
                                     uint32_t stream_opaque,
                                     uint8_t status) override;

    ENGINE_ERROR_CODE marker_rsp(uint32_t opaque, uint8_t status) override;

    ENGINE_ERROR_CODE set_vbucket_state_rsp(uint32_t opaque,
                                            uint8_t status) override;

    ENGINE_ERROR_CODE stream_end(uint32_t opaque,
                                 uint16_t vbucket,
                                 uint32_t flags) override;

    ENGINE_ERROR_CODE marker(uint32_t opaque,
                             uint16_t vbucket,
                             uint64_t start_seqno,
                             uint64_t end_seqno,
                             uint32_t flags) override;

    ENGINE_ERROR_CODE mutation(uint32_t opaque,
                               item* itm,
                               uint16_t vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t lock_time,
                               const void* meta,
                               uint16_t nmeta,
                               uint8_t nru) override;

    ENGINE_ERROR_CODE deletion(uint32_t opaque,
                               item* itm,
                               uint16_t vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               const void* meta,
                               uint16_t nmeta) override;

    ENGINE_ERROR_CODE deletion_v2(uint32_t opaque,
                                  gsl::not_null<item*> itm,
                                  uint16_t vbucket,
                                  uint64_t by_seqno,
                                  uint64_t rev_seqno,
                                  uint32_t delete_time) override;

    ENGINE_ERROR_CODE expiration(uint32_t opaque,
                                 item* itm,
                                 uint16_t vbucket,
                                 uint64_t by_seqno,
                                 uint64_t rev_seqno,
                                 const void* meta,
                                 uint16_t nmeta) override;

    ENGINE_ERROR_CODE set_vbucket_state(uint32_t opaque,
                                        uint16_t vbucket,
                                        vbucket_state_t state) override;
    ENGINE_ERROR_CODE noop(uint32_t opaque) override;

    ENGINE_ERROR_CODE buffer_acknowledgement(uint32_t opaque,
                                             uint16_t vbucket,
                                             uint32_t buffer_bytes) override;
    ENGINE_ERROR_CODE control(uint32_t opaque,
                              const void* key,
                              uint16_t nkey,
                              const void* value,
                              uint32_t nvalue) override;

    ENGINE_ERROR_CODE get_error_map(uint32_t opaque, uint16_t version) override;

protected:
    /**
     * Protected constructor so that it may only be used by MockSubclasses
     */
    Connection();

    /**
     * Update the description string for the connection. This
     * method should be called every time the authentication data
     * (or the sockname/peername) changes
     */
    void updateDescription();

    void runStateMachinery();

    /**
     * Initialize the event structure and add it to libevent
     *
     * @return true upon success, false otherwise
     */
    bool initializeEvent();

    /**
     * Ensures that there is room for another struct iovec in a connection's
     * iov list.
     *
     * @throws std::bad_alloc
     */
    void ensureIovSpace();

    /**
     * Try to enable SSL for this connection
     *
     * @param cert the SSL certificate to use
     * @param pkey the SSL private key to use
     * @return true if successful, false otherwise
     */
    bool enableSSL(const std::string& cert, const std::string& pkey) {
        if (ssl.enable(cert, pkey)) {
            if (settings.getVerbose() > 1) {
                ssl.dumpCipherList(getId());
            }

            return true;
        }

        return false;
    }

    /**
     * Read data over the SSL connection
     *
     * @param dest where to store the data
     * @param nbytes the size of the destination buffer
     * @return the number of bytes read
     */
    int sslRead(char* dest, size_t nbytes);

    /**
     * Write data over the SSL stream
     *
     * @param src the source of the data
     * @param nbytes the number of bytes to send
     * @return the number of bytes written
     */
    int sslWrite(const char* src, size_t nbytes);

    /**
     * Handle the state for the ssl connection before the ssl connection
     * is fully established
     */
    int sslPreConnection();

    // Shared DCP_DELETION write function for the v1/v2 commands.
    ENGINE_ERROR_CODE deletionInner(const item_info& info,
                                    cb::const_byte_buffer packet,
                                    cb::const_byte_buffer extendedMeta,
                                    const DocKey& key);

    /**
     * Add the provided packet to the send pipe for the connection
     */
    ENGINE_ERROR_CODE add_packet_to_send_pipe(cb::const_byte_buffer packet);

    /**
     * The actual socket descriptor used by this connection
     */
    SOCKET socketDescriptor;

    // The number of times we've been backing off and yielding
    // to allow other threads to run
    Couchbase::RelaxedAtomic<uint64_t> yields;

    /**
     * The event base this connection is bound to
     */
    event_base *base;

    /**
     * The current privilege context
     */
    cb::rbac::PrivilegeContext privilegeContext;

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

    /**
     * Pointer to engine-specific data which the engine has requested the server
     * to persist for the life of the connection.
     * See SERVER_COOKIE_API::{get,store}_engine_specific()
     */
    void* engine_storage{nullptr};

    /** Pointer to the thread object serving this connection */
    std::atomic<FrontEndThread*> thread{nullptr};

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

    /** The connections priority */
    Priority priority{Priority::Medium};

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
    int max_reqs_per_event =
            settings.getRequestsPerEventNotification(EventPriority::Default);

    /**
     * number of events this connection can process in a single worker
     * thread timeslice
     */
    int numEvents = 0;

    // Members related to libevent

    /** Is the connection currently registered in libevent? */
    bool registered_in_libevent = false;

    struct EventDeleter {
        void operator()(struct event* e) {
            event_free(e);
        }
    };

    /** The libevent object */
    std::unique_ptr<struct event, EventDeleter> event;
    /** The current flags we've registered in libevent */
    short ev_flags = 0;
    /** which events were just triggered */
    short currentEvent = 0;

    /** which state to go into after finishing current write */
    StateMachine::State write_and_go = StateMachine::State::new_cmd;

    /* data for the mwrite state */
    std::vector<iovec> iov;
    /** number of elements used in iov[] */
    size_t iovused = 0;

    /** The message list being used for transfer */
    std::vector<struct msghdr> msglist;
    /** element in msglist[] being transmitted now */
    size_t msgcurr = 0;
    /** number of bytes in current msg */
    size_t msgbytes = 0;

    /**
     * List of items we've reserved during the command (should call
     * item_release when transmit is complete)
     */
    std::vector<void*> reservedItems;

    /**
     * A vector of temporary allocations that should be freed when the
     * the connection is done sending all of the data. Use pushTempAlloc to
     * push a pointer to this list (must be allocated with malloc/calloc/strdup
     * etc.. will be freed by calling "free")
     */
    std::vector<char*> temp_alloc;

    /**
     * If the client enabled the mutation seqno feature each mutation
     * command will return the vbucket UUID and sequence number for the
     * mutation.
     */
    bool supports_mutation_extras = false;

    /**
     * The status for the async io operation
     */
    ENGINE_ERROR_CODE aiostat = ENGINE_SUCCESS;

    /**
     * Is this connection currently in an "ewouldblock" state?
     */
    bool ewouldblock = false;

    /**
     * The SSL context used by this connection (if enabled)
     */
    SslContext ssl;

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
};

/**
 * Convert a priority to a textual representation
 */
std::string to_string(Connection::Priority priority);
