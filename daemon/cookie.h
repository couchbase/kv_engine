/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/status.h>
#include <memcached/cookie_iface.h>
#include <memcached/dockey.h>
#include <memcached/engine_error.h>
#include <memcached/rbac.h>
#include <memcached/tracer.h>
#include <nlohmann/json.hpp>
#include <platform/compression/buffer.h>
#include <chrono>

// Forward decls
class Connection;
class CommandContext;
struct CookieTraceContext;
class GetAuthorizationTask;
namespace cb::mcbp {
class Header;
class Request;
class Response;
} // namespace cb::mcbp
namespace cb::compression {
class Buffer;
} // namespace cb::compression

/**
 * The Cookie class represents the cookie passed from the memcached core
 * down through the engine interface to the engine.
 *
 * A cookie represents a single command context, and contains the packet
 * it is about to execute.
 *
 * By passing a common class as the cookie our notification model may
 * know what the argument is and provide it's own logic depending on
 * which field is set
 */
class Cookie : public CookieIface {
public:
    /// All cookies must belong to a connection
    Cookie() = delete;
    /// Create a new cookie owned by the provided connection
    explicit Cookie(Connection& conn);
    ~Cookie() override;

    /**
     * Initialize this cookie.
     *
     * At some point we'll refactor this into being the constructor
     * for the cookie, but given that we allow for object reuse across
     * commands (DCP depends on this feature) we have a separate method
     * to initialize the object.
     *
     * @param packet the entire packet
     * @param tracing_enabled if tracing is enabled for this request
     */
    void initialize(const cb::mcbp::Header& packet, bool tracing_enabled);

    /// Is this object initialized or not..
    bool empty() const {
        return !packet;
    }

    /**
     * Validates the packet content, and (possibly) set the error
     * state and reason.
     *
     * @return Success if the packet was correctly encoded
     * @throw std::runtime_error if an unsupported packet is encountered
     */
    cb::mcbp::Status validate();

    /**
     * Reset the Cookie object to allow it to be reused in the same
     * context as the last time.
     */
    void reset();

    /// Is the cookie throttled or not
    bool isThrottled() const {
        return throttled.load(std::memory_order_acquire);
    }

    /// Update the throttled-state of the cookie
    void setThrottled(bool val);

    uint64_t getTotalThrottleTime() const {
        return total_throttle_time.count();
    }

    /**
     * Get a representation of the object in JSON
     */
    nlohmann::json to_json() const;

    /**
     * Get the unique event identifier created for this command (an id
     * gets created the first time this method gets called for the command
     * and the same UUID gets returned for the remaining duration of
     * the command making it easy to log multiple entries with the same
     * ID). Calling getEventId inserts the UUID into the error context
     * for the cookie causing it to be returned back to the client
     * as part of the sendResponse methods with a failure status.
     *
     * @return A "random" UUID
     */
    std::string getEventId();

    void setEventId(std::string uuid);

    void setErrorContext(std::string message) override;

    /// Get the context to send back for this command.
    std::string getErrorContext() const;

    /**
     * Add additional arbitrary JSON to the response, this is in addition to
     * any message set via setErrorContext and any id set via setEventId
     *
     * Note this has no affect for the following response codes.
     *   cb::mcbp::Status::Success
     *   cb::mcbp::Status::SubdocSuccessDeleted
     *   cb::mcbp::Status::SubdocMultiPathFailure
     *   cb::mcbp::Status::Rollback
     *   cb::mcbp::Status::NotMyVbucket
     *
     * @param json an object which is appended to the JSON response object.
     */
    void setErrorJsonExtras(const nlohmann::json& json);

    /**
     * Return the error "object" to return to the client.
     *
     * @return An empty string if no extended error information is being set
     */
    std::string getErrorJson();

    ConnectionIface& getConnectionIface() override;

    /**
     * Get the connection object the cookie is bound to.
     *
     * A cookie is bound to the conneciton at create time, and will never
     * switch connections
     */
    Connection& getConnection() const {
        return connection;
    }

    uint32_t getConnectionId() const override;

    /**
     * Execute the current packet
     *
     * Given that the method calls down into the engine it may throw a wide
     * range of exceptions, but they should all be based off std::exception
     * which is caught by the state machinery causing the connection to be
     * closed
     *
     * @return true if the command executed completely
     *         false if the command blocked (ewouldblock)
     */
    bool execute(bool useStartTime = false);

    /**
     * Set the packet used by this command context.
     *
     *
     * Note that the cookie does not _own_ the actual packet content
     * (unless copy is set to true), as we might not want to perform
     * an extra memory copy from the underlying event framework
     * into the cookie and then again into the underlying engine.
     *
     * The initial prototype of unordered execution will however
     * do the copy to simplify the state machinery logic.
     *
     * @param header the header and the full packet to use
     * @param copy Set to true if the cookie should create a copy
     *             of the data (to be returned from the getPackage)
     *
     * @throw std::invalid_argument if buffer size < a request
     * @throw std::logic_error if copy is requested and content
     *                         isn't the full packet
     * @throw std::bad_alloc if copy is set to true and we fail to
     *                       allocate a backing store.
     */
    void setPacket(const cb::mcbp::Header& header, bool copy = false);

    /**
     * Get the packet for this command / response packet
     *
     * @return the byte buffer containing the packet
     * @throws std::logic_error if the packet isn't available
     */
    cb::const_byte_buffer getPacket() const;

    /**
     * Preserve the input packet by allocating memory and copy the
     * current packet.
     */
    void preserveRequest() {
        if (!isRequestPreserved()) {
            setPacket(getHeader(), true);
        }
    }

    bool isRequestPreserved() {
        return frame_copy.get() != nullptr;
    }

    /**
     * Get the packet header for the current packet. The packet header
     * allows for getting the various common fields in a packet (request and
     * response).
     */
    const cb::mcbp::Header& getHeader() const;

    /**
     * Get the packet as a request packet
     *
     * @return the packet if it is a request
     * @throws std::invalid_argument if the packet is of an invalid type
     * @throws std::logic_error if the packet is a response
     */
    const cb::mcbp::Request& getRequest() const;

    /**
     * Get the key from the request
     *
     * @return the key from the request
     * @throws std::invalid_argument if the packet is of an invalid type
     * @throws std::logic_error if the packet is a response
     */
    const DocKey getRequestKey() const;

    /**
     * Get a printable key from the header. Replace all non-printable
     * charachters with '.'
     *
     * @param addUserDataTags when set to true the returned data contains
     *                        the userdata tags which allows for redaction
     */
    std::string getPrintableRequestKey(bool addUserDataTags = true,
                                       bool stripCollection = false) const;

    /**
     * Get a printable representation of the key's collection ID
     */
    std::string getPrintableRequestCollectionID() const;

    /**
     * Set the aiostat and return the previous value
     */
    cb::engine_errc swapAiostat(cb::engine_errc value);

    /**
     * Get the current status of the asynchrous IO
     */
    cb::engine_errc getAiostat() const;

    /**
     * Set the status code for the async IO
     */
    void setAiostat(cb::engine_errc aiostat);

    /// Is the current cookie blocked?
    bool isEwouldblock() const {
        return ewouldblock;
    }

    /// Set the ewouldblock status for the cookie
    void setEwouldblock(bool ewouldblock);

    /**
     *
     * @return
     */
    uint64_t getCas() const {
        return cas;
    }

    /**
     * Set the CAS value to inject into the response packet
     */
    void setCas(uint64_t value) {
        cas = value;
    }

    /**
     * Sent back the not my vbucket response (piggyback a vbucket
     * map and do deduplication if enabled)
     */
    void sendNotMyVBucket();

    /**
     * Send a response without a message payload back to the client.
     *
     * @param status The status message to fill into the message.
     */
    void sendResponse(cb::mcbp::Status status);

    /**
     * Map the engine error code over to the correct status message
     * and send the appropriate packet back to the client.
     */
    void sendResponse(cb::engine_errc code);

    /**
     * Form a response packet and send back to the client
     *
     * Note: we currently _copy_ the content of extras, key and value
     * into the connections write buffer.
     *
     * @param status The status code for the operation
     * @param extras The extras to add to the package
     * @param key The key to add to the package
     * @param value The value to add to the packet
     * @param datatype The datatype to add to the message
     * @param cas the Cas field to insert into the packet
     *
     * @throws std::bad_alloc for memory alloction failures
     * @throws std::runtime_error if unsupported datatypes is being used
     *                            (xattrs or compressed)
     * @throws std::logic_error if the write buffer contains data
     */
    void sendResponse(cb::mcbp::Status status,
                      std::string_view extras,
                      std::string_view key,
                      std::string_view value,
                      cb::mcbp::Datatype datatype,
                      uint64_t cas);

    /// see above, but map engine_errc to mcbp::Status
    void sendResponse(cb::engine_errc status,
                      std::string_view extras,
                      std::string_view key,
                      std::string_view value,
                      cb::mcbp::Datatype datatype,
                      uint64_t cas);

    /**
     * Get the command context stored for this command as
     * the given type or make it if it doesn't exist
     *
     * @tparam ContextType CommandContext type to create
     * @return the context object
     * @throws std::logic_error if the object is the wrong type
     */
    template <typename ContextType, typename... Args>
    ContextType& obtainContext(Args&&... args) {
        auto* context = commandContext.get();
        if (context == nullptr) {
            auto* ret = new ContextType(std::forward<Args>(args)...);
            commandContext.reset(ret);
            return *ret;
        }
        auto* ret = dynamic_cast<ContextType*>(context);
        if (ret == nullptr) {
            throw std::logic_error(std::string("Connection::obtainContext<") +
                                   typeid(ContextType).name() +
                                   ">(): context is not the requested type");
        }
        return *ret;
    }

    CommandContext* getCommandContext() {
        return commandContext.get();
    }

    void setCommandContext(CommandContext* ctx = nullptr);

    // The source code was initially written in C which didn't have the
    // concept of shared pointers so the current code use a manual
    // reference counting. If the engine wants to keep a reference to the
    // cookie it must bump the reference count to avoid the core to reuse
    // the cookie leaving the engine with a dangling pointer.

    /// Get the current reference count
    uint8_t getRefcount() {
        return refcount;
    }

    /// Add a reference to the cookie
    /// returns the incremented ref count
    uint8_t incrementRefcount() {
        if (refcount == 255) {
            throw std::logic_error(
                    "Cookie::incrementRefcount(): refcount will wrap");
        }
        return ++refcount;
    }
    uint8_t decrementRefcount() {
        if (refcount == 0) {
            throw std::logic_error(
                    "Cookie::decrementRefcount(): refcount will wrap");
        }
        return --refcount;
    }

    void setOpenTracingContext(cb::const_byte_buffer context);

    /**
     * @return true is setAuthorized has been called
     */
    bool isAuthorized() const {
        return authorized;
    }

    /**
     * This method should be used when the command has successfully passed
     * authorization check(s).
     *
     * This exists to assist with correct "would block" command processing,
     * this method should be used to tag that the authorization process has
     * returned "success| and the command shouldn't get a second authorization
     * test when unblocked and re-executed.
     */
    void setAuthorized() {
        authorized = true;
    }

    /**
     * Mark this cookie as a barrier. A barrier command cannot be executed in
     * parallel with other commands. For more information see
     * docs/UnorderedExecution.md
     */
    void setBarrier() {
        reorder = false;
    }

    /// May the execution of this command be reordered with another command
    /// in the same pipeline?
    bool mayReorder() const {
        return reorder;
    }

    std::string_view getInflatedInputPayload() const override;

    /**
     * Inflate the value (if deflated); caching the inflated value inside the
     * cookie.
     *
     * @param header The packet header
     * @return true if success, false if an error occurs (the error context
     *         contains the reason why)
     */
    bool inflateInputPayload(const cb::mcbp::Header& header);

    /**
     * Helper function to inflate the specified Snappy-compressed buffer.
     *
     * Records the time taken to decompress as SnappyDecompress tracing span
     * against this cookie.
     * @param input Snappy-compressed input buffer.
     * @param output Buffer to write uncompressed data to.
     * @returns true if successfully inflated, else false.
     */
    bool inflateSnappy(std::string_view input, cb::compression::Buffer& output);

    void setCurrentCollectionInfo(ScopeID sid,
                                  CollectionID cid,
                                  uint64_t manifestUid,
                                  bool metered) override {
        currentCollectionInfo =
                CurrentCollectionInfo(sid, cid, manifestUid, metered);
    }

    const std::pair<std::optional<ScopeID>, std::optional<CollectionID>>
    getScopeAndCollection() const {
        return {currentCollectionInfo.sid, currentCollectionInfo.cid};
    }

    /// Set the JSON extras to return the manifestId which is located in
    /// currentCollectionInfo.
    void setUnknownCollectionErrorContext();

    void setUnknownCollectionErrorContext(uint64_t manifestUid) override;

    /// Check if the current command have the requested privilege
    cb::rbac::PrivilegeAccess checkPrivilege(cb::rbac::Privilege privilege) {
        return checkPrivilege(privilege,
                              currentCollectionInfo.sid,
                              currentCollectionInfo.cid);
    }

    cb::rbac::PrivilegeAccess checkPrivilege(
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override;

    cb::rbac::PrivilegeAccess testPrivilege(
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) const override;

    cb::rbac::PrivilegeAccess testPrivilege(
            cb::rbac::Privilege privilege) const {
        return testPrivilege(privilege, {}, {});
    }

    cb::rbac::PrivilegeAccess checkForPrivilegeAtLeastInOneCollection(
            cb::rbac::Privilege privilege) const override;

    /// Get the underlying privilege context
    const cb::rbac::PrivilegeContext& getPrivilegeContext() const {
        return privilegeContext;
    }

    /**
     * Set the effective user executing this command
     *
     * @param user the effective user for the command
     * @return engine_success if the user holds the Impersonate privilege and
     *                        the user is found
     *         engine_eaccess if the user lacks the impersonate privilege
     *         engine_key_enoent if the user holds the privilege and the user
     *                           isn't found
     *         engine_not_supported if the user isn't in the local domain
     */
    cb::mcbp::Status setEffectiveUser(const cb::rbac::UserIdent& e);

    const cb::rbac::UserIdent* getEffectiveUser() const {
        return euid.get();
    }

    void addImposedUserExtraPrivilege(cb::rbac::Privilege privilege) {
        const auto idx = size_t(privilege);
        euidExtraPrivileges.set(idx);
    }

    bool isPreserveTtl() const {
        return preserveTtl;
    }

    void setPreserveTtl(bool val) {
        preserveTtl = val;
    }

    /// Set the response status code we sent for this command (to include
    /// in the log message for slow command)
    void setResponseStatus(cb::mcbp::Status status) {
        responseStatus = status;
    }

    bool isMutationExtrasSupported() const override;
    bool isCollectionsSupported() const override;
    bool isDatatypeSupported(
            protocol_binary_datatype_t datatype) const override;

    void notifyIoComplete(cb::engine_errc status) override;

    /// Mark this cookie as a durable request
    void setDurable() {
        durable = true;
    }

    /// Does this cookie represent a durable request
    bool isDurable() const {
        return durable;
    }

    /**
     * Get the amount of read/write units we should use for metering.
     *
     * According to MB-53560 a failing operation should not cost anything
     * According to MB-53127 read should come for free if we charge for write
     *
     * @return number of read and write units to charge the user for.
     */
    std::pair<size_t, size_t> getDocumentMeteringRWUnits() const;

    bool checkThrottle(size_t pendingRBytes, size_t pendingWBytes) override;
    bool sendResponse(cb::engine_errc status,
                      std::string_view extras,
                      std::string_view value) override;

protected:
    /**
     * Log the current connection if its execution time exceeds the
     * threshold for the command
     *
     * @param elapsed the time elapsed while executing the command
     */
    void maybeLogSlowCommand(std::chrono::steady_clock::duration elapsed) const;

    bool doExecute();

    /// Check if the current command have the requested privilege for
    /// for the provided scope collection identifier
    cb::rbac::PrivilegeAccess checkPrivilege(
            const cb::rbac::PrivilegeContext& ctx,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid);

    cb::rbac::PrivilegeAccess testPrivilege(
            const cb::rbac::PrivilegeContext& ctx,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) const;

    /**
     * Is OpenTelemetry enabled for this cookie or not. By querying the
     * cookie we don't have to read the atomic on/off switch unless
     * people try to set the OpenTelemetry context in the command.
     */
    bool isOpenTracingEnabled() const {
        return !openTracingContext.empty();
    }

    /**
     * Extract the trace context.
     * This method moves the tracer, OpenTelemetry context into
     * a newly created TraceContext object which may be passed to the
     * OpenTelemetry module and handled asynchronously
     */
    CookieTraceContext extractTraceContext();

    void collectTimings(const std::chrono::steady_clock::time_point& end);

    /// The tracing context provided by the client to use as the
    /// parent span
    std::string openTracingContext;

    /**
     * The connection object this cookie is bound to
     */
    Connection& connection;

    /**
     * When the server returns an error message it'll add the content
     * of the error_json in the response message. It is currently 3
     * different ways to inject data into the response:
     *
     *    * Call getEventId() which will generate a UUID. This UUID should
     *      be logged on the server side with more detailed information of
     *      the error as it get returned under { "error: { "ref" : "uuid"}}
     *      and the client should log the uuid with more details and by
     *      grep'ing in the client and server logs we should be able to
     *      locate both sides.
     *
     *    * setErrorContext(message) This adds the provided message in
     *      the response as { "error" : { "context" : "message" }}
     *
     *    * setErrorJsonExtras(json) This adds the content of the provided
     *      json into the object returned. The provided JSON cannot replace
     *      "error.ref" and "error.context", but apart from that it may
     *      contain any legal JSON.
     */
    nlohmann::json error_json;

    /**
     * The input packet used in this command context
     */
    const cb::mcbp::Header* packet = nullptr;

    /**
     * The backing store of the received packet if the cookie owns
     * the data (created by copying the input data)
     */
    std::unique_ptr<uint8_t[]> frame_copy;

    /** The cas to return back to the client */
    uint64_t cas = 0;

    /**
     * The high resolution timer value for when we started executing the
     * current command.
     */
    std::chrono::steady_clock::time_point start;

    /// The high resolution timer value for when we started throttling
    std::chrono::steady_clock::time_point throttle_start;
    std::chrono::duration<int32_t, std::micro> total_throttle_time;

    /**
     * The status for the async io operation
     */
    cb::engine_errc aiostat = cb::engine_errc::success;

    /**
     *  command-specific context - for use by command executors to maintain
     *  additional state while executing a command. For example
     *  a command may want to maintain some temporary state between retries
     *  due to engine returning EWOULDBLOCK.
     *
     *  Between each command this is deleted and reset to nullptr.
     */
    std::unique_ptr<CommandContext> commandContext;

    cb::compression::Buffer inflated_input_payload;

    /// The Scope and Collection information for the current command picked
    /// out from the incoming packet as part of packet validation. This stores
    /// the collection from the operation, it's scope and the unique-id of the
    /// collection manifest that mapped cid to sid.
    struct CurrentCollectionInfo {
        CurrentCollectionInfo(ScopeID sid,
                              CollectionID cid,
                              uint64_t uid,
                              bool metered)
            : sid(sid), cid(cid), manifestUid(uid), metered(metered) {
        }
        CurrentCollectionInfo() = default;
        void reset() {
            sid.reset();
            cid.reset();
            manifestUid = 0;
            metered = true;
        }
        std::optional<ScopeID> sid;
        std::optional<CollectionID> cid;
        uint64_t manifestUid{0};
        bool metered{true};
    } currentCollectionInfo;

    /// The privilege context the command should use for evaluating commands
    cb::rbac::PrivilegeContext privilegeContext;

    /// If the request came in with the impersonate frame info set, this
    /// is the user requested
    /// Using unique_ptr for optional semantics, without paying the memory
    /// footprint of PrivilegeContext (40B at time of writing) if
    /// impersonate not being used.
    std::unique_ptr<cb::rbac::UserIdent> euid;

    /// Fetch the privileges from the EUID
    bool fetchEuidPrivilegeSet();

    /// A shared pointer to the task if we want to fetch authorizations
    /// from the external auth service;
    std::shared_ptr<GetAuthorizationTask> getAuthorizationTask;

    /// If the request came in with the impersonate frame info set, this
    /// is the privilege context for that user (which we'll also test).
    /// Using unique_ptr for optional semantics, without paying the memory
    /// footprint of PrivilegeContext (40B at time of writing) if
    /// impersonate not being used.
    std::unique_ptr<cb::rbac::PrivilegeContext> euidPrivilegeContext;

    /// When impersonating users we may grant the user extra privileges
    /// (but the authenticated user must also have the privileges in the
    /// effective set)
    cb::rbac::PrivilegeMask euidExtraPrivileges;

    /// The response status we sent for this cookie (for a multi-response
    /// command such as STATS it would be the _last_ status code)
    cb::mcbp::Status responseStatus = cb::mcbp::Status::COUNT;

    bool validated = false;

    bool reorder = false;

    /// Is the cookie currently throttled
    std::atomic_bool throttled{false};

    /// Is this a durable operation or not
    std::atomic_bool durable{false};

    bool ewouldblock = false;

    /// The number of times someone tried to reserve the cookie (to avoid
    /// releasing it while other parties think they reserved the object.
    /// Previously reserve would lock the connection, but with OOO we
    /// might have multiple cookies in flight and needs to be able to
    /// lock them independently
    std::atomic<uint8_t> refcount = 0;

    /// see isAuthorized/setAuthorized
    bool authorized = false;

    /// should we try to preserve TTL for this operation
    bool preserveTtl{false};
};

/// Convert a const CookieIface to a Cookie.
inline Cookie& asCookie(CookieIface& cookieIface) {
    auto* cookie = dynamic_cast<Cookie*>(&cookieIface);
    if (!cookie) {
        throw std::runtime_error(
                "asCookie: The provided cookie is not a Cookie");
    }
    return *cookie;
}
