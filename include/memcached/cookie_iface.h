/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <memcached/engine_error.h>
#include <memcached/engine_storage.h>
#include <memcached/tracer.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

namespace cb {
enum class engine_errc;
}
namespace cb::mcbp {
class Header;
enum class Status : uint16_t;
} // namespace cb::mcbp

namespace cb::rbac {
class PrivilegeAccess;
enum class Privilege;
} // namespace cb::rbac

class CollectionID;
class ScopeID;
using protocol_binary_datatype_t = uint8_t;
class ConnectionIface;
struct item_info;
namespace cb::audit::document {
enum class Operation;
}

/**
 * The CookieIface is an abstract class representing a single command
 * when used from the frontend calling down into the underlying engine
 * (there is currently an exception to this, and that is that DCP connections
 * _currently_ use the Cookie to represent the connection)
 *
 * The methods in the interface are safe to call from the core and the
 * underlying engine. It is the responsibility of the implementation of the
 * method to switch to the core's memory allocation domain if memory needs
 * to be allocated.
 */
class CookieIface : public cb::tracing::Traceable {
public:
    /// Get the Connection this cookie belongs to
    virtual ConnectionIface& getConnectionIface() = 0;

    /// Get the identifier user for logging for all cookies bound to this
    /// connection.
    virtual uint32_t getConnectionId() const = 0;

    /// Notify the cookie that the engine completed its work for the cookie
    /// so the cookie is no longer blocked.
    virtual void notifyIoComplete(cb::engine_errc status) = 0;

    /**
     * Notify the core that we're holding on to this cookie for
     * future use. (The core guarantees it will not invalidate the
     * memory until the cookie is invalidated by calling release())
     */
    virtual void reserve() = 0;

    /**
     * Notify the core that we're releasing the reference to the
     * The engine is not allowed to use the cookie (the core may invalidate
     * the memory).
     */
    virtual void release() = 0;

    // The underlying engine may store information bound to the given cookie
    // in an opaque pointer. The framework will _NOT_ take ownership of the
    // data, and the engine must deal with all allocation/deallocation of the
    // memory to avoid any leaks.

    /// Get the value stored for the cookie (or a null pointer if nothing was
    /// stored). The function returns the same value until it is being set to
    /// another value by calling setEngineStorage or until the cookie becomes
    // disassociated from the engine.
    const cb::EngineStorageIface* getEngineStorage() const;
    // Transfer ownership of the value stored for the cookie to the caller,
    // leaving a null pointer in the cookie.
    cb::unique_engine_storage_ptr takeEngineStorage();
    /// Set the engine storage to the provided value.
    void setEngineStorage(cb::unique_engine_storage_ptr value);

    /**
     * Check if the view contains valid JSON (and add a trace span
     * containing the time used for JSON validation). The method must
     * <b>only</b> be called in the context of the front end thread
     * at it utilize the shared json validator for the attached front
     * end thread.
     *
     * This method will allocate memory, and the caller must toggle
     * memory allocation domain before calling the method!
     *
     * @param view The data to check
     * @return true if view represents valid JSON, false otherwise
     */
    virtual bool isValidJson(std::string_view view) = 0;

    /// Check if mutation extras is supported by the connection.
    virtual bool isMutationExtrasSupported() const = 0;
    /// Check if collections is supported by the connection
    virtual bool isCollectionsSupported() const = 0;

    /// Check if the requested datatype is supported by the connection.
    virtual bool isDatatypeSupported(
            protocol_binary_datatype_t datatype) const = 0;

    /// Get the revision number for the privilege context for the cookie to
    /// allow the engine to cache the result of a privilege check if locating
    /// the sid / cid is costly.
    virtual uint32_t getPrivilegeContextRevision() = 0;

    /**
     * Test if the cookie possess the requested privilege in its effective
     * set. If the cookie is using imposed users the privilege must be in
     * both sets.
     *
     * The caller should use checkPrivilege() if the lack of the privilege is
     * a fatal error for the operation as it will generate an audit record and
     * update the error context
     *
     * @param privilege The privilege to check
     * @param sid If the privilege is not found for the bucket, try looking in
     *            this scope.
     * @param cid If the privilege is not found for the scope, try looking in
     *            this collection.
     * @throws invalid_argument if cid defined but not sid
     */
    virtual cb::rbac::PrivilegeAccess testPrivilege(
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) const = 0;

    /**
     * Test if the cookie possess the requested privilege in its effective
     * set, and if it does _not_ hold the requested privilege in its effective
     * set an audit record gets submitted and the error context gets updated
     * with a message describing the failure.
     *
     * If the cookie is using imposed users the privilege must be in both sets
     *
     * The caller should use testPrivilege() if the lack of the privilege
     * isn't a fatal error for the operation (for instance it could just
     * return less detailed information etc).
     *
     * @param privilege The privilege to check
     * @param sid If the privilege is not found for the bucket, try looking in
     *            this scope.
     * @param cid If the privilege is not found for the scope, try looking in
     *            this collection.
     * @throws invalid_argument if cid defined but not sid
     */
    virtual cb::rbac::PrivilegeAccess checkPrivilege(
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) = 0;

    virtual cb::rbac::PrivilegeAccess checkForPrivilegeAtLeastInOneCollection(
            cb::rbac::Privilege privilege) const = 0;

    /// Get the payload from the command.
    virtual std::string_view getInflatedInputPayload() const = 0;

    /// Add the number of document bytes read
    void addDocumentReadBytes(size_t nread) {
        document_bytes_read += nread;
    }

    /// Add the number of document bytes written
    void addDocumentWriteBytes(size_t nwrite) {
        document_bytes_written += nwrite;
    }

    std::pair<size_t, size_t> getDocumentRWBytes() const {
        return {document_bytes_read.load(std::memory_order_acquire),
                document_bytes_written.load(std::memory_order_acquire)};
    }

    /// Set the current collection meta information. The packet validator
    /// is responsible for checking that the requested collection identifier
    /// is a legal scope (and return an error if it isn't) and to avoid having
    /// to redo the check as part of privilege checks (as the lookup needs
    /// a lock for the manifest) we'll keep the the result in the cookie
    virtual void setCurrentCollectionInfo(ScopeID sid,
                                          CollectionID cid,
                                          uint64_t manifestUid,
                                          bool metered) = 0;

    /**
     * Return true if throttling is required.
     *
     * The input pendingRBytes/pendingWBytes are considered in the check but do
     * not update the bucket ru/wu cost variables.
     *
     * @param pendingRBytes any pending read bytes
     * @param pendingWBytes any pending write bytes
     * @return if the connection should now be throttled
     */
    virtual bool checkThrottle(size_t pendingRBytes, size_t pendingWBytes) = 0;

    /**
     * Send a status/value in a response message
     * @param status the status to use in the response message
     * @param extras view onto extras to attach to the response
     * @param value view onto a value to attach to the response
     * @return true if successful, false if not (e.g. disconnect)
     */
    virtual bool sendResponse(cb::engine_errc status,
                              std::string_view extras,
                              std::string_view value) = 0;

    /**
     * Set the error context string to be sent in response. This should not
     * contain security sensitive information. If sensitive information needs to
     * be preserved, log it with a UUID and send the UUID.
     *
     * Note this has no affect for the following response codes.
     *   cb::mcbp::Status::Success
     *   cb::mcbp::Status::SubdocSuccessDeleted
     *   cb::mcbp::Status::SubdocMultiPathFailure
     *   cb::mcbp::Status::Rollback
     *   cb::mcbp::Status::NotMyVbucket
     *
     * @param cookie the client cookie (to look up client connection)
     * @param message the message string to be set as the error context
     */
    virtual void setErrorContext(std::string message) {
    }

    virtual std::string getErrorContext() const {
        return {};
    }

    /**
     * Set the cookie state ready for an unknown collection (scope)
     * response. This ensures the manifestUid is added as extra state
     * to the response in a consistent format.
     *
     * Note this has no affect for the following response codes.
     *   cb::mcbp::Status::Success
     *   cb::mcbp::Status::SubdocSuccessDeleted
     *   cb::mcbp::Status::SubdocMultiPathFailure
     *   cb::mcbp::Status::Rollback
     *   cb::mcbp::Status::NotMyVbucket
     *
     * @param manifestUid id to include in response
     */
    virtual void setUnknownCollectionErrorContext(uint64_t manifestUid) {
    }

    /// Generate an audit event for access to the document represented
    /// in this cookie (request key).
    /// @todo remove this method when moving withMeta operations out of
    ///       ep-engine and to the core
    virtual void auditDocumentAccess(
            cb::audit::document::Operation operation){};

    /**
     * This callback is called from the underlying engine right before
     * it is linked into the list of available documents (it is currently
     * not visible to anyone). The engine should have validated all
     * properties set in the document by the client and the core, and
     * assigned a new CAS number for the document (and sequence number if
     * the underlying engine use those).
     *
     * The callback may at this time do post processing of the document
     * content (it is allowed to modify the content data, but not
     * reallocate or change the size of the data in any way).
     *
     * Given that the engine MAY HOLD LOCKS when calling this function
     * the core is *NOT* allowed to acquire *ANY* locks (except for doing
     * some sort of memory allocation for a temporary buffer).
     *
     * @return cb::engine_errc::success means that the underlying engine should
     *                        proceed to link the item. All other
     *                        error codes means that the engine should
     *                        *NOT* link the item
     */
    virtual cb::engine_errc preLinkDocument(item_info& info) = 0;

protected:
    std::atomic<size_t> document_bytes_read = 0;
    std::atomic<size_t> document_bytes_written = 0;

    /**
     * Pointer to engine-specific data which the engine has requested the server
     * to persist for the life of the command.
     * See SERVER_COOKIE_API::{get,store}_engine_specific()
     */
    folly::Synchronized<cb::unique_engine_storage_ptr, std::mutex>
            engine_storage;
};
