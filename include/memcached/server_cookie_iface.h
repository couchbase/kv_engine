/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "engine_error.h"
#include "protocol_binary.h"
#include "rbac.h"
#include "types.h"

#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/opcode.h>
#include <nlohmann/json_fwd.hpp>
#include <string>

namespace cb::mcbp {
class Request;
} // namespace cb::mcbp

class CookieIface;
class DcpConnHandlerIface;

/**
 * Commands to operate on a specific cookie.
 */
struct ServerCookieIface {
    virtual ~ServerCookieIface() = default;

    /**
     * Set the DCP connection handler to be used for the connection the
     * provided cookie belongs to.
     *
     * NOTE: No logging or memory allocation is allowed in the impl
     *       of this as ep-engine will not try to set the memory
     *       allocation guard before calling it
     *
     * @param cookie The cookie provided by the core for the operation
     * @param handler The new handler (may be nullptr to clear the handler)
     */
    virtual void setDcpConnHandler(const CookieIface& cookie,
                                   DcpConnHandlerIface* handler) = 0;

    /**
     * Get the DCP connection handler for the connection the provided
     * cookie belongs to
     *
     * NOTE: No logging or memory allocation is allowed in the impl
     *       of this as ep-engine will not try to set the memory
     *       allocation guard before calling it
     *
     * @param cookie The cookie provided by the core for the operation
     * @return The handler stored for the connection (may be nullptr if
     *         none is specified)
     */
    virtual DcpConnHandlerIface* getDcpConnHandler(
            const CookieIface& cookie) = 0;

    /**
     * Let a connection know that IO has completed.
     * @param cookie cookie representing the connection
     * @param status the status for the io operation
     */
    virtual void notify_io_complete(const CookieIface& cookie,
                                    cb::engine_errc status) = 0;

    /**
     * Request the core to schedule a new call to dcp_step() as soon as
     * possible as the underlying engine has data to send.
     *
     * @param cookie cookie representing the connection (MUST be a DCP
     *               connection)
     */
    virtual void scheduleDcpStep(const CookieIface& cookie) = 0;

    /**
     * Notify the core that we're holding on to this cookie for
     * future use. (The core guarantees it will not invalidate the
     * memory until the cookie is invalidated by calling release())
     */
    virtual void reserve(const CookieIface& cookie) = 0;

    /**
     * Notify the core that we're releasing the reference to the
     * The engine is not allowed to use the cookie (the core may invalidate
     * the memory)
     */
    virtual void release(const CookieIface& cookie) = 0;

    /**
     * Set the priority for this connection
     */
    virtual void set_priority(const CookieIface& cookie,
                              ConnectionPriority priority) = 0;

    /**
     * Get the priority for this connection
     */
    virtual ConnectionPriority get_priority(const CookieIface& cookie) = 0;

    /**
     * Get connection id
     *
     * @param cookie the cookie sent to the engine for an operation
     * @return a unique identifier for a connection
     */
    virtual uint64_t get_connection_id(const CookieIface& cookie) = 0;

    /**
     * Check if the cookie have the specified privilege in it's active set.
     *
     * @param cookie the cookie sent to the engine for an operation
     * @param privilege the privilege to check for
     * @param sid the scope id (optional for bucket tests)
     * @param cid the collection id (optional for scope/bucket tests)
     * @throws invalid_argument if cid defined but not sid
     * @return PrivilegeAccess::Ok if the cookie have the privilege in its
     *         active set. PrivilegeAccess::Fail/FailNoPrivileges otherwise
     */
    virtual cb::rbac::PrivilegeAccess check_privilege(
            const CookieIface& cookie,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) = 0;

    virtual cb::rbac::PrivilegeAccess
    check_for_privilege_at_least_in_one_collection(
            const CookieIface& cookie, cb::rbac::Privilege privilege) = 0;

    /**
     * Set the size of the DCP flow control buffer size used by this
     * DCP producer
     *
     * @param cookie the cookie representing the DCP connection
     * @param size The new buffer size
     */
    virtual void setDcpFlowControlBufferSize(const CookieIface& cookie,
                                             std::size_t size) = 0;

    /// Get the revision number for the privilege context for the cookie to
    /// allow the engine to cache the result of a privilege check if locating
    /// the sid / cid is costly.
    virtual uint32_t get_privilege_context_revision(
            const CookieIface& cookie) = 0;

    /**
     * Method to map an engine error code to the appropriate mcbp response
     * code (the client may not support all error codes so we may have
     * to remap some).
     *
     * @param cookie the client cookie (to look up the client connection)
     * @param code the engine error code to get the mcbp response code.
     * @return the mcbp response status to use
     * @throws std::engine_error if the error code results in being
     *                           cb::engine_errc::disconnect after remapping
     *         std::logic_error if the error code doesn't make sense
     *         std::invalid_argument if the code doesn't exist
     */
    virtual cb::mcbp::Status engine_error2mcbp(const CookieIface& cookie,
                                               cb::engine_errc code) = 0;

    /**
     * Get the log information to be used for a log entry.
     *
     * The typical log entry from the core is:
     *
     *  `id> message` - Data read from ta client
     *  `id: message` - Status messages for this client
     *  `id< message` - Data sent back to the client
     *
     * If the caller wants to dump more information about the connection
     * (like socket name, peer name, user name) the pair returns this
     * info as the second field. The info may be invalidated by the core
     * at any time (but not while the engine is operating in a single call
     * from the core) so it should _not_ be cached.
     */
    virtual std::pair<uint32_t, std::string> get_log_info(
            const CookieIface& cookie) = 0;

    virtual std::string get_authenticated_user(const CookieIface& cookie) = 0;

    virtual in_port_t get_connected_port(const CookieIface& cookie) = 0;

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
    virtual void set_error_context(CookieIface& cookie,
                                   std::string_view message) = 0;

    /**
     * Set a JSON object to be included in an error response (along side
     * anything set by set_error_context).
     *
     * The json object cannot include "error" as a top-level key
     *
     * Note this has no affect for the following response codes.
     *   cb::mcbp::Status::Success
     *   cb::mcbp::Status::SubdocSuccessDeleted
     *   cb::mcbp::Status::SubdocMultiPathFailure
     *   cb::mcbp::Status::Rollback
     *   cb::mcbp::Status::NotMyVbucket
     *
     * @param cookie the client cookie (to look up client connection)
     * @param json extra json object to include in a error response.
     */
    virtual void set_error_json_extras(CookieIface& cookie,
                                       const nlohmann::json& json) = 0;

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
     * @param cookie the client cookie (to look up client connection)
     * @param manifestUid id to include in response
     */
    virtual void set_unknown_collection_error_context(CookieIface& cookie,
                                                      uint64_t manifestUid) = 0;

    /// Validate the JSON. This method must NOT be called from a background
    /// thread as it use the front-end-threads instance for a JSON validator
    virtual bool is_valid_json(CookieIface& cookie, std::string_view) = 0;

    /**
     * Send a response to the client (cookie) including the given status and
     * value.
     */
    virtual void send_response(const CookieIface& cookieIface,
                               cb::engine_errc status,
                               std::string_view view) = 0;

    /**
     * Inform the state-machine that the blocking command is now complete.
     * This is instead of the engine calling notifyIOComplete and is for
     * commands that are complete and don't require re-entry to finish.
     */
    virtual void execution_complete(const CookieIface& cookieIface) = 0;
};
