/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "engine_error.h"
#include "protocol_binary.h"
#include "rbac.h"
#include "types.h"

#include <mcbp/protocol/opcode.h>
#include <nlohmann/json_fwd.hpp>
#include <gsl/gsl>
#include <string>

namespace cb {
namespace mcbp {
class Request;
} // namespace mcbp
} // namespace cb

/**
 * Commands to operate on a specific cookie.
 */
struct ServerCookieIface {
    virtual ~ServerCookieIface() = default;

    /**
     * Store engine-specific session data on the given cookie.
     *
     * The engine interface allows for a single item to be
     * attached to the connection that it can use to track
     * connection-specific data throughout duration of the
     * connection.
     *
     * @param cookie The cookie provided by the frontend
     * @param engine_data pointer to opaque data
     */
    virtual void store_engine_specific(gsl::not_null<const void*> cookie,
                                       void* engine_data) = 0;

    /**
     * Retrieve engine-specific session data for the given cookie.
     *
     * @param cookie The cookie provided by the frontend
     *
     * @return the data provied by store_engine_specific or NULL
     *         if none was provided
     */
    virtual void* get_engine_specific(gsl::not_null<const void*> cookie) = 0;

    /**
     * Check if datatype is supported by the connection.
     *
     * @param cookie The cookie provided by the frontend
     * @param datatype The datatype to test
     *
     * @return true if connection supports the datatype or else false.
     */
    virtual bool is_datatype_supported(gsl::not_null<const void*> cookie,
                                       protocol_binary_datatype_t datatype) = 0;

    /**
     * Check if mutation extras is supported by the connection.
     *
     * @param cookie The cookie provided by the frontend
     *
     * @return true if supported or else false.
     */
    virtual bool is_mutation_extras_supported(
            gsl::not_null<const void*> cookie) = 0;

    /**
     * Check if collections are supported by the connection.
     *
     * @param cookie The cookie provided by the frontend
     *
     * @return true if supported or else false.
     */
    virtual bool is_collections_supported(
            gsl::not_null<const void*> cookie) = 0;

    /**
     * Retrieve the opcode of the connection, if
     * ewouldblock flag is set. Please note that the ewouldblock
     * flag for a connection is cleared before calling into
     * the engine interface, so this method only works in the
     * notify hooks.
     *
     * @param cookie The cookie provided by the frontend
     *
     * @return the opcode from the binary_header saved in the
     * connection.
     */
    virtual cb::mcbp::ClientOpcode get_opcode_if_ewouldblock_set(
            gsl::not_null<const void*> cookie) = 0;

    /**
     * Validate given ns_server's session cas token against
     * saved token in memached, and if so incrment the session
     * counter.
     *
     * @param cas The cas token from the request
     *
     * @return true if session cas matches the one saved in
     * memcached
     */
    virtual bool validate_session_cas(uint64_t cas) = 0;

    /**
     * Decrement session_cas's counter everytime a control
     * command completes execution.
     */
    virtual void decrement_session_ctr() = 0;

    /**
     * Let a connection know that IO has completed.
     * @param cookie cookie representing the connection
     * @param status the status for the io operation
     */
    virtual void notify_io_complete(gsl::not_null<const void*> cookie,
                                    ENGINE_ERROR_CODE status) = 0;

    /**
     * Notify the core that we're holding on to this cookie for
     * future use. (The core guarantees it will not invalidate the
     * memory until the cookie is invalidated by calling release())
     */
    virtual ENGINE_ERROR_CODE reserve(gsl::not_null<const void*> cookie) = 0;

    /**
     * Notify the core that we're releasing the reference to the
     * The engine is not allowed to use the cookie (the core may invalidate
     * the memory)
     */
    virtual ENGINE_ERROR_CODE release(gsl::not_null<const void*> cookie) = 0;

    /**
     * Set the priority for this connection
     */
    virtual void set_priority(gsl::not_null<const void*> cookie,
                              CONN_PRIORITY priority) = 0;

    /**
     * Get the priority for this connection
     */
    virtual CONN_PRIORITY get_priority(gsl::not_null<const void*> cookie) = 0;

    /**
     * Get the bucket the connection is bound to
     *
     * @cookie The connection object
     * @return the bucket identifier for a cookie
     */
    virtual bucket_id_t get_bucket_id(gsl::not_null<const void*> cookie) = 0;

    /**
     * Get connection id
     *
     * @param cookie the cookie sent to the engine for an operation
     * @return a unique identifier for a connection
     */
    virtual uint64_t get_connection_id(gsl::not_null<const void*> cookie) = 0;

    /**
     * Check if the cookie have the specified privilege in it's active set.
     *
     * @param cookie the cookie sent to the engine for an operation
     * @param privilege the privilege to check for
     * @param sid the scope id (optional for bucket tests)
     * @param cid the collection id (optional for scope/bucket tests)
     * @throws invalid_argument if cid defined but not sid
     * @return PrivilegeAccess::Ok if the cookie have the privilege in its
     *         active set PrivilegeAccess::Fail otherwise
     */
    virtual cb::rbac::PrivilegeAccess check_privilege(
            gsl::not_null<const void*> cookie,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) = 0;

    /// Get the revision number for the privilege context for the cookie to
    /// allow the engine to cache the result of a privilege check if locating
    /// the sid / cid is costly.
    virtual uint32_t get_privilege_context_revision(
            gsl::not_null<const void*> cookie) = 0;

    /**
     * Method to map an engine error code to the appropriate mcbp response
     * code (the client may not support all error codes so we may have
     * to remap some).
     *
     * @param cookie the client cookie (to look up the client connection)
     * @param code the engine error code to get the mcbp response code.
     * @return the mcbp response status to use
     * @throws std::engine_error if the error code results in being
     *                           ENGINE_DISCONNECT after remapping
     *         std::logic_error if the error code doesn't make sense
     *         std::invalid_argument if the code doesn't exist
     */
    virtual cb::mcbp::Status engine_error2mcbp(
            gsl::not_null<const void*> cookie, ENGINE_ERROR_CODE code) = 0;

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
            gsl::not_null<const void*> cookie) = 0;

    virtual std::string get_authenticated_user(
            gsl::not_null<const void*> cookie) = 0;

    virtual in_port_t get_connected_port(gsl::not_null<const void*> cookie) = 0;

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
    virtual void set_error_context(gsl::not_null<void*> cookie,
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
    virtual void set_error_json_extras(gsl::not_null<void*> cookie,
                                       const nlohmann::json& json) = 0;

    /**
     * Get the inflated payload associated with this command
     * @param cookie The cookie representing this command
     * param request The request (only used by the mock test framework
     *               to inflate on the fly)
     * @return the inflated payload
     */
    virtual std::string_view get_inflated_payload(
            gsl::not_null<const void*> cookie,
            const cb::mcbp::Request& request) = 0;
};
