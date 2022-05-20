/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <memcached/audit_interface.h>
#include <memcached/engine_common.h>
#include <memcached/engine_error.h>
#include <memcached/rbac/privilege_database.h>
#include <cstdint>
#include <string>

class Cookie;
class Connection;
class StatCollector;

/**
 * Send an audit event for an authentication failure
 *
 * @param c the connection object performing the sasl auth
 * @param ui the provided user ident used for bucket auth
 * @param reason the textual description of why auth failed
 */
void audit_auth_failure(const Connection& c,
                        const cb::rbac::UserIdent& ui,
                        const char* reason,
                        Cookie* cookie = nullptr);

/**
 * Send an audit event for a successful authentication
 *
 * @param c the connection object performing the sasl auth
 */
void audit_auth_success(const Connection& c, Cookie* cookie = nullptr);

/**
 * Send an audit event for that the specified connection
 * changed the active bucket
 *
 * @param c the connection selected the bucket
 */
void audit_bucket_selection(const Connection& c, Cookie* cookie = nullptr);

/**
 * Send an audit event for bucket flush
 * @param c the cookie performing the operation
 * @param bucket the name of the bucket
 */
void audit_bucket_flush(Cookie& c, const char* bucket);

/**
 * Send an audit event for a DCP Open
 *
 * param c the connection object performing DCP Open
 */
void audit_dcp_open(Cookie& c);

/*
 * Send an audit event for command access failure
 *
 * @param cookie the cookie repreresenting the operation
 */
void audit_command_access_failed(Cookie& cookie);

/**
 * Send an audit event for a invalid and thus rejected packet
 *
 * @param c the connection the packet arrived on
 * @param packet the packet to dump in the audit event
 */
void audit_invalid_packet(const Connection& c, cb::const_byte_buffer packet);

/**
 * Send an audit event for the change in privilege debug
 *
 * @param c the connection object toggling privilege debug
 * @param enable if privilege debug is enabled or disabled
 */
void audit_set_privilege_debug_mode(Cookie& cookie, bool enable);

/**
 * Send an audit event for privilege debug allowing a command to pass
 *
 * @param c the connection object performing the operation
 * @param command the command being executed
 * @param bucket the bucket where the command is executed
 * @param privilege the privilege granted
 * @param context the current privilege context
 */
void audit_privilege_debug(Cookie& cookie,
                           const std::string& command,
                           const std::string& bucket,
                           const std::string& privilege,
                           const std::string& context);

namespace cb::audit {

/// Enable / disable a given memcached audit event (when set to disabled
/// the audit event is discarded immediately inside mcaudit before it is
/// sent to the audit daemon (which allows for unit tests to run code
/// which performs audit calls without having the audit daemon running)
void setEnabled(uint32_t id, bool enable);

/**
 *  Add an audit event that the connection is terminated
 *
 * @param c the connection object closing the connection
 */
void addSessionTerminated(const Connection& c);

namespace document {
enum class Operation;

void add(Cookie& c, Operation operation);
} // namespace document
} // namespace cb::audit

/**
 * Initialize the audit subsystem
 */
void initialize_audit();
void shutdown_audit();
cb::engine_errc reconfigure_audit(Cookie& cookie);
void stats_audit(const StatCollector& collector, Cookie* cookie = nullptr);

bool mc_audit_event(Cookie& cookie,
                    uint32_t audit_eventid,
                    cb::const_byte_buffer payload);
