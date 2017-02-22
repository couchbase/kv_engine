/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include <string>

class Connection;
class McbpConnection;

/**
 * Send an audit event for an authentication failure
 *
 * @param c the connection object performing the sasl auth
 * @param reason the textual description of why auth failed
 */
void audit_auth_failure(const Connection* c, const char* reason);

/**
 * Send an audit event for a successful authentication
 *
 * @param c the connection object performing the sasl auth
 */
void audit_auth_success(const Connection* c);

/**
 * Send an audit event for bucket flush
 * @param c the connection performing the operation
 * @param bucket the name of the bucket
 */
void audit_bucket_flush(const Connection* c, const char* bucket);

/**
 * Send an audit event for a DCP Open
 *
 * param c the connection object performing DCP Open
 */
void audit_dcp_open(const Connection* c);

/*
 * Send an audit event for command access failure
 *
 * param c the connection object performing DCP Open
 */
void audit_command_access_failed(const McbpConnection* c);

/**
 * Send an audit event for a invalid and thus rejected packet
 *
 * param c the connection object performing DCP Open
 */
void audit_invalid_packet(const McbpConnection* c);

/**
 * Send an audit event for the change in privilege debug
 *
 * @param c the connection object toggling privilege debug
 * @param enable if privilege debug is enabled or disabled
 */
void audit_set_privilege_debug_mode(const Connection* c, bool enable);

/**
 * Send an audit event for privilege debug allowing a command to pass
 *
 * @param c the connection object performing the operation
 * @param command the command being executed
 * @param bucket the bucket where the command is executed
 * @param privilege the privilege granted
 * @param context the current privilege context
 */
void audit_privilege_debug(const Connection* c,
                           const std::string& command,
                           const std::string& bucket,
                           const std::string& privilege,
                           const std::string& context);

/**
 * Initialize the audit subsystem
 */
void initialize_audit();
