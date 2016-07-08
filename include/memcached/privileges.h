/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#ifdef __cplusplus

/**
 * RBAC in memcached
 *
 * The RBAC implemented in memcached before Spock used the opcode of
 * the command to base it's access control upon. That was a fast and
 * simple way to perform access control, because the check was
 * basically an array lookup. Unfortunately that model didn't solve
 * the problem where we had one opcode which had a sub-command (like
 * stats) where we would like to mask out some data for some users.
 *
 * Moving to Spock we're adopting more or less the same model as
 * being used in ns_server. We're dropping the concept of active
 * roles (and the least privilege concept), and instead the privilege
 * mask is the aggregated set of all privileges available in each
 * of the allowed roles (sad but true). In order to avoid having
 * to define the privileges for both the memcached binary protocol
 * and the upcoming greenstack protocol, we've decided to define a
 * small set of privileges instead (see below).
 *
 * The memcached core (as part of the command parser) has knowledge
 * of the actual command being executed, and will perform a first
 * screening of the command (for instance the access control check
 * for a get operation may check for the read privilege to the
 * current bucket). Given our current engine API we can't do all
 * of the checks in the core (because some commands only exists
 * in the underlying engine, or for instance stats which have a
 * single entry point and we should be able to separate between
 * simple and detailed stats through a privilege). In these
 * situations the underlying engine have to do the privilege check
 * (until we've refactored the code so that we can do everything
 * in the core, but I don't think we'll be able to do that in
 * the Spock timeframe). This means that the engine will have
 * to do something like:
 *
 * <pre>
 *     if (!sapi->cookie.check_privilege(cookie, Privilege::MetaRead)) {
 *         return ENGINE_EACCESS;
 *     }
 * <pre>
 *
 * We've not yet decided on how memcached shall receive the RBAC
 * configuration defined by the user. There is currently two alternatives
 *
 * 1) Use the component in `ns_server`
 *       + The component already exists; only minor changes is needed
 *         to add support for our privileges
 *       + No need to standardize the "file format" for the RBAC data
 *       + Only one implementation of the access evaluation
 *             + The same bugs exists everywhere
 *       - memcached depends on the availability of another component
 *             - what to do if it refuse to answer on the port
 *             - what to do if we're having "network" failures
 *             - what to do if we're failing to create a socket to perform
 *               the rest call (running out of file descriptors)
 *       - We need to create a mock of the server for our unit testing
 *             - need to implement enough failure scenarios to ensure we handle
 *               all kinds of problems
 *       - Is it fast engough, and does it scale?
 *       - We need to add support for HTTP in memcached
 *       - Cache invalidation
 *             - We would have to generate the privilege set every time
 *               the user selects a bucket (or performs a new auth), but
 *               we would have no clue when to invalidate this. We _could_
 *               add a TTL for the privilege set for let's say 1h to avoid
 *               having to reboot all nodes to kick out all users which no
 *               longer have access to the privileges.
 *
 * 2) Let `memcached` implement the access control
 *       + No external process dependencies affecting the availability or
 *         error situations at runtime
 *       - Need a fixed file format between `ns_server` and `memcached` and
 *         a way to signal memcached that the files changed.
 *             - Need to implement parser in multiple components
 *             + Need to create test suite to ensure that all components
 *               produce the same result of the given input
 *       + Extremely fast and scalable. Everything is lookup of internal
 *         datastructures in a "copy on write" mode. Lock only held in
 *         order to create a shared pointer to the datastructures. Cache
 *         invalidation performed by looking at two atomic variables.
 *       - No need for a mock server, all tests may be performed with
 *         real configuration data.
 *       + Simpler logic in `memcached`
 *             + No need to suspend a connection in the middle of the AUTH /
 *               Select-Bucket phase in order to request the privilege
 *               set from `ns_server`
 *
 * The rest of this task is currently blocked while we're resolving these
 * questions.
 */
enum class Privilege {
    /**
     * The `Read` privilege allows for reading documents in the selected
     * bucket.
     */
    Read,
    /**
     * The `Write` privilege allows for creating, updating or deleting
     * documents in the selected bucket.
     */
    Write,
    /**
     * The `SimpleStats` privilege allows for requesting basic statistics
     * information from the system (restricted to the selected bucket)
     */
    SimpleStats,
    /**
     * The `Stats` privilege allows for requesting all the statistics
     * information in the system (system configuration, vbucket state,
     * dcp information etc).
     */
    Stats,
    /**
     * The `BucketManagement` privilege allows for bucket management
     * (create or delete buckets, toggle vbucket states etc).
     */
    BucketManagement,
    /**
     * The `NodeManagement` privilege allows for changing verbosity
     * level, reloading configuration files (This privilege should
     * be split into multiple others)
     */
    NodeManagement,
    /**
     * The `SessionManagement` privilege allows for changing (and fetching)
     * the session context registered by ns_server
     */
    SessionManagement,
    /**
     * The `Audit` privilege allows for adding audit events to the
     * audit trail
     */
    Audit,
    /**
     * The `AuditManagement` privilege allows for reconfigure audit
     * subsystem
     */
    AuditManagement,
    /**
     * The `DcpConsumer` privilege allows for setting up a DCP stream in the
     * selected bucket to apply DCP mutations.
     */
    DcpConsumer,
    /**
     * The `DcpProducer` privilege allows for setting up a DCP stream in the
     * selected bucket.
     */
    DcpProducer,
    /**
     * All use of the `DCP` privilege should be replaced with `DcpConsumer`
     * and `DcpProducer`.
     */
    DCP,

    /**
     * The `TapProducer` privilege allows for setting up a TAP stream
     */
    TapProducer,
    /**
     * The `TapConsumer` privilege allows for consuming TAP events
     */
    TapConsumer,
    /**
     * The `MetaRead` privilege allows for reading the meta information
     * on documents.
     */
    MetaRead,
    /**
     * The `MetaWrite` privilege allows for updating the meta information
     * on documents.
     */
    MetaWrite,
    /**
     * The `IdleConnection` privilege allows a client to hold on to an
     * idle connection witout being disconnected.
     */
    IdleConnection,
};

enum class PrivilegeAccess {
    Ok,
    Fail,
    Stale
};
#else
/**
 * We still have a ton of C code in our system, but I don't want
 * to pollute the namespace with all of these names. All new code should
 * be C++ so I'm just going to create this dummy enum to put in the
 * interface
 */
typedef enum memcached_privilege_t {
    MEMCACHED_PRIVILEGE_READ
} Privilege;

typedef enum memcached_privilege_access_t {
    MEMCACHED_PRIVILEGE_ACCESS_OK
} PrivilegeAccess;

#endif
