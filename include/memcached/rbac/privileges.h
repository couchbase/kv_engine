/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <memcached/rbac/visibility.h>

#include <string>

namespace cb {
namespace rbac {

/**
 * The Privilege enum contains all of the Privileges available im memcached.
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
     * The `Tap` privilege allows for setting up a TAP stream
     */
    Tap,
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
    /**
     * The `XattrRead` privilege allows the connection to read the
     * attributes on the documents
     */
    XattrRead,
    /**
     * The `SystemXattrRead` privilege allows the connection to read
     * the system attributes on the document.
     */
    SystemXattrRead,
    /**
     * The `XattrWrite` privilege allows the connection to write to the
     * attributes on the documents
     */
    XattrWrite,
    /**
     * The `SystemXattrWrite` privilege allows the connection to write to the
     * system attributes on the documents
     */
    SystemXattrWrite,
    /**
     * The `CollectionManagement` privilege allows the connection to create or
     * delete collections.
     */
    CollectionManagement,

    /**
     * The `Impersonate` privilege allows the connection to execute commands
     * by using a different authentication context. The intented use is
     * for other components in the system which is part of the TCB so that
     * they don't have to open separate connections to memcached with the
     * users creds to run the command with the users privilege context.
     * For Spock this won't be used as all access is per bucket level, but
     * moving forward we might get per collection/doc access control and at
     * that time we can't have all components in our system to evaluate
     * RBAC access
     */
    Impersonate

    /**
     * Remember to update the rest of the internals of the RBAC module when
     * you add new privileges.
     */
};

enum class PrivilegeAccess { Ok, Fail, Stale };

/**
 * Get a textual representation of the privilege access
 */
RBAC_PUBLIC_API
std::string to_string(const PrivilegeAccess privilegeAccess);

/**
 * Convert a textual string to a Privilege
 *
 * @param str the textual representation of a privilege
 * @return The privilege
 * @throws std::invalid_argument if the text doesn't map to a privilege
 */
RBAC_PUBLIC_API
Privilege to_privilege(const std::string& str);

/**
 * Get the textual representation of a privilege
 */
RBAC_PUBLIC_API
std::string to_string(const Privilege& privilege);

}
}
