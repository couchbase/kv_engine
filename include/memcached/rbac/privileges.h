/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <string>

namespace cb::rbac {

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
     * The `Insert` privilege allows for inserting data by using the
     * 'add' command.
     */
    Insert,
    /**
     * The `Delete` privilege allows for deleting documents by using
     * the `delete` command.
     */
    Delete,
    /**
     * The `Upsert` privilege allows for adding or modifying documents
     * by using add, set, replace, append/prepend/arithmetic
     */
    Upsert,
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

    /// The `NodeSupervisor` privilege replace all of the various
    /// privileges which should only be granted to "ns_server" in order
    /// to do bucket management, session management, external auth
    /// provider etc.
    NodeSupervisor,

    /// The `Administrator` privilege may be granter to _users_
    /// which should be allowed to run "ioctl", set engine parameters
    /// etc.
    Administrator,

    /**
     * The `Audit` privilege allows for adding audit events to the
     * audit trail
     */
    Audit,
    /**
     * The `DcpConsumer` privilege allows for setting up a DCP consumer in the
     * selected bucket to apply DCP mutations.
     */
    DcpConsumer,
    /**
     * The `DcpProducer` privilege allows for setting up a DCP producer in the
     * selected bucket.
     */
    DcpProducer,
    /**
     * The `DcpStream` privilege allows for setting up a DCP producer stream.
     */
    DcpStream,
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
     * The `SystemXattrRead` privilege allows the connection to read
     * the system attributes on the document.
     */
    SystemXattrRead,
    /**
     * The `SystemXattrWrite` privilege allows the connection to write to the
     * system attributes on the documents
     */
    SystemXattrWrite,

    /**
     * The `BucketThrottleManagement` privilege allows the connection
     * to update the throttle limits for a bucket
     */
    BucketThrottleManagement,

    /// The Unthrottled privilege allows for override throttle limit
    Unthrottled,

    /// The Unmetered privilege allows for skipping recoring metering info
    Unmetered,

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
    Impersonate,

    /// The `Settings` privilege allows the connection to fetch the bucket
    /// configuration via CCCP
    Settings,

    /// The `SystemSettings` privilege allows the connection to fetch the
    /// global system configuration (Cluster topology)
    SystemSettings,

    /// The `SystemCollectionLookup` privilege grants read access to the system
    /// scopes and collections
    SystemCollectionLookup,

    /// The `SystemCollectionMutation` privilege grants write access to the
    /// system scopes and collections
    SystemCollectionMutation,

    /// The 'RangeScan' privilege allows the connection to create, continue
    /// and cancel RangeScan - effectively a bulk read of ranges of keys or
    /// keys/values
    RangeScan,

    // Let RangeScan be the last privilege in the file, as its value is
    // used to size bitsets inside the RBAC module.
};
void to_json(nlohmann::json& json, const Privilege& privilege);

/**
 * Return type of privilege checks
 */
class PrivilegeAccess {
public:
    enum class Status {
        // Requested privilege was found
        Ok,
        // Requested privilege was not found
        Fail,
        // Requested privilege was not found and the user has no privileges
        // relevant to the requested check, e.g. no privileges for a collection
        FailNoPrivileges
    };

    explicit PrivilegeAccess(Status s) : status(s) {
    }

    /// @return if the object represents a successful check
    bool success() const {
        switch (status) {
        case Status::Ok:
            return true;
        case Status::Fail:
        case Status::FailNoPrivileges:
            return false;
        }
        return false;
    }

    /// @return if the object represents a failed check
    bool failed() const {
        return !success();
    }

    bool operator==(const PrivilegeAccess& other) const {
        return status == other.status;
    }

    bool operator!=(const PrivilegeAccess& other) const {
        return status != other.status;
    }

    /// @return the current Status
    Status getStatus() const {
        return status;
    }

    /// @return string of the status
    std::string to_string() const;

    /// @return Status::Ok, required for FunctionChain
    static PrivilegeAccess getSuccessValue() {
        return PrivilegeAccess{Status::Ok};
    }

private:
    Status status{Status::Fail};
};

// declare these to reduce the boilerplate needed due to explicit constructor
const PrivilegeAccess PrivilegeAccessOk{PrivilegeAccess::Status::Ok};
const PrivilegeAccess PrivilegeAccessFail{PrivilegeAccess::Status::Fail};
const PrivilegeAccess PrivilegeAccessFailNoPrivileges{
        PrivilegeAccess::Status::FailNoPrivileges};

/// is this a privilege related to a bucket or not
bool is_bucket_privilege(Privilege);

/// is this a privilege which should be mapped to a scope / collection
bool is_collection_privilege(Privilege);

/**
 * Check to see if the privilege is a legal value or not (to be used in
 * unit tests to sanity check the privilege masks)
 */
bool is_legal_privilege(Privilege);

/**
 * Convert a textual string to a Privilege
 *
 * @param str the textual representation of a privilege
 * @return The privilege
 * @throws std::invalid_argument if the text doesn't map to a privilege
 */
Privilege to_privilege(const std::string& str);

/// Get the textual representation of a privilege
std::string format_as(Privilege privilege);
} // namespace cb::rbac
