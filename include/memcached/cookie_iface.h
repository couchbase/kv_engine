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

#include <memcached/tracer.h>
#include <memory>
#include <optional>

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
class Tenant;
using protocol_binary_datatype_t = uint8_t;

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
    /// Get the identifier user for logging for all cookies bound to this
    /// connection.
    virtual uint32_t getConnectionId() const = 0;

    /// Get the tenant the cookie is bound to (NOTE: may not be set to
    /// a tenant)
    virtual std::shared_ptr<Tenant> getTenant() = 0;

    // The underlying engine may store information bound to the given cookie
    // in an opaque pointer. The framework will _NOT_ take ownership of the
    // data, and the engine must deal with all allocation/deallocation of the
    // memory to avoid any leaks.

    /// Get the value stored for the cookie (or nullptr if nothing was stored).
    /// The function returns the same value until it is being set to another
    /// value by callin setEngineStorage
    virtual void* getEngineStorage() const = 0;
    /// Set the engine pointer to the provided value
    virtual void setEngineStorage(void* value) = 0;

    /// Check if mutation extras is supported by the connection.
    virtual bool isMutationExtrasSupported() const = 0;
    /// Check if collections is supported by the connection
    virtual bool isCollectionsSupported() const = 0;

    /// Check if the requested datatype is supported by the connection.
    virtual bool isDatatypeSupported(
            protocol_binary_datatype_t datatype) const = 0;

    /**
     * Test if the cookie posess the requested privilege in its effective
     * set.
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

    /// Get the payload from the command.
    virtual std::string_view getInflatedInputPayload() const = 0;
};
