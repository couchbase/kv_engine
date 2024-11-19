/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string_view>

enum class ConnectionPriority : uint8_t;
namespace cb::rbac {
struct UserIdent;
}
class DcpConnHandlerIface;

/**
 * ConnectionIface is a base class for all of the connection objects in
 * the system. By using an "interface class" we may use different backends
 * for unit tests (mock classes) and in the full system.
 *
 * The primary motivation for the class is to open up for the CookieIface
 * to be bound to a Connection (all cookies belong to a connection), so
 * that we may change the internals of DCP to keep references to Connections
 * instead of the current hack of reusing the same cookie object.
 */
class ConnectionIface {
public:
    ConnectionIface() = delete;
    ConnectionIface(nlohmann::json peername, nlohmann::json sockname)
        : peername(std::move(peername)), sockname(std::move(sockname)) {
    }
    virtual ~ConnectionIface();

    /**
     * Request the core to schedule a new call to dcp_step() as soon as
     * possible as the underlying engine has data to send.
     *
     * @throws std::logic_error if the connection isn't a DCP connection
     */
    virtual void scheduleDcpStep() = 0;

    /// Get the priority for this connection
    virtual ConnectionPriority getPriority() const = 0;

    /// Set the priority for this connection
    virtual void setPriority(ConnectionPriority value) = 0;

    /**
     * Returns a descriptive JSON object for the connection, of the form:
     *   {
     *      "peer": {},
     *      "socket": {},
     *      "user": {"name": "<username>", "system": bool, "ldap": bool},
     *   }
     *
     * Note that the description may be modified in the context of the front end
     * thread as part of authentication so the description should not be cached.
     */
    virtual const nlohmann::json& getDescription() const = 0;

    /**
     * Get the username this connection is authenticated as (should only be
     * called in the context of the front end thread and not be cached as
     * it may updated by the front end threads as part of authentication)
     */
    virtual const cb::rbac::UserIdent& getUser() const = 0;

    const nlohmann::json& getPeername() const {
        return peername;
    }

    const nlohmann::json& getSockname() const {
        return sockname;
    }

    bool isDCP() const {
        return dcpConnHandler.lock()->isDcp;
    }

    /**
     * Set the DCP connection handler to be used for the connection the
     * provided cookie belongs to.
     *
     * NOTE: No logging or memory allocation is allowed in the impl
     *       of this as ep-engine will not try to set the memory
     *       allocation guard before calling it
     *
     * @param handler The new handler
     */
    void setDcpConnHandler(std::shared_ptr<DcpConnHandlerIface> handler) {
        dcpConnHandler.withLock([&handler](DcpConn& dcpConn) {
            dcpConn.isDcp = true;
            dcpConn.dcpConnHandlerIface = std::move(handler);
        });
    }

    /**
     * Get the DCP connection handler for the connection the provided
     * cookie belongs to
     *
     * NOTE: No logging or memory allocation is allowed in the impl
     *       of this as ep-engine will not try to set the memory
     *       allocation guard before calling it
     *
     * @return The handler stored for the connection, by weak_ptr promotion.
     *         Caller must check for a valid pointer.
     */
    std::shared_ptr<DcpConnHandlerIface> getDcpConnHandler() const {
        return dcpConnHandler.lock()->dcpConnHandlerIface.lock();
    }

    /**
     * Set the size of the DCP flow control buffer size used by this
     * DCP producer
     *
     * @param size The new buffer size
     */
    virtual void setDcpFlowControlBufferSize(std::size_t size) {
    }

    /// Get the timestamp for when the connection was created
    const auto& getCreationTimestamp() const {
        return created;
    }

protected:
    const nlohmann::json peername;
    const nlohmann::json sockname;
    const std::chrono::steady_clock::time_point created =
            std::chrono::steady_clock::now();
    std::atomic<DcpConnHandlerIface*> dcpConnHandlerIface{nullptr};
    /// The stored DCP Connection Interface
    struct DcpConn {
        bool isDcp{false};
        std::weak_ptr<DcpConnHandlerIface> dcpConnHandlerIface;
    };
    folly::Synchronized<DcpConn, std::mutex> dcpConnHandler;
};
