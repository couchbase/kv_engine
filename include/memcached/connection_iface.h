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
    virtual ~ConnectionIface();

    /**
     * Request the core to schedule a new call to dcp_step() as soon as
     * possible as the underlying engine has data to send.
     *
     * @throws std::logic_error if the connection isn't a DCP connection
     */
    virtual void scheduleDcpStep() = 0;

    /// The engines gets passed a const reference and they're the ones
    /// to call notifyIoComplete.. cast away the constness to avoid
    /// having to clutter the code everywhere with a const cast (and at
    /// some point we should stop passing it as a const reference from the
    /// engine as they're allowed to call some methods
    void scheduleDcpStep() const {
        const_cast<ConnectionIface*>(this)->scheduleDcpStep();
    }
};