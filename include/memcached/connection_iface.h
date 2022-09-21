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
};