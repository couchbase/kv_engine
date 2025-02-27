/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/connection_iface.h>
#include <platform/cb_arena_malloc.h>

ConnectionIface::~ConnectionIface() {
    // The dcpConnHandlerIface owns memory from the bucket which created the
    // DcpConnHandler object, it must be freed under that bucket context
    dcpConnHandler.withLock([this](auto& dcpConn) {
        cb::ArenaMallocGuard guard(dcpConn.allocatedBy);
        dcpConn.dcpConnHandlerIface.reset();
    });
}
