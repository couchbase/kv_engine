/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <daemon/cookie.h>
#include <memcached/engine_error.h>
#include <memcached/rbac.h>

namespace mcbp {
static inline cb::engine_errc checkPrivilege(Cookie& cookie,
                                             cb::rbac::Privilege privilege) {
    return cookie.checkPrivilege(privilege).success()
                   ? cb::engine_errc::success
                   : cb::engine_errc::no_access;
}
}
