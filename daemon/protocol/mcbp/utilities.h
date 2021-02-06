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

#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <memcached/rbac.h>

#include <stdexcept>

namespace mcbp {
static inline cb::engine_errc checkPrivilege(Cookie& cookie,
                                             cb::rbac::Privilege privilege) {
    return cookie.checkPrivilege(privilege).success()
                   ? cb::engine_errc::success
                   : cb::engine_errc::no_access;
}

static inline cb::engine_errc haveDcpPrivilege(Cookie& cookie) {
    auto ret = checkPrivilege(cookie, cb::rbac::Privilege::DcpProducer);
    if (ret == cb::engine_errc::no_access) {
        ret = checkPrivilege(cookie, cb::rbac::Privilege::DcpConsumer);
    }
    return ret;
}
}
