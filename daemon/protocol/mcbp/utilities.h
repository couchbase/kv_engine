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

#include "../../memcached.h"

#include <memcached/rbac.h>

/**
 * Get the cookie represented by the void pointer passed as a cookie through
 * the engine interface
 *
 * @param void_cookie this is the void pointer passed to all of the engine
 *                    methods
 * @param function the name of the function trying to convert the cookie. This
 *                 is used purely for error reporting (if void_cookie is null)
 * @return The connection object
 */
McbpConnection* cookie2mcbp(const void* void_cookie,
                            const char* function);

namespace mcbp {
static inline ENGINE_ERROR_CODE checkPrivilege(McbpConnection& connection,
                                               cb::rbac::Privilege privilege) {
    switch (connection.checkPrivilege(privilege)) {
    case cb::rbac::PrivilegeAccess::Ok:
        return ENGINE_SUCCESS;
    case cb::rbac::PrivilegeAccess::Fail:
        return ENGINE_EACCESS;
    case cb::rbac::PrivilegeAccess::Stale:
        return ENGINE_AUTH_STALE;
    }

    // Just to satisfy our commit validator.
    throw std::logic_error("checkPrivilege: internal error");
}

static inline ENGINE_ERROR_CODE haveDcpPrivilege(McbpConnection& connection) {
    auto ret = checkPrivilege(connection,cb::rbac::Privilege::DcpProducer);
    if (ret == ENGINE_EACCESS) {
        ret = checkPrivilege(connection,cb::rbac::Privilege::DcpConsumer);
    }
    return ret;
}
}
