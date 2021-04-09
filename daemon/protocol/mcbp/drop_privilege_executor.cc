/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <mcbp/protocol/request.h>

void drop_privilege_executor(Cookie& cookie) {
    cookie.logCommand();
    const auto& request = cookie.getRequest();

    cb::engine_errc status;
    try {
        auto privilege = cb::rbac::to_privilege(request.getPrintableKey());
        status = cookie.getConnection().dropPrivilege(privilege);
    } catch (const std::invalid_argument&) {
        // Invalid name of privilege
        status = cb::engine_errc::no_such_key;
    }

    cookie.sendResponse(status);
}
