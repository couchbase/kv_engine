/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <daemon/connection.h>
#include <daemon/cookie.h>

void drop_privilege_executor(Cookie& cookie) {
    cookie.logCommand();
    auto* request = reinterpret_cast<cb::mcbp::DropPrivilegeRequest*>(
            cookie.getPacketAsVoidPtr());

    cb::engine_errc status;
    try {
        const auto key = request->getKey();
        std::string priv{reinterpret_cast<const char*>(key.data()), key.size()};
        auto privilege = cb::rbac::to_privilege(priv);
        status = cookie.getConnection().dropPrivilege(privilege);
    } catch (const std::invalid_argument&) {
        // Invalid name of privilege
        status = cb::engine_errc::no_such_key;
    }

    cookie.sendResponse(status);
}
