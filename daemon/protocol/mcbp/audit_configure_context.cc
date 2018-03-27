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
#include "audit_configure_context.h"

#include <daemon/cookie.h>
#include <daemon/runtime.h>
#include <daemon/settings.h>
#include <memcached/audit_interface.h>

ENGINE_ERROR_CODE AuditConfigureCommandContext::configuring() {
    // We always want to move into the next state after calling the
    // configure method.
    state = State::Done;

    if (settings.getAuditFile().empty()) {
        return ENGINE_SUCCESS;
    }

    auto ret = configure_auditdaemon(get_audit_handle(),
                                     settings.getAuditFile().c_str(),
                                     static_cast<void*>(&cookie));
    switch (ret) {
    case AUDIT_SUCCESS:
        return ENGINE_SUCCESS;
    case AUDIT_EWOULDBLOCK:
        return ENGINE_EWOULDBLOCK;
    default:
        LOG_WARNING("configuration of audit daemon failed with config file: {}",
                    settings.getAuditFile());
    }

    return ENGINE_FAILED;
}

void AuditConfigureCommandContext::done() {
    cookie.sendResponse(cb::mcbp::Status::Success);
}
