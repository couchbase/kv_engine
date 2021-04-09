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
#include "audit_configure_context.h"

#include "daemon/mcaudit.h"

#include <daemon/settings.h>
#include <logger/logger.h>
#include <memcached/audit_interface.h>

cb::engine_errc AuditConfigureCommandContext::configuring() {
    // We always want to move into the next state after calling the
    // configure method.
    state = State::Done;

    if (Settings::instance().getAuditFile().empty()) {
        return cb::engine_errc::success;
    }

    auto ret = reconfigure_audit(cookie);
    switch (ret) {
    case cb::engine_errc::success:
    case cb::engine_errc::would_block:
        break;
    default:
        LOG_WARNING("configuration of audit daemon failed with config file: {}",
                    Settings::instance().getAuditFile());
    }

    return ret;
}

void AuditConfigureCommandContext::done() {
    cookie.sendResponse(cb::mcbp::Status::Success);
}
