/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "configureevent.h"
#include "audit.h"

#include <logger/logger.h>
#include <memcached/cookie_iface.h>

bool ConfigureEvent::process(AuditImpl& audit) {
    if (audit.reconfigure(file)) {
        cookie.notifyIoComplete(cb::engine_errc::success);
        return true;
    }

    LOG_WARNING_RAW("Audit: error performing configuration");
    cookie.notifyIoComplete(cb::engine_errc::failed);
    return false;
}

bool ConfigureEvent::drop_if_audit_disabled() {
    return false;
}
