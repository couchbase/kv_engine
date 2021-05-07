/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "audit.h"

#include <logger/logger.h>
#include <memcached/audit_interface.h>
#include <platform/socket.h>
#include <stdexcept>

namespace cb::audit {

UniqueAuditPtr create_audit_daemon(const std::string& config_file,
                                   ServerCookieIface* server_cookie_api) {
    if (!cb::logger::isInitialized()) {
        throw std::invalid_argument(
                "create_audit_daemon: logger must have been created");
    }

    try {
        return std::make_unique<AuditImpl>(
                config_file, server_cookie_api, cb::net::getHostname());
    } catch (std::runtime_error& err) {
        LOG_WARNING("{}", err.what());
    } catch (std::bad_alloc&) {
        LOG_WARNING_RAW("Failed to start audit: Out of memory");
    }

    return {};
}

} // namespace cb::audit
