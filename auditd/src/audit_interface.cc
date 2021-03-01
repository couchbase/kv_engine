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

#include "audit.h"

#include <logger/logger.h>
#include <memcached/audit_interface.h>
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
        LOG_WARNING("Failed to start audit: Out of memory");
    }

    return {};
}

} // namespace cb::audit
