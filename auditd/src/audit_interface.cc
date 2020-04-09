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

#include <logger/logger.h>
#include <memcached/audit_interface.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <platform/strerror.h>
#include <algorithm>
#include <cstring>
#include <sstream>

#include "audit.h"
#include "auditd_audit_events.h"
#include "event.h"

namespace cb::audit {

static std::string gethostname() {
    char host[128];
    if (::gethostname(host, sizeof(host)) != 0) {
        throw std::runtime_error("gethostname() failed: " + cb_strerror());
    }

    return std::string(host);
}

UniqueAuditPtr create_audit_daemon(const std::string& config_file,
                                   ServerCookieIface* server_cookie_api) {
    if (!cb::logger::isInitialized()) {
        throw std::invalid_argument(
                "start_auditdaemon: logger must have been created");
    }

    try {
        UniqueAuditPtr holder;
        holder.reset(
                new AuditImpl(config_file, server_cookie_api, gethostname()));
        return holder;
    } catch (std::runtime_error& err) {
        LOG_WARNING("{}", err.what());
    } catch (std::bad_alloc&) {
        LOG_WARNING("Failed to start audit: Out of memory");
    }

    return {};
}

} // namespace cb::audit
