/*
 *     Copyright 2022 Couchbase, Inc
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

#include "audit_event_filter.h"

#include "audit.h"
#include "memcached_audit_events.h"

bool AuditEventFilter::isValid() {
    return generation == AuditImpl::generation;
}

bool AuditEventFilter::isFilteredOut(uint32_t id,
                                     const std::string& user,
                                     cb::rbac::Domain domain) {
    if (!filtering_enabled) {
        return false;
    }

    if (id < MEMCACHED_AUDIT_OPENED_DCP_CONNECTION ||
        id > MEMCACHED_AUDIT_SESSION_TERMINATED ||
        id == MEMCACHED_AUDIT_AUTHENTICATION_FAILED ||
        id == MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE ||
        id == MEMCACHED_AUDIT_PRIVILEGE_DEBUG) {
        // This isn't a memcached generated audit event or it isn't
        // allowed for filtering
        return false;
    }

    // The list of users is most likely smaller than the list of ids
    for (const auto& pair : disabled_userids) {
        // Checking the domain first as it just 1 byte
        if (pair.second == domain && pair.first == user) {
            return true;
        }
    }

    return false;
}
