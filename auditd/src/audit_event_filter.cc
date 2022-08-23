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

bool AuditEventFilter::isIdSubjectToFilter(uint32_t id) {
    if (id < MEMCACHED_AUDIT_OPENED_DCP_CONNECTION ||
        id > MEMCACHED_AUDIT_TENANT_RATE_LIMITED ||
        id == MEMCACHED_AUDIT_AUTHENTICATION_FAILED ||
        id == MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE ||
        id == MEMCACHED_AUDIT_PRIVILEGE_DEBUG ||
        id == MEMCACHED_AUDIT_PRIVILEGE_DEBUG_CONFIGURED) {
        // This isn't a memcached generated audit event, or it isn't
        // allowed for filtering
        return false;
    }
    return true;
}

bool AuditEventFilter::isFilteredOut(uint32_t id,
                                     const cb::rbac::UserIdent& user) {
    if (!filtering_enabled || !isIdSubjectToFilter(id)) {
        return false;
    }

    return std::find(disabled_userids.begin(), disabled_userids.end(), user) !=
           disabled_userids.end();
}
