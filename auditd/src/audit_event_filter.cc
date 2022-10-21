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
#include "audit_descriptor_manager.h"

bool AuditEventFilter::isValid() {
    return generation == AuditImpl::generation;
}

bool AuditEventFilter::isIdSubjectToFilter(uint32_t id) {
    const auto* entry = AuditDescriptorManager::instance().lookup(id);
    if (!entry) {
        // failed to look up the descriptor, and we drop unknown
        // events
        return true;
    }

    return entry->isFilteringPermitted();
}

bool AuditEventFilter::isFilteredOut(uint32_t id,
                                     const cb::rbac::UserIdent& uid,
                                     const cb::rbac::UserIdent* euid,
                                     std::optional<std::string_view> bucket,
                                     std::optional<ScopeID> scope,
                                     std::optional<CollectionID> collection) {
    auto isFilteredOut = [this](uint32_t id, const cb::rbac::UserIdent& user) {
        if (!filtering_enabled || !isIdSubjectToFilter(id)) {
            return false;
        }

        return std::find(disabled_userids.begin(),
                         disabled_userids.end(),
                         user) != disabled_userids.end();
    };

    if (euid) {
        return isFilteredOut(id, uid) && isFilteredOut(id, *euid);
    }
    return isFilteredOut(id, uid);
}
