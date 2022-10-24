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

/**
 * The AuditEventFilter allows for checking if a given event should be
 * filtered out or not.
 */
class AuditEventFilterImpl : public AuditEventFilter {
public:
    /**
     * Create a new instance
     *
     * @param g The generation for the servers configuration this object
     *          represents
     * @param enabled Is audit filtering enabled or not
     * @param u The list of users which should be filtered out
     */
    AuditEventFilterImpl(uint64_t g,
                         bool enabled,
                         std::vector<cb::rbac::UserIdent> u)
        : generation(g),
          filtering_enabled(enabled),
          disabled_userids(std::move(u)) {
    }

    /// Check if this filter is valid (based on the same configuration as the
    /// audit daemon is currently using)
    bool isValid() const override;

    /**
     * Check to see if the provided event should be filtered out for the
     * provided user.
     *
     * @param id The event to check
     * @param uid The user to check
     * @param euid The effective user if it is different from uid
     * @param bucket The optional bucket for the event
     * @param scope The optional scope for the event
     * @param collection The optional collection for the event
     * @return true if the event should be dropped, false if it should be
     *              submitted to the audit daemon.
     */
    bool isFilteredOut(uint32_t id,
                       const cb::rbac::UserIdent& uid,
                       const cb::rbac::UserIdent* euid,
                       std::optional<std::string_view> bucket,
                       std::optional<ScopeID> scope,
                       std::optional<CollectionID> collection) const override;

protected:
    /// Is the provided ID subject to filtering by this filter
    static bool isIdSubjectToFilter(uint32_t id);

    const uint64_t generation;
    const bool filtering_enabled;
    const std::vector<cb::rbac::UserIdent> disabled_userids;
};

std::unique_ptr<AuditEventFilter> AuditEventFilter::create(
        uint64_t g, bool enabled, std::vector<cb::rbac::UserIdent> u) {
    return std::make_unique<AuditEventFilterImpl>(g, enabled, u);
}

bool AuditEventFilterImpl::isValid() const {
    return generation == AuditImpl::generation;
}

bool AuditEventFilterImpl::isIdSubjectToFilter(uint32_t id) {
    const auto* entry = AuditDescriptorManager::instance().lookup(id);
    if (!entry) {
        // failed to look up the descriptor, and we drop unknown
        // events
        return true;
    }

    return entry->isFilteringPermitted();
}

bool AuditEventFilterImpl::isFilteredOut(
        uint32_t id,
        const cb::rbac::UserIdent& uid,
        const cb::rbac::UserIdent* euid,
        std::optional<std::string_view> bucket,
        std::optional<ScopeID> scope,
        std::optional<CollectionID> collection) const {
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
