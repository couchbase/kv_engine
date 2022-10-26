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
#pragma once

#include <memcached/rbac/privilege_database.h>
#include <cstdint>
#include <memory>
#include <string_view>

/**
 * The AuditEventFilter allows for checking if a given event should be
 * filtered out or not.
 */
class AuditEventFilter {
public:
    virtual ~AuditEventFilter() = default;

    /**
     * Create a new filter based on the provided JSON
     *
     * @param g The generation for the servers configuration this object
     *          represents
     * @param json The configuration to use (empty == everything disabled)
     */
    static std::unique_ptr<AuditEventFilter> create(uint64_t g,
                                                    const nlohmann::json& json);

    /// Clone this configuration
    virtual std::unique_ptr<AuditEventFilter> clone() const = 0;

    /// Check if this filter is valid (based on the same configuration as the
    /// audit daemon is currently using)
    virtual bool isValid() const = 0;

    /// Get a JSON description of this filter
    virtual nlohmann::json to_json() const = 0;

    /**
     * Check if the provided identifier is enabled globally (or in the provided
     * bucket). This may be used as an optimization to bypass parsing JSON.
     */
    virtual bool isEnabled(uint32_t id,
                           std::optional<std::string_view> bucket) const = 0;

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
    virtual bool isFilteredOut(
            uint32_t id,
            const cb::rbac::UserIdent& uid,
            const cb::rbac::UserIdent* euid,
            std::optional<std::string_view> bucket,
            std::optional<ScopeID> scope,
            std::optional<CollectionID> collection) const = 0;
};
