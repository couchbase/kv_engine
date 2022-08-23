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
#include <string>

/**
 * The AuditEventFilter allows for checking if a given event should be
 * filtered out or not.
 */
class AuditEventFilter {
public:
    /**
     * Create a new instance
     *
     * @param g The generation for the servers configuration this object
     *          represents
     * @param enabled Is audit filtering enabled or not
     * @param u The list of users which should be filtered out
     */
    AuditEventFilter(uint64_t g,
                     bool enabled,
                     std::vector<cb::rbac::UserIdent> u)
        : generation(g),
          filtering_enabled(enabled),
          disabled_userids(std::move(u)) {
    }

    /// Check if this filter is valid (based on the same configuration as the
    /// audit daemon is currently using)
    bool isValid();

    /**
     * Check to see if the provided event should be filtered out for the
     * provided user.
     *
     * @param id The event to check
     * @param user The user to check
     * @return true if the event should be dropped, false if it should be
     *              submitted to the audit daemon.
     */
    bool isFilteredOut(uint32_t id, const cb::rbac::UserIdent& user);

protected:
    /// Is the provided ID subject to filtering by this filter
    static bool isIdSubjectToFilter(uint32_t id);

    const uint64_t generation;
    const bool filtering_enabled;
    const std::vector<cb::rbac::UserIdent> disabled_userids;
};
