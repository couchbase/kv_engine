/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/rbac/privilege_database.h>
#include <chrono>

/// The token auth data class holds the user entity and the optional
/// lifefime for the tokens authentication data
class TokenAuthData {
public:
    TokenAuthData(cb::rbac::UserIdent user_,
                  std::unique_ptr<cb::rbac::UserEntry> entry_,
                  std::optional<std::chrono::system_clock::time_point> begin,
                  std::optional<std::chrono::system_clock::time_point> end);

    bool isStale(const std::chrono::steady_clock::time_point now) const {
        return lifetime.isStale(now);
    }

    auto& getUser() const {
        return user;
    }

    auto& getUserEntry() const {
        return *entry;
    }

protected:
    struct Lifetime {
        Lifetime(std::optional<std::chrono::system_clock::time_point> begin_,
                 std::optional<std::chrono::system_clock::time_point> end_);
        bool isStale(const std::chrono::steady_clock::time_point now) const {
            return (end.has_value() && *end < now) ||
                   (begin.has_value() && *begin > now);
        }
        std::optional<std::chrono::steady_clock::time_point> begin;
        std::optional<std::chrono::steady_clock::time_point> end;
    };
    std::unique_ptr<cb::rbac::UserEntry> entry;
    const cb::rbac::UserIdent user;
    const Lifetime lifetime;
};
