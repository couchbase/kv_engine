/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "token_auth_data.h"
#include <memcached/rbac.h>

TokenAuthData::TokenAuthData(
        cb::rbac::UserIdent user_,
        std::unique_ptr<cb::rbac::UserEntry> entry_,
        std::optional<std::chrono::system_clock::time_point> begin,
        std::optional<std::chrono::system_clock::time_point> end)
    : entry(std::move(entry_)), user(std::move(user_)), lifetime(begin, end) {
}

TokenAuthData::Lifetime::Lifetime(
        std::optional<std::chrono::system_clock::time_point> begin_,
        std::optional<std::chrono::system_clock::time_point> end_) {
    using namespace std::chrono;
    const auto system_now = system_clock::now();
    const auto steady_now = steady_clock::now();
    auto setValue = [&system_now, &steady_now](auto& field, auto& tp) {
        if (tp.has_value()) {
            if (system_now < *tp) {
                // Convert the system clock to offset from the steady clock as
                // that's cheaper to read
                field = steady_now + duration_cast<seconds>(*tp - system_now);
            } else {
                field = steady_now;
            }
        } else {
            field.reset();
        }
    };
    setValue(begin, begin_);
    setValue(end, end_);
}
