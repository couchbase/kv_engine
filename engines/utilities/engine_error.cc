/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <memcached/engine_error.h>

#include <iostream>
#include <map>
#include <string>

/**
 * engine_error_category provides the mapping from the engine error codes to
 * a textual mapping and is used together with the std::system_error.
 */
class engine_category : public std::error_category {
public:
    const char* name() const noexcept override {
        return "engine error codes";
    }

    std::string message(int code) const override {
        return to_string(cb::engine_errc(code));
    }

    std::error_condition default_error_condition(
            int code) const noexcept override {
        return std::error_condition(code, *this);
    }
};

const std::error_category& cb::engine_error_category() noexcept {
    static engine_category category_instance;
    return category_instance;
}

std::string cb::to_string(cb::engine_errc code) {
    switch (code) {
    case cb::engine_errc::success:
        return "success";
    case cb::engine_errc::no_such_key:
        return "no such key";
    case cb::engine_errc::key_already_exists:
        return "key already exists";
    case cb::engine_errc::no_memory:
        return "no memory";
    case cb::engine_errc::not_stored:
        return "not stored";
    case cb::engine_errc::invalid_arguments:
        return "invalid arguments";
    case cb::engine_errc::not_supported:
        return "not supported";
    case cb::engine_errc::would_block:
        return "would block";
    case cb::engine_errc::too_big:
        return "too big";
    case cb::engine_errc::disconnect:
        return "disconnect";
    case cb::engine_errc::no_access:
        return "no access";
    case cb::engine_errc::not_my_vbucket:
        return "not my vbucket";
    case cb::engine_errc::temporary_failure:
        return "temporary failure";
    case cb::engine_errc::out_of_range:
        return "out of range";
    case cb::engine_errc::rollback:
        return "rollback";
    case cb::engine_errc::no_bucket:
        return "no bucket";
    case cb::engine_errc::too_busy:
        return "too busy";
    case cb::engine_errc::authentication_stale:
        return "authentication stale";
    case cb::engine_errc::delta_badval:
        return "delta bad value";
    case cb::engine_errc::locked:
        return "resource is locked";
    case cb::engine_errc::locked_tmpfail:
        return "resource is locked; tmpfail";
    case cb::engine_errc::failed:
        return "generic failure";
    case cb::engine_errc::unknown_collection:
        return "unknown collection";
    case cb::engine_errc::predicate_failed:
        return "predicate_failed";
    case cb::engine_errc::cannot_apply_collections_manifest:
        return "cannot_apply_collections_manifest";
    case cb::engine_errc::unknown_scope:
        return "unknown scope";
    case engine_errc::durability_impossible:
        return "durability impossible";
    case engine_errc::sync_write_in_progress:
        return "synchronous write in progress";
    case engine_errc::sync_write_ambiguous:
        return "synchronous write ambiguous";
    case engine_errc::dcp_streamid_invalid:
        return "dcp_streamid_invalid";
    case engine_errc::durability_invalid_level:
        return "durability invalid level";
    case engine_errc::sync_write_re_commit_in_progress:
        return "synchronous write re-commit in progress";
    case engine_errc::sync_write_pending:
        return "synchronous write pending";
    case engine_errc::stream_not_found:
        return "stream not found";
    case engine_errc::opaque_no_match:
        return "opaque no match";
    case engine_errc::scope_size_limit_exceeded:
        return "scope size limit exceeded";
    case engine_errc::range_scan_cancelled:
        return "range scan cancelled";
    case engine_errc::range_scan_more:
        return "range scan more";
    case engine_errc::range_scan_complete:
        return "range scan complete";
    case engine_errc::vbuuid_not_equal:
        return "range scan vbuuid unknown";
    case engine_errc::too_many_connections:
        return "too many connections";
    case engine_errc::throttled:
        return "throttled";
    case engine_errc::bucket_paused:
        return "bucket paused";
    case engine_errc::cancelled:
        return "request cancelled";
    };
    throw std::invalid_argument(
        "engine_error_category::message: code does not represent a "
            "legal error code: " + std::to_string(int(code)));
}

void cb::PrintTo(cb::engine_errc ev, ::std::ostream* os) {
    *os << cb::to_string(ev);
}

namespace cb {
std::ostream& operator<<(std::ostream& os, cb::engine_errc ec) {
    os << to_string(ec);
    return os;
}
} // namespace cb
