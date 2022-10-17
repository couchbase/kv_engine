/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "engine_errc_2_mcbp.h"

cb::mcbp::Status cb::mcbp::to_status(cb::engine_errc code) {
    using namespace cb;
    using namespace cb::mcbp;

    switch (code) {
    case engine_errc::no_access:
        return Status::Eaccess;
    case engine_errc::success:
        return Status::Success;
    case engine_errc::no_such_key:
        return Status::KeyEnoent;
    case engine_errc::key_already_exists:
        return Status::KeyEexists;
    case engine_errc::no_memory:
        return Status::Enomem;
    case engine_errc::temporary_failure:
        return Status::Etmpfail;
    case engine_errc::not_stored:
        return Status::NotStored;
    case engine_errc::invalid_arguments:
        return Status::Einval;
    case engine_errc::not_supported:
        return Status::NotSupported;
    case engine_errc::too_big:
        return Status::E2big;
    case engine_errc::not_my_vbucket:
        return Status::NotMyVbucket;
    case engine_errc::out_of_range:
        return Status::Erange;
    case engine_errc::rollback:
        return Status::Rollback;
    case engine_errc::no_bucket:
        return Status::NoBucket;
    case engine_errc::too_busy:
        return Status::Ebusy;
    case engine_errc::authentication_stale:
        return Status::AuthStale;
    case engine_errc::delta_badval:
        return Status::DeltaBadval;
    case engine_errc::locked:
    case engine_errc::locked_tmpfail:
        return Status::Locked;
    case engine_errc::unknown_collection:
        return Status::UnknownCollection;
    case engine_errc::cannot_apply_collections_manifest:
        return Status::CannotApplyCollectionsManifest;
    case engine_errc::unknown_scope:
        return Status::UnknownScope;
    case engine_errc::failed:
        return Status::Einternal;
    case engine_errc::durability_invalid_level:
        return Status::DurabilityInvalidLevel;
    case engine_errc::durability_impossible:
        return Status::DurabilityImpossible;
    case engine_errc::sync_write_in_progress:
        return Status::SyncWriteInProgress;
    case engine_errc::sync_write_ambiguous:
        return Status::SyncWriteAmbiguous;
    case engine_errc::dcp_streamid_invalid:
        return Status::DcpStreamIdInvalid;
    case engine_errc::sync_write_re_commit_in_progress:
        return Status::SyncWriteReCommitInProgress;
    case engine_errc::stream_not_found:
        return Status::DcpStreamNotFound;
    case engine_errc::opaque_no_match:
        return Status::OpaqueNoMatch;
    case engine_errc::scope_size_limit_exceeded:
        return Status::ScopeSizeLimitExceeded;
    case engine_errc::range_scan_cancelled:
        return Status::RangeScanCancelled;
    case engine_errc::range_scan_more:
        return Status::RangeScanMore;
    case engine_errc::range_scan_complete:
        return Status::RangeScanComplete;
    case cb::engine_errc::vbuuid_not_equal:
        return Status::VbUuidNotEqual;
    case engine_errc::too_many_connections:
        return Status::RateLimitedMaxConnections;
    case engine_errc::cancelled:
        return Status::Cancelled;
    case engine_errc::bucket_paused:
        return Status::BucketPaused;

    case engine_errc::throttled:
    case engine_errc::would_block:
    case engine_errc::disconnect:
    case engine_errc::predicate_failed:
    case engine_errc::sync_write_pending:
        throw std::logic_error("mcbp::to_status: " + to_string(code) +
                               " is not a legal error code "
                               "to send to the user");
    }

    throw std::invalid_argument("mcbp::to_status: Invalid argument " +
                                std::to_string(int(code)));
}
