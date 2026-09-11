/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <gsl/gsl-lite.hpp>
#include <memcached/tracecode.h>
#include <nlohmann/json.hpp>

namespace cb::tracing {
std::string_view format_as(const Code tracecode) {
    using namespace std::string_view_literals;
    switch (tracecode) {
    case Code::Request:
        return "request"sv;
    case Code::Validate:
        return "validate"sv;
    case Code::Throttled:
        return "throttled"sv;
    case Code::Execute:
        return "execute"sv;
    case Code::AssociateBucket:
        return "associate_bucket"sv;
    case Code::DisassociateBucket:
        return "disassociate_bucket"sv;
    case Code::BucketLockWait:
        return "bucket_lock.wait"sv;
    case Code::BucketLockHeld:
        return "bucket_lock.held"sv;
    case Code::UpdatePrivilegeContext:
        return "update_privilege_context"sv;
    case Code::CreateRbacContext:
        return "create_rbac_context"sv;
    case Code::Audit:
        return "audit"sv;
    case Code::AuditReconfigure:
        return "audit.reconfigure"sv;
    case Code::AuditStats:
        return "audit.stats"sv;
    case Code::AuditValidate:
        return "audit.validate"sv;
    case Code::SnappyDecompress:
        return "snappy.decompress"sv;
    case Code::JsonValidate:
        return "json_validate"sv;
    case Code::JsonParse:
        return "json_parse"sv;
    case Code::SubdocOperate:
        return "subdoc.operate"sv;
    case Code::BackgroundWait:
        return "bg.wait"sv;
    case Code::BackgroundLoad:
        return "bg.load"sv;
    case Code::Get:
        return "get"sv;
    case Code::GetIf:
        return "get.if"sv;
    case Code::GetRandomDocument:
        return "get.random_document"sv;
    case Code::GetStats:
        return "get.stats"sv;
    case Code::SetWithMeta:
        return "set.with.meta"sv;
    case Code::Store:
        return "store"sv;
    case Code::SyncWritePrepare:
        return "sync_write.prepare"sv;
    case Code::SyncWriteAckLocal:
        return "sync_write.ack_local"sv;
    case Code::SyncWriteAckRemote:
        return "sync_write.ack_remote"sv;
    case Code::SelectBucket:
        return "select_bucket"sv;
    case Code::StreamFilterCreate:
        return "stream_req.filter"sv;
    case Code::StreamCheckRollback:
        return "stream_req.rollback"sv;
    case Code::StreamGetCollectionHighSeq:
        return "stream_req.get_collection_seq"sv;
    case Code::StreamFindMap:
        return "stream_req.find_map"sv;
    case Code::StreamUpdateMap:
        return "stream_req.update_map"sv;
    case Code::Sasl:
        return "sasl"sv;
    case Code::SaslExternalAuth:
        return "sasl.external_auth"sv;
    case Code::StorageEngineStats:
        return "storage_engine_stats"sv;
    case Code::Notified:
        return "notified"sv;
    case Code::PrepareSnapshotCreatePath:
        return "prepare_snapshot.create_path"sv;
    case Code::PrepareSnapshot:
        return "prepare_snapshot.prepare_impl"sv;
    case Code::PrepareSnapshotChecksums:
        return "prepare_snapshot.checksums"sv;
    case Code::PrepareSnapshotWriteManifest:
        return "prepare_snapshot.write_manifest"sv;
    case Code::PrepareSnapshotCleanupOnFailure:
        return "prepare_snapshot.cleanup_on_failure"sv;
    }
    Expects(false && "Unknown cb::tracing::Code");
}

void to_json(nlohmann::json& json, const Code& code) {
    json = format_as(code);
}

} // namespace cb::tracing
