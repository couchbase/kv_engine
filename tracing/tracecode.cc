/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/tracecode.h>

std::string to_string(const cb::tracing::Code tracecode) {
    using cb::tracing::Code;
    switch (tracecode) {
    case Code::Request:
        return "request";
    case Code::Throttled:
        return "throttled";
    case Code::Execute:
        return "execute";
    case Code::AssociateBucket:
        return "associate_bucket";
    case Code::DisassociateBucket:
        return "disassociate_bucket";
    case Code::BucketLockWait:
        return "bucket_lock.wait";
    case Code::BucketLockHeld:
        return "bucket_lock.held";
    case Code::UpdatePrivilegeContext:
        return "update_privilege_context";
    case Code::CreateRbacContext:
        return "create_rbac_context";
    case Code::Audit:
        return "audit";
    case Code::AuditReconfigure:
        return "audit.reconfigure";
    case Code::AuditStats:
        return "audit.stats";
    case Code::AuditValidate:
        return "audit.validate";
    case Code::SnappyDecompress:
        return "snappy.decompress";
    case Code::JsonValidate:
        return "json_validate";
    case Code::JsonParse:
        return "json_parse";
    case Code::SubdocOperate:
        return "subdoc.operate";
    case Code::BackgroundWait:
        return "bg.wait";
    case Code::BackgroundLoad:
        return "bg.load";
    case Code::Get:
        return "get";
    case Code::GetIf:
        return "get.if";
    case Code::GetStats:
        return "get.stats";
    case Code::SetWithMeta:
        return "set.with.meta";
    case Code::Store:
        return "store";
    case Code::SyncWritePrepare:
        return "sync_write.prepare";
    case Code::SyncWriteAckLocal:
        return "sync_write.ack_local";
    case Code::SyncWriteAckRemote:
        return "sync_write.ack_remote";
    case Code::SelectBucket:
        return "select_bucket";
    case Code::StreamFilterCreate:
        return "stream_req.filter";
    case Code::StreamCheckRollback:
        return "stream_req.rollback";
    case Code::StreamGetCollectionHighSeq:
        return "stream_req.get_collection_seq";
    case Code::StreamFindMap:
        return "stream_req.find_map";
    case Code::StreamUpdateMap:
        return "stream_req.update_map";
    case Code::Sasl:
        return "sasl";
    case Code::StorageEngineStats:
        return "storage_engine_stats";
    case Code::Notified:
        return "notified";
    case Code::PrepareSnapshotCreatePath:
        return "prepare_snapshot.create_path";
    case Code::PrepareSnapshot:
        return "prepare_snapshot.prepare_impl";
    case Code::PrepareSnapshotChecksums:
        return "prepare_snapshot.checksums";
    case Code::PrepareSnapshotWriteManifest:
        return "prepare_snapshot.write_manifest";
    case Code::PrepareSnapshotCleanupOnFailure:
        return "prepare_snapshot.cleanup_on_failure";
    }
    return "unknown tracecode";
}
