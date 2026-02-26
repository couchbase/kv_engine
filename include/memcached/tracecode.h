/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cstdint>
#include <string>

namespace cb::tracing {

enum class Code : uint8_t {
    /// Time spent in the entire request
    Request,
    /// Time spent throttled
    Throttled,
    /// The time spent during execution on front end thread
    Execute,
    /// The time spent associating a bucket
    AssociateBucket,
    /// The time spent disassociating a bucket
    DisassociateBucket,
    /// The time spent waiting to acquire the bucket lock
    BucketLockWait,
    /// The time spent holding the bucket lock
    BucketLockHeld,
    /// Time spent creating the RBAC context
    CreateRbacContext,
    /// Time spent updating privilege context when toggling buckets
    UpdatePrivilegeContext,
    /// Time spent generating audit event
    Audit,
    /// Time spent reconfiguring audit daemon
    AuditReconfigure,
    /// Time spent generating audit stats
    AuditStats,
    /// Time spend validating audit input
    AuditValidate,
    /// Time spent decompressing Snappy data.
    SnappyDecompress,
    /// Time spent validating if incoming value is JSON.
    JsonValidate,
    /// Time spent parsing JSON.
    JsonParse,
    /// Time spent performing subdoc lookup / mutation (all paths).
    SubdocOperate,
    /// Time spent waiting for a background fetch operation to be scheduled.
    BackgroundWait,
    /// Time spent performing the actual background load from disk.
    BackgroundLoad,
    /// Time spent in EngineIface::get
    Get,
    /// Time spent in EngineIface::get_if
    GetIf,
    /// Time spent searching for a random document in the hashtable
    /// visitor and waiting for the result to be returned.
    GetRandomDocument,
    /// Time spent in EngineIface::getStats
    GetStats,
    /// Time spent in EngineIface::setWithMeta
    SetWithMeta,
    /// Time spent in EngineIface::store and EngineIface::store_if
    Store,
    /// Time spent by a SyncWrite in Prepared state before being completed.
    SyncWritePrepare,
    /// Time when a SyncWrite local ACK is received by the Active.
    SyncWriteAckLocal,
    /// Time when a SyncWrite replica ACK is received by the Active.
    SyncWriteAckRemote,
    /// Time spent in Select Bucket
    SelectBucket,
    /// Time spent building a DCP stream filter
    StreamFilterCreate,
    /// Time spent checking for rollback
    StreamCheckRollback,
    /// Time spent looking for the high-seqno of a collection
    StreamGetCollectionHighSeq,
    /// Time spent looking for an existing stream in map
    StreamFindMap,
    /// Time spent updating stream map
    StreamUpdateMap,
    /// Time spent in running the SASL start/step call on the executor
    /// thread
    Sasl,
    /// Time spent in running the SASL start/step via ns_server
    SaslExternalAuth,
    /// Time spent looking up stats from the underlying Storage engine
    StorageEngineStats,
    /// Time spent from being notified until actually executed
    Notified,
    /// Time spent in prepareSnapshot creating the paths
    PrepareSnapshotCreatePath,
    /// Time spent in prepareSnapshot (backend dependent implementation)
    PrepareSnapshot,
    /// Time spent in prepareSnapshot checksums (for all file artefacts)
    PrepareSnapshotChecksums,
    /// Time spent in prepareSnapshot writing the manifest
    PrepareSnapshotWriteManifest,
    /// Time spent in prepareSnapshot cleaning up on failure
    PrepareSnapshotCleanupOnFailure,
};

} // namespace cb::tracing

std::string to_string(cb::tracing::Code tracecode);
