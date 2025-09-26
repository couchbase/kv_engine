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

/// clang-tidy runs on a per-file basis and don't like this file and emit
/// errors like:
///     error: C++ requires a type specifier for all declarations
///                [clang-diagnostic-error]
///     TASK(MultiBGFetcherTask, TaskType::Reader, 0)
///     ^
///
/// To work around that, just define an empty macro if not defined.
#if !defined(TASK) && defined(__clang_analyzer__)
#warning "TASK should be defined before including tasks.def.h"
#define TASK(a, b, c)
#endif

/*
 * Every task within ep-engine is declared in this file
 *
 * The TASK(name, task-type, priority) macro will be pre-processed to generate
 *   - a unique std::string name
 *   - a unique type-id
 *   - a unique priority object
 *   - a mapping from type-id to task type
 *
 * task.h and .cc include this file with a customised TASK macro.
 */

// Read IO tasks
TASK(MultiBGFetcherTask, TaskType::Reader, 0)
TASK(FetchAllKeysTask, TaskType::Reader, 0)
TASK(Warmup, TaskType::Reader, 0)
TASK(WarmupInitialize, TaskType::Reader, 0)
TASK(WarmupCreateVBuckets, TaskType::Reader, 0)
TASK(WarmupLoadingCollectionCounts, TaskType::Reader, 0)
TASK(WarmupEstimateDatabaseItemCount, TaskType::Reader, 0)
TASK(WarmupLoadPreparedSyncWrites, TaskType::Reader, 0)
TASK(WarmupPopulateVBucketMap, TaskType::Reader, 0)
TASK(WarmupKeyDump, TaskType::Reader, 0)
TASK(WarmupCheckforAccessLog, TaskType::Reader, 0)
TASK(WarmupLoadAccessLog, TaskType::Reader, 0)
TASK(WarmupLoadingKVPairs, TaskType::Reader, 0)
TASK(WarmupLoadingData, TaskType::Reader, 0)
TASK(WarmupCompletion, TaskType::Reader, 0)
TASK(VKeyStatBGFetchTask, TaskType::Reader, 3)
TASK(Core_SaslRefreshTask, TaskType::Reader, 0)
TASK(Core_RbacReloadTask, TaskType::Reader, 0)

// Aux IO tasks
TASK(Core_CreateBucketTask, TaskType::AuxIO, 0)
TASK(Core_DeleteBucketTask, TaskType::AuxIO, 0)
TASK(Core_SettingsReloadTask, TaskType::AuxIO, 0)
TASK(Core_PruneEncryptionKeysTask, TaskType::AuxIO, 2)
TASK(Core_StatsBucketAuxIoTask, TaskType::AuxIO, 1)
TASK(VBucketMemoryAndDiskDeletionTask, TaskType::AuxIO, 0)
TASK(VBucketLoadingTask, TaskType::AuxIO, 0)
TASK(AccessScanner, TaskType::AuxIO, 2)
TASK(AccessScannerVisitor, TaskType::AuxIO, 2)
TASK(BackfillManagerTask, TaskType::AuxIO, 4)
TASK(CompactVBucketTask, TaskType::AuxIO, 5)
TASK(RangeScanCreateTask, TaskType::AuxIO, 6)
TASK(RangeScanContinueTask, TaskType::AuxIO, 6)
TASK(Core_PrepareSnapshotTask, TaskType::AuxIO, 0)
TASK(Core_ReleaseSnapshotTask, TaskType::AuxIO, 6)
TASK(Core_ReadFileFragmentTask, TaskType::AuxIO, 0)
TASK(Core_IoCtlTask, TaskType::AuxIO, 0)
TASK(DownloadSnapshotTask, TaskType::AuxIO, 6)
TASK(Core_UnmountFusionVBucketTask, TaskType::AuxIO, 6)
TASK(Core_SyncFusionLogstoreTask, TaskType::AuxIO, 6)
TASK(Core_GetFusionStorageSnapshotTask, TaskType::AuxIO, 6)
TASK(Core_ReleaseFusionStorageSnapshotTask, TaskType::AuxIO, 6)
TASK(StartFusionUploaderTask, TaskType::AuxIO, 6)
TASK(StopFusionUploaderTask, TaskType::AuxIO, 6)
TASK(Core_SetChronicleAuthTokenTask, TaskType::AuxIO, 6)
TASK(Core_DeleteFusionNamespaceTask, TaskType::AuxIO, 6)
TASK(Core_GetFusionNamespacesTask, TaskType::AuxIO, 6)

// Read/Write IO tasks
TASK(RollbackTask, TaskType::Writer, 1)
TASK(PersistCollectionsManifest, TaskType::Writer, 1)
TASK(FlusherTask, TaskType::Writer, 5)

// Non-IO tasks
TASK(Core_PauseBucketTask, TaskType::NonIO, 0)
TASK(Core_SetClusterConfig, TaskType::NonIO, 0)
TASK(PendingOpsNotification, TaskType::NonIO, 0)
TASK(RespondAmbiguousNotification, TaskType::NonIO, 0)
TASK(NotifyHighPriorityReqTask, TaskType::NonIO, 0)
TASK(EphemeralMemRecovery, TaskType::NonIO, 1)
TASK(ItemPager, TaskType::NonIO, 1)
TASK(ExpiredItemPager, TaskType::NonIO, 1)
TASK(ItemPagerVisitor, TaskType::NonIO, 1)
TASK(ExpiredItemPagerVisitor, TaskType::NonIO, 1)
TASK(DcpConsumerTask, TaskType::NonIO, 2)
TASK(DurabilityCompletionTask, TaskType::NonIO, 1)
TASK(DurabilityTimeoutTask, TaskType::NonIO, 1)
TASK(DurabilityTimeoutVisitor, TaskType::NonIO, 1)
TASK(RangeScanTimeoutTask, TaskType::NonIO, 1)
TASK(ActiveStreamCheckpointProcessorTask, TaskType::NonIO, 3)
TASK(BucketQuotaChangeTask, TaskType::NonIO, 6)
TASK(CheckpointDestroyerTask, TaskType::NonIO, 6)
TASK(CheckpointMemRecoveryTask, TaskType::NonIO, 6)
TASK(VBucketMemoryDeletionTask, TaskType::NonIO, 6)
TASK(DefragmenterTask, TaskType::NonIO, 7)
TASK(ItemCompressorTask, TaskType::NonIO, 7)
TASK(EphTombstoneHTCleaner, TaskType::NonIO, 7)
TASK(EphTombstoneStaleItemDeleter, TaskType::NonIO, 7)
TASK(ItemFreqDecayerTask, TaskType::NonIO, 7)
TASK(ConnManager, TaskType::NonIO, 8)
TASK(WorkLoadMonitor, TaskType::NonIO, 10)
TASK(HashtableResizerTask, TaskType::NonIO, 211)
TASK(HashtableResizerVisitorTask, TaskType::NonIO, 7)
TASK(Core_StaleTraceDumpRemover, TaskType::NonIO, 10)
TASK(Core_TraceDumpFormatter, TaskType::NonIO, 10)
TASK(Core_StatsBucketTask, TaskType::NonIO, 10)
TASK(Core_StatsConnectionTask, TaskType::NonIO, 10)
TASK(Core_PushClustermapTask, TaskType::NonIO, 10)
TASK(Core_SaslStartTask, TaskType::NonIO, 10)
TASK(Core_SaslStepTask, TaskType::NonIO, 10)
TASK(Core_Ifconfig, TaskType::NonIO, 10)
TASK(Core_SetActiveEncryptionKeysTask, TaskType::NonIO, 10)
TASK(Core_SubdocExecuteTask, TaskType::NonIO, 10)
TASK(SeqnoPersistenceNotifyTask, TaskType::NonIO, 1)
TASK(InitialMFUTask, TaskType::NonIO, 20)
TASK(CacheTransferTask, TaskType::NonIO, 20)