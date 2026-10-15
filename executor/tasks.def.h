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
///     TASK(MultiBGFetcherTask, TaskType::Reader)
///     ^
///
/// To work around that, just define an empty macro if not defined.
#if !defined(TASK) && defined(__clang_analyzer__)
#warning "TASK should be defined before including tasks.def.h"
#define TASK(a, b)
#endif

/*
 * Every task within ep-engine is declared in this file
 *
 * The TASK(name, task-type) macro will be pre-processed to generate
 *   - a unique std::string name
 *   - a unique type-id
 *   - a mapping from type-id to task type
 *
 * task.h and .cc include this file with a customised TASK macro.
 */

// Read IO tasks
TASK(MultiBGFetcherTask, TaskType::Reader)
TASK(FetchAllKeysTask, TaskType::Reader)
TASK(Warmup, TaskType::Reader)
TASK(WarmupInitialize, TaskType::Reader)
TASK(WarmupCreateVBuckets, TaskType::Reader)
TASK(WarmupLoadingCollectionCounts, TaskType::Reader)
TASK(WarmupEstimateDatabaseItemCount, TaskType::Reader)
TASK(WarmupLoadPreparedSyncWrites, TaskType::Reader)
TASK(WarmupPopulateVBucketMap, TaskType::Reader)
TASK(WarmupKeyDump, TaskType::Reader)
TASK(WarmupCheckforAccessLog, TaskType::Reader)
TASK(WarmupLoadAccessLog, TaskType::Reader)
TASK(WarmupLoadingKVPairs, TaskType::Reader)
TASK(WarmupLoadingData, TaskType::Reader)
TASK(WarmupCompletion, TaskType::Reader)
TASK(VKeyStatBGFetchTask, TaskType::Reader)
TASK(Core_SaslRefreshTask, TaskType::Reader)
TASK(Core_RbacReloadTask, TaskType::Reader)

// Aux IO tasks
TASK(Core_CreateBucketTask, TaskType::AuxIO)
TASK(Core_DeleteBucketTask, TaskType::AuxIO)
TASK(Core_SettingsReloadTask, TaskType::AuxIO)
TASK(Core_PruneEncryptionKeysTask, TaskType::AuxIO)
TASK(Core_StatsBucketAuxIoTask, TaskType::AuxIO)
TASK(VBucketMemoryAndDiskDeletionTask, TaskType::AuxIO)
TASK(VBucketLoadingTask, TaskType::AuxIO)
TASK(AccessScanner, TaskType::AuxIO)
TASK(AccessScannerVisitor, TaskType::AuxIO)
TASK(BackfillManagerTask, TaskType::AuxIO)
TASK(CompactVBucketTask, TaskType::AuxIO)
TASK(RangeScanCreateTask, TaskType::AuxIO)
TASK(RangeScanContinueTask, TaskType::AuxIO)
TASK(Core_PrepareSnapshotTask, TaskType::AuxIO)
TASK(Core_ReleaseSnapshotTask, TaskType::AuxIO)
TASK(Core_ReadFileFragmentTask, TaskType::AuxIO)
TASK(Core_IoCtlTask, TaskType::AuxIO)
TASK(Core_UnmountFusionVBucketTask, TaskType::AuxIO)
TASK(Core_SyncFusionLogstoreTask, TaskType::AuxIO)
TASK(Core_GetFusionStorageSnapshotTask, TaskType::AuxIO)
TASK(Core_ReleaseFusionStorageSnapshotTask, TaskType::AuxIO)
TASK(StartFusionUploaderTask, TaskType::AuxIO)
TASK(StopFusionUploaderTask, TaskType::AuxIO)
TASK(Core_SetChronicleAuthTokenTask, TaskType::AuxIO)
TASK(Core_DeleteFusionNamespaceTask, TaskType::AuxIO)
TASK(Core_GetFusionNamespacesTask, TaskType::AuxIO)
TASK(Core_CreateFusionNamespaceTask, TaskType::AuxIO)

// Read/Write IO tasks
TASK(RollbackTask, TaskType::Writer)
TASK(PersistCollectionsManifest, TaskType::Writer)
TASK(FlusherTask, TaskType::Writer)

// Non-IO tasks
TASK(Core_PauseBucketTask, TaskType::NonIO)
TASK(Core_SetClusterConfig, TaskType::NonIO)
TASK(PendingOpsNotification, TaskType::NonIO)
TASK(RespondAmbiguousNotification, TaskType::NonIO)
TASK(NotifyHighPriorityReqTask, TaskType::NonIO)
TASK(MonitorTask, TaskType::NonIO)
TASK(EphemeralMemRecovery, TaskType::NonIO)
TASK(ItemPager, TaskType::NonIO)
TASK(ExpiredItemPager, TaskType::NonIO)
TASK(ItemPagerVisitor, TaskType::NonIO)
TASK(ExpiredItemPagerVisitor, TaskType::NonIO)
TASK(DurabilityTimeoutTask, TaskType::NonIO)
TASK(DurabilityTimeoutVisitor, TaskType::NonIO)
TASK(RangeScanTimeoutTask, TaskType::NonIO)
TASK(BucketQuotaChangeTask, TaskType::NonIO)
TASK(CheckpointDestroyerTask, TaskType::NonIO)
TASK(CheckpointMemRecoveryTask, TaskType::NonIO)
TASK(VBucketMemoryDeletionTask, TaskType::NonIO)
TASK(DefragmenterTask, TaskType::NonIO)
TASK(ItemCompressorTask, TaskType::NonIO)
TASK(EphTombstoneHTCleaner, TaskType::NonIO)
TASK(EphTombstoneStaleItemDeleter, TaskType::NonIO)
TASK(ItemFreqDecayerTask, TaskType::NonIO)
TASK(ConnManager, TaskType::NonIO)
TASK(GetRandomKeyVisitor, TaskType::NonIO)
TASK(WorkLoadMonitor, TaskType::NonIO)
TASK(HashtableResizerTask, TaskType::NonIO)
TASK(HashtableResizerVisitorTask, TaskType::NonIO)
TASK(Core_ExpiredTopKeysRemover, TaskType::NonIO)
TASK(Core_StaleTraceDumpRemover, TaskType::NonIO)
TASK(Core_TraceDumpFormatter, TaskType::NonIO)
TASK(Core_StatsBucketTask, TaskType::NonIO)
TASK(Core_StatsConnectionTask, TaskType::NonIO)
TASK(Core_PushClustermapTask, TaskType::NonIO)
TASK(Core_SaslStartTask, TaskType::NonIO)
TASK(Core_SaslStepTask, TaskType::NonIO)
TASK(Core_Ifconfig, TaskType::NonIO)
TASK(Core_SetActiveEncryptionKeysTask, TaskType::NonIO)
TASK(Core_SubdocExecuteTask, TaskType::NonIO)
TASK(InitialMFUTask, TaskType::NonIO)
TASK(CacheTransferTask, TaskType::NonIO)
TASK(Core_BucketConfigValidationTask, TaskType::NonIO)
TASK(Core_SetCollectionManifestTask, TaskType::NonIO)

// Quick Non-IO tasks
TASK(SeqnoPersistenceNotifyTask, TaskType::QuickNonIO)
TASK(DurabilityCompletionTask, TaskType::QuickNonIO)
TASK(DcpConsumerTask, TaskType::QuickNonIO)
TASK(ActiveStreamCheckpointProcessorTask, TaskType::QuickNonIO)

// Slow IO tasks
TASK(DownloadSnapshotTask, TaskType::SlowIO)
