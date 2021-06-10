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
TASK(MultiBGFetcherTask, READER_TASK_IDX, 0)
TASK(FetchAllKeysTask, READER_TASK_IDX, 0)
TASK(Warmup, READER_TASK_IDX, 0)
TASK(WarmupInitialize, READER_TASK_IDX, 0)
TASK(WarmupCreateVBuckets, READER_TASK_IDX, 0)
TASK(WarmupLoadingCollectionCounts, READER_TASK_IDX, 0)
TASK(WarmupEstimateDatabaseItemCount, READER_TASK_IDX, 0)
TASK(WarmupLoadPreparedSyncWrites, READER_TASK_IDX, 0)
TASK(WarmupPopulateVBucketMap, READER_TASK_IDX, 0)
TASK(WarmupKeyDump, READER_TASK_IDX, 0)
TASK(WarmupCheckforAccessLog, READER_TASK_IDX, 0)
TASK(WarmupLoadAccessLog, READER_TASK_IDX, 0)
TASK(WarmupLoadingKVPairs, READER_TASK_IDX, 0)
TASK(WarmupLoadingData, READER_TASK_IDX, 0)
TASK(WarmupCompletion, READER_TASK_IDX, 0)
TASK(VKeyStatBGFetchTask, READER_TASK_IDX, 3)

// Aux IO tasks
TASK(VBucketMemoryAndDiskDeletionTask, AUXIO_TASK_IDX, 0)
TASK(PersistCollectionsManifest, AUXIO_TASK_IDX, 1)
TASK(AccessScanner, AUXIO_TASK_IDX, 2)
TASK(AccessScannerVisitor, AUXIO_TASK_IDX, 2)
TASK(BackfillManagerTask, AUXIO_TASK_IDX, 4)

// Read/Write IO tasks
TASK(RollbackTask, WRITER_TASK_IDX, 1)
TASK(CompactVBucketTask, WRITER_TASK_IDX, 2)
TASK(FlusherTask, WRITER_TASK_IDX, 5)
TASK(StatSnap, WRITER_TASK_IDX, 9)

// Non-IO tasks
TASK(PendingOpsNotification, NONIO_TASK_IDX, 0)
TASK(RespondAmbiguousNotification, NONIO_TASK_IDX, 0)
TASK(NotifyHighPriorityReqTask, NONIO_TASK_IDX, 0)
TASK(ItemPager, NONIO_TASK_IDX, 1)
TASK(ExpiredItemPager, NONIO_TASK_IDX, 1)
TASK(ItemPagerVisitor, NONIO_TASK_IDX, 1)
TASK(ExpiredItemPagerVisitor, NONIO_TASK_IDX, 1)
TASK(DcpConsumerTask, NONIO_TASK_IDX, 2)
TASK(DurabilityCompletionTask, NONIO_TASK_IDX, 1)
TASK(DurabilityTimeoutTask, NONIO_TASK_IDX, 1)
TASK(DurabilityTimeoutVisitor, NONIO_TASK_IDX, 1)
TASK(ActiveStreamCheckpointProcessorTask, NONIO_TASK_IDX, 3)
TASK(ConnNotifierCallback, NONIO_TASK_IDX, 5)
TASK(ClosedUnrefCheckpointRemoverTask, NONIO_TASK_IDX, 6)
TASK(ClosedUnrefCheckpointRemoverVisitorTask, NONIO_TASK_IDX, 6)
TASK(VBucketMemoryDeletionTask, NONIO_TASK_IDX, 6)
TASK(StatCheckpointTask, NONIO_TASK_IDX, 7)
TASK(DefragmenterTask, NONIO_TASK_IDX, 7)
TASK(ItemCompressorTask, NONIO_TASK_IDX, 7)
TASK(EphTombstoneHTCleaner, NONIO_TASK_IDX, 7)
TASK(EphTombstoneStaleItemDeleter, NONIO_TASK_IDX, 7)
TASK(ItemFreqDecayerTask, NONIO_TASK_IDX, 7)
TASK(ConnManager, NONIO_TASK_IDX, 8)
TASK(WorkLoadMonitor, NONIO_TASK_IDX, 10)
TASK(HashtableResizerTask, NONIO_TASK_IDX, 211)
TASK(HashtableResizerVisitorTask, NONIO_TASK_IDX, 7)
