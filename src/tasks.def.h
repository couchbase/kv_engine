/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/*
 * Every task within ep-engine is declared in this file
 *
 * The TASK(name, priority) macro will be pre-processed to generate
 *   - a unique std::string name
 *   - a unique type-id
 *   - a unique priority object
 *
 * task.h and .cc include this file with a customised TASK macro.
 */

// Read IO tasks
TASK(MultiBGFetcherTask, 0)
TASK(FetchAllKeysTask, 0)
TASK(Warmup, 0)
TASK(WarmupInitialize, 0)
TASK(WarmupCreateVBuckets, 0)
TASK(WarmupEstimateDatabaseItemCount, 0)
TASK(WarmupKeyDump, 0)
TASK(WarmupCheckforAccessLog, 0)
TASK(WarmupLoadAccessLog, 0)
TASK(WarmupLoadingKVPairs, 0)
TASK(WarmupLoadingData, 0)
TASK(WarmupCompletion, 0)
TASK(SingleBGFetcherTask, 1)
TASK(VKeyStatBGFetchTask, 3)

// Aux IO tasks
TASK(BackfillDiskLoad, 1)
TASK(BGFetchCallback, 1)
TASK(AccessScanner, 3)
TASK(VBucketVisitorTask, 3)
TASK(ActiveStreamCheckpointProcessorTask, 5)
TASK(BackfillManagerTask, 8)
TASK(BackfillVisitorTask, 8)

// Read/Write IO tasks
TASK(VBDeleteTask, 1)
TASK(RollbackTask, 1)
TASK(CompactVBucketTask, 2)
TASK(FlushAllTask, 3)
TASK(FlusherTask, 5)
TASK(StatSnap, 9)

// Non-IO tasks
TASK(PendingOpsNotification, 0)
TASK(Processor, 0)
TASK(ConnNotifierCallback, 5)
TASK(ConnectionReaperCallback, 6)
TASK(ClosedUnrefCheckpointRemoverTask, 6)
TASK(ClosedUnrefCheckpointRemoverVisitorTask, 6)
TASK(VBucketMemoryDeletionTask, 6)
TASK(StatCheckpointTask, 7)
TASK(ItemPager, 7)
TASK(ExpiredItemPager, 7)
TASK(ItemPagerVisitor, 7)
TASK(ExpiredItemPagerVisitor, 7)
TASK(DefragmenterTask, 7)
TASK(ConnManager, 8)
TASK(WorkLoadMonitor, 10)
TASK(ResumeCallback, 316)
TASK(HashtableResizerTask, 211)
TASK(HashtableResizerVisitorTask, 7)
