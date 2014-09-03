/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "config.h"

#include "priority.h"

// Priorities for Read-only IO tasks
const Priority Priority::BgFetcherPriority(BGFETCHER_ID, 0);
const Priority Priority::BgFetcherGetMetaPriority(BGFETCHER_GET_META_ID, 1);
const Priority Priority::WarmupPriority(WARMUP_ID, 0);
const Priority Priority::VKeyStatBgFetcherPriority(VKEY_STAT_BGFETCHER_ID, 3);

// Priorities for Auxiliary IO tasks
const Priority Priority::TapBgFetcherPriority(TAP_BGFETCHER_ID, 1);

// Priorities for Read-Write IO tasks
const Priority Priority::VBucketDeletionPriority(VBUCKET_DELETION_ID, 1);
const Priority Priority::CompactorPriority(COMPACTOR_ID, 2);
const Priority Priority::VBucketPersistHighPriority(VBUCKET_PERSIST_HIGH_ID, 2);
const Priority Priority::FlushAllPriority(FLUSHALL_ID, 3);
const Priority Priority::FlusherPriority(FLUSHER_ID, 5);
const Priority Priority::VBucketPersistLowPriority(VBUCKET_PERSIST_LOW_ID, 9);
const Priority Priority::StatSnapPriority(STAT_SNAP_ID, 9);
const Priority Priority::MutationLogCompactorPriority(MUTATION_LOG_COMPACTOR_ID, 9);
const Priority Priority::AccessScannerPriority(ACCESS_SCANNER_ID, 3);

// Priorities for NON-IO tasks
const Priority Priority::PendingOpsPriority(PENDING_OPS_ID, 0);
const Priority Priority::TapConnNotificationPriority(TAP_CONN_NOTIFICATION_ID, 5);
const Priority Priority::CheckpointRemoverPriority(CHECKPOINT_REMOVER_ID, 6);
const Priority Priority::TapConnectionReaperPriority(TAP_CONNECTION_REAPER_ID, 6);
const Priority Priority::VBMemoryDeletionPriority(VB_MEMORY_DELETION_ID, 6);
const Priority Priority::CheckpointStatsPriority(CHECKPOINT_STATS_ID, 7);
const Priority Priority::ItemPagerPriority(ITEM_PAGER_ID, 7);
const Priority Priority::DefragmenterTaskPriority(DEFRAGMENTER_ID, 7);
const Priority Priority::TapConnMgrPriority(TAP_CONN_MGR_ID, 8);
const Priority Priority::BackfillTaskPriority(BACKFILL_TASK_ID, 8);
const Priority Priority::WorkLoadMonitorPriority(WORKLOAD_MONITOR_TASK_ID, 10);
const Priority Priority::HTResizePriority(HT_RESIZER_ID, 211);
const Priority Priority::TapResumePriority(TAP_RESUME_ID, 316);

const char *Priority::getTypeName(const type_id_t i) {
        switch (i) {
            case BGFETCHER_ID:
                return "bg_fetcher_tasks";
            case BGFETCHER_GET_META_ID:
                return "bg_fetcher_meta_tasks";
            case TAP_BGFETCHER_ID:
                return "tap_bg_fetcher_tasks";
            case VKEY_STAT_BGFETCHER_ID:
                return "vkey_stat_bg_fetcher_tasks";
            case WARMUP_ID:
                return "warmup_tasks";
            case VBUCKET_PERSIST_HIGH_ID:
                return "vbucket_persist_high_tasks";
            case VBUCKET_DELETION_ID:
                return "vbucket_deletion_tasks";
            case FLUSHER_ID:
                return "flusher_tasks";
            case FLUSHALL_ID:
                return "flush_all_tasks";
            case COMPACTOR_ID:
                return "compactor_tasks";
            case VBUCKET_PERSIST_LOW_ID:
                return "vbucket_persist_low_tasks";
            case STAT_SNAP_ID:
                return "statsnap_tasks";
            case MUTATION_LOG_COMPACTOR_ID:
                return "mutation_log_compactor_tasks";
            case ACCESS_SCANNER_ID:
                return "access_scanner_tasks";
            case TAP_CONN_NOTIFICATION_ID:
                return "conn_notification_tasks";
            case CHECKPOINT_REMOVER_ID:
                return "checkpoint_remover_tasks";
            case VB_MEMORY_DELETION_ID:
                return "vb_memory_deletion_tasks";
            case CHECKPOINT_STATS_ID:
                return "checkpoint_stats_tasks";
            case ITEM_PAGER_ID:
                return "item_pager_tasks";
            case BACKFILL_TASK_ID:
                return "backfill_tasks_tasks";
            case WORKLOAD_MONITOR_TASK_ID:
                return "workload_monitor_tasks";
            case TAP_RESUME_ID:
                return "tap_resume_tasks";
            case TAP_CONNECTION_REAPER_ID:
                return "tapconnection_reaper_tasks";
            case HT_RESIZER_ID:
                return "hashtable_resize_tasks";
            case PENDING_OPS_ID:
                return "pending_ops_tasks";
            case TAP_CONN_MGR_ID:
                return "conn_manager_tasks";
            case DEFRAGMENTER_ID:
                return "defragmenter_tasks";
            default: break;
        }

        return "error";
    }
