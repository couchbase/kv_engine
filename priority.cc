/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include "priority.hh"

// Priorities for Read-only dispatcher
const Priority Priority::BgFetcherPriority("bg_fetcher_priority", 0);
const Priority Priority::TapBgFetcherPriority("tap_bg_fetcher_priority", 1);
const Priority Priority::VKeyStatBgFetcherPriority("vkey_stat_bg_fetcher_priority", 3);

// Priorities for Read-Write dispatcher
const Priority Priority::VBucketPersistHighPriority("vbucket_persist_high_priority", 1);
const Priority Priority::FastVBucketDeletionPriority("vbucket_deletion_hi_priority", 2);
const Priority Priority::FlushAllPriority("flush_all_priority", 3);
const Priority Priority::FlusherPriority("flusher_priority", 5);
const Priority Priority::VBucketDeletionPriority("vbucket_deletion_priority", 9);
const Priority Priority::VBucketPersistLowPriority("vbucket_persist_low_priority", 9);
const Priority Priority::StatSnapPriority("statsnap_priority", 9);
const Priority Priority::InvalidItemDbPagerPriority("invalid_item_db_pager_priority", 9);

// Priorities for NON-IO dispatcher
const Priority Priority::NotifyVBStateChangePriority("notify_vb_state_change_priority", 4);
const Priority Priority::CheckpointRemoverPriority("checkpoint_remover_priority", 6);
const Priority Priority::ItemPagerPriority("item_pager_priority", 7);
const Priority Priority::BackfillTaskPriority("backfill_task_priority", 8);
const Priority Priority::HTResizePriority("hashtable_resize_priority", 211);
const Priority Priority::SyncDestroyPriority("sync_destroy_priority", 314);
const Priority Priority::ObserveRegistryCleanerPriority("obs_reg_cleaneer_priority", 315);
const Priority Priority::TapResumePriority("tap_resume_priority", 316);
const Priority Priority::SyncAbortPriority("sync_abort_priority", 316);
/**
 * The tap connection reaper run with short iterations to give memory back to the
 * system. It should normally run for a short period of time for each iteration.
 * Too bad our dispatcher don't support automatic backoff...
 */
const Priority Priority::TapConnectionReaperPriority("tapconnection_reaper_priority", 10);
