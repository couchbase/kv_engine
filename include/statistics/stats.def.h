/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

/**
 * Static definitions for statistics.
 *
 * Requires a STAT macro be defined before including this file.
 * STAT(uniqueName, ...)
 *
 * where:
 *  * uniqueName - a key which identifies the stat (used as the enum value
 *                  and cbstats key)
 * and the remaining optional parameters are:
 *  * unit - name of a cb::stats::Unit which identifies what unit the stat
 *           value represents (e.g., microseconds). default: none
 *  * familyName - the metric name used by Prometheus. This need _not_ be
 * unique, and can be shared by stats which are distinguishable by labels.
 * default: uniqueName
 *  * labelKey - key of a label to be applied to the stat. default:""
 *  * labelValue - value of a label to be applied to the stat. default:""
 *
 * Stats should be formatted as
 *
 * STAT(unique_name, unit, family_name, labelKey, labelValue)
 *
 * e.g.,
 *
 * STAT(get_cmd, microseconds, cmd_time_taken, op, get)
 * STAT(set_cmd, microseconds, cmd_time_taken, op, set)
 *
 * The uniqueName will be used as an enum key, and as the stat key for
 * backends which do not support labelled stat families (e.g., CBStats).
 *
 * The familyName and labels will be used by backends which _do_ support
 * them, like Prometheus. All stats of a given family_name should be of the same
 * unit (e.g., count, bytes, seconds, kilobytes per microsecond) and it should
 * be possible to meaningfully aggregate the stat values e.g., get_cmd and
 * set_cmd can be summed.
 *
 * Only uniqueName is mandatory. The minimal definition of a stat is therefore
 * STAT(uniqueName)
 * For stats with unspecified units and no labels. In this case, the uniqueName
 * will also be used as the familyName.
 *
 */

#ifndef STAT
#warning A STAT macro must be defined before including stats.def.h
// If this header is being checked by clang-tidy or similar _not_ as part of
// definitions.h, there will not be a STAT definition.
// Define an empty STAT macro to avoid unnecessary warnings.
#define STAT(...)
#endif

// include generated config STAT declarations
#include "stats_config.def.h" // NOLINT(*)

// EPBucket::getFileStats
STAT(ep_db_data_size, bytes, , , )
STAT(ep_db_file_size, bytes, , , )

// Timing stats
STAT(bg_wait, microseconds, , , )
STAT(bg_load, microseconds, , , )
STAT(set_with_meta, microseconds, , , )
STAT(pending_ops, microseconds, , , )
STAT(access_scanner, microseconds, , , )
STAT(checkpoint_remover, microseconds, , , )
STAT(item_pager, microseconds, , , )
STAT(expiry_pager, microseconds, , , )
STAT(storage_age, microseconds, , , )
STAT(get_cmd, microseconds, cmd_time_taken, op, get)
STAT(store_cmd, microseconds, cmd_time_taken, op, store)
STAT(arith_cmd, microseconds, cmd_time_taken, op, arith)
STAT(get_stats_cmd, microseconds, cmd_time_taken, op, get_stats)
STAT(get_vb_cmd, microseconds, cmd_time_taken, op, get_vb)
STAT(set_vb_cmd, microseconds, cmd_time_taken, op, set_vb)
STAT(del_vb_cmd, microseconds, cmd_time_taken, op, del_vb)
STAT(chk_persistence_cmd, microseconds, cmd_time_taken, op, chk_persistence)
STAT(notify_io, microseconds, , , )
STAT(batch_read, microseconds, , , )
STAT(disk_insert, microseconds, disk, op, insert)
STAT(disk_update, microseconds, disk, op, update)
STAT(disk_del, microseconds, disk, op, del)
STAT(disk_vb_del, microseconds, disk, op, vb_del)
STAT(disk_commit, microseconds, disk, op, commit)
STAT(item_alloc_sizes,
     bytes,
     ,
     , ) // TODO: this is not timing related but is in doTimingStats
STAT(bg_batch_size,
     count,
     ,
     , ) // TODO: this is not timing related but is in doTimingStats
STAT(persistence_cursor_get_all_items,
     microseconds,
     cursor_get_all_items_time,
     cursor_type,
     persistence)
STAT(dcp_cursors_get_all_items,
     microseconds,
     cursor_get_all_items_time,
     cursor_type,
     dcp)
STAT(sync_write_commit_majority,
     microseconds,
     sync_write_commit_duration,
     level,
     majority)
STAT(sync_write_commit_majority_and_persist_on_master,
     microseconds,
     sync_write_commit_duration,
     level,
     majority_and_persist_on_master)
STAT(sync_write_commit_persist_to_majority,
     microseconds,
     sync_write_commit_duration,
     level,
     persist_to_majority)

// Vbucket aggreagated stats
#define VB_AGG_STAT(name, unit, familyName)                   \
    STAT(vb_active_##name, unit, familyName, state, active)   \
    STAT(vb_replica_##name, unit, familyName, state, replica) \
    STAT(vb_pending_##name, unit, name, state, pending)

VB_AGG_STAT(num, count, num_vbuckets)
VB_AGG_STAT(curr_items, count, )
VB_AGG_STAT(hp_vb_req_size, count, num_high_pri_requests)
VB_AGG_STAT(num_non_resident, count, )
VB_AGG_STAT(perc_mem_resident, percent, )
VB_AGG_STAT(eject, count, )
VB_AGG_STAT(expired, count, )
VB_AGG_STAT(meta_data_memory, bytes, )
VB_AGG_STAT(meta_data_disk, bytes, )
VB_AGG_STAT(checkpoint_memory, bytes, )
VB_AGG_STAT(checkpoint_memory_unreferenced, bytes, )
VB_AGG_STAT(checkpoint_memory_overhead, bytes, )
VB_AGG_STAT(ht_memory, bytes, )
VB_AGG_STAT(itm_memory, bytes, )
VB_AGG_STAT(itm_memory_uncompressed, bytes, )
VB_AGG_STAT(ops_create, count, )
VB_AGG_STAT(ops_update, count, )
VB_AGG_STAT(ops_delete, count, )
VB_AGG_STAT(ops_get, count, )
VB_AGG_STAT(ops_reject, count, )
VB_AGG_STAT(queue_size, count, )
VB_AGG_STAT(queue_memory, bytes, )
VB_AGG_STAT(queue_age, milliseconds, )
VB_AGG_STAT(queue_pending, bytes, )
VB_AGG_STAT(queue_fill, count, )
VB_AGG_STAT(queue_drain, count, )
VB_AGG_STAT(rollback_item_count, count, )

#undef VB_AGG_STAT

STAT(curr_items, count, , , )
STAT(curr_temp_items, count, , , )
STAT(curr_items_tot, count, , , )

STAT(vb_active_sync_write_accepted_count, count, , , )
STAT(vb_active_sync_write_committed_count, count, , , )
STAT(vb_active_sync_write_aborted_count, count, , , )
STAT(vb_replica_sync_write_accepted_count, count, , , )
STAT(vb_replica_sync_write_committed_count, count, , , )
STAT(vb_replica_sync_write_aborted_count, count, , , )
STAT(vb_dead_num, count, , , )
STAT(ep_vb_total, count, , , )
STAT(ep_total_new_items, count, , , )
STAT(ep_total_del_items, count, , , )
STAT(ep_diskqueue_memory, bytes, , , )
STAT(ep_diskqueue_fill, count, , , )
STAT(ep_diskqueue_drain, count, , , )
STAT(ep_diskqueue_pending, count, , , )
STAT(ep_meta_data_memory, bytes, , , )
STAT(ep_meta_data_disk, bytes, , , )
STAT(ep_checkpoint_memory, bytes, , , )
STAT(ep_checkpoint_memory_unreferenced, bytes, , , )
STAT(ep_checkpoint_memory_overhead, bytes, , , )
STAT(ep_total_cache_size, bytes, , , )
STAT(rollback_item_count, count, , , )
STAT(ep_num_non_resident, count, , , )
STAT(ep_chk_persistence_remains, count, , , )
STAT(ep_active_hlc_drift, microseconds, , , )
STAT(ep_active_hlc_drift_count, count, , , )
STAT(ep_replica_hlc_drift, microseconds, , , )
STAT(ep_replica_hlc_drift_count, count, , , )
STAT(ep_active_ahead_exceptions, count, , , )
STAT(ep_active_behind_exceptions, count, , , )
STAT(ep_replica_ahead_exceptions, count, , , )
STAT(ep_replica_behind_exceptions, count, , , )
STAT(ep_clock_cas_drift_threshold_exceeded, count, , , )
