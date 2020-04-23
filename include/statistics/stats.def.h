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