/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Static definitions for statistics.
 *
 * Requires STAT, CBSTAT, PSTAT, FMT, and LABEL macros be defined before
 * including this file. LABEL wraps a key and a value. A common definition for
 * LABEL would be
 *
 * LABEL(key, value) {#key, #value}
 *
 * to stringify for insertion into a map.
 *
 * FMT(cbstatsName)
 *
 * Is used to declare that the key will need formatting as it currently contains
 * replacement specifiers. This avoids wasting time for stats which _don't_
 * require this.
 * e.g.,
 *  STAT(some_enum_key, FMT("{connection_type}:some_stat"),...)
 *
 * STAT(enumKey, cbstatName, unit, familyName, ...)
 * CBSTAT(enumKey, cbstatName, [unit])
 * PSTAT(enumKey, unit, familyName, ...)
 *
 * where:
 *  * enumKey - a key which identifies the stat
 *  * cbstatName - key to expose this stat under for cbstats (defaults to
 *                 enumKey if empty). FMT may be used to wrap this key if
 *                 it requires values to be formatted in e.g., "{scope}:foo"
 *  * unit - name of a cb::stats::Unit which identifies what unit the stat
 *           value represents (e.g., microseconds). default: none
 *  * familyName - the metric name used by Prometheus. This need _not_ be
 * unique, and can be shared by stats which are distinguishable by labels.
 * may be empty.
 * default: uniqueName
 * and the remaining optional parameters are 0 or more instances of
 *  * LABEL(key, value)
 *
 * Stats should be formatted as
 *
 * STAT(unique_name, , unit, family_name, [label]...)
 *
 * e.g.,
 *
 * STAT(get_cmd, , microseconds, cmd_time_taken, LABEL(op, get),
 *                                             LABEL(result, hit))
 * STAT(set_cmd, , microseconds, cmd_time_taken, LABEL(op, set),
 *                                             LABEL(result, miss))
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
 * STAT(uniqueName, , , )
 * For stats with unspecified units and no labels. In this case, the uniqueName
 * will also be used as the familyName.
 *
 *
 * ** Cbstats-only metrics **
 * For stats which should only be exposed for cbstats, the CBSTAT macro
 * can be used instead. Only a unique name is required.
 *
 *  CBSTAT(uniqueName)
 *
 * Units are optional, and only informative - CBStats does not use units.
 *
 *  CBSTAT(uptime, , milliseconds)
 *
 * ** Prometheus-only metrics **
 * For stats which should only be exposed for Prometheus, the PSTAT macro
 * can be used instead. An unique enum key and unit are required.
 *
 *  PSTAT(enumKey, unit, )
 *
 * Labels are optional as with the STAT macro.
 *
 *  PSTAT(cmd_duration, microseconds, LABEL(op, get))
 */

#ifndef STAT
#warning A STAT macro must be defined before including stats.def.h
// If this header is being checked by clang-tidy or similar _not_ as part of
// definitions.h, there will not be a STAT definition.
// Define an empty STAT macro to avoid unnecessary warnings.
#define STAT(...)
#endif

#ifndef CBSTAT
#warning A CBSTAT macro must be defined before including stats.def.h
#define CBSTAT(...)
#endif

#ifndef PSTAT
#warning A PSTAT macro must be defined before including stats.def.h
#define PSTAT(...)
#endif

#ifndef LABEL
#warning A LABEL macro must be defined before including stats.def.h
#define LABEL(...)
#endif

// default_engine stats
STAT(default_evictions, "evictions", count, memcache_evictions, )
STAT(default_curr_items, "curr_items", count, memcache_curr_items, )
STAT(default_total_items, "total_items", count, memcache_total_items, )
STAT(default_bytes, "bytes", bytes, memcache_mem_size, )
STAT(default_reclaimed, "reclaimed", count, memcache_reclaimed, )
STAT(default_engine_maxbytes,
     "engine_maxbytes",
     count,
     memcache_engine_maxbytes, )

// include generated config STAT declarations
#include "stats_config.def.h" // NOLINT(*)

// TODO: applying a "kv_" prefix globally would be consistent but lead to kv_ep_
//  for some stats. Providing metric family names without ep_ would avoid this
// "All" stats group (doEngineStats)
STAT(ep_storage_age, , microseconds, , )
STAT(ep_storage_age_highwat, , microseconds, , )
STAT(ep_num_workers, , count, , )
STAT(ep_bucket_priority,
     ,
     none,
     , ) // TODO: make 0/1 rather than text for Prometheus?
STAT(ep_total_enqueued, , count, , )
STAT(ep_total_deduplicated, , count, , )
STAT(ep_expired_access, , count, , )
STAT(ep_expired_compactor, , count, , )
STAT(ep_expired_pager, , count, , )
STAT(ep_queue_size, , count, , )
STAT(ep_diskqueue_items, , count, , )
STAT(ep_commit_num, , count, , )
STAT(ep_commit_time, , microseconds, , )
STAT(ep_commit_time_total, , microseconds, , )
STAT(ep_item_begin_failed, , count, , )
STAT(ep_item_commit_failed, , count, , )
STAT(ep_item_flush_expired, , count, , )
STAT(ep_item_flush_failed, , count, , )
STAT(ep_flusher_state, , none, , )
STAT(ep_flusher_todo, , count, , )
STAT(ep_total_persisted, , count, , )
STAT(ep_uncommitted_items, , count, , )
STAT(ep_chk_persistence_timeout, , seconds, , )
STAT(ep_vbucket_del, , count, , )
STAT(ep_vbucket_del_fail, , count, , )
STAT(ep_flush_duration_total, , milliseconds, , )
STAT(ep_persist_vbstate_total, , count, , )
STAT(mem_used, , bytes, , )
STAT(mem_used_primary, , bytes, domain_memory_used, LABEL(domain, primary))
STAT(mem_used_secondary, , bytes, domain_memory_used, LABEL(domain, secondary))
STAT(mem_used_estimate, , bytes, , )
STAT(ep_mem_low_wat_percent, , percent, , )
STAT(ep_mem_high_wat_percent, , percent, , )
/* TODO: it's not advised to have metric like:
 *   my_metric{label=a} 1
 *   my_metric{label=b} 6
 *   my_metric{label=total} 7
 *  as a total is inconvenient for aggregation, _but_ we do track
 * several stats which are logically totals which might include things _not_
 * available under any other metric. Exposing it under a different metric name
 * seems best. Note: "..._total" is expected to be reserved for Counters -
 * totals over time, not totals of other things.
 */
STAT(bytes, , bytes, total_memory_used, )
STAT(ep_kv_size, , bytes, memory_used, LABEL(for, hashtable))
STAT(ep_blob_num, , count, , )
STAT(ep_blob_overhead, ,
     bytes,
     memory_overhead,
     LABEL(for, blobs)) // TODO: Assess what labels would actually be _useful_ for querying
STAT(ep_value_size, ,
     bytes,
     memory_used,
     LABEL(for, blobs))
STAT(ep_storedval_size, , bytes, memory_used, LABEL(for, storedvalues))
STAT(ep_storedval_overhead, , bytes, memory_overhead, LABEL(for, storedvalues))
STAT(ep_storedval_num, , count, , )
STAT(ep_overhead, , bytes, total_memory_overhead, )
STAT(ep_item_num, , count, , )
STAT(ep_oom_errors, , count, , )
STAT(ep_tmp_oom_errors, , count, , )
STAT(ep_mem_tracker_enabled, , none, , )
STAT(ep_bg_fetched, , count, , )
STAT(ep_bg_meta_fetched, , count, , )
STAT(ep_bg_remaining_items, , count, , )
STAT(ep_bg_remaining_jobs, , count, , )
STAT(ep_num_pager_runs, , count, , )
STAT(ep_num_expiry_pager_runs, , count, , )
STAT(ep_num_freq_decayer_runs, , count, , )
STAT(ep_items_expelled_from_checkpoints, , count, , )
STAT(ep_items_rm_from_checkpoints, , count, , )
STAT(ep_num_value_ejects, , count, , )
STAT(ep_num_eject_failures, , count, , )
STAT(ep_num_not_my_vbuckets, , count, , )
STAT(ep_pending_ops, , count, , )
STAT(ep_pending_ops_total,
     ,
     count,
     , ) // TODO: are total-over-uptime stats relevant for prometheus
         //  given the ability to sum over a time period?
STAT(ep_pending_ops_max,
     ,
     count,
     , ) // TODO: standardise labelling for "high watermark" style stats
STAT(ep_pending_ops_max_duration, , microseconds, , )
STAT(ep_pending_compactions, , count, , )
STAT(ep_rollback_count, , count, , )
STAT(ep_vbucket_del_max_walltime, , microseconds, , )
STAT(ep_vbucket_del_avg_walltime, , microseconds, , )
STAT(ep_bg_num_samples, , count, , )
STAT(ep_bg_min_wait, , microseconds, , )
STAT(ep_bg_max_wait, , microseconds, , )
STAT(ep_bg_wait_avg, , microseconds, , ) // TODO: derived from two stats. Decide
                                         //  whether to expose for prometheus
STAT(ep_bg_min_load, , microseconds, , )
STAT(ep_bg_max_load, , microseconds, , )
STAT(ep_bg_load_avg, , microseconds, , ) // TODO: derived from two stats. Decide
                                         //  whether to expose for prometheus
STAT(ep_bg_wait, , microseconds, , )
STAT(ep_bg_load, , microseconds, , )
STAT(ep_degraded_mode, , none, , )
STAT(ep_num_access_scanner_runs, , count, , )
STAT(ep_num_access_scanner_skips, , count, , )
STAT(ep_access_scanner_last_runtime,
     ,
     seconds,
     , ) // TODO: relative to server start. Convert to absolute time?
STAT(ep_access_scanner_num_items, , count, , )
STAT(ep_access_scanner_task_time,
     ,
     none,
     , ) // TODO: this is a string, expose numeric time for Prometheus
STAT(ep_expiry_pager_task_time,
     ,
     none,
     , ) // TODO: this is a string, expose numeric time for Prometheus
STAT(ep_startup_time, , seconds, , )
STAT(ep_warmup_thread, , none, , )
STAT(ep_warmup_time, , microseconds, , )
STAT(ep_warmup_oom, , count, , )
STAT(ep_warmup_dups, , count, , )
STAT(ep_num_ops_get_meta, , count, ops, LABEL(op, get_meta))
STAT(ep_num_ops_set_meta, , count, ops, LABEL(op, set_meta))
STAT(ep_num_ops_del_meta, , count, ops, LABEL(op, del_meta))
STAT(ep_num_ops_set_meta_res_fail, , count, ops_failed, LABEL(op, set_meta))
STAT(ep_num_ops_del_meta_res_fail, , count, ops_failed, LABEL(op, del_meta))
STAT(ep_num_ops_set_ret_meta, , count, ops, LABEL(op, set_ret_meta))
STAT(ep_num_ops_del_ret_meta, , count, ops, LABEL(op, del_ret_meta))
STAT(ep_num_ops_get_meta_on_set_meta,
     ,
     count,
     ops,
     LABEL(op, get_meta_for_set_meta))
STAT(ep_workload_pattern, , none, , )
STAT(ep_defragmenter_num_visited, , count, , )
STAT(ep_defragmenter_num_moved, , count, , )
STAT(ep_defragmenter_sv_num_moved, , count, , )
STAT(ep_item_compressor_num_visited, , count, , )
STAT(ep_item_compressor_num_compressed, , count, , )
STAT(ep_cursors_dropped, , count, , )
STAT(ep_mem_freed_by_checkpoint_removal, , bytes, , )
STAT(ep_mem_freed_by_checkpoint_item_expel, , bytes, , )
STAT(ep_num_checkpoints, , count, , )
STAT(ep_data_write_failed, , count, , )
STAT(ep_data_read_failed, , count, , )
STAT(ep_io_document_write_bytes, , bytes, , )
STAT(ep_io_total_read_bytes, , bytes, , )
STAT(ep_io_total_write_bytes, , bytes, , )
STAT(ep_io_compaction_read_bytes, , bytes, , )
STAT(ep_io_compaction_write_bytes, , bytes, , )
STAT(ep_io_bg_fetch_read_count, , count, , )
STAT(ep_bg_fetch_avg_read_amplification, , ratio, , )

// Magma stats
STAT(ep_magma_sets, , count, , )
STAT(ep_magma_gets, , count, , )
STAT(ep_magma_inserts, , count, , )
// Compaction.
STAT(ep_magma_compactions, , count, , )
STAT(ep_magma_flushes, , count, , )
STAT(ep_magma_ttl_compactions, , count, , )
STAT(ep_magma_filecount_compactions, , count, , )
STAT(ep_magma_writer_compactions, , count, , )
// Read amp.
STAT(ep_magma_readamp, , ratio, , )
STAT(ep_magma_readamp_get, , ratio, , )
STAT(ep_magma_read_bytes, , bytes, , )
STAT(ep_magma_read_bytes_compact, , bytes, , )
STAT(ep_magma_read_bytes_get, , bytes, , )
STAT(ep_magma_bytes_outgoing, , bytes, , )
// ReadIOAmp.
STAT(ep_magma_readio, , count, , )
STAT(ep_magma_readioamp, , ratio, , )
STAT(ep_magma_bytes_per_read, , ratio, , )
// Write amp.
STAT(ep_magma_writeamp, , ratio, , )
STAT(ep_magma_bytes_incoming, , bytes, , )
STAT(ep_magma_write_bytes, , bytes, , )
STAT(ep_magma_write_bytes_compact, , bytes, , )
// Fragmentation.
STAT(ep_magma_logical_data_size, , bytes, , )
STAT(ep_magma_logical_disk_size, , bytes, , )
STAT(ep_magma_fragmentation, , ratio, , )
// Disk usage.
STAT(ep_magma_total_disk_usage, , bytes, , )
STAT(ep_magma_wal_disk_usage, , bytes, , )
// Memory usage.
STAT(ep_magma_read_ahead_buffer_mem_used, , bytes, , )
STAT(ep_magma_lsmtree_object_mem_used, , bytes, , )
STAT(ep_magma_table_object_mem_used, , bytes, , )
STAT(ep_magma_histogram_mem_used, , bytes, , )
STAT(ep_magma_block_cache_mem_used, , bytes, , )
STAT(ep_magma_write_cache_mem_used, , bytes, , )
STAT(ep_magma_wal_mem_used, , bytes, , )
STAT(ep_magma_table_meta_mem_used, , bytes, , )
STAT(ep_magma_buffer_mem_used, , bytes, , )
STAT(ep_magma_bloom_filter_mem_used, , bytes, , )
STAT(ep_magma_total_mem_used, , bytes, , )
STAT(ep_magma_index_resident_ratio, , ratio, , )
// Block cache.
STAT(ep_magma_block_cache_hits, , count, , )
STAT(ep_magma_block_cache_misses, , count, , )
STAT(ep_magma_block_cache_hit_ratio, , ratio, , )
// SST file count.
STAT(ep_magma_tables_created, , count, , )
STAT(ep_magma_tables_deleted, , count, , )
STAT(ep_magma_tables, , count, , )
// NSync.
STAT(ep_magma_syncs, , count, , )

STAT(ep_rocksdb_kMemTableTotal, , bytes, , )
STAT(ep_rocksdb_kMemTableUnFlushed, , bytes, , )
STAT(ep_rocksdb_kTableReadersTotal, , bytes, , )
STAT(ep_rocksdb_kCacheTotal, , bytes, , )
STAT(ep_rocksdb_default_kSizeAllMemTables, , bytes, , )
STAT(ep_rocksdb_seqno_kSizeAllMemTables, , bytes, , )
STAT(ep_rocksdb_block_cache_data_hit_ratio, , ratio, , )
STAT(ep_rocksdb_block_cache_index_hit_ratio, , ratio, , )
STAT(ep_rocksdb_block_cache_filter_hit_ratio, , ratio, , )
STAT(ep_rocksdb_default_kTotalSstFilesSize, , bytes, , )
STAT(ep_rocksdb_seqno_kTotalSstFilesSize, , bytes, , )
STAT(ep_rocksdb_scan_totalSeqnoHits, , count, , )
STAT(ep_rocksdb_scan_oldSeqnoHits, , count, , )

// EPBucket::getFileStats
STAT(ep_db_data_size, , bytes, , )
STAT(ep_db_file_size, , bytes, , )
STAT(ep_db_prepare_size, , bytes, , )

// Timing stats
PSTAT(cmd_duration, microseconds, )

STAT(bg_wait, , microseconds, , )
STAT(bg_load, , microseconds, , )
CBSTAT(pending_ops, , microseconds)
CBSTAT(access_scanner, , microseconds)
CBSTAT(checkpoint_remover, , microseconds)
CBSTAT(item_pager, , microseconds)
CBSTAT(expiry_pager, , microseconds)
CBSTAT(storage_age, , microseconds)
CBSTAT(set_with_meta, , microseconds)
CBSTAT(get_cmd, , microseconds)
CBSTAT(store_cmd, , microseconds)
CBSTAT(arith_cmd, , microseconds)
CBSTAT(get_stats_cmd, , microseconds)
CBSTAT(get_vb_cmd, , microseconds)
CBSTAT(set_vb_cmd, , microseconds)
CBSTAT(del_vb_cmd, , microseconds)
CBSTAT(chk_persistence_cmd, , microseconds)
CBSTAT(notify_io, , microseconds)
STAT(disk_insert, , microseconds, disk, LABEL(op, insert))
STAT(disk_update, , microseconds, disk, LABEL(op, update))
STAT(disk_del, , microseconds, disk, LABEL(op, del))
STAT(disk_vb_del, , microseconds, disk, LABEL(op, vb_del))
STAT(disk_commit, , microseconds, disk, LABEL(op, commit))
STAT(item_alloc_sizes,
     ,
     bytes,
     , ) // TODO: this is not timing related but is in doTimingStats
STAT(bg_batch_size,
     ,
     count,
     , ) // TODO: this is not timing related but is in doTimingStats
CBSTAT(persistence_cursor_get_all_items, , microseconds)
CBSTAT(dcp_cursors_get_all_items, , microseconds)
STAT(sync_write_commit_majority,
     ,
     microseconds,
     sync_write_commit_duration,
     LABEL(level, majority))
STAT(sync_write_commit_majority_and_persist_on_master,
     ,
     microseconds,
     sync_write_commit_duration,
     LABEL(level, majority_and_persist_on_master))
STAT(sync_write_commit_persist_to_majority,
     ,
     microseconds,
     sync_write_commit_duration,
     LABEL(level, persist_to_majority))

// server_stats
STAT(uptime, , seconds, , )
STAT(stat_reset,
     ,
     none,
     , ) // TODO: String indicating when stats were reset. Change
         //  to a numeric stat for Prometheus?
STAT(time, , seconds, , )

// The following 3 stats are not exposed to prometheus as we don't expect their
// values to change over time, assuming the system's clock configuration is
// constant. However it is useful to have them show in cbstats so we can read
// their current values either ad-hoc via cbstats or via cbcollect_info.
CBSTAT(clock_fine_overhead_ns, , none)
CBSTAT(clock_coarse_overhead_ns, , none)
CBSTAT(clock_measurement_period_ns, , none)

STAT(version, , none, , ) // version string
STAT(memcached_version, , none, , ) // version string
STAT(daemon_connections, , count, , )
STAT(curr_connections, , count, , )
STAT(system_connections, , count, , )
STAT(total_connections, , count, , ) // total since start/reset
STAT(connection_structures, , count, , )
CBSTAT(cmd_get, , count) // this is not exposed to Prometheus as it duplicates
                         // the information provided by get_hits+get_misses
STAT(cmd_set, , count, ops, LABEL(op, set))
STAT(cmd_flush, , count, ops, LABEL(op, flush))
STAT(cmd_lock, , count, ops, LABEL(op, lock))
STAT(cmd_subdoc_lookup, , count, subdoc_ops, LABEL(op, lookup))
STAT(cmd_subdoc_mutation, , count, subdoc_ops, LABEL(op, mutation))
STAT(bytes_subdoc_lookup_total,
     ,
     bytes,
     subdoc_lookup_searched, ) // type _bytes will be suffixed
STAT(bytes_subdoc_lookup_extracted, , bytes, subdoc_lookup_extracted, )
STAT(bytes_subdoc_mutation_total, , bytes, subdoc_mutation_updated, )
STAT(bytes_subdoc_mutation_inserted, , bytes, subdoc_mutation_inserted, )
// aggregates over all buckets
STAT(cmd_total_sets, , count, , )
STAT(cmd_total_gets, , count, , )
STAT(cmd_total_ops, , count, , )
// aggregates over multiple operations for a single bucket
STAT(cmd_mutation, , count, , )
STAT(cmd_lookup, , count, , )

STAT(auth_cmds, , count, , )
STAT(auth_errors, , count, , )
STAT(get_hits, , count, ops, LABEL(op, get), LABEL(result, hit))
STAT(get_misses, , count, ops, LABEL(op, get), LABEL(result, miss))
STAT(delete_misses, , count, ops, LABEL(op, delete), LABEL(result, miss))
STAT(delete_hits, , count, ops, LABEL(op, delete), LABEL(result, hit))
STAT(incr_misses, , count, ops, LABEL(op, incr), LABEL(result, miss))
STAT(incr_hits, , count, ops, LABEL(op, incr), LABEL(result, hit))
STAT(decr_misses, , count, ops, LABEL(op, decr), LABEL(result, miss))
STAT(decr_hits, , count, ops, LABEL(op, decr), LABEL(result, hit))
STAT(cas_misses, , count, ops, LABEL(op, cas), LABEL(result, miss))
STAT(cas_hits, , count, ops, LABEL(op, cas), LABEL(result, hit))
STAT(cas_badval, , count, ops, LABEL(op, cas), LABEL(result, badval))
STAT(bytes_read, , bytes, read, ) // type _bytes will be suffixed
STAT(bytes_written, , bytes, written, )
STAT(rejected_conns, , count, , )
STAT(threads, , count, , )
STAT(conn_yields, , count, , )
STAT(iovused_high_watermark, , none, , )
STAT(msgused_high_watermark, , none, , )
STAT(lock_errors, , count, , )
STAT(cmd_lookup_10s_count, , count, , )
// us suffix would be confusing in Prometheus as the stat is scaled to seconds
STAT(cmd_lookup_10s_duration_us, , microseconds, cmd_lookup_10s_duration, )
STAT(cmd_mutation_10s_count, , count, , )
// us suffix would be confusing in Prometheus as the stat is scaled to seconds
STAT(cmd_mutation_10s_duration_us, , microseconds, cmd_mutation_10s_duration, )
STAT(total_resp_errors, , count, , )
STAT(audit_enabled, "enabled", none, audit_enabled, )
STAT(audit_dropped_events, "dropped_events", count, audit_dropped_events, )

STAT(vb_num, FMT("vb_{state}_num"), count, num_vbuckets, )
STAT(vb_curr_items, FMT("vb_{state}_curr_items"), count, vb_curr_items, )
STAT(vb_hp_vb_req_size,
     FMT("vb_{state}_hp_vb_req_size"),
     count,
     num_high_pri_requests, )
STAT(vb_num_non_resident,
     FMT("vb_{state}_num_non_resident"),
     count,
     vb_num_non_resident, )
STAT(vb_perc_mem_resident,
     FMT("vb_{state}_perc_mem_resident"),
     percent,
     vb_perc_mem_resident, )
STAT(vb_eject, FMT("vb_{state}_eject"), count, vb_eject, )
STAT(vb_expired, FMT("vb_{state}_expired"), count, vb_expired, )
STAT(vb_meta_data_memory,
     FMT("vb_{state}_meta_data_memory"),
     bytes,
     vb_meta_data_memory, )
STAT(vb_meta_data_disk,
     FMT("vb_{state}_meta_data_disk"),
     bytes,
     vb_meta_data_disk, )
STAT(vb_checkpoint_memory,
     FMT("vb_{state}_checkpoint_memory"),
     bytes,
     vb_checkpoint_memory, )
STAT(vb_checkpoint_memory_unreferenced,
     FMT("vb_{state}_checkpoint_memory_unreferenced"),
     bytes,
     vb_checkpoint_memory_unreferenced, )
STAT(vb_checkpoint_memory_overhead,
     FMT("vb_{state}_checkpoint_memory_overhead"),
     bytes,
     vb_checkpoint_memory_overhead, )
STAT(vb_ht_memory, FMT("vb_{state}_ht_memory"), bytes, vb_ht_memory, )
STAT(vb_itm_memory, FMT("vb_{state}_itm_memory"), bytes, vb_itm_memory, )
STAT(vb_itm_memory_uncompressed,
     FMT("vb_{state}_itm_memory_uncompressed"),
     bytes,
     vb_itm_memory_uncompressed, )
STAT(vb_ops_create, FMT("vb_{state}_ops_create"), count, vb_ops_create, )
STAT(vb_ops_update, FMT("vb_{state}_ops_update"), count, vb_ops_update, )
STAT(vb_ops_delete, FMT("vb_{state}_ops_delete"), count, vb_ops_delete, )
STAT(vb_ops_get, FMT("vb_{state}_ops_get"), count, vb_ops_get, )
STAT(vb_ops_reject, FMT("vb_{state}_ops_reject"), count, vb_ops_reject, )
STAT(vb_queue_size, FMT("vb_{state}_queue_size"), count, vb_queue_size, )
STAT(vb_queue_memory, FMT("vb_{state}_queue_memory"), bytes, vb_queue_memory, )
STAT(vb_queue_age, FMT("vb_{state}_queue_age"), milliseconds, vb_queue_age, )
STAT(vb_queue_pending,
     FMT("vb_{state}_queue_pending"),
     bytes,
     vb_queue_pending, )
STAT(vb_queue_fill, FMT("vb_{state}_queue_fill"), count, vb_queue_fill, )
STAT(vb_queue_drain, FMT("vb_{state}_queue_drain"), count, vb_queue_drain, )
STAT(vb_rollback_item_count,
     FMT("vb_{state}_rollback_item_count"),
     count,
     vb_rollback_item_count, )

STAT(curr_items, , count, , )
STAT(curr_temp_items, , count, , )
STAT(curr_items_tot, , count, , )

STAT(vb_sync_write_accepted_count,
     FMT("vb_{state}_sync_write_accepted_count"),
     count,
     vb_sync_write_accepted_count, )
STAT(vb_sync_write_committed_count,
     FMT("vb_{state}_sync_write_committed_count"),
     count,
     vb_sync_write_committed_count, )
STAT(vb_sync_write_aborted_count,
     FMT("vb_{state}_sync_write_aborted_count"),
     count,
     vb_sync_write_aborted_count, )

STAT(ep_vb_total, , count, , )
STAT(ep_total_new_items, , count, , )
STAT(ep_total_del_items, , count, , )
STAT(ep_diskqueue_memory, , bytes, , )
STAT(ep_diskqueue_fill, , count, , )
STAT(ep_diskqueue_drain, , count, , )
STAT(ep_diskqueue_pending, , count, , )
STAT(ep_meta_data_memory, , bytes, , )
STAT(ep_meta_data_disk, , bytes, , )
STAT(ep_checkpoint_memory, , bytes, , )
STAT(ep_checkpoint_memory_unreferenced, , bytes, , )
STAT(ep_checkpoint_memory_overhead, , bytes, , )
STAT(ep_checkpoint_memory_pending_destruction, , bytes, , )
STAT(ep_total_cache_size, , bytes, , )
STAT(rollback_item_count, , count, , )
STAT(ep_num_non_resident, , count, , )
STAT(ep_chk_persistence_remains, , count, , )
STAT(ep_hlc_drift, FMT("ep_{state}_hlc_drift"), microseconds, , )
STAT(ep_hlc_drift_count, FMT("ep_{state}_hlc_drift_count"), count, , )
STAT(ep_ahead_exceptions, FMT("ep_{state}_ahead_exceptions"), count, , )
STAT(ep_behind_exceptions, FMT("ep_{state}_behind_exceptions"), count, , )
STAT(ep_clock_cas_drift_threshold_exceeded, , count, , )
STAT(datatype_count, FMT("ep_{vbucket_state}_datatype_{datatype}"), count, , )

STAT(vb_auto_delete_count,
     FMT("vb_{state}_auto_delete_count"),
     count,
     vb_auto_delete_count, )
STAT(vb_ht_tombstone_purged_count,
     FMT("vb_{state}_ht_tombstone_purged_count"),
     count,
     vb_ht_tombstone_purged_count, )
STAT(vb_seqlist_count,
     FMT("vb_{state}_seqlist_count"),
     count,
     vb_seqlist_count, )
STAT(vb_seqlist_deleted_count,
     FMT("vb_{state}_seqlist_deleted_count"),
     count,
     vb_seqlist_deleted_count, )
STAT(vb_seqlist_purged_count,
     FMT("vb_{state}_seqlist_purged_count"),
     count,
     vb_seqlist_purged_count, )
STAT(vb_seqlist_read_range_count,
     FMT("vb_{state}_seqlist_read_range_count"),
     count,
     vb_seqlist_read_range_count, )
STAT(vb_seqlist_stale_count,
     FMT("vb_{state}_seqlist_stale_count"),
     count,
     vb_seqlist_stale_count, )
STAT(vb_seqlist_stale_value_bytes,
     FMT("vb_{state}_seqlist_stale_value_bytes"),
     bytes,
     vb_seqlist_stale_value, )
STAT(vb_seqlist_stale_metadata_bytes,
     FMT("vb_{state}_seqlist_stale_metadata_bytes"),
     bytes,
     vb_seqlist_stale_metadata, )

STAT(connagg_connection_count,
     FMT("{connection_type}:count"),
     count,
     dcp_connection_count, )
STAT(connagg_backoff, FMT("{connection_type}:backoff"), count, dcp_backoff, )
STAT(connagg_producer_count,
     FMT("{connection_type}:producer_count"),
     count,
     dcp_producer_count, )
STAT(connagg_items_sent,
     FMT("{connection_type}:items_sent"),
     count,
     dcp_items_sent, )
STAT(connagg_items_remaining,
     FMT("{connection_type}:items_remaining"),
     count,
     dcp_items_remaining, )
STAT(connagg_total_bytes,
     FMT("{connection_type}:total_bytes"),
     bytes,
     dcp_total_data_size, )
STAT(connagg_total_uncompressed_data_size,
     FMT("{connection_type}:total_uncompressed_data_size"),
     bytes,
     dcp_total_uncompressed_data_size, )

// aggregated dcp producer stats

CBSTAT(dcp_count, "ep_dcp_count", count) // redundant in prometheus, is the sum
                                         // of consumer and producer
STAT(dcp_consumer_count,
     "ep_dcp_consumer_count",
     count,
     dcp_count,
     LABEL(type, consumer))
STAT(dcp_producer_count,
     "ep_dcp_producer_count",
     count,
     dcp_count,
     LABEL(type, producer))
STAT(dcp_total_data_size, "ep_dcp_total_bytes", bytes, , )
STAT(dcp_total_uncompressed_data_size,
     "ep_dcp_total_uncompressed_data_size",
     bytes,
     , )
CBSTAT(dcp_total_queue,
       "ep_dcp_total_queue",
       count) // derivable from fill and sent
STAT(dcp_queue_fill, "ep_dcp_queue_fill", count, , )
STAT(dcp_items_sent, "ep_dcp_items_sent", count, , )
STAT(dcp_items_remaining, "ep_dcp_items_remaining", count, , )
STAT(dcp_num_running_backfills, "ep_dcp_num_running_backfills", count, , )
STAT(dcp_max_running_backfills, "ep_dcp_max_running_backfills", count, , )

STAT(manifest_uid, , none, , )
STAT(manifest_force, "force", none, , )

STAT(collection_name, "name", none, , )
STAT(collection_scope_name, "scope_name", none, , )
STAT(collection_maxTTL, "maxTTL", seconds, , )

STAT(scope_name, "name", none, , )
STAT(scope_data_limit, "data_limit", none, , )
STAT(scope_collection_count, "collections", count, , )

STAT(collection_mem_used, "collections_mem_used", bytes, , )
STAT(collection_item_count, "items", count, , )
STAT(collection_data_size, "data_size", bytes, , )

STAT(collection_ops_store, "ops_store", count, collection_ops, LABEL(op, store))
STAT(collection_ops_delete,
     "ops_delete",
     count,
     collection_ops,
     LABEL(op, delete))
STAT(collection_ops_get, "ops_get", count, collection_ops, LABEL(op, get))
