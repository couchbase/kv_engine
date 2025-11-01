/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_parameters.h"

static const std::unordered_set<std::string_view> checkpointParamSet{
        "max_checkpoints",
        "checkpoint_memory_ratio",
        "checkpoint_memory_recovery_upper_mark",
        "checkpoint_memory_recovery_lower_mark",
        "checkpoint_max_size",
        "checkpoint_destruction_tasks",
};

static const std::unordered_set<std::string_view> flushParamSet{
        // Note: Not directly settable - triggers a bucket quota change
        "max_size",
        // Note: Not directly settable - triggers a bucket quota change
        "cache_size",
        "mem_low_wat_percent",
        "mem_high_wat_percent",
        "backfill_mem_threshold",
        "durability_min_level",
        "durability_impossible_fallback",
        "mutation_mem_ratio",
        "exp_pager_enabled",
        "exp_pager_stime",
        "exp_pager_initial_run_time",
        "flusher_total_batch_limit",
        "flush_batch_max_bytes",
        "getl_default_timeout",
        "getl_max_timeout",
        "ht_resize_interval",
        "ht_resize_algo",
        "ht_size",
        "ht_size_decrease_delay",
        "ht_temp_items_allowed_percent",
        "max_item_privileged_bytes",
        "max_item_size",
        "not_locked_returns_tmpfail",
        "access_scanner_enabled",
        "alog_path",
        "alog_max_stored_items",
        "alog_resident_ratio_threshold",
        "alog_sleep_time",
        "alog_task_time",
        "bfilter_fp_prob",
        "bfilter_key_count",
        "paging_visitor_pause_check_count",
        "expiry_visitor_items_only_duration_ms",
        "expiry_visitor_expire_after_visit_duration_ms",
        "item_eviction_age_percentage",
        "item_eviction_freq_counter_age_threshold",
        "item_eviction_initial_mfu_percentile",
        "item_eviction_initial_mfu_update_interval",
        "item_freq_decayer_chunk_duration",
        "item_freq_decayer_percent",
        "primary_warmup_min_items_threshold",
        "primary_warmup_min_memory_threshold",
        "secondary_warmup_min_memory_threshold",
        "secondary_warmup_min_items_threshold",
        "warmup_behavior",
        "bfilter_enabled",
        "bfilter_residency_threshold",
        "item_compressor_interval",
        "item_compressor_chunk_duration",
        "compaction_max_concurrent_ratio",
        "chk_expel_enabled",
        "dcp_min_compression_ratio",
        "dcp_noop_mandatory_for_v5_features",
        "ephemeral_full_policy",
        "ephemeral_mem_recovery_enabled",
        "ephemeral_mem_recovery_sleep_time",
        "ephemeral_metadata_mark_stale_chunk_duration",
        "ephemeral_metadata_purge_age",
        "ephemeral_metadata_purge_interval",
        "ephemeral_metadata_purge_stale_chunk_duration",
        "fsync_after_every_n_bytes_written",
        "xattr_enabled",
        "compression_mode",
        "min_compression_ratio",
        "max_ttl",
        "mem_used_merge_threshold_percent",
        "retain_erroneous_tombstones",
        "couchstore_tracing",
        "couchstore_write_validation",
        "couchstore_mprotect",
        "allow_sanitize_value_in_deletion",
        "persistent_metadata_purge_age",
        "couchstore_file_cache_max_size",
        "compaction_expire_from_start",
        "compaction_expiry_fetch_inline",
        "vbucket_mapping_sanity_checking",
        "vbucket_mapping_sanity_checking_error_mode",
        "seqno_persistence_timeout",
        "range_scan_max_continue_tasks",
        "bucket_quota_change_task_poll_interval",
        "range_scan_read_buffer_send_size",
        "range_scan_max_lifetime",
        "item_eviction_strategy",
        "history_retention_seconds",
        "history_retention_bytes",
        "continuous_backup_enabled",
        "continuous_backup_interval",
        "workload_monitor_enabled",
        "workload_pattern_default",
        /* defragmenter */
        "defragmenter_enabled",
        "defragmenter_interval",
        "defragmenter_age_threshold",
        "defragmenter_chunk_duration",
        "defragmenter_stored_value_age_threshold",
        "defragmenter_mode",
        "defragmenter_auto_lower_threshold",
        "defragmenter_auto_upper_threshold",
        "defragmenter_auto_max_sleep",
        "defragmenter_auto_min_sleep",
        "defragmenter_auto_pid_p",
        "defragmenter_auto_pid_i",
        "defragmenter_auto_pid_d",
        "defragmenter_auto_pid_dt",
        /* magma_fusion */
        "magma_fusion_logstore_uri",
        "magma_fusion_metadatastore_uri",
        "magma_fusion_upload_interval",
        "magma_fusion_logstore_fragmentation_threshold",
        "magma_fusion_max_log_cleaning_size_ratio",
        "magma_fusion_max_log_size",
        "magma_fusion_max_num_log_files",
        /* magma */
        "magma_fragmentation_percentage",
        "magma_flusher_thread_percentage",
        "magma_mem_quota_ratio",
        "magma_enable_block_cache",
        "magma_seq_tree_data_block_size",
        "magma_min_value_block_size_threshold",
        "magma_seq_tree_index_block_size",
        "magma_key_tree_data_block_size",
        "magma_key_tree_index_block_size",
        "magma_per_document_compression_enabled",
};

static const std::unordered_set<std::string_view> dcpParamSet{
        "dcp_backfill_in_progress_per_connection_limit",
        "dcp_consumer_buffer_ratio",
        "connection_manager_interval",
        "connection_cleanup_interval",
        "dcp_enable_noop",
        "dcp_idle_timeout",
        "dcp_noop_tx_interval",
        "dcp_oso_backfill",
        "dcp_oso_backfill_large_value_ratio",
        "dcp_oso_backfill_small_value_ratio",
        "dcp_oso_backfill_small_item_size_threshold",
        "dcp_producer_processor_run_duration_us",
        "dcp_producer_catch_exceptions",
        "dcp_takeover_max_time",
        "dcp_backfill_byte_limit",
        "dcp_oso_max_collections_per_backfill",
        "dcp_backfill_run_duration_limit",
        "dcp_backfill_idle_protection_enabled",
        "dcp_backfill_idle_limit_seconds",
        "dcp_backfill_idle_disk_threshold",
        "dcp_checkpoint_dequeue_limit",
        "dcp_cache_transfer_enabled",
};

static const std::unordered_set<std::string_view> vbucketParamSet{
        "hlc_drift_ahead_threshold_us",
        "hlc_drift_behind_threshold_us",
        "hlc_max_future_threshold_us",
        "hlc_invalid_strategy",
        "dcp_hlc_invalid_strategy",
};

static const std::unordered_set<std::string_view> allParamSet = []() {
    std::unordered_set<std::string_view> ret;
    ret.insert(checkpointParamSet.begin(), checkpointParamSet.end());
    ret.insert(flushParamSet.begin(), flushParamSet.end());
    ret.insert(dcpParamSet.begin(), dcpParamSet.end());
    ret.insert(vbucketParamSet.begin(), vbucketParamSet.end());
    return ret;
}();

bool checkSetParameterCategory(std::string_view key,
                               EngineParamCategory category) {
    switch (category) {
    case EngineParamCategory::Checkpoint:
        return checkpointParamSet.contains(key);
    case EngineParamCategory::Flush:
        return flushParamSet.contains(key);
    case EngineParamCategory::Dcp:
        return dcpParamSet.contains(key);
    case EngineParamCategory::Vbucket:
        return vbucketParamSet.contains(key);
    }
    return false;
}

std::unordered_set<std::string_view> getSetParameterKeys() {
    return allParamSet;
}
