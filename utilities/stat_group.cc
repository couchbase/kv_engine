/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/stat_group.h>

StatsGroupManager::StatsGroupManager()
    : entries({{StatGroupId::All, "", "Get the common stats", false, true},
               {StatGroupId::Reset, "reset", "Reset stats", true, true},
               {StatGroupId::WorkerThreadInfo,
                "worker_thread_info",
                "Information about the worker threads",
                true,
                false},
               {StatGroupId::Audit, "audit", "Audit subsystem", true, false},
               {StatGroupId::BucketDetails,
                "bucket_details",
                "Bucket subsystem",
                true,
                false},
               {StatGroupId::Aggregate,
                "aggregate",
                "An aggregated view of some stats",
                false,
                true},
               {StatGroupId::Connections,
                "connections",
                "Connection subsystem",
                false,
                false},
               {StatGroupId::Clocks,
                "clocks",
                "Clock information",
                false,
                false},
               {StatGroupId::JsonValidate,
                "json_validate",
                "Timings related to JSON validation",
                false,
                true},
               {StatGroupId::SnappyDecompress,
                "snappy_decompress",
                "Timings related to Snappy inflate",
                false,
                true},
               {StatGroupId::SubdocExecute,
                "subdoc_execute",
                "Timings related to subdoc execution",
                false,
                true},
               {StatGroupId::Responses,
                "responses",
                "The number of various response codes",
                false,
                true},
               {StatGroupId::Tracing,
                "tracing",
                "Tracing information",
                true,
                false},
               {StatGroupId::Allocator,
                "allocator",
                "Memory internal allocation statistics",
                true,
                false},
               {StatGroupId::Scopes,
                "scopes",
                "Stats for all scopes or a single scope (using scope name as a "
                "key)",
                false,
                true},
               {StatGroupId::ScopesById,
                "scopes-byid",
                "Stats for a single scope using the id as a key",
                false,
                true},
               {StatGroupId::ScopesDetails,
                "scopes-details",
                "Detailed vbucket-level stats for all scopes, for one or all "
                "vbuckets",
                false,
                true},
               {StatGroupId::Collections,
                "collections",
                "Stats for all collections or a single collection (using "
                "collection name as a key)",
                false,
                true},
               {StatGroupId::CollectionsById,
                "collections-byid",
                "Stats for a single collection using the id as a key",
                false,
                true},
               {StatGroupId::CollectionsDetails,
                "collections-details",
                "Detailed vbucket-level stats for all collections, for one or "
                "all vbuckets",
                false,
                true},
               {StatGroupId::TasksAll,
                "tasks-all",
                "Get information from the tasks in the executor pool for all "
                "Taskables",
                true,
                false},

               {StatGroupId::Uuid,
                "uuid",
                "Get the UUID for the bucket",
                false,
                true},
               // epe specifics
               {StatGroupId::Dcpagg,
                "dcpagg",
                "Get aggregated DCP stats",
                false,
                true},
               {StatGroupId::Dcp, "dcp", "Get DCP related stats", false, true},
               {StatGroupId::Eviction,
                "eviction",
                "Get eviction related stats",
                false,
                true},
               {StatGroupId::Hash,
                "hash",
                "Hash stats provide information on your vbucket hash tables",
                false,
                true},
               {StatGroupId::Vbucket,
                "vbucket",
                "vbucket related stats",
                false,
                true},
               {StatGroupId::PrevVbucket,
                "prev-vbucket",
                "Get the previous vbstate information",
                false,
                true},
               {StatGroupId::VbucketDurabilityState,
                "vbucket-durability-state",
                "Get the durability state for the vbucket",
                false,
                true},
               {StatGroupId::VbucketDetails,
                "vbucket-details",
                "Detailed information about a vbucket",
                false,
                true},
               {StatGroupId::VbucketSeqno,
                "vbucket-seqno",
                "Get the sequence numbers for a vbucket",
                false,
                true},
               {StatGroupId::Checkpoint,
                "checkpoint",
                "Checkpoint stats provide detailed information on per-vbucket "
                "checkpoint datastructure",
                false,
                true},
               {StatGroupId::DurabilityMonitor,
                "durability-monitor",
                "Get information from the durability monitor",
                false,
                true},
               {StatGroupId::Timings,
                "timings",
                "Timing stats provide histogram data from high resolution "
                "timers over various operations within the system.",
                false,
                true},
               {StatGroupId::FrequencyCounters,
                "frequency-counters",
                "Histogram describing the MFU values of the items in the "
                "hashtable.",
                false,
                true},
               {StatGroupId::Dispatcher,
                "dispatcher",
                "This provides the stats from AUX dispatcher and non-IO "
                "dispatcher, and from all the reader and writer threads "
                "running for the specific bucket",
                false,
                true},
               {StatGroupId::Tasks,
                "tasks",
                "Get information from the tasks in the executor pool for the "
                "given Bucket",
                false,
                true},
               {StatGroupId::Scheduler,
                "scheduler",
                "Histograms describing the scheduling overhead times and task "
                "runtimes incurred by various IO and Non-IO tasks "
                "respectively",
                false,
                true},
               {StatGroupId::Runtimes,
                "runtimes",
                "Histograms describing the scheduling overhead times and task "
                "runtimes incurred by various IO and Non-IO tasks "
                "respectively",
                false,
                true},
               {StatGroupId::Memory,
                "memory",
                "This provides various memory-related stats",
                false,
                true},
               {StatGroupId::Key,
                "key",
                "Get information for a given key",
                false,
                true},
               {StatGroupId::KeyById,
                "key-byid",
                "Get information for a given key",
                false,
                true},
               {StatGroupId::Vkey,
                "vkey",
                "Get information for a given key",
                false,
                true},
               {StatGroupId::VkeyById,
                "vkey-byid",
                "Get information for a given key",
                false,
                true},
               {StatGroupId::Kvtimings,
                "kvtimings",
                "Timing stats provide timing information from the underlying "
                "storage system",
                false,
                true},
               {StatGroupId::Kvstore,
                "kvstore",
                "Various low-level stats and timings from the underlying KV "
                "storage system",
                false,
                true},
               {StatGroupId::Warmup,
                "warmup",
                "Statistics related to warmup logic",
                false,
                true},
               {StatGroupId::Info,
                "info",
                "Get the stats-info document",
                false,
                true},
               {StatGroupId::Config,
                "config",
                "Get the bucket configuration",
                false,
                true},
               {StatGroupId::DcpVbtakeover,
                "dcp-vbtakeover",
                "Get the statistics related to VB takeover",
                false,
                true},
               {StatGroupId::Workload,
                "workload",
                "Some information about the number of shards and Executor pool "
                "information",
                false,
                true},
               {StatGroupId::Failovers,
                "failovers",
                "Get failover information",
                false,
                true},
               {StatGroupId::Diskinfo,
                "diskinfo",
                "Get information from the underlying IO layer",
                false,
                true},
               {StatGroupId::DiskFailures,
                "disk-failures",
                "Get disk failures",
                false,
                true},
               {StatGroupId::_CheckpointDump,
                "_checkpoint-dump",
                "Dump the Checkpoint Manager for the provided vbucket",
                true,
                true},
               {StatGroupId::_HashDump,
                "_hash-dump",
                "Dump the hashtable",
                true,
                true},
               {StatGroupId::_DurabilityDump,
                "_durability-dump",
                "Dump the Durability Monitor",
                true,
                true},
               {StatGroupId::_VbucketDump,
                "_vbucket-dump",
                "Dump the vbucket",
                true,
                true},
               {StatGroupId::RangeScans,
                "range-scans",
                "Get information about vbucket range-scans",
                false,
                true},

               // memcached specifics
               {StatGroupId::Slabs,
                "slabs",
                "Get information about the internal slab classes (memcached "
                "bucket)",
                false,
                true},
               {StatGroupId::Items,
                "items",
                "Get information about the items types (memcached bucket)",
                false,
                true},
               {StatGroupId::Sizes,
                "sizes",
                "Get size related information (memcached bucket)",
                false,
                true},
               {StatGroupId::Scrub,
                "scrub",
                "Try to scrub the bucket (purge deleted documents) (memcached "
                "bucket)",
                false,
                true},
               {StatGroupId::StatTimings,
                "stat-timings",
                "Get timing information for stat commands",
                false,
                true},
               {StatGroupId::Threads,
                "threads",
                "Get information about thread numbers",
                true,
                false}}) {
}

StatsGroupManager& StatsGroupManager::getInstance() {
    static StatsGroupManager instance;
    return instance;
}

const StatGroup* StatsGroupManager::doLookup(std::string_view key) {
    for (const auto& e : entries) {
        if (e.key == key) {
            return &e;
        }
    }
    return nullptr;
}

const StatGroup* StatsGroupManager::lookup(std::string_view key) {
    return getInstance().doLookup(key);
}

void StatsGroupManager::iterate(
        std::function<void(const StatGroup&)> callback) {
    for (const auto& e : getInstance().entries) {
        callback(e);
    }
}

const StatGroup* StatsGroupManager::lookup(StatGroupId id) {
    for (const auto& e : entries) {
        if (e.id == id) {
            return &e;
        }
    }
    return nullptr;
}
