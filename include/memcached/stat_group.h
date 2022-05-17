/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

enum class StatGroupId {
    All,
    Reset,
    WorkerThreadInfo,
    Audit,
    BucketDetails,
    Aggregate,
    Connections,
    Clocks,
    JsonValidate,
    SnappyDecompress,
    SubdocExecute,
    Responses,
    Tracing,
    Allocator,
    Scopes,
    ScopesById,
    ScopesDetails,
    Collections,
    CollectionsById,
    CollectionsDetails,

    // ep engine
    Dcpagg,
    Dcp,
    Eviction,
    Hash,
    Vbucket,
    PrevVbucket,
    VbucketDurabilityState,
    VbucketDetails,
    VbucketSeqno,
    Checkpoint,
    DurabilityMonitor,
    Timings,
    Dispatcher,
    Tasks,
    Scheduler,
    Runtimes,
    Memory,
    Key,
    KeyById,
    Vkey,
    VkeyById,
    Kvtimings,
    Kvstore,
    Warmup,
    Info,
    Config,
    DcpVbtakeover,
    Workload,
    Failovers,
    Diskinfo,
    DiskFailures,
    _CheckpointDump,
    _HashDump,
    _DurabilityDump,
    _VbucketDump,
    Uuid,
    RangeScans,

    // memcached bucket internal stats
    Slabs,
    Items,
    Sizes,
    Scrub,

    // stat command timings

    StatTimings,

    enum_max
};

/// The information we keep for a certain stats group
class StatGroup {
public:
    /// The identifier for the stat
    const StatGroupId id;
    /// The key used to identify the stat group
    const std::string_view key;
    /// A "longer" description of the stat
    const std::string_view description;
    /// Is this a privileged stat or not
    const bool privileged;
    /// Is this a per-bucket stat
    const bool bucket;
};

/// The stats group manager contains knowledge about all stats in the
/// system.
class StatsGroupManager {
public:
    /// Get the one and only instance of the StatsGroupManager
    static StatsGroupManager& getInstance();

    /**
     * Try to look up the stats matching the key
     *
     * @param key the stat group to look up
     * @return The stat group if found, nullptr for unknown keys
     */
    static const StatGroup* lookup(std::string_view key);

    /// Iterate over all the available stats and call the provided callback
    void iterate(std::function<void(const StatGroup&)> callback);

protected:
    StatsGroupManager();
    const StatGroup* doLookup(std::string_view id);

    const std::vector<StatGroup> entries;
};
