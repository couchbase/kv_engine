/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "ep_types.h"

#include <memcached/vbucket.h>
#include <nlohmann/json.hpp>
#include <platform/n_byte_integer.h>
#include <cstdint>
#include <string>

class Item;

// Sub structure that stores the data which only changes as part of a state
// transition.
struct vbucket_transition_state {
    bool needsToBePersisted(const vbucket_transition_state& transition) const;

    /// update the given item with a JSON version of this structure
    void toItem(Item& item) const;

    /// update this from the Item, assumes the Item's value was set by ::toItem
    void fromItem(const Item& item);

    bool operator==(const vbucket_transition_state& other) const;
    bool operator!=(const vbucket_transition_state& other) const;

    nlohmann::json failovers;

    /**
     * The replication topology for the vBucket. Can be empty if not yet set,
     * otherwise encoded as a JSON array of chains, each chain is a array of
     * node names - e.g.
     *
     *     [ ["active", "replica_1"], ["active", "replica_1", "replica_2"]]
     *
     * First GA'd in 6.5
     */
    nlohmann::json replicationTopology;

    vbucket_state_t state = vbucket_state_dead;
};

/**
 * Describes the detailed state of a VBucket, including it's high-level 'state'
 * (active, replica, etc), and the various seqnos and other properties it has.
 *
 * This is persisted to disk during flush.
 *
 * Note that over time additional fields have been added to the vBucket state.
 * Given this state is writen to disk, and we support offline upgrade between
 * versions -  newer versions must support reading older versions' disk files
 * (to a limited version range) - when new fields are added the serialization &
 * deserialization methods need to handle fiels not being present.
 *
 * At time of writing the current GA major release is v6, which supports
 * offline upgrade from v5.0 or later. Any earlier releases do not support
 * direct offline upgrade (you'd have to first upgrade to v5.x). As such we
 * only need to support fields which were added in v5.0 or later; earlier fields
 * can be assumed to already exist on disk (v5.0 would have already handled the
 * upgrade).
 */
struct vbucket_state {
    /**
     * Current version of vbucket_state structure.
     * This value is supposed to increase every time we make a
     * change to the structure (ie, adding/removing members) or we make a change
     * in the usage/interpretation of any member.
     * History:
     * v1: Implicit, pre 5.5.4-MP, 6.0.2 and mad-hatter.
     * v2: 5.5.4-MP, 6.0.2 and Mad-Hatter (pre GA), added with MB-34173.
     *     Indicates snapshot start/end are sanitized with respect to
     *     high_seqno.
     * v3: Mad-Hatter. high_completed_seqno and high_prepared_seqno added along
     *     with counter for number of prepares on disk. Checkpoint-ID no longer
     *     stored (and ignored during upgrade)
     * v4: 6.6.1, added with MB-32670. Adds onDiskPrepareBytes.
     */
    static constexpr int CurrentVersion = 4;

    uint64_t getOnDiskPrepareBytes() const {
        return onDiskPrepareBytes;
    }

    void setOnDiskPrepareBytes(int64_t value) {
        onDiskPrepareBytes = value;
    }

    /**
     * Apply the given delta to the value of onDiskPrepareBytes. Clamps
     * onDiskPrepareBytes at zero (avoids underflow).
     */
    void updateOnDiskPrepareBytes(int64_t delta);

    void reset();

    bool operator==(const vbucket_state& other) const;
    bool operator!=(const vbucket_state& other) const;

    cb::uint48_t maxDeletedSeqno = 0;
    int64_t highSeqno = 0;
    uint64_t purgeSeqno = 0;

    /**
     * Start seqno of the last snapshot persisted.
     * First GA'd in v3.0
     */
    uint64_t lastSnapStart = 0;

    /**
     * End seqno of the last snapshot persisted.
     * First GA'd in v3.0
     */
    uint64_t lastSnapEnd = 0;

    /**
     * Maximum CAS value in this vBucket.
     * First GA'd in v4.0
     */
    uint64_t maxCas = 0;

    /**
     * The seqno at which CAS started to be encoded as a hybrid logical clock.
     * First GA'd in v5.0
     */
    int64_t hlcCasEpochSeqno = HlcCasSeqnoUninitialised;

    /**
     * True if this vBucket _might_ contain documents with eXtended Attributes.
     * first GA'd in v5.0
     */
    bool mightContainXattrs = false;

    /**
     * Does this vBucket file support namespaces (leb128 prefix on keys).
     * First GA'd in v6.5
     */
    bool supportsNamespaces = true;

    /**
     * Version of vbucket_state. See comments against CurrentVersion for
     * details.
     */
    int version = CurrentVersion;

    /**
     * Stores the seqno of the last completed (Committed or Aborted) Prepare.
     * Added for SyncReplication in 6.5.
     */
    uint64_t persistedCompletedSeqno = 0;

    /**
     * Stores the seqno of the last prepare (Pending SyncWrite). Added for
     * SyncReplication in 6.5.
     */
    uint64_t persistedPreparedSeqno = 0;

    /**
     * Stores the seqno of the most recent prepare (Pending SyncWrite) seen in a
     * completed snapshot. This reflects the in-memory highPreparedSeqno
     * behaviour, and is used to initialise the in-memory value on warmup. This
     * is relevant for a replica, which should not acknowledge a seqno as
     * Prepared until the entire snapshot is received. Using the
     * persistedPreparedSeqno would lead to a replica acking a seqno which is
     * too high. Added for SyncReplication in 6.5.
     */
    uint64_t highPreparedSeqno = 0;

    /**
     * Stores the seqno of the most recent "visible" item persisted to disk
     * i.e., a seqno representing committed state exposed to end users, in
     * contrast to internally used data. Visible items include system events,
     * mutations, commits, and deletions but excludes prepares and aborts.
     * Backfills should end on this seqno if the connection has not negotiated
     * sync writes. Added for SyncReplication in 6.5.
     */
    uint64_t maxVisibleSeqno = 0;

    /**
     * Number of on disk prepares (Pending SyncWrites). Required to correct the
     * vBucket level on disk document counts (for Full Eviction). Added for
     * SyncReplication in 6.5.
     */
    uint64_t onDiskPrepares = 0;

private:
    /**
     * Size in bytes of on disk prepares (Pending SyncWrites). Required to
     * estimate the size of completed prepares as part of calculating the
     * couchstore 'stale' data overhead.
     * Note this is private as care needs to be taken when modifying it to
     * ensure it doesn't underflow.
     *
     * Added for SyncReplication (MB-42306) in 6.6.1.
     */
    uint64_t onDiskPrepareBytes = 0;

public:
    /**
     * The type of the most recently persisted snapshot disk. Required as we
     * cannot rely on the HCS in the middle of a snapshot to optimise warmup and
     * we need to be able to determine whether or not it is appropriate to do
     * so. Added for SyncReplication in 6.5.
     */
    CheckpointType checkpointType = CheckpointType::Memory;

    /**
     * Data that is changed as part of a vbucket state transition is stored
     * in this member.
     */
    vbucket_transition_state transition;
};

/// Method to allow nlohmann::json to convert vbucket_state to JSON.
void to_json(nlohmann::json& json, const vbucket_state& vbs);

/// Method to allow nlohmann::json to convert from JSON to vbucket_state.
void from_json(const nlohmann::json& j, vbucket_state& vbs);

/// Method to allow nlohmann::json to convert vbucket_transition_state to JSON.
void to_json(nlohmann::json& json, const vbucket_transition_state& vbs);

/// Method to allow nlohmann::json to convert from JSON to
/// vbucket_transition_state.
void from_json(const nlohmann::json& j, vbucket_transition_state& vbs);

std::ostream& operator<<(std::ostream& os, const vbucket_state& vbs);

std::ostream& operator<<(std::ostream& os, const vbucket_transition_state& vbs);
