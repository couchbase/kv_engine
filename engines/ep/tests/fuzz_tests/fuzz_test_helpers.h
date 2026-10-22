/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/response.h"
#include "fuzz_test_stringify.h"
#include "tests/module_tests/test_helpers.h"

#include <fuzztest/domain.h>
#include <memcached/storeddockey.h>
#include <platform/define_yes_no_enum.h>
#include <utilities/test_manifest.h>
#include <unordered_map>

namespace cb::fuzzing {

/**
 * Represents the type of a checkpoint action (item mutation or checkpoint
 * creation).
 * Includes only the "interesting" types.
 * If more are needed, update testCheckpointActionType and make them
 * conditional.
 */
enum class CheckpointActionType {
    CreateCheckpoint,
    Mutation,
    Prepare,
    Commit,
    Abort,
};

std::string_view format_as(CheckpointActionType item);

/**
 * Represents a checkpoint action (item mutation or checkpoint creation).
 */
struct CheckpointAction {
    /// The key for the queued_item to create.
    StoredDocKey key;
    /// The type of action to perform.
    CheckpointActionType type;
    /// Sequence number of the queued_item (set when queued in the CM). Remains
    /// 0 if the action was skipped, as an active could not have queued it.
    mutable uint64_t bySeqno{0};
};

std::string format_as(const CheckpointAction& action);

/**
 * Formats a list of checkpoint actions.
 *
 * @note Deliberately not named format_as. fmt/ranges.h already supplies a
 * formatter for std::vector, and a format_as overload for the same type makes
 * the choice of fmt::formatter specialization ambiguous as of fmt 12.
 */
std::string formatActions(const std::vector<CheckpointAction>& actions);

/**
 * Formats a list of checkpoint actions, in which std::nullopt is a seqno
 * without an action.
 */
std::string formatActions(
        const std::vector<std::optional<CheckpointAction>>& actions);

/**
 * The Prepares which have not been committed or aborted yet, used to only
 * generate the sequences of SyncWrites an active could have generated.
 */
class InFlightPrepares {
public:
    /**
     * Whether an active could queue the action. It rejects a Mutation or a
     * Prepare of a key with a Prepare in flight, and completes the Prepares
     * in the order they were queued.
     */
    bool canQueue(const StoredDocKey& key, CheckpointActionType type) const;

    /**
     * Records a Prepare of the key queued at the seqno.
     */
    void prepare(const StoredDocKey& key, uint64_t seqno);

    /**
     * Records the completion of the Prepare of the key.
     *
     * @return The seqno of the Prepare.
     */
    uint64_t complete(const StoredDocKey& key);

    /**
     * Whether the key has a Prepare in flight.
     */
    bool isInFlight(const StoredDocKey& key) const;

    /**
     * The seqno of the oldest Prepare in flight, if any.
     */
    std::optional<uint64_t> getOldestSeqno() const;

private:
    /// The seqno of each Prepare in flight, by key.
    std::unordered_map<StoredDocKey, uint64_t> prepares;
};

/**
 * Creates a test manifest which contains the collections used in the fuzz
 * tests.
 */
CollectionsManifest createManifest();

/**
 * Creates a queued_item from a CheckpointAction.
 */
queued_item createItem(DocKeyView key, CheckpointActionType type);

/**
 * Creates the DCP message a consumer receives for a CheckpointAction.
 *
 * @param action The action, other than CreateCheckpoint.
 * @param seqno The seqno of the message.
 * @param prepareSeqno The seqno of the Prepare a Commit or an Abort completes.
 */
std::unique_ptr<DcpResponse> createMessageForConsumer(
        const CheckpointAction& action, uint64_t seqno, uint64_t prepareSeqno);

/**
 * Creates a json filter for an ActiveStream.
 */
std::string createJsonFilter(std::optional<CollectionID> collectionFilter);

/**
 * Processes a list of checkpoint actions and returns a cursor to a saved
 * position before processing the actions.
 *
 * Actions an active could not queue are skipped (see InFlightPrepares).
 *
 * @param cm The checkpoint manager.
 * @param actions The actions to perform.
 * @param inFlightPrepares The Prepares in flight, updated by the actions.
 * @return A cursor to a saved position before processing the actions.
 */
std::shared_ptr<CheckpointCursor> processCheckpointActions(
        CheckpointManager& cm,
        const std::vector<CheckpointAction>& actions,
        InFlightPrepares& inFlightPrepares);

/**
 * Creates a DcpSnapshotMarkerFlag.
 */
DcpSnapshotMarkerFlag createSnapshotMarkerFlag(bool isMemory,
                                               bool isCheckpoint);

/**
 * Creates a snapshot marker.
 *
 * @param baseSeqno The base sequence number of the snapshot.
 * @param mutations The number of mutations (end = baseSeqno + mutations).
 * @param flags The flags of the snapshot.
 * @param maxVisible The index of the max visible item (msv = baseSeqno +
 * maxVisible).
 * @param highCompleted The index of the high completed item (hcs = baseSeqno +
 * highCompleted).
 * @param highPrepared The index of the high prepared item (hps = baseSeqno +
 * highPrepared).
 * @param purge The index of the purge item (purge = baseSeqno + purge).
 * @note Any index out of range will be set to nullopt.
 */
SnapshotMarker createSnapshotMarkerFromOffset(uint64_t baseSeqno,
                                              uint64_t mutations,
                                              DcpSnapshotMarkerFlag flags,
                                              uint64_t maxVisible,
                                              uint64_t highCompleted,
                                              uint64_t highPrepared,
                                              uint64_t purge);

/**
 * Re-bases a snapshot marker to a new base sequence number.
 */
SnapshotMarker rebaseSnapshotMarker(SnapshotMarker marker, uint64_t baseSeqno);

/**
 * Type alias for the result of fuzztest::ElementOf.
 * Allows us to un-inline the function definition of simple domains.
 */
template <typename T>
using ElementOfDomain =
        decltype(fuzztest::ElementOf(std::declval<std::initializer_list<T>>()));

/**
 * Domain for generating collection IDs used in the test manifest.
 */
ElementOfDomain<CollectionEntry::Entry> collectionEntry();

/** Whether to generate CreateCheckpoint actions as well. */
DEFINE_YES_NO_ENUM(CreateCheckpoints)

/** Whether to generate Prepare, Commit and Abort actions as well. */
DEFINE_YES_NO_ENUM(SyncWrites)

/**
 * Domain for generating CheckpointActionTypes.
 *
 * @param createCheckpoints Whether to generate CreateCheckpoint.
 * @param syncWrites Whether to generate Prepare, Commit and Abort.
 */
ElementOfDomain<CheckpointActionType> checkpointActionType(
        CreateCheckpoints createCheckpoints, SyncWrites syncWrites);

/**
 * Domain for generating datatypes.
 */
ElementOfDomain<protocol_binary_datatype_t> datatype();

/**
 * Domain for generating DocKeys.
 */
inline auto docKey(bool defaultCollectionOnly = false) {
    return fuzztest::Map(
            [](char key, CollectionID collectionId) {
                return StoredDocKey(std::string_view(&key, &key + 1),
                                    collectionId);
            },
            // Operate on up to 4 doc keys.
            fuzztest::InRange<char>('a', 'c'),
            defaultCollectionOnly ? fuzztest::Just(CollectionEntry::defaultC)
                                  : collectionEntry());
}

/** Domain for generating CheckpointActions */
inline auto checkpointAction() {
    return fuzztest::StructOf<CheckpointAction>(
            docKey(),
            checkpointActionType(CreateCheckpoints::Yes, SyncWrites::Yes),
            fuzztest::Just(uint64_t{0}));
}

inline auto snapshotMarkerFlag() {
    return fuzztest::Map(createSnapshotMarkerFlag,
                         fuzztest::Arbitrary<bool>(),
                         fuzztest::Arbitrary<bool>());
}

constexpr uint64_t snapshotMarkerMaxMutations = 3;

inline auto snapshotMarker() {
    auto range = fuzztest::InRange<uint64_t>(1, snapshotMarkerMaxMutations);
    // An offset of the snapshot size is out of range, so omitted.
    // TODO: Generate a Disk marker HPS outside the snapshot range, including 0,
    // as older actives sent (MB-66577, MB-66831).
    auto offset = fuzztest::InRange<uint64_t>(0, snapshotMarkerMaxMutations);
    return fuzztest::Map(createSnapshotMarkerFromOffset,
                         range,
                         range,
                         snapshotMarkerFlag(),
                         offset,
                         offset,
                         offset,
                         offset);
}

/** Domain for generating the CheckpointActions a consumer receives */
inline auto consumerAction(bool defaultCollectionOnly = false) {
    return fuzztest::StructOf<CheckpointAction>(
            docKey(defaultCollectionOnly),
            checkpointActionType(CreateCheckpoints::No, SyncWrites::Yes),
            fuzztest::Just(uint64_t{0}));
}

} // namespace cb::fuzzing

std::string format_as(const SnapshotMarker& marker);
