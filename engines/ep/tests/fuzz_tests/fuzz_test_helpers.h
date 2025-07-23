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

#include "fuzz_test_stringify.h"
#include "tests/module_tests/test_helpers.h"

#include <fuzztest/domain.h>
#include <memcached/storeddockey.h>
#include <utilities/test_manifest.h>

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
    // TODO(MB-66315): Add SyncWrite support
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
    /// Sequence number of the queued_item (set when queued in the CM).
    mutable uint64_t bySeqno{0};
};

std::string format_as(const CheckpointAction& action);

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
 * Creates a json filter for an ActiveStream.
 */
std::string createJsonFilter(std::optional<CollectionID> collectionFilter);

/**
 * Processes a list of checkpoint actions and returns a cursor to a saved
 * position before processing the actions.
 *
 * @param cm The checkpoint manager.
 * @param actions The actions to perform.
 * @return A cursor to a saved position before processing the actions.
 */
std::shared_ptr<CheckpointCursor> processCheckpointActions(
        CheckpointManager& cm, const std::vector<CheckpointAction>& actions);

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

/**
 * Domain for generating CheckpointActionTypes.
 */
ElementOfDomain<CheckpointActionType> checkpointActionType();

/**
 * Domain for generating datatypes.
 */
ElementOfDomain<protocol_binary_datatype_t> datatype();

/**
 * Domain for generating DocKeys.
 */
inline auto docKey() {
    return fuzztest::Map(
            [](char key, CollectionID collectionId) {
                return StoredDocKey(std::string_view(&key, &key + 1),
                                    collectionId);
            },
            // Operate on up to 4 doc keys.
            fuzztest::InRange<char>('a', 'c'),
            collectionEntry());
}

/** Domain for generating CheckpointActions */
inline auto checkpointAction() {
    return fuzztest::StructOf<CheckpointAction>(
            docKey(), checkpointActionType(), fuzztest::Just(uint64_t(0)));
}

} // namespace cb::fuzzing
