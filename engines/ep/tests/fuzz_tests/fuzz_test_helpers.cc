/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "fuzz_test_helpers.h"

#include "checkpoint_manager.h"
#include <algorithm>
#include <utility>

namespace cb::fuzzing {

std::string_view format_as(CheckpointActionType item) {
    switch (item) {
    case CheckpointActionType::Mutation:
        return "Mutation";
    case CheckpointActionType::CreateCheckpoint:
        return "Checkpoint";
    }
    return "Unknown";
}

std::string format_as(const CheckpointAction& action) {
    nlohmann::ordered_json j;
    j["seqno"] = action.bySeqno;
    j["type"] = fmt::to_string(action.type);
    if (action.type != CheckpointActionType::CreateCheckpoint) {
        j["key"] = action.key.to_string();
    }
    return j.dump();
}

std::string format_as(const std::vector<CheckpointAction>& actions) {
    std::string result;
    result += fmt::format("CheckpointAction{{size={}}}\n", actions.size());
    for (const auto& action : actions) {
        result += fmt::format("  {}\n", action);
    }
    return result;
}

/**
 * Creates a test manifest which contains the collections used in the fuzz
 * tests.
 */
CollectionsManifest createManifest() {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable);
    return cm;
}

queued_item createItem(DocKeyView key, CheckpointActionType type) {
    using namespace cb::testing;
    auto storedValue = sv::create(key,
                                  sv::State::Document,
                                  sv::HasValue::No,
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  sv::Resident::Yes,
                                  sv::Persisted::Yes,
                                  sv::Deleted::No,
                                  sv::Expired::No,
                                  sv::Locked::No);

    auto item = storedValue->toItem(Vbid(0),
                                    StoredValue::HideLockedCas::No,
                                    StoredValue::IncludeValue::Yes,
                                    {});
    item->setQueuedTime(cb::time::steady_clock::now());

    switch (type) {
    case CheckpointActionType::Mutation:
        break;
    case CheckpointActionType::CreateCheckpoint:
        throw std::runtime_error("not supported");
    }
    return queued_item(item.release());
}

MutationResponse createMutationResponse(StoredDocKey key,
                                        CheckpointActionType type) {
    auto item = createItem(key, type);
    return {item,
            0 /*opaque*/,
            IncludeValue::No,
            IncludeXattrs::No,
            IncludeDeleteTime::No,
            IncludeDeletedUserXattrs::No,
            DocKeyEncodesCollectionId::Yes,
            cb::mcbp::DcpStreamId(0) /*sid*/};
}

std::string createJsonFilter(std::optional<CollectionID> collectionFilter) {
    if (collectionFilter) {
        return fmt::format(R"({{"collections":["{}"]}})",
                           collectionFilter->to_string(false));
    }
    return {};
}

std::shared_ptr<CheckpointCursor> processCheckpointActions(
        CheckpointManager& cm, const std::vector<CheckpointAction>& actions) {
    // Register cursor with randomly generated name.
    auto cursor = cm.registerCursorBySeqno(
                            fmt::format("backup_cursor_{}", fmt::ptr(&actions)),
                            0,
                            CheckpointCursor::Droppable::No)
                          .takeCursor()
                          .lock();

    for (const auto& action : actions) {
        if (action.type == CheckpointActionType::CreateCheckpoint) {
            cm.createNewCheckpoint();
            action.bySeqno = cm.getHighSeqno();
            continue;
        }

        auto item = createItem(action.key, action.type);
        cm.queueDirty(item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr);
        action.bySeqno = item->getBySeqno();
    }

    return cursor;
}

DcpSnapshotMarkerFlag createSnapshotMarkerFlag(bool isMemory,
                                               bool isCheckpoint) {
    auto flags = isMemory ? DcpSnapshotMarkerFlag::Memory
                          : DcpSnapshotMarkerFlag::Disk;
    if (isCheckpoint) {
        flags |= DcpSnapshotMarkerFlag::Checkpoint;
    }
    return flags;
}

SnapshotMarker createSnapshotMarkerFromOffset(uint64_t baseSeqno,
                                              uint64_t mutations,
                                              DcpSnapshotMarkerFlag flags,
                                              uint64_t maxVisible,
                                              uint64_t highCompleted,
                                              uint64_t highPrepared,
                                              uint64_t purge) {
    // Ensure the seqno is below the endSeqno.
    // This implicitly models missing values.
    auto makeOptional = [mutations](uint64_t index) {
        return index < mutations ? std::make_optional(index) : std::nullopt;
    };
    auto mvsToSend = makeOptional(maxVisible);
    auto hcsToSend = makeOptional(highCompleted);
    auto hpsToSend = makeOptional(highPrepared);
    auto psToSend = makeOptional(purge);

    // A disk checkpoint requires HCS.
    if (isFlagSet(flags, DcpSnapshotMarkerFlag::Disk) && !hcsToSend) {
        hcsToSend = 0;
    }

    // Ensure we send MVS when HCS is sent.
    if (hcsToSend && !mvsToSend) {
        mvsToSend = mutations;
    }

    return rebaseSnapshotMarker({0,
                                 Vbid(0),
                                 0,
                                 mutations - 1,
                                 flags,
                                 hcsToSend,
                                 hpsToSend,
                                 mvsToSend,
                                 psToSend,
                                 cb::mcbp::DcpStreamId(0)},
                                baseSeqno);
}

SnapshotMarker rebaseSnapshotMarker(SnapshotMarker marker, uint64_t s) {
    auto rebase = [s](uint64_t x) { return s + x; };
    return {marker.getOpaque(),
            marker.getVBucket(),
            rebase(marker.getStartSeqno()),
            rebase(marker.getEndSeqno()),
            marker.getFlags(),
            marker.getMaxVisibleSeqno().transform(rebase),
            marker.getHighCompletedSeqno().transform(rebase),
            marker.getHighPreparedSeqno().transform(rebase),
            marker.getPurgeSeqno().transform(rebase),
            marker.getStreamId()};
}

ElementOfDomain<CollectionEntry::Entry> collectionEntry() {
    return fuzztest::ElementOf({
            CollectionEntry::defaultC,
            CollectionEntry::vegetable,
    });
}

ElementOfDomain<CheckpointActionType> checkpointActionType(
        bool createCheckpoint) {
    std::vector<CheckpointActionType> types = {
            CheckpointActionType::Mutation,
    };

    if (createCheckpoint) {
        types.push_back(CheckpointActionType::CreateCheckpoint);
    }

    return fuzztest::ElementOf(std::move(types));
}

ElementOfDomain<protocol_binary_datatype_t> datatype() {
    return fuzztest::ElementOf(
            cb::testing::sv::datatypeValues(cb::mcbp::datatype::highest));
}

} // namespace cb::fuzzing

struct FormatPair {
    std::string_view key;
    std::string value;

    template <typename T>
    FormatPair(std::string_view name, const T& arg)
        : key(name), value(fmt::format("{}", arg)) {
    }

    template <typename T>
    FormatPair(std::string_view name, std::string arg)
        : key(name), value(std::move(arg)) {
    }
};

auto format_as(const FormatPair& pair) {
    return fmt::format("{}={}", pair.key, pair.value);
}

std::string formatStruct(std::string_view name,
                         std::initializer_list<FormatPair> pairs) {
    return fmt::format(
            "{}{{{}}}", name, fmt::join(pairs.begin(), pairs.end(), ", "));
}

std::string format_as(const SnapshotMarker& marker) {
    // Print values needed to re-create the marker in a fuzz test.
    return formatStruct(
            "SnapshotMarker",
            {
                    {"start_seqno", marker.getStartSeqno()},
                    {"end_seqno", marker.getEndSeqno()},
                    {"flags", marker.getFlags()},
                    {"high_completed_seqno", marker.getHighCompletedSeqno()},
                    {"high_prepared_seqno", marker.getHighPreparedSeqno()},
                    {"max_visible_seqno", marker.getMaxVisibleSeqno()},
                    {"purge_seqno", marker.getPurgeSeqno()},
            });
}

std::string format_as(const MutationResponse& mutation) {
    // Seqno is not printed as it is set by the test.
    return formatStruct("MutationResponse",
                        {
                                {"key", mutation.getItem()->getKey()},
                                {"operation",
                                 to_string(mutation.getItem()->getOperation())},
                        });
}
