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

ElementOfDomain<CollectionEntry::Entry> collectionEntry() {
    return fuzztest::ElementOf({
            CollectionEntry::defaultC,
            CollectionEntry::vegetable,
    });
}

ElementOfDomain<CheckpointActionType> checkpointActionType() {
    return fuzztest::ElementOf({
            CheckpointActionType::Mutation,
            CheckpointActionType::CreateCheckpoint,
    });
}

ElementOfDomain<protocol_binary_datatype_t> datatype() {
    return fuzztest::ElementOf(
            cb::testing::sv::datatypeValues(cb::mcbp::datatype::highest));
}

} // namespace cb::fuzzing
