/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "download_snapshot_controller.h"
#include <nlohmann/json.hpp>
#include <statistics/collector.h>
#include <optional>

namespace cb ::snapshot {

struct DownloadSnapshotController::TaskListener
    : public DownloadSnapshotTaskListener {
    TaskListener(Vbid vb) : vbid(vb) {
    }
    ~TaskListener() override = default;

    void stateChanged(DownloadSnapshotTaskState state_) override {
        state = state_;
    }
    void setManifest(Manifest manifest_) override {
        manifest = std::move(manifest_);
    }
    void failed(std::string reason_) override {
        error = std::move(reason_);
        state = DownloadSnapshotTaskState::Failed;
    }

    void addStats(const StatCollector& collector) const {
        nlohmann::json json = {{"state", format_as(state.load())}};
        auto value = *error.lock();
        if (!value.empty()) {
            json["error"] = value;
        }

        auto manifest_ = *manifest.lock();
        if (manifest_.has_value()) {
            json["manifest"] = *manifest_;
        }

        collector.addStat(
                std::string_view{fmt::format("vb_{}:download", vbid.get())},
                json.dump());
    }

    const Vbid vbid;
    std::atomic<DownloadSnapshotTaskState> state{
            DownloadSnapshotTaskState::PrepareSnapshot};
    folly::Synchronized<std::string, std::mutex> error;
    folly::Synchronized<std::optional<Manifest>, std::mutex> manifest;
};

std::shared_ptr<DownloadSnapshotTaskListener>
DownloadSnapshotController::createListener(Vbid vbid) {
    auto ret = listeners.withLock([&vbid](auto& map) {
        auto iter = map.find(vbid);
        if (iter != map.end()) {
            auto state = iter->second->state.load();
            if (state != DownloadSnapshotTaskState::Finished &&
                state != DownloadSnapshotTaskState::Failed) {
                return std::shared_ptr<TaskListener>{};
            }
        }
        auto ptr = std::make_shared<TaskListener>(vbid);
        map[vbid] = ptr;
        return ptr;
    });

    return ret;
}

void DownloadSnapshotController::addStats(
        const StatCollector& collector) const {
    listeners.withLock([&collector](auto& map) {
        for (const auto& [vbid, listener] : map) {
            listener->addStats(collector);
        }
    });
}
} // namespace cb::snapshot
