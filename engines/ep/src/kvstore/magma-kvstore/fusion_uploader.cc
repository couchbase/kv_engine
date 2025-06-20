/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "fusion_uploader.h"

#include "bucket_logger.h"
#include "ep_task.h"
#include "magma-kvstore.h"
#include <executor/executorpool.h>

class StartFusionUploaderTask : public EpTask {
public:
    StartFusionUploaderTask(EventuallyPersistentEngine& engine,
                            MagmaKVStore& kvstore,
                            Vbid vbid,
                            uint64_t term);

    std::chrono::microseconds maxExpectedDuration() const override {
        // The task performs IO, expected duration fairly large
        return std::chrono::seconds(1);
    }

    std::string getDescription() const override {
        return "StartFusionUploaderTask " + vbid.to_string();
    }

    bool run() override;

private:
    MagmaKVStore& kvstore;
    const Vbid vbid;
    const uint64_t term;
};

class StopFusionUploaderTask : public EpTask {
public:
    StopFusionUploaderTask(EventuallyPersistentEngine& engine,
                           MagmaKVStore& kvstore,
                           Vbid vbid);

    std::chrono::microseconds maxExpectedDuration() const override {
        // The task performs IO, expected duration fairly large
        return std::chrono::seconds(1);
    }

    std::string getDescription() const override {
        return "StopFusionUploaderTask " + vbid.to_string();
    }

    bool run() override;

private:
    MagmaKVStore& kvstore;
    const Vbid vbid;
};

cb::engine_errc FusionUploaderManager::startUploader(
        EventuallyPersistentEngine& engine, Vbid vbid, uint64_t term) {
    return vbMap.withWLock([this, &engine, vbid, term](auto& map) {
        if (map.contains(vbid)) {
            EP_LOG_WARN_CTX(
                    "FusionUploaderManager::startUploader: Uploader state "
                    "change in progress ",
                    {"vb", vbid});
            return cb::engine_errc::key_already_exists;
        }

        if (kvstore.isFusionUploader(vbid) &&
            kvstore.getFusionUploaderTerm(vbid) == term) {
            return cb::engine_errc::success;
        }

        if (!map.emplace(vbid, UploaderToggle::Start).second) {
            const auto msg = fmt::format(
                    "FusionUploaderManager::startUploader: map::emplace failed "
                    "{}",
                    vbid);
            throw std::logic_error(msg);
        }

        ExecutorPool::get()->schedule(std::make_shared<StartFusionUploaderTask>(
                engine, kvstore, vbid, term));
        return cb::engine_errc::success;
    });
}

cb::engine_errc FusionUploaderManager::stopUploader(
        EventuallyPersistentEngine& engine, Vbid vbid) {
    return vbMap.withWLock([this, &engine, vbid](auto& map) {
        if (map.contains(vbid)) {
            EP_LOG_WARN_CTX(
                    "FusionUploaderManager::stopUploader: Uploader state "
                    "change in progress ",
                    {"vb", vbid});
            return cb::engine_errc::key_already_exists;
        }

        if (!kvstore.isFusionUploader(vbid)) {
            return cb::engine_errc::success;
        }

        if (!map.emplace(vbid, UploaderToggle::Stop).second) {
            const auto msg = fmt::format(
                    "FusionUploaderManager::stopUploader: map::emplace failed "
                    "{}",
                    vbid);
            throw std::logic_error(msg);
        }

        ExecutorPool::get()->schedule(std::make_shared<StopFusionUploaderTask>(
                engine, kvstore, vbid));
        return cb::engine_errc::success;
    });
}

void FusionUploaderManager::onToggleComplete(Vbid vbid) {
    const auto res = vbMap.wlock()->erase(vbid);
    Ensures(res == 1);
}

FusionUploaderState FusionUploaderManager::getUploaderState(Vbid vbid) const {
    const auto locked = vbMap.rlock();
    if (locked->contains(vbid)) {
        switch (locked->at(vbid)) {
        case UploaderToggle::Start:
            return FusionUploaderState::Enabling;
        case UploaderToggle::Stop:
            return FusionUploaderState::Disabling;
        }
    }
    return kvstore.isFusionUploader(vbid) ? FusionUploaderState::Enabled
                                          : FusionUploaderState::Disabled;
}

void to_json(nlohmann::json& json, const FusionUploaderState state) {
    switch (state) {
    case FusionUploaderState::Enabling:
        json = "enabling";
        return;
    case FusionUploaderState::Enabled:
        json = "enabled";
        return;
    case FusionUploaderState::Disabling:
        json = "disabling";
        return;
    case FusionUploaderState::Disabled:
        json = "disabled";
        return;
    }
    folly::assume_unreachable();
}

StartFusionUploaderTask::StartFusionUploaderTask(
        EventuallyPersistentEngine& engine,
        MagmaKVStore& kvstore,
        Vbid vbid,
        uint64_t term)
    : EpTask(engine, TaskId::StartFusionUploaderTask),
      kvstore(kvstore),
      vbid(vbid),
      term(term){};

bool StartFusionUploaderTask::run() {
    kvstore.doStartFusionUploader(vbid, term);
    // Never reschedule the task
    return false;
}

StopFusionUploaderTask::StopFusionUploaderTask(
        EventuallyPersistentEngine& engine, MagmaKVStore& kvstore, Vbid vbid)
    : EpTask(engine, TaskId::StopFusionUploaderTask),
      kvstore(kvstore),
      vbid(vbid){};

bool StopFusionUploaderTask::run() {
    kvstore.doStopFusionUploader(vbid);
    // Never reschedule the task
    return false;
}