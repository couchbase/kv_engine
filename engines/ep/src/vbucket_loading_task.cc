/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vbucket_loading_task.h"

#include "bucket_logger.h"
#include "ep_engine.h"
#include "ep_vb.h"

std::shared_ptr<VBucketLoadingTask> VBucketLoadingTask::makeMountingTask(
        CookieIface& cookie,
        EPBucket& st,
        Vbid vbid,
        VBucketSnapshotSource source,
        std::vector<std::string> paths) {
    return std::make_shared<VBucketLoadingTask>(true,
                                                LoadingState::MountVBucket,
                                                cookie,
                                                st,
                                                vbid,
                                                source,
                                                std::move(paths),
                                                vbucket_state_dead,
                                                nlohmann::json{});
}

std::shared_ptr<VBucketLoadingTask> VBucketLoadingTask::makeCreationTask(
        CookieIface& cookie,
        EPBucket& st,
        Vbid vbid,
        VBucketSnapshotSource source,
        std::vector<std::string> paths,
        vbucket_state_t toState,
        nlohmann::json replicationTopology) {
    auto state = LoadingState::MountVBucket;
    if (source == VBucketSnapshotSource::FusionGuestVolumes ||
        source == VBucketSnapshotSource::FusionLogStore) {
        state = LoadingState::CreateVBucket;
    }
    return std::make_shared<VBucketLoadingTask>(false,
                                                state,
                                                cookie,
                                                st,
                                                vbid,
                                                source,
                                                std::move(paths),
                                                toState,
                                                std::move(replicationTopology));
}

VBucketLoadingTask::VBucketLoadingTask(bool justMounting,
                                       LoadingState state,
                                       CookieIface& cookie,
                                       EPBucket& st,
                                       Vbid vbid,
                                       VBucketSnapshotSource source,
                                       std::vector<std::string> paths,
                                       vbucket_state_t toState,
                                       nlohmann::json replicationTopology)
    : EpTask(st.getEPEngine(), TaskId::VBucketLoadingTask, 0, false),
      justMounting(justMounting),
      shardId(st.getShardId(vbid)),
      loader(st, st.getConfiguration(), {}, shardId),
      state(state),
      errorCode(cb::engine_errc::would_block),
      cookie(cookie),
      store(st),
      vbid(vbid),
      source(source),
      paths(std::move(paths)),
      toState(toState),
      replicationTopology(std::move(replicationTopology)) {
}

VBucketLoadingTask::~VBucketLoadingTask() = default;

std::string VBucketLoadingTask::getDescription() const {
    if (justMounting) {
        return "Mounting VBucket " + to_string(vbid);
    }
    return "Loading VBucket " + to_string(vbid);
}

std::chrono::microseconds VBucketLoadingTask::maxExpectedDuration() const {
    return std::chrono::milliseconds(200);
}

bool VBucketLoadingTask::run() {
    try {
        switch (state) {
        case LoadingState::MountVBucket:
            mountVBucket();
            break;
        case LoadingState::CreateVBucket:
            createVBucket();
            break;
        case LoadingState::LoadCollectionStats:
            loadCollectionStats();
            break;
        case LoadingState::LoadItemCount:
            loadItemCount();
            break;
        case LoadingState::LoadPreparedSyncWrites:
            loadPreparedSyncWrites();
            break;
        case LoadingState::AddToVBucketMap:
            addToVBucketMap();
            break;
        case LoadingState::Done:
            break;
        case LoadingState::Failed:
            break;
        }
    } catch (const std::exception& ex) {
        fail(__func__, "Exception", {{"error", ex.what()}});
        return false;
    }
    return state != LoadingState::Done && state != LoadingState::Failed;
}

void VBucketLoadingTask::notifyComplete(cb::engine_errc result) {
    {
        std::lock_guard vbset(store.getVbSetMutexLock());
        errorCode = result;
    }
    cookie.notifyIoComplete(cb::engine_errc::success);
}

void VBucketLoadingTask::checkTransition(
        LoadingState newState,
        std::initializer_list<LoadingState> expected) const {
    for (auto item : expected) {
        if (newState == item) {
            return;
        }
    }
    throw std::logic_error(
            fmt::format("VBucketLoadingTask: invalid transition from {} to {}",
                        state,
                        newState));
}

void VBucketLoadingTask::transition(LoadingState newState) {
    EP_LOG_INFO_CTX("VBucketLoadingTask::transition",
                    {"conn_id", cookie.getConnectionId()},
                    {"vb", vbid},
                    {"from", state},
                    {"to", newState});
    if (newState == LoadingState::Failed) {
        try {
            deleteVBucket();
        } catch (const std::exception& ex) {
            EP_LOG_WARN_CTX(
                    "VBucketLoadingTask::transition: "
                    "Exception while deleting vbucket",
                    {"conn_id", cookie.getConnectionId()},
                    {"vb", vbid},
                    {"error", ex.what()});
        }
        state = newState;
        notifyComplete(cb::engine_errc::failed);
        return;
    }
    switch (state) {
    case LoadingState::MountVBucket:
        checkTransition(newState,
                        {LoadingState::CreateVBucket, LoadingState::Done});
        if (newState == LoadingState::Done) {
            Expects(source == VBucketSnapshotSource::FusionGuestVolumes ||
                    source == VBucketSnapshotSource::FusionLogStore);
        }
        break;
    case LoadingState::CreateVBucket:
        checkTransition(newState,
                        {LoadingState::LoadCollectionStats,
                         LoadingState::AddToVBucketMap});
        break;
    case LoadingState::LoadCollectionStats:
        checkTransition(newState, {LoadingState::LoadItemCount});
        break;
    case LoadingState::LoadItemCount:
        checkTransition(newState, {LoadingState::LoadPreparedSyncWrites});
        break;
    case LoadingState::LoadPreparedSyncWrites:
        checkTransition(newState, {LoadingState::AddToVBucketMap});
        break;
    case LoadingState::AddToVBucketMap:
        checkTransition(newState, {LoadingState::Done});
        break;
    case LoadingState::Done:
    case LoadingState::Failed:
        checkTransition(newState, {});
        break;
    }
    state = newState;
    if (newState == LoadingState::Done) {
        notifyComplete(cb::engine_errc::success);
    }
}

void VBucketLoadingTask::fail(const char* func,
                              std::string_view message,
                              cb::logger::Json ctx) {
    if (ctx.is_object()) {
        auto& object = ctx.get_ref<cb::logger::Json::object_t&>();
        object.insert(object.begin(), {"vb", vbid});
        object.insert(object.begin(), {"conn_id", cookie.getConnectionId()});
    } else if (ctx.is_null()) {
        ctx = cb::logger::Json{{"conn_id", cookie.getConnectionId()},
                               {"vb", vbid}};
    } else {
        ctx = cb::logger::Json{{"conn_id", cookie.getConnectionId()},
                               {"vb", vbid},
                               {"ctx", std::move(ctx)}};
    }
    const auto log = fmt::format("VBucketLoadingTask::{}: {}", func, message);
    cookie.setErrorContext(fmt::format("{} {}", log, ctx));
    EP_LOG_ERR_CTX(log, std::move(ctx));
    transition(LoadingState::Failed);
}

void VBucketLoadingTask::deleteVBucket() {
    auto* kvstore = store.getRWUnderlyingByShard(shardId);
    auto rev = kvstore->prepareToDelete(vbid);
    kvstore->delVBucket(vbid, std::move(rev));
}

void VBucketLoadingTask::mountVBucket() {
    auto [errc, deks] = store.getRWUnderlyingByShard(shardId)->mountVBucket(
            vbid, source, paths);
    if (errc != cb::engine_errc::success) {
        fail(__func__,
             "KVStore::mountVBucket failed",
             {{"error", to_string(errc)}});
        return;
    }
    transition(justMounting ? LoadingState::Done : LoadingState::CreateVBucket);
}

void VBucketLoadingTask::createVBucket() {
    auto loadSnapshotResult =
            store.getRWUnderlyingByShard(shardId)->loadVBucketSnapshot(
                    vbid, toState, replicationTopology);
    auto nextState = LoadingState::LoadCollectionStats;
    using ReadVBStateStatus = KVStoreIface::ReadVBStateStatus;
    if (loadSnapshotResult.status != ReadVBStateStatus::Success) {
        if (loadSnapshotResult.status != ReadVBStateStatus::NotFound) {
            fail(__func__,
                 "KVStore::loadVBucketSnapshot failed",
                 {{"status", to_string(loadSnapshotResult.status)}});
            return;
        }
        // The vbstate LocalDoc was not found, so the VBucket is empty.
        // This can happen if the snapshot was taken before the first flush.
        // We will skip loading the collections manifest/stats, and go straight
        // to adding the empty VBucket to the vbmap.
        loadSnapshotResult.state = {};
        loadSnapshotResult.state.transition.state = toState;
        nextState = LoadingState::AddToVBucketMap;
    }
    const auto status = loader.createVBucket(
            vbid,
            loadSnapshotResult.state,
            store.getEPEngine().getMaxFailoverEntries(),
            true,
            // readCollectionsManifest
            loadSnapshotResult.status == ReadVBStateStatus::Success);
    using Status = VBucketLoader::CreateVBucketStatus;
    switch (status) {
    case Status::Success:
    case Status::SuccessFailover:
        transition(nextState);
        break;
    case Status::FailedReadingCollectionsManifest:
        fail(__func__, "Failed to read collections manifest from disk");
        break;
    case Status::AlreadyExists:
        fail(__func__, "Already exists");
        break;
    }
}

void VBucketLoadingTask::loadCollectionStats() {
    const auto* kvstore = store.getROUnderlyingByShard(shardId);
    Expects(kvstore);
    const auto status = loader.loadCollectionStats(*kvstore);
    using Status = VBucketLoader::LoadCollectionStatsStatus;
    switch (status) {
    case Status::Success:
        transition(LoadingState::LoadItemCount);
        break;
    case Status::Failed:
        fail(__func__, "Failed");
        break;
    case Status::NoFileHandle:
        fail(__func__, "Could not make KVFileHandle");
        break;
    }
}

void VBucketLoadingTask::loadItemCount() {
    auto& epVb = static_cast<EPVBucket&>(*loader.getVBucketPtr());
    epVb.setNumTotalItems(*store.getRWUnderlyingByShard(shardId));
    transition(LoadingState::LoadPreparedSyncWrites);
}

void VBucketLoadingTask::loadPreparedSyncWrites() {
    auto result = loader.loadPreparedSyncWrites();
    if (result.success) {
        transition(LoadingState::AddToVBucketMap);
    } else {
        fail(__func__, "Failed");
    }
}

void VBucketLoadingTask::addToVBucketMap() {
    auto result = loader.addToVBucketMap();
    if (result.moreAvailable == EPBucket::MoreAvailable::Yes) {
        // if flusher returned MoreAvailable::Yes, this indicates the single
        // flush of the vbucket state failed.
        EP_LOG_CRITICAL_CTX(
                "VBucketLoadingTask: Flush state failed",
                {"conn_id", cookie.getConnectionId()},
                {"vb", vbid},
                {"highSeqno", loader.getVBucketPtr()->getHighSeqno()});
    }
    transition(LoadingState::Done);
}

std::string_view format_as(VBucketLoadingTask::LoadingState state) {
#define X(state)                                  \
    case VBucketLoadingTask::LoadingState::state: \
        return #state
    switch (state) {
        X(MountVBucket);
        X(CreateVBucket);
        X(LoadCollectionStats);
        X(LoadItemCount);
        X(LoadPreparedSyncWrites);
        X(AddToVBucketMap);
        X(Done);
        X(Failed);
    }
#undef X
    folly::assume_unreachable();
}

std::string to_string(VBucketLoadingTask::LoadingState state) {
    return std::string(format_as(state));
}
