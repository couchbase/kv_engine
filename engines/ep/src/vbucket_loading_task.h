/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "ep_bucket.h"
#include "ep_task.h"
#include "kvstore/kvstore_iface.h"
#include "vbucket_loader.h"

#include <platform/json_log.h>

struct vbucket_state;

/**
 * Task which loads a VBucket from a snapshot.
 *
 * Can be instantiated as a mounting or creation task:
 *
 *   * A mounting task only performs the mounting phase to retrieve the DEK ids
 *     used by a snapshot.
 *   * A creation task task will perform all the phases, except in the case of
 *     a Fusion snapshot, where the mounting phase will be skipped as it was
 *     already done.
 *
 *  The task synchronises using the vbsetMutex to set the result such that it
 *  can be safely read by the frontend thread.
 */
class VBucketLoadingTask : public EpTask {
public:
    enum class LoadingState {
        MountVBucket,
        CreateVBucket,
        LoadCollectionStats,
        LoadItemCount,
        LoadPreparedSyncWrites,
        AddToVBucketMap,
        Done,
        Failed
    };

    /**
     * Creates a task which performs only the mounting phase to retrieve the DEK
     * ids used by a snapshot.
     *
     * @param cookie Cookie to notify the result
     * @param st Bucket that will contain the mounted VBucket
     * @param vbid ID of the VBucket to mount
     * @param source The type of snapshot to use
     * @param paths File paths comprising the snapshot, if any
     */
    static std::shared_ptr<VBucketLoadingTask> makeMountingTask(
            CookieIface& cookie,
            EPBucket& st,
            Vbid vbid,
            VBucketSnapshotSource source,
            std::vector<std::string> paths);

    /**
     * Creates a task which perform all the phases, except in the case of a
     * Fusion snapshot, where the mounting phase will be skipped as it was
     * already done.
     *
     * @param cookie Cookie to notify the result
     * @param st Bucket that will contain the created VBucket
     * @param vbid ID of the VBucket to load from snapshot
     * @param source The type of snapshot to use
     * @param paths File paths comprising the snapshot, if any
     * @param toState State to set the VBucket
     * @param replicationTopology Topology to set in the VBucket
     */
    static std::shared_ptr<VBucketLoadingTask> makeCreationTask(
            CookieIface& cookie,
            EPBucket& st,
            Vbid vbid,
            VBucketSnapshotSource source,
            std::vector<std::string> paths,
            vbucket_state_t toState,
            std::optional<vbucket_state_t> expectedNextState,
            nlohmann::json replicationTopology);

    // Not be used directly, use makeMountingTask or makeCreationTask.
    // Public so that it can be used by std::make_shared.
    VBucketLoadingTask(bool justMounting,
                       LoadingState state,
                       CookieIface& cookie,
                       EPBucket& st,
                       Vbid vbid,
                       VBucketSnapshotSource source,
                       std::vector<std::string> paths,
                       vbucket_state_t toState,
                       std::optional<vbucket_state_t> expectedNextState,
                       nlohmann::json replicationTopology);

    ~VBucketLoadingTask() override;

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

    bool run() override;

    /**
     * Returns true when this task instance only performs VBucket mounting.
     */
    bool isMountingTask() const {
        return justMounting;
    }

    /**
     * Returns the cookie that scheduled the task.
     */
    CookieIface& getCookie() const {
        return cookie;
    }

    /**
     * Returns the error code (success normally) resulting from running the
     * task.
     * @param vbset Locked vbsetMutex
     */
    cb::engine_errc getErrorCode(
            const std::unique_lock<std::mutex>& vbset) const {
        (void)vbset;
        return errorCode;
    }

    /**
     * Returns the DEK ids retrieved while mounting the VBucket.
     * @param vbset Locked vbsetMutex
     */
    std::vector<std::string> getDekIds(
            const std::unique_lock<std::mutex>& vbset) const {
        (void)vbset;
        return dekIds;
    }

protected:
    /**
     * Stores the @p result and notifies the cookie.
     */
    void notifyComplete(cb::engine_errc result);

    /**
     * Checks that @p newState is one of the @p expected states.
     * @throws std::logic_error
     */
    void checkTransition(LoadingState newState,
                         std::initializer_list<LoadingState> expected) const;

    /**
     * Logs the message and context, sets the cookie's error context,
     * and transitions to the Failed state.
     */
    void fail(const char* func,
              std::string_view message,
              cb::logger::Json ctx = {});

    /**
     * Transitions to @p newState after checking that the transition is valid.
     * @throws std::logic_error
     */
    void transition(LoadingState newState);

    /**
     * Deletes the KVStore vBucket, is any. Used for error cleanup.
     */
    void deleteVBucket();

    /**
     * Mounts the KVStore vBucket.
     */
    void mountVBucket();

    /**
     * Creates the vBucket by loading its snapshot,
     * but doesn't add it to the map.
     */
    void createVBucket();

    void loadCollectionStats();

    void loadItemCount();

    void loadPreparedSyncWrites();

    /**
     * Adds the vBucket to the map, after queueSetVBState() and flushing.
     */
    void addToVBucketMap();

    /// Whether this task will only mount the vBucket and not create one
    const bool justMounting;
    const uint16_t shardId;
    VBucketLoader loader;
    LoadingState state;
    /// Error code that EPBucket::loadVBucket_UNLOCKED() returns.
    /// Guarded by vbsetMutex.
    cb::engine_errc errorCode;
    /// Data Encryption Key ids used by the vBucket files
    /// Guarded by vbsetMutex.
    std::vector<std::string> dekIds;
    CookieIface& cookie;
    EPBucket& store;
    const Vbid vbid;
    /// Are we loading from Fusion or FBR (Local)?
    const VBucketSnapshotSource source;
    /// Paths to vBucket files if Local
    const std::vector<std::string> paths;
    /// VBucket state to set
    const vbucket_state_t toState;
    /// Expected next state for this vbucket
    const std::optional<vbucket_state_t> expectedNextState;
    /// Topology to set in the vbstate, if any
    const nlohmann::json replicationTopology;
};

std::string_view format_as(VBucketLoadingTask::LoadingState);

std::string to_string(VBucketLoadingTask::LoadingState);

template <typename BasicJsonType>
inline void to_json(BasicJsonType& json,
                    VBucketLoadingTask::LoadingState state) {
    json = format_as(state);
}
