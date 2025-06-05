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

#include <folly/Synchronized.h>
#include <memcached/engine_error.h>
#include <memcached/vbucket.h>
#include <string>
#include <unordered_map>

class EventuallyPersistentEngine;
class MagmaKVStore;

enum class FusionUploaderState : uint8_t {
    Enabling,
    Enabled,
    Disabling,
    Disabled
};

void to_json(nlohmann::json& json, const FusionUploaderState state);

/**
 * The class handles Start/StopFusionUploader requests.
 *   - Start/StopFusionUploader are blocking calls into magma
 *   - API in this class schedule a bg-task to run and perform the proper call
 *     into magma
 *   - Calls return immediately after the bg-task is scheduled
 *   - Only one request is accepted at any time for a specific vbid
 *   - Subsequent requests are accepted only once any in-progress bg-task has
 *     completed its execution
 */
class FusionUploaderManager {
public:
    FusionUploaderManager(MagmaKVStore& kvstore) : kvstore(kvstore) {
    }

    /**
     * Schedules a bg-task for starting the fusion uploader for vbid.
     *
     * @return Success if there's no in-progress task for vbid, thus a task is
     *         scheduled to run. key_already_exists otherwise.
     */
    cb::engine_errc startUploader(EventuallyPersistentEngine& engine,
                                  Vbid vbid,
                                  uint64_t term);

    /**
     * Schedules a bg-task for stopping the fusion uploader for vbid.
     *
     * @return Success if there's no in-progress task for vbid, thus a task is
     *         scheduled to run. key_already_exists otherwise.
     */
    cb::engine_errc stopUploader(EventuallyPersistentEngine& engine, Vbid vbid);

    /**
     * Callback to clear this FusionUploaderManager internal state for vbid.
     * Used when any Start/StopFusionUploader task completes its execution.
     */
    void onToggleComplete(Vbid vbid);

    /**
     * Returns the uploader state for vbid.
     */
    FusionUploaderState getUploaderState(Vbid vbid) const;

private:
    MagmaKVStore& kvstore;

    enum class UploaderToggle : uint8_t { Start, Stop };

    /**
     * Tracks which vbuckets have any Start/StopFusionUploader task already
     * scheduled.
     * The UploaderToggle information is used for inferring the precise
     * FusionUploaderState at ::getUploaderState(Vbid).
     */
    folly::Synchronized<std::unordered_map<Vbid, UploaderToggle>> vbMap;
};