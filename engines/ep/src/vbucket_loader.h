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

/**
 * Functionality shared by Warmup and loading a VBucket from a snapshot
 */
class VBucketLoader {
public:
    VBucketLoader(EPBucket& st,
                  const Configuration& config,
                  VBucketPtr vb,
                  uint16_t shardId);

    ~VBucketLoader();

    const auto& getVBucketPtr() const {
        return vb;
    }

    enum class CreateVBucketStatus {
        Success,
        SuccessFailover,
        FailedReadingCollectionsManifest,
        AlreadyExists
    };

    CreateVBucketStatus createVBucket(Vbid vbid,
                                      const vbucket_state& vbs,
                                      size_t maxFailoverEntries,
                                      bool cleanShutdown,
                                      CreateVbucketMethod creationMethod,
                                      bool readCollectionsManifest = true);

    enum class LoadCollectionStatsStatus { Success, Failed, NoFileHandle };

    /**
     * Read the collections stats for the VBucket from disk.
     */
    LoadCollectionStatsStatus loadCollectionStats(const KVStoreIface& kvstore);

    KVBucketIface::LoadPreparedSyncWritesResult loadPreparedSyncWrites();

    EPBucket::FlushResult addToVBucketMap();

protected:
    EPBucket& store;
    const Configuration& config;
    VBucketPtr vb;
    const uint16_t shardId;
};
