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

#include "download_snapshot_task_listener.h"
#include <folly/Synchronized.h>
#include <memcached/vbucket.h>
#include <unordered_map>

class StatCollector;
class VBucketFilter;

namespace cb ::snapshot {

/**
 * The DownloadSnapshotController is responsible for managing the download of
 * snapshots from other servers.
 *
 * @todo add pruning of old listeners. Currently we'll keep 1 listener around
 *       for each vbucket which has had a download request. They should
 *       probably be pruned after a while (perhaps we could do that as
 *       part of promoting the snapshot? or after lets say 5 minutes of
 *       idle time)
 */
class DownloadSnapshotController {
public:
    /**
     * Create a new listener for the given vbucket.
     * Returns {} if one already exists (and is running) for the vbucket
     */
    std::shared_ptr<DownloadSnapshotTaskListener> createListener(Vbid vbid);

    /**
     * Remove the listener for the given vbucket or uuid (dependend upon the
     * variant of releaseSnapshot calling this method).
     */
    void removeListener(std::variant<Vbid, std::string_view> snapshotToRelease);

    /**
     * Add statistics information about the current registered snapshot
     * downloads.
     */
    void addStats(const StatCollector& collector,
                  const VBucketFilter& filter) const;

    /**
     * Lookup the state of the snapshot associated with vbid. There may not
     * be one
     * @return optional state (nullopt if nothing found).
     */
    std::optional<DownloadSnapshotTaskState> findState(Vbid vbid) const;

protected:
    struct TaskListener;
    folly::Synchronized<std::unordered_map<Vbid, std::shared_ptr<TaskListener>>,
                        std::mutex>
            listeners;
};

} // namespace cb::snapshot
