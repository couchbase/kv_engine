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

#include <snapshot/manifest.h>
#include <string>

namespace cb::snapshot {

enum class DownloadSnapshotTaskState {
    PrepareSnapshot,
    DownloadFiles,
    ReleaseSnapshot,
    Finished,
    Failed
};
std::string format_as(DownloadSnapshotTaskState state);

/**
 * The listener interface for the DownloadSnapshotTask
 */
class DownloadSnapshotTaskListener {
public:
    virtual ~DownloadSnapshotTaskListener() = default;
    /// The state of the task has changed
    virtual void stateChanged(DownloadSnapshotTaskState state) = 0;
    /// Notify the listener that the manifest has been set
    virtual void setManifest(Manifest manifest) = 0;
    /// Notify the listener that an error has occurred and the download
    /// task has failed
    virtual void failed(std::string reason) = 0;
};

} // namespace cb::snapshot
