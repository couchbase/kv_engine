/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "download_snapshot_task_listener.h"
#include <stdexcept>

namespace cb::snapshot {
std::string format_as(DownloadSnapshotTaskState state) {
    switch (state) {
    case DownloadSnapshotTaskState::PrepareSnapshot:
        return "PrepareSnapshot";
    case DownloadSnapshotTaskState::DownloadFiles:
        return "DownloadFiles";
    case DownloadSnapshotTaskState::ReleaseSnapshot:
        return "ReleaseSnapshot";
    case DownloadSnapshotTaskState::Finished:
        return "Finished";
    case DownloadSnapshotTaskState::Failed:
        return "Failed";
    }
    throw std::invalid_argument(
            "format_as(DownloadSnapshotTaskState state): Unknown state");
}
} // namespace cb::snapshot
