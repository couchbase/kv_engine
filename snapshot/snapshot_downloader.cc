/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "snapshot_downloader.h"
#include "file_downloader.h"
#include "manifest.h"

namespace cb::snapshot {
void download(std::unique_ptr<MemcachedConnection> connection,
              const std::filesystem::path& directory,
              const Manifest& snapshot,
              std::size_t fsync_interval,
              const std::function<void(spdlog::level::level_enum,
                                       std::string_view,
                                       cb::logger::Json json)>& log_callback) {
    FileDownloader downloader(std::move(connection),
                              std::move(directory),
                              snapshot.uuid,
                              fsync_interval,
                              log_callback);

    auto download_with_retry = [&downloader](auto& file) -> void {
        int retry = 5;
        while (retry > 0) {
            if (downloader.download(file)) {
                return;
            }
            --retry;
        }
        throw std::runtime_error(fmt::format(
                "Failed to download \"{}\" after 5 attempts. Giving up.",
                file.path.string()));
    };

    for (const auto& file : snapshot.files) {
        download_with_retry(file);
    }

    for (const auto& file : snapshot.deks) {
        download_with_retry(file);
    }
}
} // namespace cb::snapshot
