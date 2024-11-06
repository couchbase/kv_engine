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

namespace snapshot {
void download(std::unique_ptr<MemcachedConnection> connection,
              const std::filesystem::path& directory,
              const nlohmann::json& snapshot,
              const std::function<void(spdlog::level::level_enum,
                                       std::string_view,
                                       cb::logger::Json json)>& log_callback) {
    if (!exists(directory)) {
        create_directories(directory);
    }

    if (!exists(directory / "manifest.json")) {
        FILE* fp = fopen((directory / "manifest.json").string().c_str(), "w");
        if (!fp) {
            throw std::system_error(
                    std::make_error_code(static_cast<std::errc>(errno)),
                    fmt::format("Failed to open file {}",
                                (directory / "manifest.json").string()));
        }
        fprintf(fp, "%s\n", snapshot.dump().c_str());
        fflush(fp);
        fclose(fp);
    }

    auto downloader = FileDownloader::create(std::move(connection),
                                             std::move(directory),
                                             snapshot.value("uuid", ""),
                                             50 * 1024 * 1024,
                                             log_callback);

    for (const auto& file : snapshot["files"]) {
        downloader->download(file);
    }

    if (snapshot.contains("deks")) {
        for (const auto& file : snapshot["deks"]) {
            downloader->download(file);
        }
    }
}
} // namespace snapshot
