/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "file_downloader.h"
#include "manifest.h"

#include <cbcrypto/digest.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/crc32c.h>
#include <platform/json_log_conversions.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <cstdio>
#include <filesystem>

using namespace spdlog::level;

namespace cb::snapshot {
FileDownloader::FileDownloader(
        std::unique_ptr<MemcachedConnection> connection,
        std::filesystem::path directory,
        std::string uuid,
        std::size_t fsync_interval,
        std::function<void(spdlog::level::level_enum,
                           std::string_view,
                           cb::logger::Json json)> log_callback)
    : connection(std::move(connection)),
      directory(std::move(directory)),
      uuid(std::move(uuid)),
      fsync_interval(fsync_interval),
      log_callback(std::move(log_callback)) {
    // Empty
}

bool FileDownloader::download(const FileInfo& meta) const {
    std::size_t size = meta.size;

    std::filesystem::path local = directory / meta.path;
    if (local.has_parent_path()) {
        create_directories(local.parent_path());
    }
    std::size_t offset = 0;

    if (exists(local)) {
        offset = file_size(local);
        if (size == offset) {
            if (validateChecksum(local, meta)) {
                // we already have the file
                log_callback(info,
                             "Skipping file; already downloaded",
                             {{"path", meta.path.string()}, {"size", size}});
                return true;
            }
            // Checksum error.. Remove the file and try again
            remove(local);
        }
    }

    auto sink = openFile(local);
    nlohmann::json file_meta{{"id", meta.id}};
    const auto start = std::chrono::steady_clock::now();
    while (offset < size) {
        std::size_t chunk = size - offset;
        file_meta["offset"] = std::to_string(offset);
        file_meta["length"] = std::to_string(chunk);
        log_callback(info,
                     "Request fragment",
                     {{"uuid", uuid},
                      {"path", meta.path.string()},
                      {"chunk", file_meta}});
        offset +=
                connection->getFileFragment(uuid, meta.id, offset, chunk, sink);
    }
    sink.close();
    const auto end = std::chrono::steady_clock::now();

    if (!validateChecksum(local, meta)) {
        log_callback(
                warn,
                "Fetch file complete invalid checksum",
                {{"path", meta.path.string()},
                 {"duration", end - start},
                 {"throughput", cb::calculateThroughput(size, end - start)}});
        remove(local);
        return false;
    }

    log_callback(info,
                 "Fetch file complete",
                 {{"path", meta.path.string()},
                  {"duration", end - start},
                  {"throughput", cb::calculateThroughput(size, end - start)}});
    return true;
}

bool FileDownloader::validateChecksum(const std::filesystem::path& file,
                                      const FileInfo& meta) const {
    if (meta.sha512.empty()) {
        return true;
    }

    try {
        auto generated = cb::crypto::sha512sum(file, meta.size);
        if (meta.sha512 == generated) {
            return true;
        }
        log_callback(warn,
                     "File checksum failed",
                     {{"path", file.string()},
                      {"expected", meta.sha512},
                      {"calculated", generated}});
    } catch (const std::exception& e) {
        log_callback(warn,
                     "Failed to calculate SHA-512",
                     {{"path", file.string()}, {"error", e.what()}});
    }
    return false;
}

cb::io::FileSink FileDownloader::openFile(
        const std::filesystem::path& filename) const {
    if (filename.has_parent_path() && !exists(filename.parent_path())) {
        create_directories(filename.parent_path());
    }
    return cb::io::FileSink(
            filename, cb::io::FileSink::Mode::Append, fsync_interval);
}
} // namespace cb::snapshot
