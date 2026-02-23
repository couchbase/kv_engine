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
        std::size_t write_size,
        std::size_t checksum_length,
        bool allow_fail_fast,
        std::function<void(spdlog::level::level_enum,
                           std::string_view,
                           cb::logger::Json json)> log_callback,
        std::function<void(std::size_t)> stats_collect_callback)
    : connection(std::move(connection)),
      directory(std::move(directory)),
      uuid(std::move(uuid)),
      fsync_interval(fsync_interval),
      write_size(write_size),
      checksum_length(checksum_length),
      allow_fail_fast(allow_fail_fast),
      log_callback(std::move(log_callback)),
      stats_collect_callback(std::move(stats_collect_callback)) {
    // Empty
}

cb::engine_errc FileDownloader::download(const FileInfo& meta) const {
    const auto download_start = std::chrono::steady_clock::now();

    std::size_t size = meta.size;

    std::filesystem::path local = directory / meta.path;
    if (local.has_parent_path()) {
        create_directories(local.parent_path());
    }
    std::size_t offset = 0;

    std::optional<std::chrono::steady_clock::duration> existingChecksumDuration;
    if (exists(local)) {
        offset = file_size(local);
        if (size == offset) {
            const auto start_checksum = std::chrono::steady_clock::now();
            const auto checksumOk = validateChecksum(local, meta);
            const auto checksum_end = std::chrono::steady_clock::now();
            existingChecksumDuration = checksum_end - start_checksum;
            if (checksumOk) {
                // we already have the file
                log_callback(info,
                             "Skipping file; already downloaded",
                             {{"path", meta.path.string()}, {"size", size}});
                return cb::engine_errc::success;
            }
            // Checksum error.. Remove the file and try again
            remove(local);
        }
    }

    if (allow_fail_fast) {
        std::error_code ec;
        const auto space_info = std::filesystem::space(local.parent_path(), ec);
        if (!ec) {
            if (space_info.available < size) {
                log_callback(warn,
                             "Not enough disk space to download file",
                             {{"path", meta.path.string()},
                              {"size", size},
                              {"available_space", space_info.available}});
                return cb::engine_errc::too_big;
            }
        }
    }

    auto sink = openFile(local);
    nlohmann::json file_meta{{"id", meta.id}};
    const auto file_download_start = std::chrono::steady_clock::now();
    while (offset < size) {
        std::size_t chunk = size - offset;
        // When checksumming, it must not exceed the chunk size
        const std::size_t chunk_checksum_length =
                std::min(std::max(std::size_t{0}, checksum_length), chunk);
        file_meta["offset"] = std::to_string(offset);
        file_meta["length"] = std::to_string(chunk);
        log_callback(info,
                     "Request fragment",
                     {{"uuid", uuid},
                      {"path", meta.path.string()},
                      {"chunk", file_meta},
                      {"chunk_checksum_length", chunk_checksum_length}});
        offset += connection->getFileFragment(uuid,
                                              meta.id,
                                              offset,
                                              chunk,
                                              chunk_checksum_length,
                                              write_size,
                                              sink.get(),
                                              stats_collect_callback);
    }
    sink->close();
    const auto file_download_end = std::chrono::steady_clock::now();
    const auto start_checksum = file_download_end;
    const auto checksumOk = validateChecksum(local, meta);
    const auto checksum_end = std::chrono::steady_clock::now();

    if (!checksumOk) {
        log_callback(
                warn,
                "Fetch file complete invalid checksum",
                {{"path", meta.path.string()},
                 {"duration", checksum_end - download_start},
                 {"file_download_duration",
                  file_download_end - file_download_start},
                 {"checksum_duration", checksum_end - start_checksum},
                 {"existing_checksum_duration", existingChecksumDuration},
                 {"size", size},
                 {"throughput",
                  cb::calculateThroughput(
                          size, file_download_end - file_download_start)}});
        remove(local);
        return cb::engine_errc::checksum_mismatch;
    }

    log_callback(info,
                 "Fetch file complete",
                 {{"path", meta.path.string()},
                  {"duration", checksum_end - download_start},
                  {"file_download_duration",
                   file_download_end - file_download_start},
                  {"checksum_duration", checksum_end - start_checksum},
                  {"existing_checksum_duration", existingChecksumDuration},
                  {"size", size},
                  {"throughput",
                   cb::calculateThroughput(
                           size, file_download_end - file_download_start)}});
    return cb::engine_errc::success;
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

std::unique_ptr<cb::io::FileSink> FileDownloader::openFile(
        const std::filesystem::path& filename) const {
    if (filename.has_parent_path() && !exists(filename.parent_path())) {
        create_directories(filename.parent_path());
    }
    return std::make_unique<cb::io::FileSink>(
            filename, cb::io::FileSink::Mode::Append, fsync_interval);
}
} // namespace cb::snapshot
