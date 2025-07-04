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
#include <memcached/engine_error.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/file_sink.h>
#include <platform/json_log.h>
#include <spdlog/spdlog.h>
#include <filesystem>

class MemcachedConnection;
class BinprotGenericCommand;

namespace cb::snapshot {
struct FileInfo;

/**
 * The file downloader class is responsible for downloading files over
 * the MCBP protocol by using GetFileFragment opcode.
 *
 * The primary motivation for this class is because the client object
 * we use spool the entire packet in memory we would like to bypass
 * that and instead write the data to the disk as it arrives over the
 * network. On Linux we may utilize "splice" and copy the pages directly
 * from the socket to the file.
 *
 * Unfortunately if we're using TLS we (at least now) need to use the
 * client instance and copy chunk by chunk.
 */
class FileDownloader {
public:
    FileDownloader() = delete;
    virtual ~FileDownloader() = default;

    /**
     * Create an instance of the FileDownloader objects
     *
     * @param connection The connection to the server to fetch the files
     *                   from
     * @param directory destination directory for the file
     * @param uuid the uuid for the the snapshot the file belongs to
     * @param fsync_interval the number of bytes between each call to fsync()
     * @param log_callback a callback function to add information to the log
     * @param stats_collect_callback a callback function for stat collection
     * received)
     */
    FileDownloader(std::unique_ptr<MemcachedConnection> connection,
                   std::filesystem::path directory,
                   std::string uuid,
                   std::size_t fsync_interval,
                   std::function<void(spdlog::level::level_enum,
                                      std::string_view,
                                      cb::logger::Json json)> log_callback,
                   std::function<void(std::size_t)> stats_collect_callback);
    /**
     * Download the file provided in the meta section
     *
     * @param meta The meta information describing the file to download
     * @return error code indicating the result of the operation
     */
    cb::engine_errc download(const FileInfo& meta) const;

protected:
    /**
     * Validate the checksum for the provided file as match the checksum
     * in the provided meta.
     *
     * @param file the file to check
     * @param meta the metadata for the file (size and checksum)
     * @return true if the file match the checksum, false otherwise
     */
    bool validateChecksum(const std::filesystem::path& file,
                          const FileInfo& meta) const;

    /**
     * Open a file stream for appending for the provided path and create
     * any missing directories
     *
     * @param filename the path to open
     * @return a FILE stream (always)
     * @throws std::exception if an error occurs (either failing to create
     *         directories or opening the file)
     */
    cb::io::FileSink openFile(const std::filesystem::path& filename) const;

    /// The connection to the server used to fetch the files from
    std::unique_ptr<MemcachedConnection> connection;
    /// The directory where the files should be written to
    const std::filesystem::path directory;
    /// The uuid for the snapshot
    const std::string uuid;
    /// The number of bytes between each call to fsync()
    const std::size_t fsync_interval;
    /// The callback method to log information
    const std::function<void(
            spdlog::level::level_enum, std::string_view, cb::logger::Json json)>
            log_callback;
    /// The callback method to log progress
    const std::function<void(std::size_t)> stats_collect_callback;
};
} // namespace cb::snapshot
