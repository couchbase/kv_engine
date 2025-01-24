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
#include <nlohmann/json_fwd.hpp>
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
     * @param fsync_size the number of bytes between each call to fsync()
     * @param log_callback a callback function to add information to the log
     */
    [[nodiscard]] static std::unique_ptr<FileDownloader> create(
            std::unique_ptr<MemcachedConnection> connection,
            std::filesystem::path directory,
            std::string uuid,
            std::size_t fsync_size,
            std::function<void(spdlog::level::level_enum,
                               std::string_view,
                               cb::logger::Json json)> log_callback);

    /**
     * Download the file provided in the meta section
     *
     * @param meta The meta information describing the file to download
     */
    bool download(const FileInfo& meta);

protected:
    /**
     * Request section of the file to be appended to the file
     * @param cmd The cmd object containing the command to send
     * @param fp The file pointer to append the data to
     * @return the number of bytes actually returned from the server
     *         in the case it reduced a smaller fragment (if you requested
     *         more than could fit in 32 bits for instance)
     */
    virtual std::size_t downloadFileFragment(const BinprotGenericCommand& cmd,
                                             FILE* fp) = 0;

    /**
     * Write the provided data to the file
     *
     * @param fp the file to write the data to
     * @param data the data to write
     * @throws std::exception for file write errors
     */
    void fwriteData(FILE* fp, std::string_view data) const;

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
    FILE* openFile(const std::filesystem::path& filename) const;

    /// Get the max chunk size this downloader wants to use. The
    /// implementation which copies the entire response packet into memory
    /// don't want to allocate lets say 2GB of memory ;)
    [[nodiscard]] virtual std::size_t getMaxChunkSize() const = 0;

    /**
     * Protected constructor as this is a pure virtual class which needs to
     * be created from the factory method
     *
     * @param directory destination directory for the file
     * @param uuid the uuid for the the snapshot the file belongs to
     * @param fsync_size the number of bytes between each call to fsync()
     * @param log_callback a callback function to add information to the log
     */
    FileDownloader(std::filesystem::path directory,
                   std::string uuid,
                   std::size_t fsync_size,
                   std::function<void(spdlog::level::level_enum,
                                      std::string_view,
                                      cb::logger::Json json)> log_callback);

    /// The directory where the files should be written to
    const std::filesystem::path directory;
    /// The uuid for the snapshot
    const std::string uuid;
    /// The number of bytes between each call to fsync()
    const std::size_t fsync_size;
    /// The callback method to log information
    const std::function<void(
            spdlog::level::level_enum, std::string_view, cb::logger::Json json)>
            log_callback;
};
} // namespace cb::snapshot
