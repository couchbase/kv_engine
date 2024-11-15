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
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/crc32c.h>
#include <platform/string_utilities.h>
#include <platform/timeutils.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <filesystem>

using namespace spdlog::level;

namespace cb::snapshot {
FileDownloader::FileDownloader(
        std::filesystem::path directory,
        std::string uuid,
        std::size_t fsync_size,
        std::function<void(spdlog::level::level_enum,
                           std::string_view,
                           cb::logger::Json json)> log_callback)
    : directory(std::move(directory)),
      uuid(std::move(uuid)),
      fsync_size(fsync_size),
      log_callback(std::move(log_callback)) {
    // Empty
}

class MemcachedClientFileDownloader : public FileDownloader {
public:
    MemcachedClientFileDownloader(
            std::unique_ptr<MemcachedConnection> connection,
            std::filesystem::path directory,
            std::string uuid,
            std::size_t fsync_size,
            std::function<void(spdlog::level::level_enum,
                               std::string_view,
                               cb::logger::Json json)> lcb)
        : FileDownloader(std::move(directory),
                         std::move(uuid),
                         fsync_size,
                         std::move(lcb)),
          connection(std::move(connection)) {
        log_callback(trace,
                     "Using internal memcached client",
                     {{"chunk_size",
                       cb::size2human(getMaxChunkSizeImpl(fsync_size))},
                      {"fsync_size", cb::size2human(fsync_size)}});
    };

    size_t downloadFileFragment(const BinprotGenericCommand& cmd,
                                FILE* fp) override;

    [[nodiscard]] std::size_t getMaxChunkSize() const override {
        return getMaxChunkSizeImpl(fsync_size);
    }

    [[nodiscard]] bool supportsChecksum() const override {
        return true;
    }

protected:
    static std::size_t getMaxChunkSizeImpl(std::size_t fsync_size) {
        return std::min(static_cast<std::size_t>(50 * 1024 * 1024), fsync_size);
    }
    std::unique_ptr<MemcachedConnection> connection;
};

size_t MemcachedClientFileDownloader::downloadFileFragment(
        const BinprotGenericCommand& cmd, FILE* fp) {
    auto rsp = connection->execute(cmd);
    if (!rsp.isSuccess()) {
        throw ConnectionError("Failed to fetch fragment", rsp);
    }
    auto data = rsp.getDataView();
    Expects(data.size() >= sizeof(uint32_t));
    auto checksum_view = data.substr(data.size() - sizeof(uint32_t));
    data.remove_suffix(sizeof(uint32_t));
    uint32_t checksum;
    std::copy(checksum_view.begin(), checksum_view.end(), &checksum);
    checksum = ntohl(checksum);
    if (crc32c(data, 0) != checksum) {
        throw std::runtime_error("Invalid checksum");
    }

    fwriteData(fp, data);
    fsync(fileno(fp));
    return data.size();
}

class RawSocketFileDownloader : public FileDownloader {
public:
    RawSocketFileDownloader(SOCKET sfd,
                            std::filesystem::path directory,
                            std::string uuid,
                            std::size_t fsync_size,
                            std::function<void(spdlog::level::level_enum,
                                               std::string_view,
                                               cb::logger::Json json)> lcb)
        : FileDownloader(std::move(directory),
                         std::move(uuid),
                         fsync_size,
                         std::move(lcb)),
          sfd(sfd) {
        log_callback(trace,
                     "Using Raw sockets",
                     {{"chunk_size",
                       cb::size2human(std::numeric_limits<int32_t>::max())},
                      {"fsync_size", cb::size2human(fsync_size)}});
    }

    std::size_t downloadFileFragment(const BinprotGenericCommand& cmd,
                                     FILE* fp) override;

protected:
    [[nodiscard]] std::size_t getMaxChunkSize() const override {
        // (we could probably go up to uint32_t max, but ehh.. do we
        // really need to?)
        return std::numeric_limits<int32_t>::max();
    }

    [[nodiscard]] bool supportsChecksum() const override {
        return false;
    }

    void sendBinprotCommand(const BinprotGenericCommand& cmd) const;
    [[nodiscard]] cb::mcbp::Header recvHeader() const;
    [[nodiscard]] std::size_t recvFileFragment(FILE* fp, size_t len) const;
    SOCKET sfd;
};

std::size_t RawSocketFileDownloader::downloadFileFragment(
        const BinprotGenericCommand& cmd, FILE* fp) {
    sendBinprotCommand(cmd);
    auto header = recvHeader();

    if (!isStatusSuccess(header.getResponse().getStatus())) {
        // @todo add the context etc
        throw std::runtime_error(fmt::format("Server failed with: {}",
                                             header.getResponse().getStatus()));
    }
    std::size_t size = header.getBodylen();
    std::size_t offset = 0;

    while (offset < size) {
        auto chunk = std::min(fsync_size, size - offset);
        auto nr = recvFileFragment(fp, chunk);
        offset += nr;
        log_callback(info,
                     "Received and synced",
                     {{"offset", offset}, {"size", size}});
    }
    return size;
}

void RawSocketFileDownloader::sendBinprotCommand(
        const BinprotGenericCommand& cmd) const {
    std::vector<uint8_t> data;
    cmd.encode(data);
    std::size_t offset = 0;
    while (offset < data.size()) {
        auto nw = cb::net::send(
                sfd, data.data() + offset, data.size() - offset, 0);
        if (nw == -1) {
            auto error = cb::net::get_socket_error();
            if (cb::net::is_interrupted(error)) {
                continue;
            }
            throw std::system_error(
                    std::make_error_code(static_cast<std::errc>(error)),
                    "Failed to send data");
        }
        offset += static_cast<std::size_t>(nw);
    }
}

cb::mcbp::Header RawSocketFileDownloader::recvHeader() const {
    cb::mcbp::Header header{};
    auto* ptr = reinterpret_cast<uint8_t*>(&header);
    std::size_t offset = 0;
    while (offset < sizeof(header)) {
        auto nr = cb::net::recv(sfd, ptr + offset, sizeof(header) - offset, 0);
        if (nr == -1) {
            auto error = cb::net::get_socket_error();
            if (cb::net::is_interrupted(error)) {
                continue;
            }
            throw std::system_error(
                    std::make_error_code(static_cast<std::errc>(error)),
                    "Failed to recv data");
        }
        if (nr == 0) {
            throw std::runtime_error("EOF");
        }
        offset += static_cast<std::size_t>(nr);
    }
    return header;
}

std::size_t RawSocketFileDownloader::recvFileFragment(FILE* fp,
                                                      size_t len) const {
    Expects(len != 0);
    Expects(fp != nullptr);

#ifdef linux
    auto ret = splice(
            sfd, nullptr, fileno(fp), nullptr, len, SPLICE_F_MORE) if (ret ==
                                                                       -1) {
        throw std::system_error(
                std::make_error_code(static_cast<std::errc>(errno)),
                "splice failed");
    }
    if (ret > 0) {
        fsync(fileno(fp));
    }
    return static_cast<std::size_t>(ret);
#else
    std::vector<char> buf(len);
    ssize_t ret = cb::net::recv(sfd, buf.data(), len, MSG_WAITALL);
    if (ret == -1) {
        throw std::system_error(
                std::make_error_code(static_cast<std::errc>(errno)),
                "recv failed");
    }
    if (ret > 0) {
        fwriteData(fp, {buf.data(), len});
        fsync(fileno(fp));
    }
    return static_cast<size_t>(ret);
#endif
}

std::unique_ptr<FileDownloader> FileDownloader::create(
        std::unique_ptr<MemcachedConnection> connection,
        std::filesystem::path directory,
        std::string uuid,
        std::size_t fsync_size,
        std::function<void(spdlog::level::level_enum,
                           std::string_view,
                           cb::logger::Json json)> log_callback) {
    if (connection->isSsl()) {
        return std::make_unique<MemcachedClientFileDownloader>(
                std::move(connection),
                std::move(directory),
                std::move(uuid),
                fsync_size,
                std::move(log_callback));
    }
    return std::make_unique<RawSocketFileDownloader>(
            connection->releaseSocket(),
            std::move(directory),
            std::move(uuid),
            fsync_size,
            std::move(log_callback));
}

void FileDownloader::download(const FileInfo& meta) {
    std::size_t size = meta.size;

    std::filesystem::path local = directory / meta.path;
    if (local.has_parent_path()) {
        create_directories(local.parent_path());
    }
    std::size_t offset = 0;

    if (exists(local)) {
        offset = file_size(local);
        if (size == offset) {
            // we already have the file
            log_callback(info,
                         "Skipping file; already downloaded",
                         {{"path", meta.path.string()}, {"size", size}});
            return;
        }
    }

    log_callback(info, "Fetch file", {{"path", meta.path.string()}});
    auto* fp = openFile(local);
    fseek(fp, 0, SEEK_END);

    nlohmann::json file_meta{{"id", meta.id}};
    std::size_t chunksize = getMaxChunkSize();

    const auto start = std::chrono::steady_clock::now();
    while (offset < size) {
        std::size_t chunk = std::min(size - offset, chunksize);
        file_meta["offset"] = std::to_string(offset);
        file_meta["length"] = std::to_string(chunk);

        log_callback(info,
                     "Request fragment",
                     {{"path", meta.path.string()},
                      {"offset", offset},
                      {"chunk", chunk}});

        try {
            downloadFileFragment(
                    BinprotGenericCommand{
                            cb::mcbp::ClientOpcode::GetFileFragment,
                            uuid,
                            file_meta.dump()},
                    fp);
        } catch (const std::exception&) {
            (void)fclose(fp);
            throw;
        }
        offset += chunk;
    }
    const auto end = std::chrono::steady_clock::now();
    log_callback(info,
                 "Fetch file complete",
                 {{"path", meta.path.string()},
                  {"duration", cb::time2text(end - start)},
                  {"throughput", cb::calculateThroughput(size, end - start)}});
    (void)fclose(fp);
}

void FileDownloader::fwriteData(FILE* fp, std::string_view data) const {
    if (fwrite(data.data(), data.size(), 1, fp) != 1) {
        throw std::system_error(
                std::make_error_code(static_cast<std::errc>(errno)),
                "fwrite failed");
    }
}

FILE* FileDownloader::openFile(const std::filesystem::path& filename) const {
    if (filename.has_parent_path() && !exists(filename.parent_path())) {
        create_directories(filename.parent_path());
    }
    auto* fp = fopen(filename.string().c_str(), "ab");
    if (fp == nullptr) {
        throw std::system_error(
                std::make_error_code(static_cast<std::errc>(errno)),
                fmt::format(R"(fopen({}, "ab") failed)", filename.string()));
    }
    return fp;
}
} // namespace cb::snapshot