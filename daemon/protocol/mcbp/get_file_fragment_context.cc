/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "get_file_fragment_context.h"

#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <daemon/settings.h>
#include <executor/executorpool.h>
#include <folly/portability/Fcntl.h>
#include <folly/portability/Unistd.h>
#include <logger/logger.h>
#include <memcached/engine.h>
#include <platform/crc32c.h>
#include <platform/dirutils.h>
#include <platform/strerror.h>

#include <algorithm>

GetFileFragmentContext::GetFileFragmentContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      uuid(cookie.getRequest().getKeyString()),
      state(State::Initialize),
      chunk_size(Settings::instance().getFileFragmentMaxChunkSize()) {
    // The validator checked that the payload was JSON and that it contains
    // the mandatory fields
    const auto json =
            nlohmann::json::parse(cookie.getRequest().getValueString());
    id = json["id"].get<std::size_t>();
    try {
        offset = stoll(json.value("offset", "0"));
        length = stoll(json["length"].get<std::string>());
        checksum_length = stoll(json.value("checksum_length", "0"));
    } catch (const std::exception& e) {
        // When testing sometimes stoll would fail for say a negative number,
        // decorate any exception and throw more detail
        throw std::invalid_argument(
                fmt::format("Failed to parse JSON: {} {}",
                            cookie.getRequest().getValueString(),
                            e.what()));
    }

    const size_t max_read_size =
            Settings::instance().getFileFragmentMaxReadSize();
    length = std::min(length, max_read_size);
    network_length = length;

    if (checksum_length > 0) {
        // When checksumming, we will write to the network the file data,
        // interspersed with the checksums. E.g.
        // {file data}{checksum}{file data}{checksum}...
        //
        // Caller (file_downloader) will set the checksum_length to match this
        // requirement.
        Expects(checksum_length <= length);

        // When checksumming set chunk_size to the checksum_length. This means
        // we read in checksum_length chunks and then generate the sum for each
        // chunk.
        chunk_size = checksum_length;

        // When checksumming we need to recalculate the network_length because
        // the addition of checksums per chunk increases the transmited network
        // length.
        network_length = get_network_length(length, checksum_length);
        if (network_length > max_read_size) {
            // network_length can though exceed the max_read_size, which means
            // we must reduce the requested length and recompute the
            // network_length.
            const size_t max_chunks =
                    max_read_size / (checksum_length + sizeof(uint32_t));
            length = max_chunks * checksum_length;
            network_length = get_network_length(length, checksum_length);
        }
    }

    filestream.exceptions(std::ifstream::failbit | std::ifstream::badbit);
}

size_t GetFileFragmentContext::get_network_length(size_t file_read_length,
                                                  size_t file_checksum_length) {
    const size_t chunk_size = file_checksum_length + sizeof(uint32_t);
    const size_t tail_remainder = file_read_length % file_checksum_length;
    const size_t whole_chunks = file_read_length / file_checksum_length;
    if (tail_remainder) {
        return whole_chunks * chunk_size + (tail_remainder + sizeof(uint32_t));
    }
    return whole_chunks * chunk_size;
}

GetFileFragmentContext::~GetFileFragmentContext() {
    if (fd != -1 && ::close(fd) == -1) {
        LOG_WARNING_CTX("Failed to close file descriptor",
                        {"conn_id", cookie.getConnectionId()},
                        {"fd", fd},
                        {"error", cb_strerror()});
    }
    if (filestream.is_open()) {
        try {
            filestream.close();
        } catch (const std::exception& e) {
            LOG_WARNING_CTX("Failed to close file descriptor",
                            {"conn_id", cookie.getConnectionId()},
                            {"error", e.what()});
        }
    }
}

cb::engine_errc GetFileFragmentContext::step() {
    auto ret = cb::engine_errc::success;
    while (ret == cb::engine_errc::success) {
        switch (state) {
        case State::Initialize:
            ret = initialize();
            break;
        case State::SendResponseHeader:
            ret = send_response_header();
            break;
        case State::ReadFileChunk:
            ret = read_file_chunk();
            break;
        case State::ChainFileChunk:
            ret = chain_file_chunk();
            break;
        case State::Done:
            cookie.clearEwouldblock();
            return cb::engine_errc::success;
        }
    }
    return ret;
}

cb::engine_errc GetFileFragmentContext::initialize() {
    ExecutorPool::get()->schedule(std::make_shared<
                                  OneShotLimitedConcurrencyTask>(
            TaskId::Core_ReadFileFragmentTask,
            "Read file fragment-initialize",
            [this]() {
                try {
                    nlohmann::json file_meta;
                    auto rv =
                            connection.getBucketEngine().get_snapshot_file_info(
                                    cookie,
                                    uuid,
                                    id,
                                    [&file_meta](const auto& obj) {
                                        file_meta = obj;
                                    });
                    if (rv != cb::engine_errc::success) {
                        cookie.notifyIoComplete(rv);
                        return;
                    }

                    filename = file_meta["path"].get<std::string>();
                    const auto file_size =
                            std::stoull(file_meta["size"].get<std::string>());

                    if ((offset + length) > file_size) {
                        cookie.setErrorContext("Requested offset > file size");
                        cookie.notifyIoComplete(
                                cb::engine_errc::invalid_arguments);
                        return;
                    }

                    filestream.open(filename, std::ios::binary | std::ios::in);
                    filestream.seekg(offset);

                    state = State::SendResponseHeader;
                    cookie.notifyIoComplete(cb::engine_errc::success);
                } catch (const std::exception& exception) {
                    LOG_WARNING_CTX("Failed to open file",
                                    {"conn_id", cookie.getConnectionId()},
                                    {"error", exception.what()});
                    cookie.notifyIoComplete(cb::engine_errc::failed);
                }
            },
            ConcurrencySemaphores::instance().read_vbucket_chunk));

    return cb::engine_errc::would_block;
}

cb::engine_errc GetFileFragmentContext::send_response_header() {
    connection.sendResponseHeaders(cookie,
                                   cb::mcbp::Status::Success,
                                   {},
                                   {},
                                   network_length,
                                   PROTOCOL_BINARY_RAW_BYTES);
    state = State::ReadFileChunk;
    return cb::engine_errc::success;
}

cb::engine_errc GetFileFragmentContext::read_file_chunk() {
    if (length) {
        state = State::ChainFileChunk;
        ExecutorPool::get()->schedule(std::make_shared<
                                      OneShotLimitedConcurrencyTask>(
                TaskId::Core_ReadFileFragmentTask,
                "Read file fragment",
                [this]() {
                    try {
                        const auto to_read = std::min(length, chunk_size);
                        auto iob = create_io_buf(to_read);
                        filestream.read(
                                reinterpret_cast<char*>(iob->writableTail()),
                                to_read);
                        iob->append(to_read);
                        calculate_and_append_checksum(*iob);
                        chunk.swap(iob);
                        length -= to_read;
                        offset += to_read;

                        cookie.notifyIoComplete(cb::engine_errc::success);
                    } catch (const std::exception& e) {
                        LOG_WARNING_CTX("Failed to read file chunk",
                                        {"conn_id", cookie.getConnectionId()},
                                        {"exception", e.what()});
                        cookie.notifyIoComplete(cb::engine_errc::disconnect);
                    }
                },
                ConcurrencySemaphores::instance().read_vbucket_chunk));

        return cb::engine_errc::would_block;
    }
    // // No more data to send; we're done!
    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc GetFileFragmentContext::chain_file_chunk() {
    std::unique_ptr<folly::IOBuf> iob;
    chunk.swap(iob);
    if (!iob) {
        Expects(length == 0 && "Pending data to send");
        state = State::Done;
        return cb::engine_errc::success;
    }

    std::string_view view{reinterpret_cast<const char*>(iob->data()),
                          iob->length()};
    connection.chainDataToOutputStream(
            std::make_unique<IOBufSendBuffer>(std::move(iob), view));
    if (length) {
        // We need to read another chunk
        state = State::ReadFileChunk;
    } else {
        state = State::Done;
    }
    return cb::engine_errc::too_much_data_in_output_buffer;
}

void GetFileFragmentContext::calculate_and_append_checksum(folly::IOBuf& iob) {
    if (checksum_length == 0) {
        return;
    }
    // Compute a checksum (put it into network order) and append it to the IOBuf
    uint32_t crc = htonl(crc32c(iob.data(), iob.length(), 0));
    std::memcpy(iob.writableTail(), &crc, sizeof(crc));
    iob.append(sizeof(crc));
}

std::unique_ptr<folly::IOBuf> GetFileFragmentContext::create_io_buf(
        size_t length) const {
    if (checksum_length) {
        length += sizeof(uint32_t);
    }
    return folly::IOBuf::createCombined(length);
}
