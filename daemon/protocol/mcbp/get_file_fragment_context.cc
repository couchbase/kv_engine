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
      useSendfile(cookie.getConnection().isSendfileSupported()),
      max_read_size(Settings::instance().getFileFragmentMaxReadSize()),
      state(State::Initialize),
      chunk_size(Settings::instance().getFileFragmentMaxChunkSize()) {
    // The validator checked that the payload was JSON and that it contains
    // the mandatory fields
    const auto json =
            nlohmann::json::parse(cookie.getRequest().getValueString());
    id = json["id"].get<std::size_t>();
    offset = stoll(json.value("offset", "0"));
    length = stoll(json["length"].get<std::string>());
    length = std::min(length, max_read_size);
    if (!useSendfile) {
        filestream.exceptions(std::ifstream::failbit | std::ifstream::badbit);
    }
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
        case State::TransferWithSendFile:
            ret = transfer_with_sendfile();
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

                    if (useSendfile) {
                        fd = ::open(filename.c_str(), O_RDONLY);
                        if (fd == -1) {
                            LOG_WARNING_CTX(
                                    "Failed to open file",
                                    {"conn_id", cookie.getConnectionId()},
                                    {"error", cb_strerror()},
                                    {"file", filename});
                            if (errno == ENOENT) {
                                cookie.notifyIoComplete(
                                        cb::engine_errc::no_such_key);
                                return;
                            }
                            if (errno == EACCES) {
                                cookie.notifyIoComplete(
                                        cb::engine_errc::no_access);
                                return;
                            }
                            cookie.notifyIoComplete(cb::engine_errc::failed);
                            return;
                        }
                    } else {
                        filestream.open(filename,
                                        std::ios::binary | std::ios::in);
                        filestream.seekg(offset);
                    }

                    length = std::min(length, max_read_size);
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
                                   length,
                                   PROTOCOL_BINARY_RAW_BYTES);
    if (connection.isSendfileSupported()) {
        state = State::TransferWithSendFile;
    } else {
        state = State::ReadFileChunk;
    }
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
                        auto iob = folly::IOBuf::createCombined(to_read);
                        filestream.read(
                                reinterpret_cast<char*>(iob->writableTail()),
                                to_read);
                        iob->append(to_read);
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

cb::engine_errc GetFileFragmentContext::transfer_with_sendfile() {
    const auto ret = connection.sendFile(fd, offset, length);
    if (ret == cb::engine_errc::success) {
        // evbuffer took the ownership..
        fd = -1;
    }
    state = State::Done;
    return ret;
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
