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

#include "platform/dirutils.h"

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
#include <platform/strerror.h>

constexpr std::size_t MaxReadSize = 2 * 1024 * 1024 * 1024ULL;

GetFileFragmentContext::GetFileFragmentContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      uuid(cookie.getRequest().getKeyString()),
      state(State::Initialize) {
    // The validator checked that the payload was JSON and that it contains
    // the mandatory fields
    const auto json =
            nlohmann::json::parse(cookie.getRequest().getValueString());
    id = json["id"].get<std::size_t>();
    offset = stoll(json.value("offset", "0"));
    length = stoll(json["length"].get<std::string>());
    if (length > MaxReadSize) {
        length = MaxReadSize;
    }
    using namespace std::string_literals;
    if (json.contains("checksum") && json.value("checksum", ""s) == "crc32c"s) {
        checksum_crc32c = 0;
    }
}

GetFileFragmentContext::~GetFileFragmentContext() {
    if (fd != -1 && ::close(fd) == -1) {
        LOG_WARNING_CTX("Failed to close file descriptor",
                        {"conn_id", cookie.getConnectionId(), "fd", fd},
                        {"error", cb_strerror()});
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

                    fd = ::open(filename.c_str(), O_RDONLY);
                    if (fd == -1) {
                        LOG_WARNING_CTX("Failed to open file",
                                        {"conn_id", cookie.getConnectionId()},
                                        {"error", cb_strerror()},
                                        {"file", filename});
                        if (errno == ENOENT) {
                            cookie.notifyIoComplete(
                                    cb::engine_errc::no_such_key);
                            return;
                        }
                        if (errno == EACCES) {
                            cookie.notifyIoComplete(cb::engine_errc::no_access);
                            return;
                        }
                        cookie.notifyIoComplete(cb::engine_errc::failed);
                        return;
                    }

#ifdef __linux__
                    // Give the kernel a hint that we'll read the data
                    // sequentially and won't need it more than once
                    (void)posix_fadvise(
                            fd,
                            0,
                            0,
                            POSIX_FADV_SEQUENTIAL | POSIX_FADV_NOREUSE);
#endif
                    length = std::min(length, MaxReadSize);
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
    static constexpr const char* checksum_msg =
            R"({ "checksum": { "type" : "crc32c", "size": 4, "location": "tail" } })";
    std::string extras = checksum_crc32c.has_value() ? checksum_msg : "";

    connection.sendResponseHeaders(cookie,
                                   cb::mcbp::Status::Success,
                                   extras,
                                   {},
                                   length,
                                   PROTOCOL_BINARY_RAW_BYTES);
    if (!checksum_crc32c.has_value() && connection.isSendfileSupported()) {
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
                        constexpr std::size_t ChunkSize = 20 * 1024 * 1024;
                        const auto to_read = std::min(length, ChunkSize);

                        auto iob = folly::IOBuf::createCombined(to_read);
                        auto nr = ::pread(
                                fd, iob->writableTail(), to_read, offset);
                        if (nr == -1) {
                            LOG_WARNING_CTX(
                                    "Failed to read file chunk",
                                    {"conn_id", cookie.getConnectionId()},
                                    {"error", cb_strerror()});
                            cookie.notifyIoComplete(
                                    cb::engine_errc::disconnect);
                            return;
                        }

                        if (nr > 0) {
                            if (checksum_crc32c.has_value()) {
                                checksum_crc32c = crc32c(iob->writableTail(),
                                                         nr,
                                                         *checksum_crc32c);
                            }
                            length -= nr;
                            offset += nr;
                            iob->append(nr);
                            chunk.swap(iob);
                        }

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
    if (checksum_crc32c.has_value()) {
        uint32_t network = htonl(*checksum_crc32c);
        std::string_view view = {reinterpret_cast<const char*>(&network),
                                 sizeof(network)};
        connection.copyToOutputStream(view);
    }

    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc GetFileFragmentContext::transfer_with_sendfile() {
    auto ret = connection.sendFile(
            fd, static_cast<off_t>(offset), static_cast<off_t>(length));
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
        state = State::Done;
        return cb::engine_errc::success;
    }

    std::string_view view{reinterpret_cast<const char*>(iob->data()),
                          iob->length()};
    connection.chainDataToOutputStream(
            std::make_unique<NotifySendBuffer>(std::move(iob), view, cookie));
    state = State::ReadFileChunk;
    return cb::engine_errc::would_block;
}
