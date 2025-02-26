/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "stats_tasks.h"

#include "buckets.h"
#include "concurrency_semaphores.h"
#include "connection.h"
#include "cookie.h"
#include "daemon/protocol/mcbp/engine_wrapper.h"
#include "daemon/sendbuffer.h"
#include "daemon/settings.h"
#include "front_end_thread.h"
#include "mcaudit.h"
#include "mcbp/codec/stats_codec.h"
#include "memcached.h"
#include "memcached/engine_error.h"
#include "nobucket_taskable.h"

#include <dek/manager.h>
#include <logger/logger.h>
#include <statistics/cbstat_collector.h>

#include <memory>

std::size_t StatsTask::TaskData::append(std::string_view k,
                                        std::string_view v) {
    cb::mcbp::response::StatsResponse rsp(k.size(), v.size());
    if (validator && validator->validate(v)) {
        rsp.setDatatype(cb::mcbp::Datatype::JSON);
    }
    rsp.setOpaque(opaque);
    auto header = rsp.getBuffer();
    const auto total = header.size() + k.size() + v.size();
    if (stats_buf.empty() || stats_buf.back()->tailroom() < total) {
        stats_buf.emplace_back(folly::IOBuf::createCombined(BUFFER_CAPACITY));
    }

    // Write the mcbp response into the task's buffer (header, key, value).
    auto& iob = *stats_buf.back();
    iob.reserve(0, total);
    std::ranges::copy(header, iob.writableTail());
    iob.append(sizeof(rsp));
    std::ranges::copy(k, iob.writableTail());
    iob.append(k.size());
    std::ranges::copy(v, iob.writableTail());
    iob.append(v.size());
    return total;
}

StatsTask::StatsTask(TaskId id, Cookie& cookie_)
    : GlobalTask(NoBucketTaskable::instance(), id), cookie(cookie_) {
    taskData.withLock([this](auto& data) {
        data.opaque = cookie.getHeader().getOpaque();
        if (cookie.getConnection().isDatatypeEnabled(
                    PROTOCOL_BINARY_DATATYPE_JSON)) {
            data.validator = cb::json::SyntaxValidator::New();
        }
    });
}

cb::engine_errc StatsTask::getCommandError() const {
    return taskData.lock()->command_error;
}

cb::engine_errc StatsTask::drainBufferedStatsToOutput(bool notifyCookieOnSend) {
    taskData.withLock([this, &notifyCookieOnSend](auto& data) {
        auto& stats_buf = data.stats_buf;
        if (stats_buf.empty()) {
            // No data to send, so we won't be notifying the cookie and should
            // return success.
            notifyCookieOnSend = false;
            return;
        }

        while (!stats_buf.empty()) {
            auto& iob = stats_buf.front();
            std::string_view view = {reinterpret_cast<const char*>(iob->data()),
                                     iob->length()};
            cookie.getConnection().chainDataToOutputStream(
                    std::make_unique<IOBufSendBuffer>(std::move(iob), view));
            stats_buf.pop_front();
            statsBufSize -= view.size();
        }
    });

    // Sanity check: we've drained the buffer.
    Expects(statsBufSize == 0);
    if (notifyCookieOnSend) {
        return cb::engine_errc::too_much_data_in_output_buffer;
    }
    return cb::engine_errc::success;
}

size_t StatsTask::getBufferSize() const {
    return statsBufSize.load();
}

bool StatsTask::run() {
    taskData.withLock([this](auto& data) {
        auto addStatFn = [this, &data](std::string_view key,
                                       std::string_view value,
                                       CookieIface&) {
            statsBufSize += data.append(key, value);
        };
        getStats(data.command_error, addStatFn);
        // If the handler isn't would_block we should signal the cookie
        // with "success" causing the state machine to read the actual
        // status from the task. If it is "would block" the underlying
        // engine will do this notification once its done.
        if (data.command_error != cb::engine_errc::would_block) {
            cookie.notifyIoComplete(cb::engine_errc::success);
        }
    });
    return false;
}

StatsTaskBucketStats::StatsTaskBucketStats(TaskId taskId,
                                           Cookie& cookie,
                                           std::string key,
                                           std::string value)
    : StatsTask(taskId, cookie), key(std::move(key)), value(std::move(value)) {
}

void StatsTaskBucketStats::getStats(cb::engine_errc& command_error,
                                    const AddStatFn& add_stat_callback) {
    const auto max_send_size = Settings::instance().getMaxSendQueueSize();
    const auto check_yield_callback = [this, max_send_size]() {
        return getBufferSize() >= max_send_size;
    };

    // The underlying engines seems to throttle before reaching the size
    // limit
    do {
        command_error = bucket_get_stats(
                cookie, key, value, add_stat_callback, check_yield_callback);
    } while (command_error == cb::engine_errc::throttled &&
             !check_yield_callback());

    if (key.empty() && command_error == cb::engine_errc::success) {
        CBStatCollector collector(add_stat_callback, cookie);
        command_error =
                server_stats(collector, cookie.getConnection().getBucket());
    }
}

std::string StatsTaskBucketStats::getDescription() const {
    return "bucket stats";
}

std::chrono::microseconds StatsTaskBucketStats::maxExpectedDuration() const {
    return std::chrono::seconds(1);
}

StatsTaskConnectionStats::StatsTaskConnectionStats(Cookie& cookie, int64_t fd)
    : StatsTask(TaskId::Core_StatsConnectionTask, cookie), fd(fd) {
}

void StatsTaskConnectionStats::getStats(cb::engine_errc& command_error,
                                        const AddStatFn& add_stat_callback) {
    try {
        iterate_all_connections(
                [this, &add_stat_callback](Connection& c) -> void {
                    if (fd == -1 || c.getId() == fd) {
                        add_stat_callback(std::to_string(c.getId()), c.to_json().dump(), cookie);
                    }
                });
    } catch (const std::exception& exception) {
        LOG_WARNING_CTX(
                "StatsTaskConnectionStats::getStats(): An exception occurred",
                {"conn_id", cookie.getConnectionId()},
                {"error", exception.what()});
        cookie.setErrorContext("An exception occurred");
        command_error = cb::engine_errc::failed;
    }
}

std::string StatsTaskConnectionStats::getDescription() const {
    if (fd == -1) {
        return "stats connections";
    }
    return "stats connection " + std::to_string(fd);
}

std::chrono::microseconds StatsTaskConnectionStats::maxExpectedDuration()
        const {
    return std::chrono::seconds(1);
}

StatsTaskClientConnectionDetails::StatsTaskClientConnectionDetails(
        Cookie& cookie)
    : StatsTask(TaskId::Core_StatsConnectionTask, cookie) {
}

void StatsTaskClientConnectionDetails::getStats(
        cb::engine_errc& command_error, const AddStatFn& add_stat_callback) {
    const auto clientConnectionMap =
            FrontEndThread::getClientConnectionDetails();
    const auto now = std::chrono::steady_clock::now();
    for (const auto& [ip, entry] : clientConnectionMap) {
        add_stat_callback(std::string(ip), entry.to_json(now).dump(), cookie);
    }
}

std::string StatsTaskClientConnectionDetails::getDescription() const {
    return "stats client connection info";
}

std::chrono::microseconds
StatsTaskClientConnectionDetails::maxExpectedDuration() const {
    return std::chrono::seconds(1);
}

std::string StatsTaskEncryptionKeyIds::getDescription() const {
    return "stats encryption-key-ids";
}

void StatsTaskEncryptionKeyIds::getStats(cb::engine_errc& command_error,
                                         const AddStatFn& add_stat_callback) {
    auto& semaphore = ConcurrencySemaphores::instance()
                              .encryption_and_snapshot_management;
    if (semaphore.try_acquire()) {
        cb::SemaphoreGuard<> semaphoreGuard(&semaphore, cb::adopt_token_t{});
        if (cookie.getConnection().getBucket().type == BucketType::NoBucket) {
            nlohmann::json json = {{"@audit", cb::audit::getDeksInUse()},
                                   {"@logs", cb::logger::getDeksInUse()}};
            add_stat_callback("encryption-key-ids", json.dump(), cookie);
            command_error = cb::engine_errc::success;
        } else {
            StatsTaskBucketStats::getStats(command_error, add_stat_callback);
        }
    } else {
        command_error = cb::engine_errc::temporary_failure;
    }
}
