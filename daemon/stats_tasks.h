/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <executor/globaltask.h>
#include <folly/Synchronized.h>
#include <folly/io/IOBuf.h>
#include <json/syntax_validator.h>
#include <memcached/engine_common.h>
#include <memcached/engine_error.h>
#include <deque>

class Connection;
class Cookie;

/// Base class for tasks scheduled by the Stats Context
class StatsTask : public GlobalTask {
public:
    StatsTask() = delete;
    StatsTask(const StatsTask&) = delete;

    /// Get the result from the command
    cb::engine_errc getCommandError() const;

    /**
     * Writes any buffered stats to the output buffer.
     * @param notifyOnIoCompete Whether to notifyIoComplete() the cookie once
     * the buffer has left the connection sendQueue.
     * @return cb::engine_errc::would_block if the cookie will be notified later
     */
    [[nodiscard]] cb::engine_errc drainBufferedStatsToOutput(
            bool notifyOnIoCompete);

    /**
     * Return the size of the stats response buffer in bytes.
     */
    size_t getBufferSize() const;

protected:
    bool run() final;

    /**
     * The sub-classes of the task should override this method and
     * populate the error code and the stats vector with the values
     * by calling add_stat_callback(k, v, cookie).
     *
     * @param command_error The result of the command
     * @param add_stat_callback The callback to append stats
     */
    virtual void getStats(cb::engine_errc& command_error,
                          const AddStatFn& add_stat_callback) = 0;

    StatsTask(TaskId id, Cookie& cookie);
    Cookie& cookie;

private:
    /// The initial capacity of the buffer where we store the stats responses.
    static constexpr size_t BUFFER_CAPACITY = 16 * 1024;

    struct TaskData {
        /**
         * Append an MCBP frame to the end of the stream containing
         * the key-value pair
         *
         * @param k the stat key
         * @param v she stat value
         * @return the total size of the key value pair
         */
        std::size_t append(std::string_view k, std::string_view v);
        /// The opaque field to add to all of the packets in the stream
        uint32_t opaque;
        /// validator used to check if the provided value is JSON
        std::unique_ptr<cb::json::SyntaxValidator> validator;
        cb::engine_errc command_error{cb::engine_errc::success};
        std::deque<std::unique_ptr<folly::IOBuf>> stats_buf;
    };

    folly::Synchronized<TaskData, std::mutex> taskData;

    /**
     * The total size of buffered responses. Logically the same as the sum of
     * taskData.stats_buf[i]->length(), but maintained separately for
     * efficiency.
     */
    std::atomic<size_t> statsBufSize{0};
};

/**
 * Task gathering bucket stats.
 *
 * Used for stat groups which may be expensive to gather (e.g., per collection
 * stats), to avoid occupying a frontend thread.
 */
class StatsTaskBucketStats : public StatsTask {
public:
    StatsTaskBucketStats(TaskId taskId,
                         Cookie& cookie,
                         std::string key,
                         std::string value);
    std::string getDescription() const override;
    std::chrono::microseconds maxExpectedDuration() const override;

protected:
    void getStats(cb::engine_errc& command_error,
                  const AddStatFn& add_stat_callback) override;

    std::string key;
    std::string value;
};

class StatsTaskConnectionStats : public StatsTask {
public:
    StatsTaskConnectionStats(Cookie& cookie, int64_t fd);
    std::string getDescription() const override;
    std::chrono::microseconds maxExpectedDuration() const override;

protected:
    void getStats(cb::engine_errc& command_error,
                  const AddStatFn& add_stat_callback) override;
    const int64_t fd;
};

/// Task to collect the connection details stats
class StatsTaskClientConnectionDetails : public StatsTask {
public:
    StatsTaskClientConnectionDetails(Cookie& cookie);
    std::string getDescription() const override;
    std::chrono::microseconds maxExpectedDuration() const override;

protected:
    void getStats(cb::engine_errc& command_error,
                  const AddStatFn& add_stat_callback) override;
};

class StatsTaskEncryptionKeyIds : public StatsTaskBucketStats {
public:
    StatsTaskEncryptionKeyIds(TaskId taskId,
                              Cookie& cookie,
                              std::string key,
                              std::string value)
        : StatsTaskBucketStats(
                  taskId, cookie, std::move(key), std::move(value)) {
    }
    std::string getDescription() const override;

protected:
    void getStats(cb::engine_errc& command_error,
                  const AddStatFn& add_stat_callback) override;
};
