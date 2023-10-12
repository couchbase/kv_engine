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
#include <memcached/engine_common.h>
#include <memcached/engine_error.h>
#include <vector>

class Connection;
class Cookie;

/// Base class for tasks scheduled by the Stats Context
class StatsTask : public GlobalTask {
public:
    StatsTask() = delete;
    StatsTask(const StatsTask&) = delete;

    /// Get the result from the command
    cb::engine_errc getCommandError() const;

    /// Iterate over all the collected stats pairs and call the provided
    /// callback
    void iterateStats(std::function<void(std::string_view, std::string_view)>
                              callback) const;

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
    using StatVector = std::vector<std::pair<std::string, std::string>>;
    struct TaskData {
        cb::engine_errc command_error{cb::engine_errc::success};
        StatVector stats;
    };

    /**
     * Callback from the getStats implementation of the sub-class.
     *
     * @param writable_data Provides synchronised access to the task data.
     * @param k The stat key.
     * @param v The stat value.
     */
    void addStatCallback(TaskData& writable_data,
                         std::string_view k,
                         std::string_view v);

    folly::Synchronized<TaskData, std::mutex> taskData;
};

/**
 * Task gathering bucket stats.
 *
 * Used for stat groups which may be expensive to gather (e.g., per collection
 * stats), to avoid occupying a frontend thread.
 */
class StatsTaskBucketStats : public StatsTask {
public:
    StatsTaskBucketStats(Cookie& cookie, std::string key, std::string value);
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
