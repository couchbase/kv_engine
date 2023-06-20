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

    using StatVector = std::vector<std::pair<std::string, std::string>>;

    /**
     * The sub-classes of the task should override this method and
     * populate the error code and the stats vector with the values
     *
     * @param command_error The result of the command
     * @param stats The statistics generated
     */
    virtual void getStats(cb::engine_errc& command_error,
                          StatVector& stats) = 0;

    StatsTask(TaskId id, Cookie& cookie);
    Cookie& cookie;

    struct TaskData {
        cb::engine_errc command_error{cb::engine_errc::success};
        StatVector stats;
    };
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
    void getStats(
            cb::engine_errc& command_error,
            std::vector<std::pair<std::string, std::string>>& stats) override;

    std::string key;
    std::string value;
};

class StatsTaskConnectionStats : public StatsTask {
public:
    StatsTaskConnectionStats(Cookie& cookie, int64_t fd);
    std::string getDescription() const override;
    std::chrono::microseconds maxExpectedDuration() const override;

protected:
    void getStats(
            cb::engine_errc& command_error,
            std::vector<std::pair<std::string, std::string>>& stats) override;
    const int64_t fd;
};

/// Task to collect the connection details stats
class StatsTaskClientConnectionDetails : public StatsTask {
public:
    StatsTaskClientConnectionDetails(Cookie& cookie);
    std::string getDescription() const override;
    std::chrono::microseconds maxExpectedDuration() const override;

protected:
    void getStats(
            cb::engine_errc& command_error,
            std::vector<std::pair<std::string, std::string>>& stats) override;
};
