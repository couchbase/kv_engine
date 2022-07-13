/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <atomic>
#include <vector>

class Connection;
class Cookie;

/// Base class for tasks scheduled by the Stats Context
class StatsTask : public GlobalTask {
public:
    StatsTask() = delete;
    StatsTask(const StatsTask&) = delete;

    cb::engine_errc getCommandError() const {
        return command_error;
    }

    /// get all of the stats pairs produced by the task
    std::vector<std::pair<std::string, std::string>> getStats() {
        std::vector<std::pair<std::string, std::string>> ret;
        stats.swap(ret);
        return ret;
    }

protected:
    StatsTask(TaskId id, Cookie& cookie);
    Cookie& cookie;
    std::atomic<cb::engine_errc> command_error{cb::engine_errc::success};
    folly::Synchronized<std::vector<std::pair<std::string, std::string>>,
                        std::mutex>
            stats;
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
    bool run() override;

    std::string key;
    std::string value;
};

class StatsTaskConnectionStats : public StatsTask {
public:
    StatsTaskConnectionStats(Cookie& cookie, int64_t fd);
    std::string getDescription() const override;
    std::chrono::microseconds maxExpectedDuration() const override;

protected:
    bool run() override;
    const int64_t fd;
};
