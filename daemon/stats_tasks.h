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

#include <memcached/engine_common.h>
#include <memcached/engine_error.h>

class Connection;

class Cookie;

/**
 * Background LIBEVENT tasks which can be run as part of the StatsCommandContext
 * execution.
 */
class StatsTask {
public:
    virtual ~StatsTask() = default;

    StatsTask() = delete;
    StatsTask(const StatsTask&) = delete;

    explicit StatsTask(Cookie& cookie);

    virtual void execute() = 0;

    cb::engine_errc getCommandError() const {
        return command_error;
    }

    /// get all of the stats pairs produced by the task
    const std::vector<std::pair<std::string, std::string>>& getStats() const {
        return stats;
    }

protected:
    Cookie& cookie;
    cb::engine_errc command_error = cb::engine_errc::success;
    std::vector<std::pair<std::string, std::string>> stats;
};

class StatsTaskConnectionStats : public StatsTask {
public:
    StatsTaskConnectionStats() = delete;

    StatsTaskConnectionStats(const StatsTaskConnectionStats&) = delete;

    StatsTaskConnectionStats(Cookie& cookie, int64_t fd);

    void execute() override;

protected:
    const int64_t fd;
};
