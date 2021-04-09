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

#include <spdlog/common.h>

namespace spdlog {
class logger;
}

struct ServerLogIface {
    virtual ~ServerLogIface() = default;

    /** Returns a pointer to the file logger that resides in the
     * memcached_logger library. Used by consumers to log messages.
     */
    virtual spdlog::logger* get_spdlogger() = 0;

    /**
     * Registers the given spdlogger in the spdlog registry belonging to the
     * memcached_logger library. Subscribes the given spdlogger to any
     * runtime verbosity changes.
     */
    virtual void register_spdlogger(std::shared_ptr<spdlog::logger> logger) = 0;

    /**
     * Unregister the given spdlogger in the spdlog registry belonging to the
     * memcached_logger library. Unsubscribes the given spdlogger from any
     * runtime verbosity changes.
     */
    virtual void unregister_spdlogger(const std::string& name) = 0;

    /**
     * Set the verbosity of all registered loggers to the given severity.
     * Only works for trace, debug, and info levels. Any level greater than
     * info will set the severity to info.
     */
    virtual void set_level(spdlog::level::level_enum severity) = 0;
};
