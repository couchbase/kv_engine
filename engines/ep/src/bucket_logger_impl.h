/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <spdlog/fmt/ostr.h>

/*
 * Definitions of BucketLogger code which must be inline.
 */

template <typename S, typename... Args>
void BucketLogger::log(spdlog::level::level_enum lvl,
                       const S& fmt,
                       Args&&... args) {
    if (!should_log(lvl)) {
        return;
    }
    logInner(lvl, fmt, fmt::make_args_checked<Args...>(fmt, args...));
}

template <typename... Args>
void BucketLogger::log(spdlog::level::level_enum lvl, const char* msg) {
    if (!should_log(lvl)) {
        return;
    }
    logInner(lvl, msg, {});
}

template <typename T>
void BucketLogger::log(spdlog::level::level_enum lvl, const T& msg) {
    if (!should_log(lvl)) {
        return;
    }
    logInner(lvl, "{}", fmt::make_args_checked<T>("{}", msg));
}

template <typename... Args>
void BucketLogger::trace(const char* fmt, const Args&... args) {
    log(spdlog::level::trace, fmt, args...);
}

template <typename... Args>
void BucketLogger::debug(const char* fmt, const Args&... args) {
    log(spdlog::level::debug, fmt, args...);
}

template <typename... Args>
void BucketLogger::info(const char* fmt, const Args&... args) {
    log(spdlog::level::info, fmt, args...);
}

template <typename... Args>
void BucketLogger::warn(const char* fmt, const Args&... args) {
    log(spdlog::level::warn, fmt, args...);
}

template <typename... Args>
void BucketLogger::error(const char* fmt, const Args&... args) {
    log(spdlog::level::err, fmt, args...);
}

template <typename... Args>
void BucketLogger::critical(const char* fmt, const Args&... args) {
    log(spdlog::level::critical, fmt, args...);
}

template <typename T>
void BucketLogger::trace(const T& msg) {
    log(spdlog::level::trace, msg);
}

template <typename T>
void BucketLogger::debug(const T& msg) {
    log(spdlog::level::debug, msg);
}

template <typename T>
void BucketLogger::info(const T& msg) {
    log(spdlog::level::info, msg);
}

template <typename T>
void BucketLogger::warn(const T& msg) {
    log(spdlog::level::warn, msg);
}

template <typename T>
void BucketLogger::error(const T& msg) {
    log(spdlog::level::err, msg);
}

template <typename T>
void BucketLogger::critical(const T& msg) {
    log(spdlog::level::critical, msg);
}
