/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include "settings.h"

// Helper macros to make it nicer to write log messages
#define LOGGER settings.extensions.logger->log

// Detail should be printed if verbose > 2
#define LOG_DETAIL(COOKIE, ...) \
    do { \
        if (settings.verbose > 2) { \
            LOGGER(EXTENSION_LOG_DETAIL, COOKIE, __VA_ARGS__); \
        } \
    } while (0)


// Debug should be printed if verbose > 1
#define LOG_DEBUG(COOKIE, ...) \
    do { \
        if (settings.verbose > 1) { \
            LOGGER(EXTENSION_LOG_DEBUG, COOKIE, __VA_ARGS__); \
        } \
    } while (0)


// Info should be printed if verbose > 0
#define LOG_INFO(COOKIE, ...) \
    do { \
        if (settings.verbose > 0) { \
            LOGGER(EXTENSION_LOG_INFO, COOKIE, __VA_ARGS__); \
        } \
    } while (0)

// Notice should always be printed
#define LOG_NOTICE(COOKIE, ...) \
    do { \
        LOGGER(EXTENSION_LOG_NOTICE, COOKIE, __VA_ARGS__); \
    } while (0)

// Warnings should always be printed
#define LOG_WARNING(COOKIE, ...) \
    do { \
        LOGGER(EXTENSION_LOG_WARNING, COOKIE, __VA_ARGS__); \
    } while (0)

/*
 * This macro records a fatal error to the log and
 * terminates memcached.
 * It calls exit() and therefore should only be used in
 * extreme cases because we want to keep memcached
 * available if at all possible.  Hence it should only be
 * used where memcached cannot make any sensible progress
 * or the possbility of data corruption arises.
 */

#define FATAL_ERROR(EXIT_STATUS, ...) \
    do { \
        LOGGER(EXTENSION_LOG_FATAL, NULL, __VA_ARGS__); \
        exit(EXIT_STATUS); \
    } while (0)
