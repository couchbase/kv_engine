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

#include <logger/logger.h>
#include <cstdlib>

/*
 * This macro records a fatal error to the log and
 * terminates memcached.
 * It calls exit() and therefore should only be used in
 * extreme cases because we want to keep memcached
 * available if at all possible.  Hence it should only be
 * used where memcached cannot make any sensible progress
 * or the possbility of data corruption arises.
 */

#define FATAL_ERROR(EXIT_STATUS, ...)             \
    do {                                          \
        cb::logger::get()->critical(__VA_ARGS__); \
        cb::logger::get()->flush();               \
        exit(EXIT_STATUS);                        \
    } while (false)
