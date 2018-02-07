/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
