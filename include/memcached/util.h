/*
 *     Copyright 2018 Couchbase, Inc
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

/*
 * Wrappers around strtoull/strtoll that are safer and easier to
 * use.  For tests and assumptions, see internal_tests.c.
 *
 * str   a NULL-terminated base decimal 10 unsigned integer
 * out   out parameter, if conversion succeeded
 *
 * returns true if conversion succeeded.
 */

#include <memcached/engine.h>
#include <memcached/mcd_util-visibility.h>
#include <platform/dynamic.h>
#include <string>

MCD_UTIL_PUBLIC_API
bool safe_strtoull(const char* str, uint64_t& out) CB_ATTR_NONNULL(1);

MCD_UTIL_PUBLIC_API
bool safe_strtoll(const char* str, int64_t& out) CB_ATTR_NONNULL(1);

MCD_UTIL_PUBLIC_API
bool safe_strtoul(const char* str, uint32_t& out) CB_ATTR_NONNULL(1);

MCD_UTIL_PUBLIC_API
bool safe_strtol(const char* str, int32_t& out) CB_ATTR_NONNULL(1);

MCD_UTIL_PUBLIC_API
bool safe_strtof(const char* str, float& out) CB_ATTR_NONNULL(1);

/**
 * Get a textual representation of the given bucket compression mode
 *
 * @throws std::invalid_argument if the mode isn't one of the legal values
 */
MCD_UTIL_PUBLIC_API
std::string to_string(BucketCompressionMode mode);

/**
 * Parse a textual representation of a bucket mode
 *
 * @throws std::invalid_argument if the mode isn't one of the legal values
 */
MCD_UTIL_PUBLIC_API
BucketCompressionMode parseCompressionMode(const std::string& mode);
