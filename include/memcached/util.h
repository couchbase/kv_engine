/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Danga Interactive
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
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
#include <platform/dynamic.h>
#include <string>

bool safe_strtoull(const char* str, uint64_t& out) CB_ATTR_NONNULL(1);

bool safe_strtoll(const char* str, int64_t& out) CB_ATTR_NONNULL(1);

bool safe_strtoul(const char* str, uint32_t& out) CB_ATTR_NONNULL(1);

bool safe_strtol(const char* str, int32_t& out) CB_ATTR_NONNULL(1);

bool safe_strtof(const char* str, float& out) CB_ATTR_NONNULL(1);

/**
 * Get a textual representation of the given bucket compression mode
 *
 * @throws std::invalid_argument if the mode isn't one of the legal values
 */
std::string to_string(BucketCompressionMode mode);

/**
 * Parse a textual representation of a bucket mode
 *
 * @throws std::invalid_argument if the mode isn't one of the legal values
 */
BucketCompressionMode parseCompressionMode(const std::string& mode);
