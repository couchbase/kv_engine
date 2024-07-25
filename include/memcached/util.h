/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Danga Interactive
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
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
#include <charconv>
#include <string>

bool safe_strtoull(std::string_view s, uint64_t& out);

bool safe_strtoll(std::string_view s, int64_t& out);

bool safe_strtoul(std::string_view s, uint32_t& out);

bool safe_strtol(std::string_view s, int32_t& out);

bool safe_strtous(std::string_view s, uint16_t& out);

bool safe_strtof(const std::string& s, float& out);

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
BucketCompressionMode parseCompressionMode(std::string_view mode);
