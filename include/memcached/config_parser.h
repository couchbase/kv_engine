/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2009 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */
#pragma once

#include <cstdint>
#include <cstdio>
#include <functional>
#include <string_view>

// Need ssize_t
#include <folly/portability/SysTypes.h>

namespace cb::config {

/// Internal functions made available in order to create unit tests
namespace internal {
/// trim (remove leading and trailing spaces and escaped characters) the
/// next "token" from src (and remove it from src). Stop at the provided
/// character
std::string next_field(std::string_view& src, char stop);
} // namespace internal

size_t value_as_size_t(const std::string& val);
ssize_t value_as_ssize_t(const std::string& val);
bool value_as_bool(const std::string& val);
float value_as_float(const std::string& val);

/// Tokenize the configuration string and call the provided callback
/// for each configuration key - value pair.
void tokenize(std::string_view str,
              std::function<void(std::string_view, std::string)> callback);

} // namespace cb::config
