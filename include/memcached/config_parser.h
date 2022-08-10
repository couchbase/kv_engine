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

/**
 * The supported datatypes the config file parser can handle
 */
enum config_datatype { DT_SIZE, DT_SSIZE, DT_FLOAT, DT_BOOL, DT_STRING };

/**
 * I don't like casting, so let's create a union to keep all the values in
 */
union config_value {
    size_t* dt_size;
    ssize_t* dt_ssize;
    float* dt_float;
    bool* dt_bool;
    // Allocated via cb_malloc. Should be freed with cb_free().
    char** dt_string;
};

/**
 * An entry for a single item in the config file.
 */
struct config_item {
    /** The name of the key */
    const char* key;
    /** The datatype for the value */
    enum config_datatype datatype;
    /** Where to store the value from the config file */
    union config_value value;
    /** If the item was found in the config file or not */
    bool found;
};

/**
 * Parse the configuration argument and populate the values into the
 * config items.
 *
 * @param str the encoded configuration string
 * @param items the config items to look for
 * @param error stream to write error messages to
 * @return 0 if config successfully parsed
 *         1 if config successfully parsed, but unknown tokens found
 *        -1 if illegal values was found in the config
 */
int parse_config(const char* str, struct config_item items[], FILE* error);

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
