/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <unordered_map>
#include <string>
#include <vector>
#include <platform/sized_buffer.h>

/**
 * Splits a string
 *
 * @param s String to be split
 * @param delim Delimiter used to split 's'
 * @param limit Max number of times to split the string (into limit + 1 pieces),
 *              or 0 for unlimited times.
 * @return A vector of the pieces of the split string
 */
std::vector<std::string> split_string(const std::string& s,
                                      const std::string& delim,
                                      size_t limit = 0);

/**
 * Decodes a percent encoded string
 * @param s Percent encoded string
 * @return Decoded string
 */
std::string percent_decode(const std::string& s);

using StrToStrMap = std::unordered_map<std::string, std::string>;

/**
 * Decodes a query string
 *
 * Accepts a string like 'key?arg=val&arg2=val2' and turns it into a
 * structure like:
 *
 * ["key", {"arg": "val", "arg2": "val2"}]
 *
 * Supports percent encoding (like URLs) so an amphersand can be encoded
 * as '%26' ('%' character followed by the corresponding Hex ASCII value).
 * For example 'key?%25=%26' becomes:
 *
 * ["key", {"%": "&"}]
 *
 * @param s String to be decoded
 * @return A pair containing the string before the '?'
 *         and a map of the split up query arguments
 */
std::pair<std::string, StrToStrMap> decode_query(const std::string& s);

/**
 * Convert a const char pointer into a constant byte buffer
 *
 * @param s character pointer that needs to be converted
 * @return the constant byte buffer containing s as a byte
 *         array and its associated length
 */
cb::const_byte_buffer to_const_byte_buffer(const char* s);

/**
 * Convert a byte buffer into a string
 *
 * @param buf byte buffer that needs to be converted to a string
 * @return the string representation of the byte buffer
 */
std::string to_string(cb::byte_buffer buf);
