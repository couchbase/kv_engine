/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/sized_buffer.h>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

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

template <typename T>
std::ostream& operator<<(std::ostream& ostr, const std::optional<T>& optional) {
    if (optional) {
        return ostr << *optional;
    }

    return ostr << "<none>";
}

/**
 * Get the name of "our" thread pools from the name of a thread. Inside
 * our system we use different schemes for our naming (some use: name:number,
 * other use name_number etc). This method tries to pick out the "name" field
 * from our known mappings, or an empty view if it doesn't look like one of
 * our schemes.
 *
 * @param threadname the name of the thread
 * @return the part of the name which represents the thread pool
 */
std::string_view get_thread_pool_name(std::string_view threadname);

/**
 * Base64 encode the provided value with a prefix and value
 *
 * @param view the data to encode
 * @return a std::string with the following syntax:
 *           'cb-content-base64-encoded:value'
 */
std::string base64_encode_value(std::string_view view);

/**
 * Base64 decode the provided value with a prefix and value
 *
 * @param view the data to decode
 * @return a std::string with the following syntax:
 *           'cb-content-base64-encoded:value'
 * @throws std::invalid_argument if the data doesn't contain the correct
 *            format
 */
std::string base64_decode_value(std::string_view view);
