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

#include "string_utilities.h"

std::vector<std::string> split_string(const std::string& s,
                                      const std::string& delim,
                                      size_t limit) {
    std::vector<std::string> result;

    size_t n = 0;
    size_t m = 0;
    while (result.size() < limit || limit == 0) {
        m = s.find(delim, n);
        if (m == std::string::npos) {
            break;
        }

        result.emplace_back(s, n, m - n);
        n = m + delim.size();
    }
    result.emplace_back(s, n);

    return result;
}

// based on http://stackoverflow.com/a/4823686/3133303
std::string percent_decode(const std::string& s) {
    std::string ret;
    for (unsigned i = 0; i < s.length(); i++) {
        if (int(s[i]) == '%') {
            if (s.length() <= i + 2) {
                throw std::invalid_argument(
                        "percent_decode: string terminated before end of "
                        "percent encoded char");
            }

            unsigned int ii;
            if(sscanf(s.substr(i + 1, 2).c_str(), "%x", &ii) != 1) {
                throw std::invalid_argument("percent_decode: invalid percent "
                                            "encoded char ('"
                                            + s.substr(i + 1, 2) +
                                            "' did not decode as hexadecimal)");
            }
            ret += static_cast<char>(ii);
            i += 2;
        } else {
            ret += s[i];
        }
    }

    return ret;
}


std::pair<std::string, StrToStrMap> decode_query(const std::string& s) {

    std::pair<std::string, StrToStrMap> result;
    auto parts = split_string(s, "?", 1);

    // Get the part before the '?'
    result.first = parts.at(0);

    // Split up the query arguments (looks like "key=value&key2=value2")
    if (parts.size() == 2 && !parts.at(1).empty()) {
        for (const auto& str_pair : split_string(parts.at(1), "&")) {
            auto pair = split_string(str_pair, "=", 1);

            if (pair.size() != 2) {
                throw std::invalid_argument(
                        "decode_query(): Query pair '"
                        + str_pair + "' did not contain '='");
            } else if (pair[0].empty()) {
                throw std::invalid_argument(
                        "decode_query(): Query pair had empty argument name");
            } else {
                result.second.emplace(percent_decode(pair[0]),
                                      percent_decode(pair[1]));
            }
        }
    }
    return result;
}

std::string_view get_thread_pool_name(std::string_view threadname) {
    using namespace std::string_view_literals;
    std::string_view thread_pool;
    auto prefix_end = threadname.find_first_of("0123456789");
    if (prefix_end != threadname.npos) {
        // Clean up the thread pool name by removing any
        // uninteresting characters from the end of a non-empty
        // prefix. We have different naming conventions between
        // memcached, ep-engine and magma - some have no
        // separator between pool name and thread number, some
        // have ':' and others have '_'. However, we cannot
        // simply include ':' and '_' in the prefix end search
        // as they are also used in the middle of some thread
        // names. If the last character is one of the possible
        // separators then drop it.
        if (prefix_end > 0) {
            const auto last_prefix_char = threadname.at(prefix_end - 1);
            const auto skip_chars = ":_"sv;
            if (skip_chars.find(last_prefix_char) != skip_chars.npos) {
                --prefix_end;
            }
        }
        thread_pool = {threadname.data(), prefix_end};
    }
    return thread_pool;
}