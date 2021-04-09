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

#include "string_utils.h"
#include <cstring>

bool cb_stob(const std::string& s) {
    if (s == "true") {
        return true;
    } else if (s == "false") {
        return false;
    } else {
        throw invalid_argument_bool("Argument was not `true` or `false`");
    }
}

bool cb_isPrefix(const std::string& input, const std::string& prefix) {
    return (input.compare(0, prefix.length(), prefix) == 0);
}

bool cb_isPrefix(std::string_view input, const std::string& prefix) {
    if (prefix.size() > input.size()) {
        return false;
    }

    return std::memcmp(input.data(), prefix.data(), prefix.size()) == 0;
}
