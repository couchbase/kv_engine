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

bool cb_stob(const std::string_view s) {
    using namespace std::string_view_literals;
    if (s == "true"sv) {
        return true;
    }
    if (s == "false"sv) {
        return false;
    }
    throw invalid_argument_bool("Argument was not `true` or `false`");
}
