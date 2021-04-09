/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/vbucket.h>
#include <spdlog/fmt/ostr.h>

std::ostream& operator<<(std::ostream& os, const Vbid& d) {
    return os << d.to_string();
}

std::string to_string(const Vbid& vbucket) {
    return vbucket.to_string();
}