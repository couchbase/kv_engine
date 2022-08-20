/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstdint>
#include <string>

enum class BucketType : uint8_t {
    Unknown,
    NoBucket,
    Memcached,
    Couchbase,
    ClusterConfigOnly,
    EWouldBlock
};

std::string to_string(BucketType type);
BucketType parse_bucket_type(std::string_view type);
