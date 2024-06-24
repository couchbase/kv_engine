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

template <typename BasicJsonType>
void to_json(BasicJsonType& j, BucketType type) {
    j = to_string(type);
}

BucketType parse_bucket_type(std::string_view type);

/**
 * Convert from a module name to a bucket type
 *
 * @param module The engine's shared object name, e.g. BucketType::Couchstore is
 *               ep.so. The input will be processed by basename, e.g.
 *               /path/to/ep.so would be valid.
 * @return The BucketType for the given module, or BucketType::Unknown for
 *         invalid input.
 */
BucketType module_to_bucket_type(const std::string& module);

/// Get the module name for the given bucket type
std::string bucket_type_to_module(BucketType type);
