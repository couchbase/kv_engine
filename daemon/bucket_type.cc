/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/bucket_type.h>

std::string to_string(BucketType type) {
    switch (type) {
    case BucketType::Memcached:
        return "Memcached";
    case BucketType::Couchbase:
        return "Couchbase";
    case BucketType::ClusterConfigOnly:
        return "ClusterConfigOnly";
    case BucketType::EWouldBlock:
        return "EWouldBlock";
    case BucketType::NoBucket:
        return "No Bucket";
    case BucketType::Unknown:
        return "Unknown";
    }

    // We don't want to throw a new exception when we try to format
    // an error message with an invalid bucket type
    return "[to_string(BucketType) - Illegal type: " +
           std::to_string(int(type)) + "]";
}
