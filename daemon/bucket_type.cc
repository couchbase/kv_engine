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
#include <platform/dirutils.h>

#include <string_view>

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

BucketType parse_bucket_type(std::string_view type) {
    using namespace std::string_view_literals;

    if (type == "Memcached"sv) {
        return BucketType::Memcached;
    }

    if (type == "Couchbase"sv) {
        return BucketType::Couchbase;
    }

    if (type == "ClusterConfigOnly"sv) {
        return BucketType::ClusterConfigOnly;
    }

    if (type == "EWouldBlock"sv) {
        return BucketType::EWouldBlock;
    }

    if (type == "No Bucket"sv) {
        return BucketType::NoBucket;
    }
    return BucketType::Unknown;
}

BucketType module_to_bucket_type(const std::string& module) {
    std::string nm = cb::io::basename(module);
    if (nm == "nobucket.so") {
        return BucketType::NoBucket;
    }
    if (nm == "default_engine.so") {
        return BucketType::Memcached;
    }
    if (nm == "ep.so") {
        return BucketType::Couchbase;
    }
    if (nm == "ewouldblock_engine.so") {
        return BucketType::EWouldBlock;
    }
    return BucketType::Unknown;
}

std::string bucket_type_to_module(BucketType type) {
    switch (type) {
    case BucketType::ClusterConfigOnly:
    case BucketType::Unknown:
        break;
    case BucketType::NoBucket:
        return "nobucket.so";
    case BucketType::Memcached:
        return "default_engine.so";
    case BucketType::Couchbase:
        return "ep.so";
    case BucketType::EWouldBlock:
        return "ewouldblock_engine.so";
    }
    throw std::logic_error(
            "bucket_type_to_module: type does not have a module");
}
