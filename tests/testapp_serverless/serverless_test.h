/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cstddef>
#include <memory>

namespace cb::test {
/// Reduce the maximum number of connections per bucket to 16 to avoid
/// having to create 500 connections to test the limit
constexpr size_t MaxConnectionsPerBucket = 16;

/// Forward decl
class Cluster;

/// The cluster to use for testing. It contains 5 buckets named
/// [bucket-0, bucket-4] and there is 5 users available where the username
/// and password equals the name of the bucket they have access to.
extern std::unique_ptr<cb::test::Cluster> cluster;
} // namespace cb::test
