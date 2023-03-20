/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstdint>
#include <ostream>
#include <string>

enum class ResourceAllocationDomain : uint8_t {
    /// Allocations accounted for by the bucket
    Bucket,
    /// Allocation accounted for by the global pool
    Global,
    /// Allocations we don't want to track somewhere (privilege override for
    /// instance)
    None
};

std::string to_string(const ResourceAllocationDomain domain);
std::ostream& operator<<(std::ostream& os,
                         const ResourceAllocationDomain& domain);
