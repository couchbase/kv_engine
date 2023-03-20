/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "resource_allocation_domain.h"
#include <fmt/format.h>

std::string to_string(const ResourceAllocationDomain domain) {
    switch (domain) {
    case ResourceAllocationDomain::Bucket:
        return "Bucket";
    case ResourceAllocationDomain::Global:
        return "Global";
    case ResourceAllocationDomain::None:
        return "None";
    }
    return fmt::format("Unknown resource allocation domain: {}", int(domain));
}

std::ostream& operator<<(std::ostream& os,
                         const ResourceAllocationDomain& domain) {
    os << to_string(domain);
    return os;
}
