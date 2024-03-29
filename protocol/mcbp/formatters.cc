/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <fmt/format.h>
#include <mcbp/protocol/json_utilities.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>

template <typename T>
std::string flag2string(T flag) {
    nlohmann::json value = flag;
    return value.dump();
}

namespace cb::mcbp {
std::string format_as(DcpOpenFlag flag) {
    return flag2string(flag);
}

std::string format_as(DcpAddStreamFlag flag) {
    return flag2string(flag);
}

namespace request {
std::string format_as(DcpSnapshotMarkerFlag flag) {
    return flag2string(flag);
}
} // namespace request

namespace subdoc {
std::string format_as(PathFlag flag) {
    return flag2string(flag);
}

std::string format_as(DocFlag flag) {
    return flag2string(flag);
}
} // namespace subdoc
} // namespace cb::mcbp
