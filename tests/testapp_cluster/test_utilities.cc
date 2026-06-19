/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "test_utilities.h"

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <protocol/connection/client_connection.h>

#include <fmt/format.h>
#include <thread>

namespace cb::test {

void do_snapshot_status(std::string_view context,
                        Vbid vbid,
                        MemcachedConnection& requestConnection,
                        const std::vector<std::string_view>& failStates,
                        const std::vector<std::string_view>& successStates,
                        std::chrono::seconds waitFor) {
    bool done = false;
    const auto timeout = std::chrono::steady_clock::now() + waitFor;
    std::string value = "uninitialised";
    do {
        std::string key;
        requestConnection.stats(
                [&key, &value](auto k, auto v) {
                    key = k;
                    value = v;
                },
                fmt::format("snapshot-status {}", vbid.get()));

        ASSERT_EQ(key, fmt::format("vb_{}:status", vbid.get()));
        if (std::ranges::find(failStates, value) != failStates.end()) {
            ASSERT_FALSE(true)
                    << "called from " << context
                    << " snapshot-status returned an unexpected state " << key
                    << " " << value << " while waiting for "
                    << nlohmann::json(successStates).dump()
                    << " snapshot-details: "
                    << requestConnection.stats("snapshot-details").dump();
        }

        if (std::ranges::find(successStates, value) != successStates.end()) {
            done = true;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    } while (!done && std::chrono::steady_clock::now() < timeout);
    ASSERT_TRUE(done) << "called from " << context
                      << ". Timeout waiting for snapshot-status to reach a "
                         "desired state. waited for: "
                      << waitFor.count()
                      << "s with last observed state: " << value;
}

} // namespace cb::test
