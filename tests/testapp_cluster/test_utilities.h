/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/vbucket.h>

#include <chrono>
#include <string_view>
#include <vector>

class MemcachedConnection;

namespace cb::test {

/**
 * Poll snapshot-status on vbid until it reaches one of successStates or one
 * of failStates (which causes an immediate test failure).
 *
 * @param context included in failure messages to identify the call site
 * @param vbid the vbucket to query snapshot-status for
 * @param requestConnection connection used to issue the stats request
 * @param failStates states that cause an immediate test failure if observed
 * @param successStates states that indicate polling is complete
 * @param waitFor how long to poll before timing out (default: no waiting)
 */
void do_snapshot_status(std::string_view context,
                        Vbid vbid,
                        MemcachedConnection& requestConnection,
                        const std::vector<std::string_view>& failStates,
                        const std::vector<std::string_view>& successStates,
                        std::chrono::seconds waitFor = std::chrono::seconds(0));

} // namespace cb::test
