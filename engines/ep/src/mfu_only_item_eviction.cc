/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Versisn 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mfu_only_item_eviction.h"

#include "eviction_ratios.h"
#include "memcached/vbucket.h"

#include <folly/lang/Assume.h>

MFUOnlyItemEviction::MFUOnlyItemEviction(Thresholds thresholds)
    : thresholds(thresholds) {
}

bool MFUOnlyItemEviction::shouldTryEvict(uint8_t freq,
                                         uint64_t /* age */,
                                         vbucket_state_t state) {
    switch (state) {
    case vbucket_state_active:
    case vbucket_state_pending:
        return freq <= thresholds.activePending;
    case vbucket_state_replica:
        return freq <= thresholds.replica;
    case vbucket_state_dead:
        // don't expect to visit dead vbuckets, but if we did
        // we don't want to evict anything (vb is going away soon anyway)
        return false;
    }
    folly::assume_unreachable();
}
