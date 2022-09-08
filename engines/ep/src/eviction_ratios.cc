/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "eviction_ratios.h"

#include "memcached/vbucket.h"

#include <folly/lang/Assume.h>

double EvictionRatios::getForState(vbucket_state_t state) const {
    switch (state) {
    case vbucket_state_replica:
        return replica;
    case vbucket_state_active:
    case vbucket_state_pending:
        return activeAndPending;
    case vbucket_state_dead:
        return 0;
    }
    folly::assume_unreachable();
}

void EvictionRatios::setForState(vbucket_state_t state, double value) {
    switch (state) {
    case vbucket_state_replica:
        replica = value;
        return;
    case vbucket_state_active:
    case vbucket_state_pending:
        activeAndPending = value;
        return;
    case vbucket_state_dead:
        // no-op
        return;
    }
    folly::assume_unreachable();
}