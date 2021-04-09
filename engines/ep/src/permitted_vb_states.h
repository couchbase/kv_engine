/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/vbucket.h>
#include <platform/bitset.h>

struct PermittedVBStatesMap {
    size_t map(vbucket_state_t in) {
        return in - 1;
    }
};

using PermittedVBStates = cb::bitset<4, vbucket_state_t, PermittedVBStatesMap>;

/**
 * Set of vbucket_state_t's that are considered alive
 */
const PermittedVBStates aliveVBStates = PermittedVBStates{
        vbucket_state_active, vbucket_state_replica, vbucket_state_pending};
