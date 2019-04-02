/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include "ep_types.h"

#include <memcached/vbucket.h>
#include <platform/n_byte_integer.h>
#include <cstdint>
#include <string>

/**
 * Describes the detailed state of a VBucket, including it's high-level 'state'
 * (active, replica, etc), and the various seqnos and other properties it has.
 *
 * This is persisted to disk during flush.
 */
struct vbucket_state {
    std::string toJSON() const;

    bool needsToBePersisted(const vbucket_state& vbstate);

    void reset();

    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    cb::uint48_t maxDeletedSeqno = 0;
    int64_t highSeqno = 0;
    uint64_t purgeSeqno = 0;
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;
    uint64_t maxCas = 0;
    int64_t hlcCasEpochSeqno = HlcCasSeqnoUninitialised;
    bool mightContainXattrs = false;
    std::string failovers = "";
    bool supportsNamespaces = true;
};
