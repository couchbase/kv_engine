/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "config.h"

#include "vbucket.h"

class EphemeralVBucket : public VBucket {
public:
    EphemeralVBucket(id_type i,
                     vbucket_state_t newState,
                     EPStats& st,
                     CheckpointConfig& chkConfig,
                     KVShard* kvshard,
                     int64_t lastSeqno,
                     uint64_t lastSnapStart,
                     uint64_t lastSnapEnd,
                     std::unique_ptr<FailoverTable> table,
                     std::shared_ptr<Callback<id_type> > cb,
                     NewSeqnoCallback newSeqnoCb,
                     Configuration& config,
                     item_eviction_policy_t evictionPolicy,
                     vbucket_state_t initState = vbucket_state_dead,
                     uint64_t purgeSeqno = 0,
                     uint64_t maxCas = 0);
};
