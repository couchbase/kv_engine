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
    vbucket_state();

    vbucket_state(vbucket_state_t _state,
                  uint64_t _chkid,
                  uint64_t _maxDelSeqNum,
                  int64_t _highSeqno,
                  uint64_t _purgeSeqno,
                  uint64_t _lastSnapStart,
                  uint64_t _lastSnapEnd,
                  uint64_t _maxCas,
                  int64_t _hlcCasEpochSeqno,
                  bool _mightContainXattrs,
                  std::string _failovers,
                  bool _supportsNamespaces);

    std::string toJSON() const;

    bool needsToBePersisted(const vbucket_state& vbstate);

    void reset();

    vbucket_state_t state;
    uint64_t checkpointId;
    cb::uint48_t maxDeletedSeqno;
    int64_t highSeqno;
    uint64_t purgeSeqno;
    uint64_t lastSnapStart;
    uint64_t lastSnapEnd;
    uint64_t maxCas;
    int64_t hlcCasEpochSeqno;
    bool mightContainXattrs;
    std::string failovers;
    bool supportsNamespaces;
};
