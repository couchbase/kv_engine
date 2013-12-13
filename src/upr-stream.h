/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_UPR_STREAM_H_
#define SRC_UPR_STREAM_H_ 1

#include "config.h"

typedef enum {
    STREAM_ACTIVE,
    STREAM_PENDING,
    STREAM_DEAD
} stream_state_t;

class Stream {
public:
    Stream(uint32_t flags, uint32_t opaque, uint16_t vb, uint64_t start_seqno,
           uint64_t end_seqno, uint64_t vb_uuid, uint64_t high_seqno,
           stream_state_t state) :
        flags_(flags), opaque_(opaque), vb_(vb),
        start_seqno_(start_seqno), end_seqno_(end_seqno), vb_uuid_(vb_uuid),
        high_seqno_(high_seqno), state_(state) {}

    ~Stream() {}

    uint32_t getFlags() { return flags_; }

    uint16_t getVBucket() { return vb_; }

    uint32_t getOpaque() { return opaque_; }

    uint64_t getStartSeqno() { return start_seqno_; }

    uint64_t getEndSeqno() { return end_seqno_; }

    uint64_t getVBucketUUID() { return vb_uuid_; }

    uint64_t getHighSeqno() { return high_seqno_; }

    stream_state_t getState() { return state_; }

    void setState(stream_state_t newState) { state_ = newState; }

private:
    uint32_t flags_;
    uint32_t opaque_;
    uint16_t vb_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint64_t vb_uuid_;
    uint64_t high_seqno_;
    stream_state_t state_;
};

#endif  // SRC_UPR_STREAM_H_