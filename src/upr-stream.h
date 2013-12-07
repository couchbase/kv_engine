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

#include <queue>

class EventuallyPersistentEngine;
class UprResponse;

typedef enum {
    STREAM_PENDING,
    STREAM_BACKFILLING,
    STREAM_IN_MEMORY,
    STREAM_READING,
    STREAM_DEAD
} stream_state_t;

typedef enum {
    END_STREAM_OK,
    END_STREAM_CLOSED,
    END_STREAM_STATE
} end_stream_status_t;

class Stream {
public:
    Stream(std::string &name, uint32_t flags, uint32_t opaque, uint16_t vb,
           uint64_t start_seqno, uint64_t end_seqno, uint64_t vb_uuid,
           uint64_t high_seqno) :
        name_(name), flags_(flags), opaque_(opaque), vb_(vb),
        start_seqno_(start_seqno), end_seqno_(end_seqno), vb_uuid_(vb_uuid),
        high_seqno_(high_seqno), state_(STREAM_PENDING) {}

    virtual ~Stream() {}

    uint32_t getFlags() { return flags_; }

    uint16_t getVBucket() { return vb_; }

    uint32_t getOpaque() { return opaque_; }

    uint64_t getStartSeqno() { return start_seqno_; }

    uint64_t getEndSeqno() { return end_seqno_; }

    uint64_t getVBucketUUID() { return vb_uuid_; }

    uint64_t getHighSeqno() { return high_seqno_; }

    stream_state_t getState() { return state_; }

    void setState(stream_state_t newState) { state_ = newState; }

    void addStats(ADD_STAT add_stat, const void *c);

    bool isActive() {
        return state_ != STREAM_DEAD && state_ != STREAM_PENDING;
    }

    virtual void setActive() {
        state_ = STREAM_READING;
    }

protected:

    const char* stateName(stream_state_t st) const;

    std::string &name_;
    uint32_t flags_;
    uint32_t opaque_;
    uint16_t vb_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint64_t vb_uuid_;
    uint64_t high_seqno_;
    stream_state_t state_;
};

class ActiveStream : public Stream {
public:
    ActiveStream(EventuallyPersistentEngine* e, std::string &name,
                 uint32_t flags, uint32_t opaque, uint16_t vb,
                 uint64_t st_seqno, uint64_t en_seqno, uint64_t vb_uuid,
                 uint64_t hi_seqno);

    ~ActiveStream() {
        LockHolder lh(streamMutex);
        transitionState(STREAM_DEAD);
        clear_UNLOCKED();
    }

    UprResponse* next();

    void setActive() {
        LockHolder lh(streamMutex);
        if (state_ == STREAM_PENDING) {
            transitionState(STREAM_BACKFILLING);
        }
    }

    void clear() {
        LockHolder lh(streamMutex);
        clear_UNLOCKED();
    }

    void incrBackfillRemaining(size_t by) {
        backfillRemaining += by;
    }

    void backfillReceived(Item* itm);

    void completeBackfill();

private:

    void transitionState(stream_state_t newState);

    UprResponse* backfillPhase();

    UprResponse* inMemoryPhase();

    UprResponse* deadPhase();

    void endStream(end_stream_status_t reason);

    void clear_UNLOCKED();

    //! The last sequence number queued from disk or memory
    uint64_t lastReadSeqno;
    //! The last sequence number sent to the network layer
    uint64_t lastSentSeqno;
    //! The last known seqno pointed to by the checkpoint cursor
    uint64_t curChkSeqno;
    size_t backfillRemaining;

    Mutex streamMutex;
    EventuallyPersistentEngine* engine;
    std::queue<UprResponse*> readyQ;
    bool isBackfillTaskRunning;
};

#endif  // SRC_UPR_STREAM_H_