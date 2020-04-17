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

#pragma once

#include "cursor.h"
#include "dcp/dcp-types.h"

#include <memcached/dcp_stream_id.h>
#include <memcached/engine_common.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <string>

class CheckpointCursor;
class DcpResponse;
class EventuallyPersistentEngine;
class MutationResponse;
class SetVBucketState;
class SnapshotMarker;

enum backfill_source_t {
    BACKFILL_FROM_MEMORY,
    BACKFILL_FROM_DISK
};

class Stream {
public:
    enum class Snapshot {
           None,
           Disk,
           Memory
    };

    Stream(std::string name,
           uint32_t flags,
           uint32_t opaque,
           Vbid vb,
           uint64_t start_seqno,
           uint64_t end_seqno,
           uint64_t vb_uuid,
           uint64_t snap_start_seqno,
           uint64_t snap_end_seqno);

    virtual ~Stream();

    uint32_t getFlags() { return flags_; }

    Vbid getVBucket() {
        return vb_;
    }

    uint32_t getOpaque() { return opaque_; }

    uint64_t getStartSeqno() { return start_seqno_; }

    uint64_t getEndSeqno() { return end_seqno_; }

    uint64_t getVBucketUUID() { return vb_uuid_; }

    uint64_t getSnapStartSeqno() { return snap_start_seqno_; }

    uint64_t getSnapEndSeqno() { return snap_end_seqno_; }

    virtual void addStats(const AddStatFn& add_stat, const void* c);

    virtual std::unique_ptr<DcpResponse> next() = 0;

    virtual uint32_t setDead(end_stream_status_t status) = 0;

    virtual void notifySeqnoAvailable(uint64_t seqno) {}

    const std::string& getName() {
        return name_;
    }

    virtual const Cursor& getCursor() const {
        return noCursor;
    }

    virtual void setActive() {
        // Stream defaults to do nothing
    }

    /// @returns the name of this stream type - "Active", "Passive", "Notifier"
    virtual std::string getStreamTypeName() const = 0;

    /**
     * @returns the name of the state the stream is in - "active",
     * "backfilling" etc.
     */
    virtual std::string getStateName() const = 0;

    /// @returns true if state_ is not in the Dead state.
    virtual bool isActive() const = 0;

    void clear();

    virtual bool compareStreamId(cb::mcbp::DcpStreamId id) const {
        return id == cb::mcbp::DcpStreamId(0);
    }

    virtual void closeIfRequiredPrivilegesLost(const void* cookie) = 0;

protected:
    void clear_UNLOCKED();

    /* To be called after getting streamMutex lock */
    void pushToReadyQ(std::unique_ptr<DcpResponse> resp);

    /* To be called after getting streamMutex lock */
    std::unique_ptr<DcpResponse> popFromReadyQ();

    uint64_t getReadyQueueMemory();

    std::string name_;
    const uint32_t flags_;
    const uint32_t opaque_;
    const Vbid vb_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint64_t vb_uuid_;
    uint64_t snap_start_seqno_;
    uint64_t snap_end_seqno_;

    std::atomic<bool> itemsReady;
    mutable std::mutex streamMutex;

    /**
     * Ordered queue of DcpResponses to be sent on the stream.
     * Elements are added to this queue by reading from disk/memory etc, and
     * are removed when sending over the network to our peer.
     * The readyQ owns the elements in it.
     */
    std::queue<std::unique_ptr<DcpResponse>> readyQ;

    // Number of items in the readyQ that are not meta items. Used for
    // calculating getItemsRemaining(). Atomic so it can be safely read by
    // getItemsRemaining() without acquiring streamMutex.
    std::atomic<size_t> readyQ_non_meta_items;

    const static uint64_t dcpMaxSeqno;

    Cursor noCursor;

private:
    /* readyQueueMemory tracks the memory occupied by elements
     * in the readyQ.  It is an atomic because otherwise
       getReadyQueueMemory would need to acquire streamMutex.
     */
    std::atomic <uint64_t> readyQueueMemory;
};

const char* to_string(Stream::Snapshot type);
