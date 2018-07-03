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

#include "config.h"

#include "dcp/dcp-types.h"

#include <memcached/engine_common.h>
#include <memcached/extension.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_set>

class EventuallyPersistentEngine;
class MutationResponse;
class SetVBucketState;
class SnapshotMarker;
class DcpResponse;

enum backfill_source_t {
    BACKFILL_FROM_MEMORY,
    BACKFILL_FROM_DISK
};

class Stream {
public:

    enum class Type {
        Active,
        Notifier,
        Passive
    };

    enum class Snapshot {
           None,
           Disk,
           Memory
    };

    Stream(const std::string &name,
           uint32_t flags,
           uint32_t opaque,
           uint16_t vb,
           uint64_t start_seqno,
           uint64_t end_seqno,
           uint64_t vb_uuid,
           uint64_t snap_start_seqno,
           uint64_t snap_end_seqno,
           Type type);

    virtual ~Stream();

    uint32_t getFlags() { return flags_; }

    uint16_t getVBucket() { return vb_; }

    uint32_t getOpaque() { return opaque_; }

    uint64_t getStartSeqno() { return start_seqno_; }

    uint64_t getEndSeqno() { return end_seqno_; }

    uint64_t getVBucketUUID() { return vb_uuid_; }

    uint64_t getSnapStartSeqno() { return snap_start_seqno_; }

    uint64_t getSnapEndSeqno() { return snap_end_seqno_; }

    virtual void addStats(ADD_STAT add_stat, const void *c);

    virtual std::unique_ptr<DcpResponse> next() = 0;

    virtual uint32_t setDead(end_stream_status_t status) = 0;

    virtual void notifySeqnoAvailable(uint64_t seqno) {}

    const std::string& getName() {
        return name_;
    }

    virtual void setActive() {
        // Stream defaults to do nothing
    }

    Type getType() { return type_; }

    /// @returns true if the stream type is Active
    bool isTypeActive() const;

    /// @returns true if state_ is not Dead
    bool isActive() const;

    /// @Returns true if state_ is Backfilling
    bool isBackfilling() const;

    /// @Returns true if state_ is InMemory
    bool isInMemory() const;

    /// @Returns true if state_ is Pending
    bool isPending() const;

    /// @Returns true if state_ is TakeoverSend
    bool isTakeoverSend() const;

    /// @Returns true if state_ is TakeoverWait
    bool isTakeoverWait() const;

    void clear();

protected:

    // The StreamState is protected as it needs to be accessed by sub-classes
    enum class StreamState {
          Pending,
          Backfilling,
          InMemory,
          TakeoverSend,
          TakeoverWait,
          Reading,
          Dead
      };

    static const std::string to_string(Stream::StreamState type);

    StreamState getState() const { return state_; }

    void clear_UNLOCKED();

    /* To be called after getting streamMutex lock */
    void pushToReadyQ(std::unique_ptr<DcpResponse> resp);

    /* To be called after getting streamMutex lock */
    std::unique_ptr<DcpResponse> popFromReadyQ(void);

    uint64_t getReadyQueueMemory(void);

    /**
     * Uses the associated connection logger to log the message if the
     * connection is alive else uses a default logger
     *
     * @param severity Desired logging level
     * @param fmt Format string
     * @param ... Variable list of params as per the fmt
     */
    virtual void log(EXTENSION_LOG_LEVEL severity,
                     const char* fmt,
                     ...) const = 0;

    const std::string &name_;
    uint32_t flags_;
    uint32_t opaque_;
    uint16_t vb_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint64_t vb_uuid_;
    uint64_t snap_start_seqno_;
    uint64_t snap_end_seqno_;
    std::atomic<StreamState> state_;
    Type type_;

    std::atomic<bool> itemsReady;
    std::mutex streamMutex;

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

private:
    /* readyQueueMemory tracks the memory occupied by elements
     * in the readyQ.  It is an atomic because otherwise
       getReadyQueueMemory would need to acquire streamMutex.
     */
    std::atomic <uint64_t> readyQueueMemory;
};

const char* to_string(Stream::Snapshot type);
const std::string to_string(Stream::Type type);

class NotifierStream : public Stream {
public:
    NotifierStream(EventuallyPersistentEngine* e,
                   std::shared_ptr<DcpProducer> producer,
                   const std::string& name,
                   uint32_t flags,
                   uint32_t opaque,
                   uint16_t vb,
                   uint64_t start_seqno,
                   uint64_t end_seqno,
                   uint64_t vb_uuid,
                   uint64_t snap_start_seqno,
                   uint64_t snap_end_seqno);

    std::unique_ptr<DcpResponse> next() override;

    uint32_t setDead(end_stream_status_t status) override;

    void notifySeqnoAvailable(uint64_t seqno) override;

    void addStats(ADD_STAT add_stat, const void* c) override;

private:

    void transitionState(StreamState newState);

    void log(EXTENSION_LOG_LEVEL severity, const char* fmt, ...) const override;

    /**
     * Notifies the producer connection that the stream has items ready to be
     * pick up.
     */
    void notifyStreamReady();

    std::weak_ptr<DcpProducer> producerPtr;
};
