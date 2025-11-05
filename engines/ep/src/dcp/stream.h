/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "cursor.h"
#include "dcp/dcp-types.h"

#include <mcbp/protocol/dcp_stream_end_status.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/engine_common.h>
#include <platform/json_log.h>
#include <spdlog/common.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <string>

namespace cb::mcbp {
enum class DcpAddStreamFlag : uint32_t;
}

class CheckpointCursor;
class CookieIface;
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
        None, // Used only at PassiveStream initialization
        Disk,
        Memory
    };

    Stream(std::string name,
           cb::mcbp::DcpAddStreamFlag flags,
           uint32_t opaque,
           Vbid vb,
           uint64_t start_seqno,
           uint64_t vb_uuid,
           uint64_t snap_start_seqno,
           uint64_t snap_end_seqno);

    virtual ~Stream();

    cb::mcbp::DcpAddStreamFlag getFlags() const {
        return flags_;
    }

    Vbid getVBucket() const {
        return vb_;
    }

    uint32_t getOpaque() const {
        return opaque_;
    }

    uint64_t getStartSeqno() const {
        std::lock_guard<std::mutex> lg(streamMutex);
        return start_seqno_;
    }

    uint64_t getSnapStartSeqno() const {
        std::lock_guard<std::mutex> lg(streamMutex);
        return snap_start_seqno_;
    }

    uint64_t getSnapEndSeqno() const {
        std::lock_guard<std::mutex> lg(streamMutex);
        return snap_end_seqno_;
    }

    virtual void addStats(const AddStatFn& add_stat, CookieIface& c);

    virtual void setDead(cb::mcbp::DcpStreamEndStatus status) = 0;

    const std::string& getName() const {
        return name_;
    }

    virtual const Cursor& getCursor() const {
        return noCursor;
    }

    virtual void setActive() {
        // Stream defaults to do nothing
    }

    /// @returns the name of this stream type - "Active", "Passive"
    virtual std::string getStreamTypeName() const = 0;

    /**
     * @returns the name of the state the stream is in - "active",
     * "backfilling" etc.
     */
    virtual std::string getStateName() const = 0;

    /// @returns true if state_ is not in the Dead state.
    virtual bool isActive() const = 0;

    virtual bool compareStreamId(cb::mcbp::DcpStreamId id) const {
        return id == cb::mcbp::DcpStreamId(0);
    }

    /**
     * Required method for OBJ_LOG macros.
     * @param level The level to check.
     * @return true if the stream should log the given level.
     */
    bool shouldLog(spdlog::level::level_enum level) const;

    /**
     * Required method for OBJ_LOG macros.
     * @param level The level to check.
     * @param msg The message to log.
     * @param ctx The context to log.
     */
    virtual void logWithContext(spdlog::level::level_enum level,
                                std::string_view msg,
                                cb::logger::Json ctx) const = 0;

    /**
     * Required method for OBJ_LOG macros.
     * @param level The level to check.
     * @param msg The message to log.
     */
    void log(spdlog::level::level_enum level, std::string_view msg) const {
        logWithContext(level, msg, cb::logger::Json::object());
    }

    size_t getReadyQueueMemory() const;

    bool areItemsReady() const {
        return itemsReady;
    }

protected:
    /* To be called after getting streamMutex lock */
    void pushToReadyQ(std::unique_ptr<DcpResponse> resp);

    /* To be called after getting streamMutex lock */
    std::unique_ptr<DcpResponse> popFromReadyQ();

    const std::string name_;
    const cb::mcbp::DcpAddStreamFlag flags_;
    const uint32_t opaque_;
    const Vbid vb_;
    uint64_t start_seqno_;
    uint64_t vb_uuid_;
    uint64_t snap_start_seqno_;
    uint64_t snap_end_seqno_;

    // Notification flag set to true by notifyStreamReady() (and next()) if
    // there are new item(s) waiting to be processed by this Stream. Cleared
    // once the Stream wants to be notified again about new seqnos.
    std::atomic<bool> itemsReady;
    mutable std::mutex streamMutex;

    /**
     * Ordered queue of DcpResponses to be sent on the stream.
     * Elements are added to this queue by reading from disk/memory etc, and
     * are removed when sending over the network to our peer.
     * The readyQ owns the elements in it.
     */
    std::queue<std::unique_ptr<DcpResponse>> readyQ;

    const static uint64_t dcpMaxSeqno;

    Cursor noCursor;

private:
    /* readyQueueMemory tracks the memory occupied by elements
     * in the readyQ.  It is an atomic because otherwise
       getReadyQueueMemory would need to acquire streamMutex.
     */
    std::atomic<size_t> readyQueueMemory;
};

const char* to_string(Stream::Snapshot type);
