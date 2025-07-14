/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "dcp/stream.h"
#include "dcp/response.h"

#include <platform/checked_snprintf.h>

#include <engines/ep/src/bucket_logger.h>
#include <statistics/cbstat_collector.h>
#include <memory>
#include <utility>

const char* to_string(Stream::Snapshot type) {
    switch (type) {
    case Stream::Snapshot::None:
        return "none";
    case Stream::Snapshot::Disk:
        return "disk";
    case Stream::Snapshot::Memory:
        return "memory";
    }
    throw std::logic_error("to_string(Stream::Snapshot): called with invalid "
            "Snapshot type:" + std::to_string(int(type)));
}

const uint64_t Stream::dcpMaxSeqno = std::numeric_limits<uint64_t>::max();

Stream::Stream(std::string name,
               cb::mcbp::DcpAddStreamFlag flags,
               uint32_t opaque,
               Vbid vb,
               uint64_t start_seqno,
               uint64_t vb_uuid,
               uint64_t snap_start_seqno,
               uint64_t snap_end_seqno)
    : name_(std::move(name)),
      flags_(flags),
      opaque_(opaque),
      vb_(vb),
      start_seqno_(start_seqno),
      vb_uuid_(vb_uuid),
      snap_start_seqno_(snap_start_seqno),
      snap_end_seqno_(snap_end_seqno),
      itemsReady(false),
      readyQueueMemory(0) {
}

Stream::~Stream() = default;

void Stream::pushToReadyQ(std::unique_ptr<DcpResponse> resp) {
    /* expect streamMutex.ownsLock() == true */
    if (resp) {
        readyQueueMemory.fetch_add(resp->getMessageSize(),
                                   std::memory_order_relaxed);
        readyQ.push(std::move(resp));
    }
}

std::unique_ptr<DcpResponse> Stream::popFromReadyQ() {
    /* expect streamMutex.ownsLock() == true */
    if (!readyQ.empty()) {
        auto front = std::move(readyQ.front());
        readyQ.pop();

        // Decrement the readyQ size
        const auto respSize = front->getMessageSize();
        if (respSize <= readyQueueMemory.load(std::memory_order_relaxed)) {
            readyQueueMemory.fetch_sub(respSize, std::memory_order_relaxed);
        } else {
            EP_LOG_DEBUG_CTX(
                    "readyQ size underflow, likely wrong stat calculation",
                    {"dcp_name", name_.c_str()},
                    {"vb", getVBucket()},
                    {"current_size",
                     readyQueueMemory.load(std::memory_order_relaxed)},
                    {"new_size", respSize});
            readyQueueMemory.store(0, std::memory_order_relaxed);
        }

        return front;
    }

    return nullptr;
}

size_t Stream::getReadyQueueMemory() const {
    return readyQueueMemory.load(std::memory_order_relaxed);
}

void Stream::addStats(const AddStatFn& add_stat, CookieIface& c) {
    try {
        uint32_t flags{0};
        uint64_t opaque{0};
        uint64_t startSeqno{0};
        uint64_t vbUuid{0};
        uint64_t snapStartSeqno{0};
        uint64_t snapEndSeqno{0};
        std::string stateName;
        size_t readyQSize{0};

        {
            std::lock_guard<std::mutex> lh(streamMutex);
            flags = static_cast<uint32_t>(flags_);
            opaque = opaque_;
            startSeqno = start_seqno_;
            vbUuid = vb_uuid_;
            snapStartSeqno = snap_start_seqno_;
            snapEndSeqno = snap_end_seqno_;
            stateName = getStateName();
            readyQSize = readyQ.size();
        }

        add_casted_stat("flags", flags, add_stat, c);
        add_casted_stat("opaque", opaque, add_stat, c);
        add_casted_stat("start_seqno", startSeqno, add_stat, c);
        add_casted_stat("vb_uuid", vbUuid, add_stat, c);
        add_casted_stat("snap_start_seqno", snapStartSeqno, add_stat, c);
        add_casted_stat("snap_end_seqno", snapEndSeqno, add_stat, c);
        add_casted_stat("state", stateName, add_stat, c);
        add_casted_stat("items_ready", itemsReady.load(), add_stat, c);
        add_casted_stat("readyQ_items", readyQSize, add_stat, c);
    } catch (std::exception& error) {
        EP_LOG_WARN_CTX("Stream::addStats: Failed to build stats",
                        {"error", error.what()});
    }
}
