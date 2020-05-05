/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
#include "dcp/stream.h"
#include "dcp/response.h"
#include "statistics/collector.h"
#include "vbucket.h"

#include <platform/checked_snprintf.h>

#include <engines/ep/src/bucket_logger.h>
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
               uint32_t flags,
               uint32_t opaque,
               Vbid vb,
               uint64_t start_seqno,
               uint64_t end_seqno,
               uint64_t vb_uuid,
               uint64_t snap_start_seqno,
               uint64_t snap_end_seqno)
    : name_(std::move(name)),
      flags_(flags),
      opaque_(opaque),
      vb_(vb),
      start_seqno_(start_seqno),
      end_seqno_(end_seqno),
      vb_uuid_(vb_uuid),
      snap_start_seqno_(snap_start_seqno),
      snap_end_seqno_(snap_end_seqno),
      itemsReady(false),
      readyQ_non_meta_items(0),
      readyQueueMemory(0) {
}

Stream::~Stream() {
    // NB: reusing the "unlocked" method without a lock because we're
    // destructing and should not take any locks.
    clear_UNLOCKED();
}

void Stream::clear_UNLOCKED() {
    while (!readyQ.empty()) {
        popFromReadyQ();
    }
}

void Stream::pushToReadyQ(std::unique_ptr<DcpResponse> resp) {
    /* expect streamMutex.ownsLock() == true */
    if (resp) {
        if (!resp->isMetaEvent()) {
            readyQ_non_meta_items++;
        }
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

        if (!front->isMetaEvent()) {
            readyQ_non_meta_items--;
        }
        const uint32_t respSize = front->getMessageSize();

        /* Decrement the readyQ size */
        if (respSize <= readyQueueMemory.load(std::memory_order_relaxed)) {
            readyQueueMemory.fetch_sub(respSize, std::memory_order_relaxed);
        } else {
            EP_LOG_DEBUG(
                    "readyQ size for stream {} ({}) underflow, likely wrong "
                    "stat calculation! curr size: {}; new size: {}",
                    name_.c_str(),
                    getVBucket(),
                    readyQueueMemory.load(std::memory_order_relaxed),
                    respSize);
            readyQueueMemory.store(0, std::memory_order_relaxed);
        }

        return front;
    }

    return nullptr;
}

uint64_t Stream::getReadyQueueMemory() {
    return readyQueueMemory.load(std::memory_order_relaxed);
}

void Stream::addStats(const AddStatFn& add_stat, const void* c) {
    try {
        const int bsize = 1024;
        char buffer[bsize];
        checked_snprintf(
                buffer, bsize, "%s:stream_%d_flags", name_.c_str(), vb_.get());
        add_casted_stat(buffer, flags_, add_stat, c);
        checked_snprintf(
                buffer, bsize, "%s:stream_%d_opaque", name_.c_str(), vb_.get());
        add_casted_stat(buffer, opaque_, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_start_seqno",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, start_seqno_, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_end_seqno",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, end_seqno_, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_vb_uuid",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, vb_uuid_, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_snap_start_seqno",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, snap_start_seqno_, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_snap_end_seqno",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, snap_end_seqno_, add_stat, c);

        checked_snprintf(
                buffer, bsize, "%s:stream_%d_state", name_.c_str(), vb_.get());
        add_casted_stat(buffer, getStateName(), add_stat, c);

        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_items_ready",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, itemsReady.load(), add_stat, c);

        size_t readyQsize;
        {
            std::lock_guard<std::mutex> lh(streamMutex);
            readyQsize = readyQ.size();
        }
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_readyQ_items",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, readyQsize, add_stat, c);
    } catch (std::exception& error) {
        EP_LOG_WARN("Stream::addStats: Failed to build stats: {}",
                    error.what());
    }
}

void Stream::clear() {
    LockHolder lh(streamMutex);
    clear_UNLOCKED();
}
