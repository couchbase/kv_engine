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

#include "config.h"

#include "dcp/producer.h"
#include "dcp/response.h"
#include "dcp/stream.h"
#include "ep_engine.h"
#include "statwriter.h"
#include "vbucket.h"

#include <platform/checked_snprintf.h>

#include <memory>


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

const std::string to_string(Stream::Type type) {
    switch (type) {
    case Stream::Type::Active:
        return "Active";
    case Stream::Type::Notifier:
        return "Notifier";
    case Stream::Type::Passive:
        return "Passive";
    }
    throw std::logic_error("to_string(Stream::Type): called with invalid "
            "type:" + std::to_string(int(type)));
}

const uint64_t Stream::dcpMaxSeqno = std::numeric_limits<uint64_t>::max();

Stream::Stream(const std::string &name, uint32_t flags, uint32_t opaque,
               uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
               uint64_t vb_uuid, uint64_t snap_start_seqno,
               uint64_t snap_end_seqno, Type type)
    : name_(name),
      flags_(flags),
      opaque_(opaque),
      vb_(vb),
      start_seqno_(start_seqno),
      end_seqno_(end_seqno),
      vb_uuid_(vb_uuid),
      snap_start_seqno_(snap_start_seqno),
      snap_end_seqno_(snap_end_seqno),
      state_(StreamState::Pending),
      type_(type),
      itemsReady(false),
      readyQ_non_meta_items(0),
      readyQueueMemory(0) {
}

Stream::~Stream() {
    // NB: reusing the "unlocked" method without a lock because we're
    // destructing and should not take any locks.
    clear_UNLOCKED();
}

const std::string Stream::to_string(Stream::StreamState st) {
    switch(st) {
    case StreamState::Pending:
        return "pending";
    case StreamState::Backfilling:
        return "backfilling";
    case StreamState::InMemory:
        return "in-memory";
    case StreamState::TakeoverSend:
        return "takeover-send";
    case StreamState::TakeoverWait:
        return "takeover-wait";
    case StreamState::Reading:
        return "reading";
    case StreamState::Dead:
        return "dead";
    }
    throw std::invalid_argument(
        "Stream::to_string(StreamState): " + std::to_string(int(st)));
}

bool Stream::isTypeActive() const {
    return type_ == Type::Active;
}

bool Stream::isActive() const {
    return state_.load() != StreamState::Dead;
}

bool Stream::isBackfilling() const {
    return state_.load() == StreamState::Backfilling;
}

bool Stream::isInMemory() const {
    return state_.load() == StreamState::InMemory;
}

bool Stream::isPending() const {
    return state_.load() == StreamState::Pending;
}

bool Stream::isTakeoverSend() const {
    return state_.load() == StreamState::TakeoverSend;
}

bool Stream::isTakeoverWait() const {
    return state_.load() == StreamState::TakeoverWait;
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

std::unique_ptr<DcpResponse> Stream::popFromReadyQ(void) {
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
            LOG(EXTENSION_LOG_DEBUG, "readyQ size for stream %s (vb %d)"
                "underflow, likely wrong stat calculation! curr size: %" PRIu64
                "; new size: %d",
                name_.c_str(), getVBucket(),
                readyQueueMemory.load(std::memory_order_relaxed), respSize);
            readyQueueMemory.store(0, std::memory_order_relaxed);
        }

        return front;
    }

    return nullptr;
}

uint64_t Stream::getReadyQueueMemory() {
    return readyQueueMemory.load(std::memory_order_relaxed);
}

void Stream::addStats(ADD_STAT add_stat, const void *c) {
    try {
        const int bsize = 1024;
        char buffer[bsize];
        checked_snprintf(buffer, bsize, "%s:stream_%d_flags", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, flags_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_opaque", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, opaque_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_start_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, start_seqno_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_end_seqno", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, end_seqno_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_vb_uuid", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, vb_uuid_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_snap_start_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, snap_start_seqno_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_snap_end_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, snap_end_seqno_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_state", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, to_string(state_.load()), add_stat, c);
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "Stream::addStats: Failed to build stats: %s", error.what());
    }
}

void Stream::clear() {
    LockHolder lh(streamMutex);
    clear_UNLOCKED();
}

NotifierStream::NotifierStream(EventuallyPersistentEngine* e,
                               std::shared_ptr<DcpProducer> p,
                               const std::string& name,
                               uint32_t flags,
                               uint32_t opaque,
                               uint16_t vb,
                               uint64_t st_seqno,
                               uint64_t en_seqno,
                               uint64_t vb_uuid,
                               uint64_t snap_start_seqno,
                               uint64_t snap_end_seqno)
    : Stream(name,
             flags,
             opaque,
             vb,
             st_seqno,
             en_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno,
             Type::Notifier),
      producerPtr(p) {
    LockHolder lh(streamMutex);
    VBucketPtr vbucket = e->getVBucket(vb_);
    if (vbucket && static_cast<uint64_t>(vbucket->getHighSeqno()) > st_seqno) {
        pushToReadyQ(std::make_unique<StreamEndResponse>(
                opaque_, END_STREAM_OK, vb_));
        transitionState(StreamState::Dead);
        itemsReady.store(true);
    }

    p->getLogger().log(EXTENSION_LOG_NOTICE,
                       "(vb %d) stream created with start seqno %" PRIu64
                       " and end seqno %" PRIu64,
                       vb,
                       st_seqno,
                       en_seqno);
}

uint32_t NotifierStream::setDead(end_stream_status_t status) {
    std::unique_lock<std::mutex> lh(streamMutex);
    if (isActive()) {
        transitionState(StreamState::Dead);
        if (status != END_STREAM_DISCONNECTED) {
            pushToReadyQ(
                    std::make_unique<StreamEndResponse>(opaque_, status, vb_));
            lh.unlock();
            notifyStreamReady();
        }
    }
    return 0;
}

void NotifierStream::notifySeqnoAvailable(uint64_t seqno) {
    std::unique_lock<std::mutex> lh(streamMutex);
    if (isActive() && start_seqno_ < seqno) {
        pushToReadyQ(std::make_unique<StreamEndResponse>(
                opaque_, END_STREAM_OK, vb_));
        transitionState(StreamState::Dead);
        lh.unlock();
        notifyStreamReady();
    }
}

std::unique_ptr<DcpResponse> NotifierStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return nullptr;
    }

    auto& response = readyQ.front();
    auto producer = producerPtr.lock();
    if (producer && producer->bufferLogInsert(response->getMessageSize())) {
        return popFromReadyQ();
    }
    return nullptr;
}

void NotifierStream::transitionState(StreamState newState) {
    log(EXTENSION_LOG_INFO,
        "NotifierStream::transitionState: (vb %d) "
        "Transitioning from %s to %s",
        vb_,
        to_string(state_.load()).c_str(),
        to_string(newState).c_str());

    if (state_ == newState) {
        return;
    }

    bool validTransition = false;
    switch (state_.load()) {
        case StreamState::Pending:
            if (newState == StreamState::Dead) {
                validTransition = true;
            }
            break;

        case StreamState::Backfilling:
        case StreamState::InMemory:
        case StreamState::TakeoverSend:
        case StreamState::TakeoverWait:
        case StreamState::Reading:
        case StreamState::Dead:
            // No other state transitions are valid for a notifier stream.
            break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "NotifierStream::transitionState:"
                " newState (which is " +
                to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state_.load()) + ")");
    }
    state_ = newState;
}

void NotifierStream::addStats(ADD_STAT add_stat, const void* c) {
    Stream::addStats(add_stat, c);
}

void NotifierStream::log(EXTENSION_LOG_LEVEL severity,
                         const char* fmt,
                         ...) const {
    va_list va;
    va_start(va, fmt);
    auto producer = producerPtr.lock();
    if (producer) {
        producer->getLogger().vlog(severity, fmt, va);
    } else {
        static Logger defaultLogger =
                Logger("DCP (Notifier): **Deleted conn**");
        defaultLogger.vlog(severity, fmt, va);
    }
    va_end(va);
}

void NotifierStream::notifyStreamReady() {
    auto producer = producerPtr.lock();
    if (!producer) {
        return;
    }

    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        producer->notifyStreamReady(vb_);
    }
}
