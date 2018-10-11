/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "notifier_stream.h"

#include "bucket_logger.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "ep_engine.h"
#include "locks.h"
#include "vbucket.h"

const std::string notifierStreamLoggingPrefix =
        "DCP (Notifier): **Deleted conn**";

NotifierStream::NotifierStream(EventuallyPersistentEngine* e,
                               std::shared_ptr<DcpProducer> p,
                               const std::string& name,
                               uint32_t flags,
                               uint32_t opaque,
                               Vbid vb,
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
                opaque_, END_STREAM_OK, vb_, DcpStreamId{}));
        transitionState(StreamState::Dead);
        itemsReady.store(true);
    }
    p->getLogger().log(spdlog::level::level_enum::info,
                       "({}) stream created with start seqno {} and "
                       "end seqno {}",
                       vb,
                       st_seqno,
                       en_seqno);
}

uint32_t NotifierStream::setDead(end_stream_status_t status) {
    std::unique_lock<std::mutex> lh(streamMutex);
    if (isActive()) {
        transitionState(StreamState::Dead);
        if (status != END_STREAM_DISCONNECTED) {
            pushToReadyQ(std::make_unique<StreamEndResponse>(
                    opaque_, status, vb_, DcpStreamId{}));
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
                opaque_, END_STREAM_OK, vb_, DcpStreamId{}));
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
    log(spdlog::level::level_enum::debug,
        "NotifierStream::transitionState: ({}) Transitioning from {} to {}",
        vb_,
        to_string(state_.load()),
        to_string(newState));

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
                "NotifierStream::transitionState: newState (which is " +
                to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state_.load()) + ")");
    }
    state_ = newState;
}

void NotifierStream::addStats(ADD_STAT add_stat, const void* c) {
    Stream::addStats(add_stat, c);
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

template <typename... Args>
void NotifierStream::log(spdlog::level::level_enum severity,
                         const char* fmt,
                         Args... args) const {
    auto producer = producerPtr.lock();
    if (producer) {
        producer->getLogger().log(severity, fmt, args...);
    } else {
        if (globalBucketLogger->should_log(severity)) {
            globalBucketLogger->log(
                    severity,
                    std::string{notifierStreamLoggingPrefix}.append(fmt).data(),
                    args...);
        }
    }
}
