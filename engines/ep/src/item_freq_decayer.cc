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

#include "item_freq_decayer.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "executorpool.h"
#include "item_freq_decayer_visitor.h"
#include "kv_bucket.h"
#include "stored-value.h"

#include <phosphor/phosphor.h>

#include <limits>

ItemFreqDecayerTask::ItemFreqDecayerTask(EventuallyPersistentEngine* e,
                                         uint16_t percentage_)
    : GlobalTask(e, TaskId::ItemFreqDecayerTask, 0, false),
      completed(false),
      epstore_position(engine->getKVBucket()->startPosition()),
      notified(false),
      percentage(percentage_) {
}

ItemFreqDecayerTask::~ItemFreqDecayerTask() = default;

bool ItemFreqDecayerTask::run() noexcept {
    TRACE_EVENT0("ep-engine/task", "ItemFreqDecayerTask");

    // Setup so that we will sleep before clearing notified.
    snooze(std::numeric_limits<int>::max());

    ++(engine->getEpStats().freqDecayerRuns);

    // Get our pause/resume visitor. If we didn't finish the previous pass,
    // then resume from where we last were, otherwise create a new visitor
    // starting from the beginning.
    if (!prAdapter) {
        prAdapter = std::make_unique<PauseResumeVBAdapter>(
                std::make_unique<ItemFreqDecayerVisitor>(percentage));
        epstore_position = engine->getKVBucket()->startPosition();
        completed = false;
    }

    // Print start status.
    if (globalBucketLogger->should_log(spdlog::level::debug)) {
        std::stringstream ss;
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (epstore_position == engine->getKVBucket()->startPosition()) {
            ss << " starting. ";
        } else {
            ss << " resuming from " << epstore_position << ", ";
            ss << prAdapter->getHashtablePosition() << ".";
        }
        ss << " Using chunk_duration=" << getChunkDuration().count() << " ms.";
        EP_LOG_DEBUG("{}", ss.str());
    }

    // Prepare the underlying visitor.
    auto& visitor = getItemFreqDecayerVisitor();
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + getChunkDuration();
    visitor.setDeadline(deadline);
    visitor.clearStats();

    // Do it - set off the visitor.
    epstore_position = engine->getKVBucket()->pauseResumeVisit(
            *prAdapter, epstore_position);
    const auto end = std::chrono::steady_clock::now();

    // Check if the visitor completed a full pass.
    completed = (epstore_position == engine->getKVBucket()->endPosition());

    // Print status.
    if (globalBucketLogger->should_log(spdlog::level::debug)) {
        std::stringstream ss;
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (completed) {
            ss << " finished.";
        } else {
            ss << " paused at position " << epstore_position << ".";
        }
        std::chrono::microseconds duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                      start);
        ss << " Took " << duration.count() << " us. to visit "
           << visitor.getVisitedCount() << " documents.";
        EP_LOG_DEBUG("{}", ss.str());
    }

    // Delete(reset) visitor and allow to be notified if it finished.
    if (completed) {
        prAdapter.reset();
        notified.store(false);
    } else {
        // We have not completed decaying all the items so wake the task back
        // up
        wakeUp();
    }

    if (engine->getEpStats().isShutdown) {
        return false;
    }

    return true;
}

void ItemFreqDecayerTask::stop() {
    if (uid) {
        ExecutorPool::get()->cancel(uid);
    }
}

void ItemFreqDecayerTask::wakeup() {
    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        ExecutorPool::get()->wake(getId());
    }
}

std::string ItemFreqDecayerTask::getDescription() {
    return "Item frequency count decayer task";
}

std::chrono::microseconds ItemFreqDecayerTask::maxExpectedDuration() {
    // ItemFreqDecayerTask processes items in chunks, with each chunk
    // constrained by a ChunkDuration runtime, so we expect to only take that
    // long.  However, the ProgressTracker used estimates the time remaining,
    // so apply some headroom to that figure so we don't get inundated with
    // spurious "slow tasks" which only just exceed the limit.
    return getChunkDuration() * 10;
}

std::chrono::milliseconds ItemFreqDecayerTask::getChunkDuration() const {
    return std::chrono::milliseconds(
            engine->getConfiguration().getItemFreqDecayerChunkDuration());
}

ItemFreqDecayerVisitor& ItemFreqDecayerTask::getItemFreqDecayerVisitor() {
    return dynamic_cast<ItemFreqDecayerVisitor&>(prAdapter->getHTVisitor());
}
