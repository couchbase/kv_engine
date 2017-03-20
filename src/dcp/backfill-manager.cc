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

#include "connmap.h"
#include "ep_engine.h"

#include "dcp/backfill-manager.h"
#include "dcp/backfill_disk.h"
#include "dcp/dcpconnmap.h"
#include "dcp/producer.h"

#include <phosphor/phosphor.h>

static const size_t sleepTime = 1;

class BackfillManagerTask : public GlobalTask {
public:
    BackfillManagerTask(EventuallyPersistentEngine& e,
                        std::weak_ptr<BackfillManager> mgr,
                        double sleeptime = 0,
                        bool completeBeforeShutdown = false)
        : GlobalTask(&e,
                     TaskId::BackfillManagerTask,
                     sleeptime,
                     completeBeforeShutdown),
          weak_manager(mgr) {
    }

    bool run();

    cb::const_char_buffer getDescription();

private:
    // A weak pointer to the backfill manager which owns this
    // task. The manager is owned by the DcpProducer, but we need to
    // give the BackfillManagerTask access to the manager as it runs
    // concurrently in a different thread.
    // If the manager is deleted (by the DcpProducer) then the
    // ManagerTask simply cancels itself and stops running.
    std::weak_ptr<BackfillManager> weak_manager;
};

bool BackfillManagerTask::run() {
    TRACE_EVENT0("ep-engine/task", "BackFillManagerTask");
    // Create a new shared_ptr to the manager for the duration of this
    // execution.
    auto manager = weak_manager.lock();
    if (!manager) {
        // backfill manager no longer exists - cancel ourself and stop
        // running.
        cancel();
        return false;
    }

    backfill_status_t status = manager->backfill();
    if (status == backfill_finished) {
        return false;
    } else if (status == backfill_snooze) {
        snooze(sleepTime);
    }

    if (engine->getEpStats().isShutdown) {
        return false;
    }

    return true;
}

cb::const_char_buffer BackfillManagerTask::getDescription() {
    return "Backfilling items for a DCP Connection";
}

BackfillManager::BackfillManager(EventuallyPersistentEngine& e)
    : engine(e), managerTask(NULL) {
    Configuration& config = e.getConfiguration();

    scanBuffer.bytesRead = 0;
    scanBuffer.itemsRead = 0;
    scanBuffer.maxBytes = config.getDcpScanByteLimit();
    scanBuffer.maxItems = config.getDcpScanItemLimit();

    buffer.bytesRead = 0;
    buffer.maxBytes = config.getDcpBackfillByteLimit();
    buffer.nextReadSize = 0;
    buffer.full = false;
}

void BackfillManager::addStats(connection_t conn, ADD_STAT add_stat,
                               const void *c) {
    LockHolder lh(lock);
    conn->addStat("backfill_buffer_bytes_read", buffer.bytesRead, add_stat, c);
    conn->addStat("backfill_buffer_max_bytes", buffer.maxBytes, add_stat, c);
    conn->addStat("backfill_buffer_full", buffer.full, add_stat, c);
    conn->addStat("backfill_num_active", activeBackfills.size(), add_stat, c);
    conn->addStat("backfill_num_snoozing", snoozingBackfills.size(), add_stat, c);
    conn->addStat("backfill_num_pending", pendingBackfills.size(), add_stat, c);
}

BackfillManager::~BackfillManager() {
    if (managerTask) {
        managerTask->cancel();
        managerTask.reset();
    }

    while (!activeBackfills.empty()) {
        UniqueDCPBackfillPtr backfill = std::move(activeBackfills.front());
        activeBackfills.pop_front();
        backfill->cancel();
        engine.getDcpConnMap().decrNumActiveSnoozingBackfills();
    }

    while (!snoozingBackfills.empty()) {
        UniqueDCPBackfillPtr backfill =
                std::move((snoozingBackfills.front()).second);
        snoozingBackfills.pop_front();
        backfill->cancel();
        engine.getDcpConnMap().decrNumActiveSnoozingBackfills();
    }

    while (!pendingBackfills.empty()) {
        UniqueDCPBackfillPtr backfill = std::move(pendingBackfills.front());
        pendingBackfills.pop_front();
        backfill->cancel();
    }
}

void BackfillManager::schedule(VBucket& vb,
                               const active_stream_t& stream,
                               uint64_t start,
                               uint64_t end) {
    LockHolder lh(lock);
    UniqueDCPBackfillPtr backfill =
            vb.createDCPBackfill(engine, stream, start, end);
    if (engine.getDcpConnMap().canAddBackfillToActiveQ()) {
        activeBackfills.push_back(std::move(backfill));
    } else {
        pendingBackfills.push_back(std::move(backfill));
    }

    if (managerTask && !managerTask->isdead()) {
        ExecutorPool::get()->wake(managerTask->getId());
        return;
    }

    managerTask.reset(new BackfillManagerTask(engine, shared_from_this()));
    ExecutorPool::get()->schedule(managerTask, AUXIO_TASK_IDX);
}

bool BackfillManager::bytesRead(uint32_t bytes) {
    LockHolder lh(lock);
    if (scanBuffer.itemsRead >= scanBuffer.maxItems) {
        return false;
    }

    // Always allow an item to be backfilled if the scan buffer is empty,
    // otherwise check to see if there is room for the item.
    if (scanBuffer.bytesRead + bytes <= scanBuffer.maxBytes ||
        scanBuffer.bytesRead == 0) {
        scanBuffer.bytesRead += bytes;
    } else {
        /* Subsequent items for this backfill will be read in next run */
        return false;
    }

    if (buffer.bytesRead == 0 || buffer.bytesRead + bytes <= buffer.maxBytes) {
        buffer.bytesRead += bytes;
    } else {
        scanBuffer.bytesRead -= bytes;
        buffer.full = true;
        buffer.nextReadSize = bytes;
        return false;
    }

    scanBuffer.itemsRead++;

    return true;
}

void BackfillManager::bytesSent(uint32_t bytes) {
    LockHolder lh(lock);
    if (bytes > buffer.bytesRead) {
        throw std::invalid_argument("BackfillManager::bytesSent: bytes "
                "(which is" + std::to_string(bytes) + ") is greater than "
                "buffer.bytesRead (which is" + std::to_string(buffer.bytesRead) + ")");
    }
    buffer.bytesRead -= bytes;

    if (buffer.full) {
        uint32_t bufferSize = buffer.bytesRead;
        bool canFitNext = buffer.maxBytes - bufferSize >= buffer.nextReadSize;
        bool enoughCleared = bufferSize < (buffer.maxBytes * 3 / 4);
        if (canFitNext && enoughCleared) {
            buffer.nextReadSize = 0;
            buffer.full = false;
            if (managerTask) {
                ExecutorPool::get()->wake(managerTask->getId());
            }
        }
    }
}

backfill_status_t BackfillManager::backfill() {
    std::unique_lock<std::mutex> lh(lock);

    if (activeBackfills.empty() && snoozingBackfills.empty()
        && pendingBackfills.empty()) {
        managerTask.reset();
        return backfill_finished;
    }

    if (engine.getKVBucket()->isMemoryUsageTooHigh()) {
        LOG(EXTENSION_LOG_NOTICE, "DCP backfilling task temporarily suspended "
            "because the current memory usage is too high");
        return backfill_snooze;
    }

    moveToActiveQueue();

    if (activeBackfills.empty()) {
        return backfill_snooze;
    }

    if (buffer.full) {
        // If the buffer is full check to make sure we don't have any backfills
        // that no longer have active streams and remove them. This prevents an
        // issue where we have dead backfills taking up buffer space.
        std::list<UniqueDCPBackfillPtr> toDelete;
        for (auto a_itr = activeBackfills.begin();
             a_itr != activeBackfills.end();) {
            if ((*a_itr)->isStreamDead()) {
                (*a_itr)->cancel();
                toDelete.push_back(std::move(*a_itr));
                a_itr = activeBackfills.erase(a_itr);
                engine.getDcpConnMap().decrNumActiveSnoozingBackfills();
            } else {
                ++a_itr;
            }
        }

        lh.unlock();
        bool reschedule = !toDelete.empty();
        while (!toDelete.empty()) {
            UniqueDCPBackfillPtr backfill = std::move(toDelete.front());
            toDelete.pop_front();
        }
        return reschedule ? backfill_success : backfill_snooze;
    }

    UniqueDCPBackfillPtr backfill = std::move(activeBackfills.front());
    activeBackfills.pop_front();

    lh.unlock();
    backfill_status_t status = backfill->run();
    lh.lock();

    scanBuffer.bytesRead = 0;
    scanBuffer.itemsRead = 0;

    switch (status) {
        case backfill_success:
            activeBackfills.push_back(std::move(backfill));
            break;
        case backfill_finished:
            lh.unlock();
            engine.getDcpConnMap().decrNumActiveSnoozingBackfills();
            break;
        case backfill_snooze: {
            uint16_t vbid = backfill->getVBucketId();
            RCPtr<VBucket> vb = engine.getVBucket(vbid);
            if (vb) {
                snoozingBackfills.push_back(
                        std::make_pair(ep_current_time(), std::move(backfill)));
            } else {
                lh.unlock();
                LOG(EXTENSION_LOG_WARNING, "Deleting the backfill, as vbucket %d "
                    "seems to have been deleted!", vbid);
                backfill->cancel();
                engine.getDcpConnMap().decrNumActiveSnoozingBackfills();
            }
            break;
        }
    }

    return backfill_success;
}

void BackfillManager::moveToActiveQueue() {
    // Order in below AND is important
    while (!pendingBackfills.empty() &&
           engine.getDcpConnMap().canAddBackfillToActiveQ()) {
        activeBackfills.splice(activeBackfills.end(),
                               pendingBackfills,
                               pendingBackfills.begin());
    }

    while (!snoozingBackfills.empty()) {
        std::pair<rel_time_t, UniqueDCPBackfillPtr> snoozer =
                std::move(snoozingBackfills.front());
        snoozingBackfills.pop_front();
        // If snoozing task is found to be sleeping for greater than
        // allowed snoozetime, push into active queue
        if (snoozer.first + sleepTime <= ep_current_time()) {
            activeBackfills.push_back(std::move(snoozer.second));
        } else {
            // Push back the popped snoozing backfill
            snoozingBackfills.push_back(std::move(snoozer));
            break;
        }
    }
}

void BackfillManager::wakeUpTask() {
    LockHolder lh(lock);
    if (managerTask) {
        ExecutorPool::get()->wake(managerTask->getId());
    }
}
