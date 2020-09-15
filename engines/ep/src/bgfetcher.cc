/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#include "bgfetcher.h"
#include "bucket_logger.h"
#include "cb3_executorthread.h"
#include "ep_engine.h"
#include "executorpool.h"
#include "kv_bucket.h"
#include "tasks.h"
#include "vbucket_bgfetch_item.h"
#include <phosphor/phosphor.h>
#include <algorithm>
#include <climits>
#include <vector>

BgFetcher::BgFetcher(KVBucket& s) : BgFetcher(s, s.getEPEngine().getEpStats()) {
}

BgFetcher::~BgFetcher() {
    LockHolder lh(queueMutex);
    if (!pendingVbs.empty()) {
        EP_LOG_DEBUG(
                "Terminating database reader without completing "
                "background fetches for {} vbuckets.",
                pendingVbs.size());
        pendingVbs.clear();
    }
}

void BgFetcher::start() {
    ExecutorPool* iom = ExecutorPool::get();
    auto task =
            std::make_shared<MultiBGFetcherTask>(&(store.getEPEngine()), this);
    this->setTaskId(task->getId());
    iom->schedule(task);
}

void BgFetcher::stop() {
    bool inverse = true;
    pendingFetch.compare_exchange_strong(inverse, false);
    ExecutorPool::get()->cancel(taskId);
}

void BgFetcher::addPendingVB(Vbid vbid) {
    { // Scope for queueMutex
        LockHolder lh(queueMutex);
        pendingVbs.insert(vbid);
    }
    ++stats.numRemainingBgItems;
    wakeUpTaskIfSnoozed();
}

void BgFetcher::wakeUpTaskIfSnoozed() {
    bool expected = false;
    if (pendingFetch.compare_exchange_strong(expected, true)) {
        ExecutorPool::get()->wake(taskId);
    }
}

size_t BgFetcher::doFetch(Vbid vbId, vb_bgfetch_queue_t& itemsToFetch) {
    TRACE_EVENT2("BgFetcher",
                 "doFetch",
                 "vbid",
                 vbId.get(),
                 "#itemsToFetch",
                 itemsToFetch.size());
    std::chrono::steady_clock::time_point startTime(
            std::chrono::steady_clock::now());
    EP_LOG_DEBUG(
            "BgFetcher is fetching data, {} numDocs:{} "
            "startTime:{}",
            vbId,
            itemsToFetch.size(),
            std::chrono::duration_cast<std::chrono::milliseconds>(
                    startTime.time_since_epoch())
                    .count());

    store.getROUnderlying(vbId)->getMulti(vbId, itemsToFetch);

    std::vector<bgfetched_item_t> fetchedItems;
    for (const auto& fetch : itemsToFetch) {
        auto& key = fetch.first;
        const vb_bgfetch_item_ctx_t& bg_item_ctx = fetch.second;

        for (const auto& itm : bg_item_ctx.bgfetched_list) {
            // We don't want to transfer ownership of itm here as we clean it
            // up at the end of this method in clearItems()
            fetchedItems.push_back(std::make_pair(key, itm.get()));
        }
    }

    if (!fetchedItems.empty()) {
        store.completeBGFetchMulti(vbId, fetchedItems, startTime);
        stats.getMultiHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - startTime),
                fetchedItems.size());
        stats.getMultiBatchSizeHisto.addValue(fetchedItems.size());
    }

    return fetchedItems.size();
}

bool BgFetcher::run(GlobalTask *task) {
    // Setup to snooze forever, and *then* clear the pending flag.
    // The ordering of these two statements is important - if we were
    // to clear the flag *before* snoozing, then we could have a Lost
    // Wakeup (and sleep forever) - consider the following scenario:
    //
    //     Time   Reader Thread                   Frontend Thread
    //            (notifyBGEvent)                 (BgFetcher::run)
    //
    //     1                                      pendingFetch = false
    //     2      if (pendingFetch == false)
    //     3          wake(task)
    //     4                                      snooze(INT_MAX)  /* BAD */
    //
    // By clearing pendingFlag after the snooze() we ensure the wake()
    // must happen after snooze().
    task->snooze(INT_MAX);
    pendingFetch.store(false);

    std::vector<Vbid> bg_vbs(pendingVbs.size());
    {
        LockHolder lh(queueMutex);
        bg_vbs.assign(pendingVbs.begin(), pendingVbs.end());
        pendingVbs.clear();
    }

    size_t num_fetched_items = 0;

    for (const auto vbId : bg_vbs) {
        VBucketPtr vb = store.getVBucket(vbId);
        if (vb) {
            // Requeue the bg fetch task if vbucket DB file is not created yet.
            if (vb->isBucketCreation()) {
                {
                    LockHolder lh(queueMutex);
                    pendingVbs.insert(vbId);
                }
                wakeUpTaskIfSnoozed();
                continue;
            }

            auto items = vb->getBGFetchItems();
            if (!items.empty()) {
                num_fetched_items += doFetch(vbId, items);
            }
        }
    }

    stats.numRemainingBgItems.fetch_sub(num_fetched_items);

    return true;
}
