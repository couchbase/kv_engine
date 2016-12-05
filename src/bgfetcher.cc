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

#include "config.h"

#include <algorithm>
#include <vector>

#include "bgfetcher.h"
#include "ep.h"
#include "kvshard.h"
#include "executorthread.h"

const double BgFetcher::sleepInterval = MIN_SLEEP_TIME;

void BgFetcher::start() {
    bool inverse = false;
    pendingFetch.compare_exchange_strong(inverse, true);
    ExecutorPool* iom = ExecutorPool::get();
    ExTask task = new MultiBGFetcherTask(&(store->getEPEngine()), this, false);
    this->setTaskId(task->getId());
    iom->schedule(task, READER_TASK_IDX);
}

void BgFetcher::stop() {
    bool inverse = true;
    pendingFetch.compare_exchange_strong(inverse, false);
    ExecutorPool::get()->cancel(taskId);
}

void BgFetcher::notifyBGEvent(void) {
    ++stats.numRemainingBgJobs;
    bool inverse = false;
    if (pendingFetch.compare_exchange_strong(inverse, true)) {
        ExecutorPool::get()->wake(taskId);
    }
}

size_t BgFetcher::doFetch(VBucket::id_type vbId,
                          vb_bgfetch_queue_t& itemsToFetch) {
    hrtime_t startTime(gethrtime());
    LOG(EXTENSION_LOG_DEBUG, "BgFetcher is fetching data, vBucket = %d "
        "numDocs = %" PRIu64 ", startTime = %" PRIu64,
        vbId, uint64_t(itemsToFetch.size()), startTime/1000000);

    shard->getROUnderlying()->getMulti(vbId, itemsToFetch);

    std::vector<bgfetched_item_t> fetchedItems;
    for (const auto& fetch : itemsToFetch) {
        auto& key = fetch.first;
        const vb_bgfetch_item_ctx_t& bg_item_ctx = fetch.second;

        for (const auto& itm : bg_item_ctx.bgfetched_list) {
            fetchedItems.push_back(std::make_pair(key, itm));
        }
    }

    if (fetchedItems.size() > 0) {
        store->completeBGFetchMulti(vbId, fetchedItems, startTime);
        stats.getMultiHisto.add((gethrtime() - startTime) / 1000,
                                fetchedItems.size());
    }

    // failed requests will get requeued for retry within clearItems()
    clearItems(vbId, itemsToFetch);
    return fetchedItems.size();
}

void BgFetcher::clearItems(VBucket::id_type vbId,
                           const vb_bgfetch_queue_t& itemsToFetch) {
    for (const auto& fetch : itemsToFetch) {
        // every fetched item belonging to the same key shares
        // a single data buffer, just delete it from the first fetched item
        const vb_bgfetch_item_ctx_t& bg_item_ctx = fetch.second;
        const auto& doneItems = bg_item_ctx.bgfetched_list;
        VBucketBGFetchItem *firstItem = doneItems.front();
        firstItem->delValue();

        for (const auto& done : doneItems) {
            delete done;
        }
    }
}

bool BgFetcher::run(GlobalTask *task) {
    size_t num_fetched_items = 0;
    bool inverse = true;
    pendingFetch.compare_exchange_strong(inverse, false);

    std::vector<uint16_t> bg_vbs(pendingVbs.size());
    {
        LockHolder lh(queueMutex);
        bg_vbs.assign(pendingVbs.begin(), pendingVbs.end());
        pendingVbs.clear();
    }

    for (const uint16_t vbId : bg_vbs) {
        if (store->getVBuckets().isBucketCreation(vbId)) {
            // Requeue the bg fetch task if a vbucket DB file is not
            // created yet.
            {
                LockHolder lh(queueMutex);
                pendingVbs.insert(vbId);
            }
            bool inverse = false;
            pendingFetch.compare_exchange_strong(inverse, true);
            continue;
        }
        RCPtr<VBucket> vb = shard->getBucket(vbId);
        if (vb) {
            auto items = vb->getBGFetchItems();
            if (items.size() > 0) {
                num_fetched_items += doFetch(vbId, items);
            }
        }
    }

    stats.numRemainingBgJobs.fetch_sub(num_fetched_items);

    if (!pendingFetch.load()) {
        // wait a bit until next fetch request arrives
        double sleep = std::max(store->getBGFetchDelay(), sleepInterval);
        task->snooze(sleep);

        if (pendingFetch.load()) {
            // check again a new fetch request could have arrived
            // right before calling above snooze()
            task->snooze(0);
        }
    }
    return true;
}

bool BgFetcher::pendingJob() const {
    for (const auto vbid : shard->getVBuckets()) {
        RCPtr<VBucket> vb = shard->getBucket(vbid);
        if (vb && vb->hasPendingBGFetchItems()) {
            return true;
        }
    }
    return false;
}
