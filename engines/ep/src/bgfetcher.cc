/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "bgfetcher.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "tasks.h"
#include "vbucket_bgfetch_item.h"
#include <executor/executorpool.h>
#include <memcached/vbucket.h>
#include <phosphor/phosphor.h>
#include <algorithm>
#include <climits>
#include <vector>

BgFetcher::BgFetcher(KVBucket& s, EPStats& st)
    : store(s),
      taskId(0),
      stats(st),
      pendingFetch(false),
      queue(s.getVBuckets().getSize()) {
}

BgFetcher::BgFetcher(KVBucket& s) : BgFetcher(s, s.getEPEngine().getEpStats()) {
}

BgFetcher::~BgFetcher() {
    if (!queue.empty()) {
        EP_LOG_DEBUG(
                "Terminating database reader without completing "
                "background fetches for {} vbuckets.",
                queue.size());
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
    queue.pushUnique(vbid);
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

        for (const auto& itm : bg_item_ctx.getRequests()) {
            // We don't want to transfer ownership of itm here as we clean it
            // up at the end of this method in clearItems()
            fetchedItems.emplace_back(key, itm.get());
        }
    }

    preCompleteHook();

    if (!fetchedItems.empty()) {
        store.completeBGFetchMulti(vbId, fetchedItems, startTime);
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

    size_t num_fetched_items = 0;

    // Take size so we will yield after processing what was originally in the
    // queue (this is MPSC so nothing else should drain). We'll guard against
    // the case that something else does anyway
    auto size = queue.size();
    Vbid vbid;
    while (size > 0 && queue.popFront(vbid)) {
        size--;

        auto vb = store.getVBucket(vbid);
        if (vb) {
            // Requeue the bg fetch task if vbucket DB file is not created yet.
            if (vb->isBucketCreation()) {
                queue.pushUnique(vbid);
                wakeUpTaskIfSnoozed();
                continue;
            }

            auto items = vb->getBGFetchItems();
            if (!items.empty()) {
                num_fetched_items += doFetch(vbid, items);
            }
        }
    }

    stats.numRemainingBgItems.fetch_sub(num_fetched_items);

    return true;
}
