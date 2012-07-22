/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "ep.hh"

const double BgFetcher::sleepInterval = 1.0;

bool BgFetcherCallback::callback(Dispatcher &, TaskId t) {
    return bgfetcher->run(t);
}

void BgFetcher::start() {
    LockHolder lh(taskMutex);
    dispatcher->schedule(shared_ptr<BgFetcherCallback>(new BgFetcherCallback(this)),
                         &task, Priority::BgFetcherPriority);
    assert(task.get());
}

void BgFetcher::stop() {
    LockHolder lh(taskMutex);
    assert(task.get());
    dispatcher->cancel(task);
}

void BgFetcher::doFetch(uint16_t vbId) {
    hrtime_t startTime(gethrtime());
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "BgFetcher is fetching data, vBucket = %d numDocs = %d, "
                     "startTime = %lld\n",
                     vbId, items2fetch.size(), startTime/1000000);

    store->getROUnderlying()->getMulti(vbId, items2fetch);

    int totalfetches = 0;
    std::vector<VBucketBGFetchItem *> fetchedItems;
    vb_bgfetch_queue_t::iterator itr = items2fetch.begin();
    for (; itr != items2fetch.end(); itr++) {
        std::list<VBucketBGFetchItem *> &requestedItems = (*itr).second;
        std::list<VBucketBGFetchItem *>::iterator itm = requestedItems.begin();
        for(; itm != requestedItems.end(); itm++) {
            fetchedItems.push_back(*itm);
            ++totalfetches;
        }
    }
    store->completeBGFetchMulti(vbId, fetchedItems, startTime);
    stats.getMultiHisto.add(startTime-gethrtime()/1000, totalfetches);
    clearItems();
}

void BgFetcher::clearItems(void) {
    vb_bgfetch_queue_t::iterator itr = items2fetch.begin();
    for(; itr != items2fetch.end(); itr++) {
        // every fetched item belonging to the same seq_id shares
        // the same buffer, just delete it from the first fetched item
        std::list<VBucketBGFetchItem *> &doneItems = (*itr).second;
        VBucketBGFetchItem *firstItem = doneItems.front();
        delete firstItem;
    }
}

bool BgFetcher::run(TaskId tid) {
    assert(tid.get());
    size_t num_fetched_items = 0;

    const VBucketMap &vbMap = store->getVBuckets();
    size_t numVbuckets = vbMap.getSize();
    for (size_t vbid = 0; vbid < numVbuckets; vbid++) {
        RCPtr<VBucket> vb = vbMap.getBucket(vbid);
        assert(items2fetch.empty());
        if (vb && vb->getBGFetchItems(items2fetch)) {
            doFetch(vbid);
            num_fetched_items += items2fetch.size();
            items2fetch.clear();
        }
    }

    if (numRemainingItems.decr(num_fetched_items) == 0) {
        // wait a bit until next fetche request arrives
        double sleep = std::max(store->getBGFetchDelay(), sleepInterval);
        dispatcher->snooze(tid, sleep);
    }
    return true;
}

