/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "vbucket.hh"
#include "ep_engine.h"
#include "ep.hh"
#include "backfill.hh"

double BackFillVisitor::backfillResidentThreshold = DEFAULT_BACKFILL_RESIDENT_THRESHOLD;

static bool isMemoryUsageTooHigh(EPStats &stats) {
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    double maxSize = static_cast<double>(stats.getMaxDataSize());
    return memoryUsed > (maxSize * BACKFILL_MEM_THRESHOLD);
}

void BackfillDiskLoad::callback(GetValue &gv) {
    CompletedBGFetchTapOperation tapop(true);
    // if the tap connection is closed, then free an Item instance
    if (!connMap.performTapOp(name, tapop, gv.getValue())) {
        delete gv.getValue();
    }

    NotifyPausedTapOperation notifyOp;
    connMap.performTapOp(name, notifyOp, engine);
}

bool BackfillDiskLoad::callback(Dispatcher &d, TaskId t) {
    bool valid = false;

    if (isMemoryUsageTooHigh(engine->getEpStats())) {
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "VBucket %d backfill task from disk is temporarily suspended "
                         "because the current memory usage is too high.\n",
                         vbucket);
        d.snooze(t, 1);
        return true;
    }

    if (connMap.checkConnectivity(name) && !engine->getEpStore()->isFlushAllScheduled()) {
        store->dump(vbucket, *this);
        valid = true;
    }

    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                     "VBucket %d backfill task from disk is completed.\n",
                     vbucket);

    // Should decr the disk backfill counter regardless of the connectivity status
    CompleteDiskBackfillTapOperation op;
    connMap.performTapOp(name, op, static_cast<void*>(NULL));

    if (valid && connMap.checkBackfillCompletion(name)) {
        engine->notifyNotificationThread();
    }

    return false;
}

std::string BackfillDiskLoad::description() {
    std::stringstream rv;
    rv << "Loading TAP backfill from disk for vb " << vbucket;
    return rv.str();
}

bool BackFillVisitor::visitBucket(RCPtr<VBucket> vb) {
    apply();

    if (vBucketFilter(vb->getId())) {
        // When the backfill is scheduled for a given vbucket, set the TAP cursor to
        // the beginning of the open checkpoint.
        engine->tapConnMap.SetCursorToOpenCheckpoint(name, vb->getId());

        VBucketVisitor::visitBucket(vb);
        // If the current resident ratio for a given vbucket is below the resident threshold
        // for memory backfill only, schedule the disk backfill for more efficient bg fetches.
        double numItems = static_cast<double>(vb->ht.getNumItems());
        double numNonResident = static_cast<double>(vb->ht.getNumNonResidentItems());
        if (numItems == 0) {
            return true;
        }
        residentRatioBelowThreshold =
            ((numItems - numNonResident) / numItems) < backfillResidentThreshold ? true : false;
        if (efficientVBDump && residentRatioBelowThreshold) {
            vbuckets.push_back(vb->getId());
            ScheduleDiskBackfillTapOperation tapop;
            engine->tapConnMap.performTapOp(name, tapop, static_cast<void*>(NULL));
        }
        return true;
    }
    return false;
}

void BackFillVisitor::visit(StoredValue *v) {
    // If efficient VBdump is supported and an item is not resident,
    // skip the item as it will be fetched by the disk backfill.
    if (efficientVBDump && residentRatioBelowThreshold && !v->isResident()) {
        return;
    }
    std::string k = v->getKey();
    queued_item qi(new QueuedItem(k, currentBucket->getId(), queue_op_set,
                                  -1, v->getId()));
    uint16_t shardId = engine->kvstore->getShardId(*qi);
    found.push_back(std::make_pair(shardId, qi));
}

void BackFillVisitor::apply(void) {
    // If efficient VBdump is supported, schedule all the disk backfill tasks.
    if (efficientVBDump) {
        std::vector<uint16_t>::iterator it = vbuckets.begin();
        for (; it != vbuckets.end(); it++) {
            Dispatcher *d(engine->epstore->getRODispatcher());
            KVStore *underlying(engine->epstore->getROUnderlying());
            assert(d);
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Schedule a full backfill from disk for vbucket %d.\n",
                              *it);
            shared_ptr<DispatcherCallback> cb(new BackfillDiskLoad(name,
                                                                   engine,
                                                                   engine->tapConnMap,
                                                                   underlying,
                                                                   *it,
                                                                   validityToken));
            d->schedule(cb, NULL, Priority::TapBgFetcherPriority);
        }
        vbuckets.clear();
    }

    setEvents();
}

void BackFillVisitor::setResidentItemThreshold(double residentThreshold) {
    if (residentThreshold < MINIMUM_BACKFILL_RESIDENT_THRESHOLD) {
        std::stringstream ss;
        ss << "Resident item threshold " << residentThreshold
           << " for memory backfill only is too low. Ignore this new threshold...";
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
        return;
    }
    backfillResidentThreshold = residentThreshold;
}

void BackFillVisitor::setEvents() {
    if (checkValidity()) {
        if (!found.empty()) {
            // Don't notify unless we've got some data..
            TaggedQueuedItemComparator<uint16_t> comparator;
            std::sort(found.begin(), found.end(), comparator);

            std::vector<std::pair<uint16_t, queued_item> >::iterator it(found.begin());
            for (; it != found.end(); ++it) {
                queue->push_back(it->second);
            }
            found.clear();
            engine->tapConnMap.setEvents(name, queue);
        }
    }
}

bool BackFillVisitor::pauseVisitor() {
    bool pause(true);

    ssize_t theSize(engine->tapConnMap.backfillQueueDepth(name));
    if (!checkValidity() || theSize < 0) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "TapProducer %s went away.  Stopping backfill.\n",
                         name.c_str());
        valid = false;
        return false;
    }

    pause = theSize > maxBackfillSize || isMemoryUsageTooHigh(engine->getEpStats());

    if (pause) {
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Tap queue depth is too big for %s or memory usage too high, "
                         "Pausing temporarily...\n",
                         name.c_str());
    }
    return pause;
}

void BackFillVisitor::complete() {
    apply();
    CompleteBackfillTapOperation tapop;
    engine->tapConnMap.performTapOp(name, tapop, static_cast<void*>(NULL));
    if (engine->tapConnMap.checkBackfillCompletion(name)) {
        engine->notifyNotificationThread();
    }
    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                     "Backfill dispatcher task for TapProducer %s is completed.\n",
                     name.c_str());
}

bool BackFillVisitor::checkValidity() {
    if (valid) {
        valid = engine->tapConnMap.checkConnectivity(name);
        if (!valid) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Backfilling connectivity for %s went invalid. "
                             "Stopping backfill.\n",
                             name.c_str());
        }
    }
    return valid;
}

bool BackfillTask::callback(Dispatcher &d, TaskId t) {
    (void) t;
    epstore->visit(bfv, "Backfill task", &d, Priority::BackfillTaskPriority, true, 1);
    return false;
}
