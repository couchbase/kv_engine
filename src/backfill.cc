/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "vbucket.hh"
#include "ep_engine.h"
#include "ep.hh"
#include "backfill.hh"


static bool isMemoryUsageTooHigh(EPStats &stats) {
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    double maxSize = static_cast<double>(stats.getMaxDataSize());
    return memoryUsed > (maxSize * BACKFILL_MEM_THRESHOLD);
}

/**
 * Callback class used to process an item backfilled from disk and push it into
 * the corresponding TAP queue.
 */
class BackfillDiskCallback : public Callback<GetValue> {
public:
    BackfillDiskCallback(hrtime_t token, const std::string &n,
                         TapConnMap &tcm, EventuallyPersistentEngine* e)
        : connToken(token), tapConnName(n), connMap(tcm), engine(e) {
        assert(engine);
    }

    void callback(GetValue &val);

private:

    hrtime_t                    connToken;
    const std::string           tapConnName;
    TapConnMap                 &connMap;
    EventuallyPersistentEngine *engine;
};

void BackfillDiskCallback::callback(GetValue &gv) {
    assert(gv.getValue());
    CompletedBGFetchTapOperation tapop(connToken, gv.getValue()->getVBucketId(), true);
    // if the tap connection is closed, then free an Item instance
    if (!connMap.performTapOp(tapConnName, tapop, gv.getValue())) {
        delete gv.getValue();
    }
}

bool BackfillDiskLoad::callback(Dispatcher &d, TaskId &t) {
    if (isMemoryUsageTooHigh(engine->getEpStats())) {
        LOG(EXTENSION_LOG_INFO, "VBucket %d backfill task from disk is "
            "temporarily suspended  because the current memory usage is too high",
            vbucket);
        d.snooze(t, 1);
        return true;
    }

    if (connMap.checkConnectivity(name) && !engine->getEpStore()->isFlushAllScheduled()) {
        shared_ptr<Callback<GetValue> > backfill_cb(new BackfillDiskCallback(connToken,
                                                                             name, connMap,
                                                                             engine));
        if (backfillType == ALL_MUTATIONS) {
            store->dump(vbucket, backfill_cb);
        } else if (store->getStorageProperties().hasPersistedDeletions() &&
                   backfillType == DELETIONS_ONLY) {
            store->dumpDeleted(vbucket, backfill_cb);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Underlying KVStore doesn't support this kind of backfill");
            abort();
        }
    }

    LOG(EXTENSION_LOG_INFO,"VBucket %d backfill task from disk is completed",
        vbucket);

    // Should decr the disk backfill counter regardless of the connectivity status
    CompleteDiskBackfillTapOperation op;
    connMap.performTapOp(name, op, static_cast<void*>(NULL));

    return false;
}

std::string BackfillDiskLoad::description() {
    std::stringstream rv;
    rv << "Loading TAP backfill from disk for vb " << vbucket;
    return rv.str();
}

bool BackFillVisitor::visitBucket(RCPtr<VBucket> &vb) {
    apply();

    if (vBucketFilter(vb->getId())) {
        // When the backfill is scheduled for a given vbucket, set the TAP cursor to
        // the beginning of the open checkpoint.
        engine->tapConnMap->SetCursorToOpenCheckpoint(name, vb->getId());

        VBucketVisitor::visitBucket(vb);
        double num_items = static_cast<double>(vb->ht.getNumItems());
        double num_non_resident = static_cast<double>(vb->ht.getNumNonResidentItems());
        double num_res_items = 0;
        size_t num_backfill_items = 0;

        if (num_items == 0) {
            return false;
        }
        if (num_items > num_non_resident) {
            num_res_items = num_items - num_non_resident;
        }

        double resident_threshold = engine->getTapConfig().getBackfillResidentThreshold();
        residentRatioBelowThreshold =
            (num_res_items / num_items) < resident_threshold ? true : false;

        if (efficientVBDump && residentRatioBelowThreshold) {
            // disk backfill for persisted items + memory backfill for resident items
            size_t num_create = vb->opsCreate;
            size_t num_delete = vb->opsDelete;
            size_t persisted_items = 0;
            if (num_create > num_delete) {
                persisted_items = num_create - num_delete;
            }
            num_backfill_items = persisted_items + static_cast<size_t>(num_res_items);
            vbuckets[vb->getId()] = ALL_MUTATIONS;
            ScheduleDiskBackfillTapOperation tapop;
            engine->tapConnMap->performTapOp(name, tapop, static_cast<void*>(NULL));
        } else {
            if (engine->epstore->getStorageProperties().hasPersistedDeletions()) {
                vbuckets[vb->getId()] = DELETIONS_ONLY;
                ScheduleDiskBackfillTapOperation tapop;
                engine->tapConnMap->performTapOp(name, tapop, static_cast<void*>(NULL));
            }
            num_backfill_items = static_cast<size_t>(num_items);
        }

        engine->tapConnMap->incrBackfillRemaining(name, num_backfill_items);
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

    if (v->isTempItem()) {
        return;
    }

    queued_item qi(new QueuedItem(v->getKey(), currentBucket->getId(),
                                  queue_op_set, v->getSeqno()));
    queue->push_back(qi);
}

void BackFillVisitor::apply(void) {
    // If efficient VBdump is supported, schedule all the disk backfill tasks.
    if (efficientVBDump) {
        std::map<uint16_t, backfill_t>::iterator it = vbuckets.begin();
        for (; it != vbuckets.end(); it++) {
            Dispatcher *d(engine->epstore->getAuxIODispatcher());
            KVStore *underlying(engine->epstore->getAuxUnderlying());
            assert(d);
            LOG(EXTENSION_LOG_INFO,
                "Schedule a full backfill from disk for vbucket %d.\n",
                it->first);
            shared_ptr<DispatcherCallback> cb(new BackfillDiskLoad(name,
                                                                   engine,
                                                                   *engine->tapConnMap,
                                                                   underlying,
                                                                   it->first,
                                                                   it->second,
                                                                   connToken));
            d->schedule(cb, NULL, Priority::TapBgFetcherPriority);
        }
        vbuckets.clear();
    }

    setEvents();
}

void BackFillVisitor::setEvents() {
    if (checkValidity()) {
        if (!queue->empty()) {
            engine->tapConnMap->setEvents(name, queue);
        }
    }
}

bool BackFillVisitor::pauseVisitor() {
    bool pause(true);

    ssize_t theSize(engine->tapConnMap->backfillQueueDepth(name));
    if (!checkValidity() || theSize < 0) {
        LOG(EXTENSION_LOG_WARNING, "TapProducer %s went away. Stopping backfill",
            name.c_str());
        valid = false;
        return false;
    }

    ssize_t maxBackfillSize = engine->getTapConfig().getBackfillBacklogLimit();
    pause = theSize > maxBackfillSize;

    if (pause) {
        LOG(EXTENSION_LOG_INFO, "Tap queue depth is too big for %s!!! ",
            "Pausing backfill temporarily...\n", name.c_str());
    }
    return pause;
}

void BackFillVisitor::complete() {
    apply();
    CompleteBackfillTapOperation tapop;
    engine->tapConnMap->performTapOp(name, tapop, static_cast<void*>(NULL));
    LOG(EXTENSION_LOG_INFO,
        "Backfill dispatcher task for TapProducer %s is completed.\n",
        name.c_str());
}

bool BackFillVisitor::checkValidity() {
    if (valid) {
        valid = engine->tapConnMap->checkConnectivity(name);
        if (!valid) {
            LOG(EXTENSION_LOG_WARNING, "Backfilling connectivity for %s went "
                "invalid. Stopping backfill.\n", name.c_str());
        }
    }
    return valid;
}

bool BackfillTask::callback(Dispatcher &d, TaskId &t) {
    (void) t;
    engine->getEpStore()->visit(bfv, "Backfill task", &d,
                                Priority::BackfillTaskPriority, true, 1);
    return false;
}
