/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include <string>
#include <vector>

#include "atomic.h"
#include "backfill.h"
#include "ep.h"
#include "iomanager/iomanager.h"
#include "vbucket.h"

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

bool BackfillDiskLoad::run() {
    if (isMemoryUsageTooHigh(engine->getEpStats())) {
        LOG(EXTENSION_LOG_INFO, "VBucket %d backfill task from disk is "
            "temporarily suspended  because the current memory usage is too high",
            vbucket);
        snooze(DEFAULT_BACKFILL_SNOOZE_TIME, true);
        return true;
    }

    if (connMap.checkConnectivity(name) && !engine->getEpStore()->isFlushAllScheduled()) {
        shared_ptr<Callback<GetValue> > backfill_cb(new BackfillDiskCallback(connToken,
                                                                             name, connMap,
                                                                             engine));
        store->dump(vbucket, backfill_cb);
    }

    LOG(EXTENSION_LOG_INFO,"VBucket %d backfill task from disk is completed",
        vbucket);

    // Should decr the disk backfill counter regardless of the connectivity status
    CompleteDiskBackfillTapOperation op;
    connMap.performTapOp(name, op, static_cast<void*>(NULL));

    return false;
}

std::string BackfillDiskLoad::getDescription() {
    std::stringstream rv;
    rv << "Loading TAP backfill from disk: vb " << vbucket;
    return rv.str();
}

bool BackFillVisitor::visitBucket(RCPtr<VBucket> &vb) {
    apply();

    if (VBucketVisitor::visitBucket(vb)) {
        double num_items = static_cast<double>(vb->ht.getNumItems());
        double num_non_resident = static_cast<double>(vb->ht.getNumNonResidentItems());

        if (num_items == 0) {
            return false;
        }

        // disk backfill for persisted items + memory backfill for resident items
        size_t num_backfill_items = (vb->opsCreate - vb->opsDelete) +
            static_cast<size_t>(num_items - num_non_resident);
        vbuckets.push_back(vb->getId());
        ScheduleDiskBackfillTapOperation tapop;
        engine->tapConnMap->performTapOp(name, tapop, static_cast<void*>(NULL));
        engine->tapConnMap->incrBackfillRemaining(name, num_backfill_items);
    }
    return false;
}

void BackFillVisitor::visit(StoredValue*) {
    abort();
}

void BackFillVisitor::apply(void) {
    std::vector<uint16_t>::iterator it = vbuckets.begin();
    for (; it != vbuckets.end(); ++it) {
        KVStore *underlying(engine->epstore->getAuxUnderlying());
        LOG(EXTENSION_LOG_INFO,
            "Schedule a full backfill from disk for vbucket %d.", *it);
        ExTask task = new BackfillDiskLoad(name, engine, *engine->tapConnMap,
                                           underlying, *it, connToken,
                                           Priority::TapBgFetcherPriority,
                                           0, 0, false, false);
        IOManager::get()->scheduleTask(task, AUXIO_TASK_IDX);
    }
    vbuckets.clear();
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
