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
#include "vbucket.h"

class ItemResidentCallback : public Callback<CacheLookup> {
public:
    ItemResidentCallback(hrtime_t token, const std::string &n,
                         TapConnMap &cm, EventuallyPersistentEngine* e)
    : connToken(token), tapConnName(n), connMap(cm), engine(e) {
        if (engine == nullptr) {
            throw std::invalid_argument("ItemResidentCallback: engine must "
                            "be non-NULL");
        }
    }

    void callback(CacheLookup &lookup);

private:
    hrtime_t                    connToken;
    const std::string           tapConnName;
    TapConnMap                    &connMap;
    EventuallyPersistentEngine *engine;
};

void ItemResidentCallback::callback(CacheLookup &lookup) {
    RCPtr<VBucket> vb = engine->getEpStore()->getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(lookup.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(lookup.getKey(), bucket_num);
    if (v && v->isResident() && v->getBySeqno() == lookup.getBySeqno()) {
        Item* it = v->toItem(false, lookup.getVBucketId());
        lh.unlock();
        CompletedBGFetchTapOperation tapop(connToken,
                                           lookup.getVBucketId(), true);
        if (!connMap.performOp(tapConnName, tapop, it)) {
            delete it;
        }
        setStatus(ENGINE_KEY_EEXISTS);
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

/**
 * Callback class used to process an item backfilled from disk and push it into
 * the corresponding TAP queue.
 */
class BackfillDiskCallback : public Callback<GetValue> {
public:
    BackfillDiskCallback(hrtime_t token, const std::string &n, TapConnMap &cm)
        : connToken(token), tapConnName(n), connMap(cm) {}

    void callback(GetValue &val);

private:

    hrtime_t                    connToken;
    const std::string           tapConnName;
    TapConnMap                 &connMap;
};

void BackfillDiskCallback::callback(GetValue &gv) {
    if (gv.getValue() == nullptr) {
        LOG(EXTENSION_LOG_WARNING,
        "BackfillDiskCallback::callback: gv must be non-NULL."
        "Ignoring callback for tapConnName:%s",
        tapConnName.c_str());
        return;
    }
    CompletedBGFetchTapOperation tapop(connToken,
                                       gv.getValue()->getVBucketId(), true);
    // if the tap connection is closed, then free an Item instance
    if (!connMap.performOp(tapConnName, tapop, gv.getValue())) {
        delete gv.getValue();
    }
}

bool BackfillDiskLoad::run() {
    if (engine->getEpStore()->isMemoryUsageTooHigh()) {
        LOG(EXTENSION_LOG_INFO, "VBucket %d backfill task from disk is "
         "temporarily suspended  because the current memory usage is too high",
         vbucket);
        snooze(DEFAULT_BACKFILL_SNOOZE_TIME);
        return true;
    }

    if (connMap.checkConnectivity(name) &&
                               !engine->getEpStore()->isFlushAllScheduled()) {
        size_t num_items;
        size_t num_deleted;
        try {
            num_items = store->getItemCount(vbucket);
            num_deleted = store->getNumPersistedDeletes(vbucket);
        } catch (std::system_error& e) {
            if (e.code() == std::error_code(ENOENT, std::system_category())) {
                // File creation hasn't completed yet; backoff and wait.
                LOG(EXTENSION_LOG_NOTICE,
                    "BackfillDiskLoad::run: Failed to get itemCount for "
                    "vBucket %" PRIu16 " - database file does not yet exist. "
                    "(%s) Snoozing for %f seconds", vbucket,
                    e.what(), DEFAULT_BACKFILL_SNOOZE_TIME);
                snooze(DEFAULT_BACKFILL_SNOOZE_TIME);
                return true;
            } else {
                // Some other (unexpected) system_error exception - re-throw
                throw e;
            }
        }
        connMap.incrBackfillRemaining(name, num_items + num_deleted);

        std::shared_ptr<Callback<GetValue> >
            cb(new BackfillDiskCallback(connToken, name, connMap));
        std::shared_ptr<Callback<CacheLookup> >
            cl(new ItemResidentCallback(connToken, name, connMap, engine));

        ScanContext* ctx = store->initScanContext(cb, cl, vbucket, startSeqno,
                                                  DocumentFilter::ALL_ITEMS,
                                                  ValueFilter::VALUES_DECOMPRESSED);
        if (ctx) {
            store->scan(ctx);
            store->destroyScanContext(ctx);
        }
    }

    LOG(EXTENSION_LOG_INFO,"VBucket %d backfill task from disk is completed",
        vbucket);

    // Should decr the disk backfill counter regardless of the connectivity
    // status
    CompleteDiskBackfillTapOperation op;
    connMap.performOp(name, op, static_cast<void*>(NULL));

    return false;
}

std::string BackfillDiskLoad::getDescription() {
    std::stringstream rv;
    rv << "Loading TAP backfill from disk: vb " << vbucket;
    return rv.str();
}

bool BackFillVisitor::visitBucket(RCPtr<VBucket> &vb) {
    if (VBucketVisitor::visitBucket(vb)) {
        item_eviction_policy_t policy =
            engine->getEpStore()->getItemEvictionPolicy();
        double num_items = static_cast<double>(vb->getNumItems(policy));

        if (num_items == 0) {
            return false;
        }

        KVStore *underlying(engine->getEpStore()->
                            getROUnderlying(vb->getId()));
        LOG(EXTENSION_LOG_INFO,
            "Schedule a full backfill from disk for vbucket %d.", vb->getId());
        ExTask task = new BackfillDiskLoad(name, engine, connMap,
                                          underlying, vb->getId(), 0, connToken,
                                          0, false);
        ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
    }
    return false;
}

void BackFillVisitor::visit(StoredValue*) {
    abort();
}

bool BackFillVisitor::pauseVisitor() {
    bool pause(true);

    ssize_t theSize(connMap.backfillQueueDepth(name));
    if (!checkValidity() || theSize < 0) {
        LOG(EXTENSION_LOG_WARNING,
            "TapProducer %s went away. Stopping backfill",
            name.c_str());
        valid = false;
        return false;
    }

    ssize_t maxBackfillSize = engine->getTapConfig().getBackfillBacklogLimit();
    pause = theSize > maxBackfillSize;

    if (pause) {
        LOG(EXTENSION_LOG_INFO, "Tap queue depth is too big for %s!!! "
            "Pausing backfill temporarily...\n", name.c_str());
    }
    return pause;
}

void BackFillVisitor::complete() {
    CompleteBackfillTapOperation tapop;
    connMap.performOp(name, tapop, static_cast<void*>(NULL));
    LOG(EXTENSION_LOG_INFO,
        "Backfill dispatcher task for TapProducer %s is completed.\n",
        name.c_str());
}

bool BackFillVisitor::checkValidity() {
    if (valid) {
        valid = connMap.checkConnectivity(name);
        if (!valid) {
            LOG(EXTENSION_LOG_WARNING, "Backfilling connectivity for %s went "
                "invalid. Stopping backfill.\n", name.c_str());
        }
    }
    return valid;
}

bool BackfillTask::run(void) {
    engine->getEpStore()->visit(bfv, "Backfill task", NONIO_TASK_IDX,
                                TaskId::BackfillVisitorTask, 1);
    return false;
}
