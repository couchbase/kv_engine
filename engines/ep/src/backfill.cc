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

#include <phosphor/phosphor.h>

#include "atomic.h"
#include "backfill.h"
#include "ep_engine.h"
#include "kv_bucket_iface.h"
#include "tapconnmap.h"
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
    VBucketPtr vb = engine->getKVBucket()->getVBucket(
                                                        lookup.getVBucketId());
    if (!vb) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    auto hbl = vb->ht.getLockedBucket(lookup.getKey());
    StoredValue* v = vb->ht.unlocked_find(lookup.getKey(),
                                          hbl.getBucketNum(),
                                          WantsDeleted::No,
                                          TrackReference::Yes);
    if (v && v->isResident() && v->getBySeqno() == lookup.getBySeqno()) {
        auto it = v->toItem(false, lookup.getVBucketId());
        hbl.getHTLock().unlock();
        CompletedBGFetchTapOperation tapop(connToken,
                                           lookup.getVBucketId(), true);
        if (connMap.performOp(tapConnName, tapop, it.get())) {
            // On success performOp has taken ownership of the item.
            it.release();
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
    if (!gv.item) {
        LOG(EXTENSION_LOG_WARNING,
        "BackfillDiskCallback::callback: gv must be non-NULL."
        "Ignoring callback for tapConnName:%s",
        tapConnName.c_str());
        return;
    }
    CompletedBGFetchTapOperation tapop(
            connToken, gv.item->getVBucketId(), true);
    // if the tap connection is closed, then free an Item instance
    auto* itemPtr = gv.item.release(); // Take ownership of item
    if (!connMap.performOp(tapConnName, tapop, itemPtr)) {
        delete itemPtr;
    }
}

BackfillDiskLoad::BackfillDiskLoad(const std::string& n,
                                   EventuallyPersistentEngine* e,
                                   TapConnMap& cm,
                                   KVStore* s,
                                   uint16_t vbid,
                                   uint64_t start_seqno,
                                   hrtime_t token,
                                   double sleeptime,
                                   bool shutdown)
    : GlobalTask(e, TaskId::BackfillDiskLoad, sleeptime, shutdown),
      name(n),
      description("Loading TAP backfill from disk: vb " + std::to_string(vbid)),
      engine(e),
      connMap(cm),
      store(s),
      vbucket(vbid),
      startSeqno(start_seqno),
      connToken(token) {
    ScheduleDiskBackfillTapOperation tapop;
    cm.performOp(name, tapop, static_cast<void*>(NULL));
}

bool BackfillDiskLoad::run() {
    TRACE_EVENT0("ep-engine/task", "BackfillDiskload");
    if (engine->getKVBucket()->isMemoryUsageTooHigh()) {
        LOG(EXTENSION_LOG_INFO, "VBucket %d backfill task from disk is "
         "temporarily suspended  because the current memory usage is too high",
         vbucket);
        snooze(DEFAULT_BACKFILL_SNOOZE_TIME);
        return true;
    }

    if (connMap.checkConnectivity(name) &&
        !engine->getKVBucket()->isDeleteAllScheduled()) {
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

cb::const_char_buffer BackfillDiskLoad::getDescription() {
    return description;
}
