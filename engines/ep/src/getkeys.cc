/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "getkeys.h"

#include "bucket_logger.h"
#include "callbacks.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "vbucket.h"
#include <mcbp/protocol/status.h>
#include <memcached/cookie_iface.h>
#include <phosphor/phosphor.h>
#include <utility>

/**
 * Callback class used by AllKeysAPI, for caching fetched keys
 *
 * As by default (or in most cases), number of keys is 1000,
 * and an average key could be 32B in length, initialize buffersize of
 * allKeys to 34000 (1000 * 32 + 1000 * 2), the additional 2 bytes per
 * key is for the keylength.
 *
 * This initially allocated buffersize is doubled whenever the length
 * of the buffer holding all the keys, crosses the buffersize.
 */
class AllKeysCallback : public StatusCallback<const DiskDocKey&> {
public:
    AllKeysCallback(std::vector<char>& buffer,
                    std::optional<CollectionID> collection,
                    uint32_t maxCount)
        : buffer(buffer),
          collection(std::move(collection)),
          maxCount(maxCount) {
        buffer.reserve(avgKeySize * expNumKeys);
    }

    void callback(const DiskDocKey& key) override;

private:
    std::vector<char>& buffer;
    std::optional<CollectionID> collection;
    uint32_t addedKeyCount = 0;
    uint32_t maxCount = 0;
    static const int avgKeySize = 32 + sizeof(uint16_t);
    static const int expNumKeys = 1000;
};

void AllKeysCallback::callback(const DiskDocKey& key) {
    setStatus(cb::engine_errc::not_stored);
    if (addedKeyCount >= maxCount) {
        return;
    }

    auto outKey = key.getDocKey();
    if (outKey.isInSystemEventCollection() || key.isPrepared()) {
        // Skip system-event and durability-prepared keys
        return;
    }

    if (collection) {
        if (outKey.getCollectionID() != collection.value()) {
            return;
        }
    } else {
        if (outKey.isInDefaultCollection()) {
            outKey = outKey.makeDocKeyWithoutCollectionID();
        } else {
            // Only default collection key can be sent back if collections is
            // not supported, implied by 'collection' not having a value
            return;
        }
    }

    uint16_t outlen = htons(outKey.size());
    // insert 1 x u16
    const auto* outlenPtr = reinterpret_cast<const char*>(&outlen);
    buffer.insert(buffer.end(), outlenPtr, outlenPtr + sizeof(uint16_t));
    // insert the char buffer
    buffer.insert(buffer.end(), outKey.data(), outKey.data() + outKey.size());

    addedKeyCount++;
    setStatus(cb::engine_errc::success);
}

/// An upper bound for the number of keys to return
static constexpr uint32_t FetchAllKeysLimit = 100000U;
FetchAllKeysTask::FetchAllKeysTask(EventuallyPersistentEngine& e,
                                   CookieIface& c,
                                   const DocKeyView start_key_,
                                   Vbid vbucket,
                                   uint32_t count_,
                                   std::optional<CollectionID> collection)
    : EpTask(e, TaskId::FetchAllKeysTask, 0, false),
      cookie(c),
      description("Running the ALL_DOCS api on " + vbucket.to_string()),
      start_key(start_key_),
      vbid(vbucket),
      count(std::min(count_, FetchAllKeysLimit)),
      collection(std::move(collection)) {
    if (count != count_) {
        EP_LOG_WARN("{}: FetchAllKeysTask: Limit max keys to {}",
                    cookie.getConnectionId(),
                    FetchAllKeysLimit);
    }
}

bool FetchAllKeysTask::run() {
    TRACE_EVENT0("ep-engine/task", "FetchAllKeysTask");
    status = doRun();
    engine->notifyIOComplete(cookie, status);
    return false;
}

cb::engine_errc FetchAllKeysTask::doRun() {
    VBucketPtr vb = engine->getVBucket(vbid);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    std::shared_lock rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        return cb::engine_errc::not_my_vbucket;
    }

    if (!vb->isBucketCreation()) {
        auto cb = std::make_shared<AllKeysCallback>(keys, collection, count);
        return engine->getKVBucket()->getROUnderlying(vbid)->getAllKeys(
                vbid, start_key, count, cb);
    }

    return cb::engine_errc::success;
}
