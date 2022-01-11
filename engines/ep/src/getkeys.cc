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

#include "ep_engine.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "vbucket.h"

#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/status.h>
#include <phosphor/phosphor.h>

#include <utility>

void AllKeysCallback::callback(const DiskDocKey& key) {
    setStatus(static_cast<int>(AllKeysCallbackStatus::KeySkipped));
    if (addedKeyCount >= maxCount) {
        return;
    }

    auto outKey = key.getDocKey();
    if (outKey.isInSystemCollection() || key.isPrepared()) {
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

    if (buffer.size() + outKey.size() + sizeof(uint16_t) > buffer.size()) {
        // Reserve the 2x space for the copy-to buffer.
        buffer.reserve(buffer.size() * 2);
    }
    uint16_t outlen = htons(outKey.size());
    // insert 1 x u16
    const auto* outlenPtr = reinterpret_cast<const char*>(&outlen);
    buffer.insert(buffer.end(), outlenPtr, outlenPtr + sizeof(uint16_t));
    // insert the char buffer
    buffer.insert(buffer.end(), outKey.data(), outKey.data() + outKey.size());

    addedKeyCount++;
    setStatus(static_cast<int>(AllKeysCallbackStatus::KeyAdded));
    return;
}

FetchAllKeysTask::FetchAllKeysTask(EventuallyPersistentEngine* e,
                                   const CookieIface* c,
                                   AddResponseFn resp,
                                   const DocKey start_key_,
                                   Vbid vbucket,
                                   uint32_t count_,
                                   std::optional<CollectionID> collection)
    : GlobalTask(e, TaskId::FetchAllKeysTask, 0, false),
      cookie(c),
      description("Running the ALL_DOCS api on " + vbucket.to_string()),
      response(std::move(resp)),
      start_key(start_key_),
      vbid(vbucket),
      count(count_),
      collection(std::move(collection)) {
}

bool FetchAllKeysTask::run() {
    TRACE_EVENT0("ep-engine/task", "FetchAllKeysTask");
    cb::engine_errc err;
    if (engine->getKVBucket()
                ->getVBuckets()
                .getBucket(vbid)
                ->isBucketCreation()) {
        // Returning an empty packet with a SUCCESS response as
        // there aren't any keys during the vbucket file creation.
        err = response({}, // key
                       {}, // extra
                       {}, // body
                       PROTOCOL_BINARY_RAW_BYTES,
                       cb::mcbp::Status::Success,
                       0,
                       cookie)
                      ? cb::engine_errc::success
                      : cb::engine_errc::failed;
    } else {
        auto cb = std::make_shared<AllKeysCallback>(collection, count);
        err = engine->getKVBucket()->getROUnderlying(vbid)->getAllKeys(
                vbid, start_key, count, cb);
        if (err == cb::engine_errc::success) {
            err = response({}, // key
                           {}, // extra
                           {cb->getAllKeysPtr(), cb->getAllKeysLen()},
                           PROTOCOL_BINARY_RAW_BYTES,
                           cb::mcbp::Status::Success,
                           0,
                           cookie)
                          ? cb::engine_errc::success
                          : cb::engine_errc::failed;
        }
    }
    engine->addLookupAllKeys(cookie, err);
    engine->notifyIOComplete(cookie, err);
    return false;
}
