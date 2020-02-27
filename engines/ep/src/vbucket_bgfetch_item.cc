/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "vbucket_bgfetch_item.h"

#include "ep_engine.h"
#include "vbucket.h"
#include "vbucket_fwd.h"

FrontEndBGFetchItem::FrontEndBGFetchItem(
        GetValue* value,
        std::chrono::steady_clock::time_point initTime,
        bool metaOnly,
        const void* cookie)
    : BGFetchItem(value, initTime), cookie(cookie), metaOnly(metaOnly) {
    auto* traceable = cookie2traceable(cookie);
    if (traceable && traceable->isTracingEnabled()) {
        NonBucketAllocationGuard guard;
        traceSpanId = traceable->getTracer().begin(
                cb::tracing::Code::BackgroundWait, initTime);
    }
}

void FrontEndBGFetchItem::complete(
        EventuallyPersistentEngine& engine,
        VBucketPtr& vb,
        std::chrono::steady_clock::time_point startTime,
        const DiskDocKey& key) const {
    ENGINE_ERROR_CODE status =
            vb->completeBGFetchForSingleItem(key, *this, startTime);
    engine.notifyIOComplete(cookie, status);
}

void FrontEndBGFetchItem::abort(
        EventuallyPersistentEngine& engine,
        ENGINE_ERROR_CODE status,
        std::map<const void*, ENGINE_ERROR_CODE>& toNotify) const {
    toNotify[cookie] = status;
    engine.storeEngineSpecific(cookie, nullptr);
}

void CompactionBGFetchItem::complete(
        EventuallyPersistentEngine& engine,
        VBucketPtr& vb,
        std::chrono::steady_clock::time_point startTime,
        const DiskDocKey& key) const {
    // @TODO implement
}

void CompactionBGFetchItem::abort(
        EventuallyPersistentEngine& engine,
        ENGINE_ERROR_CODE status,
        std::map<const void*, ENGINE_ERROR_CODE>& toNotify) const {
    // Do nothing. If we abort a CompactionBGFetch then an item that we may have
    // expire simply won't be expired. The next op/compaction can expire the
    // item if still required.
}
