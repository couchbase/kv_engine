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
#include "ep_vb.h"
#include "kvstore.h"
#include "vbucket.h"
#include "vbucket_fwd.h"

FrontEndBGFetchItem::FrontEndBGFetchItem(
        std::chrono::steady_clock::time_point initTime,
        ValueFilter filter,
        const void* cookie)
    : BGFetchItem(initTime), cookie(cookie), filter(filter) {
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
    auto* epvb = dynamic_cast<EPVBucket*>(vb.get());
    Expects(epvb);
    epvb->completeCompactionExpiryBgFetch(key, *this);
}

void CompactionBGFetchItem::abort(
        EventuallyPersistentEngine& engine,
        ENGINE_ERROR_CODE status,
        std::map<const void*, ENGINE_ERROR_CODE>& toNotify) const {
    // Do nothing. If we abort a CompactionBGFetch then an item that we may have
    // expire simply won't be expired. The next op/compaction can expire the
    // item if still required.
}

ValueFilter CompactionBGFetchItem::getValueFilter() const {
    // Don't care about values here
    return ValueFilter::KEYS_ONLY;
}

void vb_bgfetch_item_ctx_t::addBgFetch(
        std::unique_ptr<BGFetchItem> itemToFetch) {
    itemToFetch->value = &value;
    bgfetched_list.push_back(std::move(itemToFetch));
}

ValueFilter vb_bgfetch_item_ctx_t::getValueFilter() const {
    // Want to fetch the minimum amount of data:
    // 1. If all requests against this key are meta only; fetch just metadata
    // 2. If all requests are for compressed data, fetch compressed.
    // 3. Otherwise fetch uncompressed value.
    static_assert(ValueFilter::KEYS_ONLY < ValueFilter::VALUES_COMPRESSED);
    static_assert(ValueFilter::VALUES_COMPRESSED <
                  ValueFilter::VALUES_DECOMPRESSED);

    auto overallFilter = ValueFilter::KEYS_ONLY;
    for (const auto& request : bgfetched_list) {
        overallFilter = std::max(overallFilter, request->getValueFilter());
    }
    return overallFilter;
}
