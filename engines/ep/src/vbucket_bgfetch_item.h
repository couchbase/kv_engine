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

#pragma once

#include "callbacks.h"
#include "diskdockey.h"
#include "item.h"
#include "objectregistry.h"
#include "trace_helpers.h"
#include "vbucket_fwd.h"

#include <list>
#include <unordered_map>

enum class GetMetaOnly;

/**
 * Base BGFetch context class
 */
class BGFetchItem {
public:
    BGFetchItem(GetValue* value, std::chrono::steady_clock::time_point initTime)
        : value(value), initTime(initTime) {
    }

    virtual ~BGFetchItem() = default;

    /**
     * Complete the BG Fetch.
     *
     * @param engine EPEngine instance
     * @param vb VBucket pointer
     * @param startTime Time at which we started the BG Fetch
     * @param key Key of the item
     */
    virtual void complete(EventuallyPersistentEngine& engine,
                          VBucketPtr& vb,
                          std::chrono::steady_clock::time_point startTime,
                          const DiskDocKey& key) const = 0;

    /**
     * Abort the BG Fetch and add it to the toNotify map.
     *
     * @param engine EPEngine instance
     * @param status Status code of abort
     * @param [out]toNotify Map to add cookie and status code to
     */
    virtual void abort(
            EventuallyPersistentEngine& engine,
            ENGINE_ERROR_CODE status,
            std::map<const void*, ENGINE_ERROR_CODE>& toNotify) const = 0;

    /**
     * @return Should we BGFetch just the metadata?
     */
    virtual bool metaDataOnly() const = 0;

    GetValue* value;
    const std::chrono::steady_clock::time_point initTime;
};

/**
 * BGFetch context class for a front end driven BG Fetch (i.e. for a Get or
 * SetWithMeta etc.).
 */
class FrontEndBGFetchItem : public BGFetchItem {
public:
    FrontEndBGFetchItem(const void* cookie, bool metaOnly)
        : FrontEndBGFetchItem(
                  nullptr, std::chrono::steady_clock::now(), metaOnly, cookie) {
    }

    FrontEndBGFetchItem(GetValue* value,
                        std::chrono::steady_clock::time_point initTime,
                        bool metaOnly,
                        const void* cookie);

    void complete(EventuallyPersistentEngine& engine,
                  VBucketPtr& vb,
                  std::chrono::steady_clock::time_point startTime,
                  const DiskDocKey& key) const override;

    void abort(
            EventuallyPersistentEngine& engine,
            ENGINE_ERROR_CODE status,
            std::map<const void*, ENGINE_ERROR_CODE>& toNotify) const override;

    bool metaDataOnly() const override {
        return metaOnly;
    }

    const void* cookie;
    cb::tracing::SpanId traceSpanId;
    bool metaOnly;
};

/**
 * BGFetch context class for a compaction driven BG Fetch (for if we need to
 * pull a non-resident item into memory to see if we should expire it).
 */
class CompactionBGFetchItem : public BGFetchItem {
public:
    explicit CompactionBGFetchItem(const Item& item)
        : BGFetchItem(nullptr /*GetValue*/, std::chrono::steady_clock::now()),
          compactionItem(item) {
    }

    void complete(EventuallyPersistentEngine& engine,
                  VBucketPtr& vb,
                  std::chrono::steady_clock::time_point startTime,
                  const DiskDocKey& key) const override;

    void abort(
            EventuallyPersistentEngine& engine,
            ENGINE_ERROR_CODE status,
            std::map<const void*, ENGINE_ERROR_CODE>& toNotify) const override;

    bool metaDataOnly() const override {
        // Don't care about values here
        return true;
    }

    /**
     * We copy the entire Item here because if we need to expire (delete) the
     * Item then we need the value as well to keep the xattrs.
     */
    Item compactionItem;
};

struct vb_bgfetch_item_ctx_t {
    std::list<std::unique_ptr<BGFetchItem>> bgfetched_list;
    GetMetaOnly isMetaOnly;
    GetValue value;
};
