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

#pragma once

#include "callbacks.h"
#include "diskdockey.h"
#include "item.h"
#include "trace_helpers.h"
#include "vbucket_fwd.h"

#include <list>
#include <unordered_map>

enum class ValueFilter;

/**
 * Base BGFetch context class
 */
class BGFetchItem {
public:
    BGFetchItem(std::chrono::steady_clock::time_point initTime)
        : initTime(initTime) {
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
            cb::engine_errc status,
            std::map<const void*, cb::engine_errc>& toNotify) const = 0;

    /// @returns The ValueFilter for this request.
    virtual ValueFilter getValueFilter() const = 0;

    /**
     * Pointer to the result of the BGFetch. Set by the vb_bgfetch_item_ctx_t
     * when this BGFetchItem is added to the set of outstanding items.
     */
    GetValue* value{nullptr};
    const std::chrono::steady_clock::time_point initTime;
};

/**
 * BGFetch context class for a front end driven BG Fetch (i.e. for a Get or
 * SetWithMeta etc.).
 */
class FrontEndBGFetchItem : public BGFetchItem {
public:
    FrontEndBGFetchItem(const void* cookie, ValueFilter filter)
        : FrontEndBGFetchItem(
                  std::chrono::steady_clock::now(), filter, cookie) {
    }

    FrontEndBGFetchItem(std::chrono::steady_clock::time_point initTime,
                        ValueFilter filter,
                        const void* cookie);

    void complete(EventuallyPersistentEngine& engine,
                  VBucketPtr& vb,
                  std::chrono::steady_clock::time_point startTime,
                  const DiskDocKey& key) const override;

    void abort(EventuallyPersistentEngine& engine,
               cb::engine_errc status,
               std::map<const void*, cb::engine_errc>& toNotify) const override;

    ValueFilter getValueFilter() const override {
        return filter;
    }

    const void* cookie;
    cb::tracing::SpanId traceSpanId;
    ValueFilter filter;
};

/**
 * BGFetch context class for a compaction driven BG Fetch (for if we need to
 * pull a non-resident item into memory to see if we should expire it).
 */
class CompactionBGFetchItem : public BGFetchItem {
public:
    explicit CompactionBGFetchItem(const Item& item)
        : BGFetchItem(std::chrono::steady_clock::now()), compactionItem(item) {
    }

    void complete(EventuallyPersistentEngine& engine,
                  VBucketPtr& vb,
                  std::chrono::steady_clock::time_point startTime,
                  const DiskDocKey& key) const override;

    void abort(EventuallyPersistentEngine& engine,
               cb::engine_errc status,
               std::map<const void*, cb::engine_errc>& toNotify) const override;

    ValueFilter getValueFilter() const override;

    /**
     * We copy the entire Item here because if we need to expire (delete) the
     * Item then we need the value as well to keep the xattrs.
     */
    Item compactionItem;
};

/**
 * Tracks all outstanding requests for a single document to be fetched from
 * disk.
 * Multiple requests (from different clients) can be enqueued against a single
 * key; the document will only be fetched from disk once; then the result
 * will be returned to each client which requested it.
 */
class vb_bgfetch_item_ctx_t {
public:
    using FetchList = std::list<std::unique_ptr<BGFetchItem>>;
    /**
     * Add a request to fetch an item from disk to the set of outstanding
     * BGfetches.
     */
    void addBgFetch(std::unique_ptr<BGFetchItem> itemToFetch);

    /// @returns The list of outstanding fetch requests.
    const FetchList& getRequests() const {
        return bgfetched_list;
    }

    /**
     * @returns the ValueFilter to be used when fetching the item for the set
     * of outstanding fetch requests.
     */
    ValueFilter getValueFilter() const;

    /// Result the fetch.
    GetValue value;

private:
    /// List of outstanding requests for a given key.
    FetchList bgfetched_list;
};
