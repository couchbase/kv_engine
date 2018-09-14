/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "config.h"

#include "callbacks.h"
#include "item.h"
#include "trace_helpers.h"

enum class GetMetaOnly;

class VBucketBGFetchItem {
public:
    VBucketBGFetchItem(const void* c, bool meta_only)
        : VBucketBGFetchItem(nullptr, c, ProcessClock::now(), meta_only) {
    }

    VBucketBGFetchItem(GetValue* value_,
                       const void* c,
                       ProcessClock::time_point init_time,
                       bool meta_only)
        : value(value_),
          cookie(c),
          initTime(init_time),
          metaDataOnly(meta_only) {
        if (cookie) {
            TRACE_BEGIN(cookie, cb::tracing::TraceCode::BG_WAIT, init_time);
        }
    }

    GetValue* value;
    const void* cookie;
    ProcessClock::time_point initTime;
    bool metaDataOnly;
};

struct vb_bgfetch_item_ctx_t {
    std::list<std::unique_ptr<VBucketBGFetchItem>> bgfetched_list;
    GetMetaOnly isMetaOnly;
    GetValue value;
};

using vb_bgfetch_queue_t =
        std::unordered_map<StoredDocKey, vb_bgfetch_item_ctx_t>;
using bgfetched_item_t = std::pair<StoredDocKey, const VBucketBGFetchItem*>;
