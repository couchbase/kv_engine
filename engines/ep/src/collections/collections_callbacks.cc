/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/collections_callbacks.h"
#include "kv_bucket.h"
#include "vbucket.h"

namespace Collections {
namespace VB {

void LogicallyDeletedCallback::callback(CacheLookup& lookup) {
    VBucketPtr vb = store.getVBucket(lookup.getVBucketId());
    if (!vb) {
        return;
    }
    // Check with collections if this key should be loaded, status EEXISTS is
    // the only way to inform the scan to not continue with this key.
    if (vb->lockCollections().isLogicallyDeleted(
                lookup.getKey(), lookup.getBySeqno(), lookup.getSeparator())) {
        setStatus(ENGINE_KEY_EEXISTS);
        return;
    }
}

} // end namespace VB
} // end namespace Collections