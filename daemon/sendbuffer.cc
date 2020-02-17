/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "sendbuffer.h"

#include "buckets.h"

ItemSendBuffer::ItemSendBuffer(cb::unique_item_ptr itm,
                               std::string_view view,
                               Bucket& bucket)
    : SendBuffer(view), item(std::move(itm)), bucket(bucket) {
    // We need to bump a reference to the bucket, because if we try to tear
    // down the bucket and disconnect clients it might try to release the
    // objects _AFTER_ the bucket started its deletion (because we decrement
    // the reference
    std::lock_guard<std::mutex> guard(bucket.mutex);
    bucket.clients++;
}

ItemSendBuffer::~ItemSendBuffer() {
    item.reset();
    std::lock_guard<std::mutex> guard(bucket.mutex);
    bucket.clients--;
    if (bucket.clients == 0 && bucket.state == Bucket::State::Destroying) {
        bucket.cond.notify_one();
    }
}
