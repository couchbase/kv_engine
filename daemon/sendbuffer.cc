/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "sendbuffer.h"

#include "buckets.h"

ItemSendBuffer::ItemSendBuffer(cb::unique_item_ptr itm,
                               std::string_view view,
                               Bucket& bucket)
    : SendBuffer(view), item(std::move(itm)), bucket(bucket) {
    bucket.items_in_transit++;
}

ItemSendBuffer::~ItemSendBuffer() {
    item.reset();
    bucket.items_in_transit--;
}
