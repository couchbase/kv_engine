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

#include "item.h"
#include "kv_magma_common/magma-kvstore_metadata.h"
#include "kvstore/kvstore_priv.h"

class BucketLogger;

class MagmaRequest : public IORequest {
public:
    /**
     * Constructor
     *
     * @param item Item instance to be persisted
     * @param callback Persistence Callback
     */
    MagmaRequest(queued_item it);

    const std::string_view getDocMeta() const {
        return docMeta;
    }

    size_t getRawKeyLen() const {
        return key.size();
    }

    const char* getRawKey() const {
        return reinterpret_cast<const char*>(key.data());
    }

    size_t getBodySize() const {
        return docBody ? docBody->valueSize() : 0;
    }

    char* getBodyData() const {
        return docBody ? const_cast<char*>(docBody->getData()) : nullptr;
    }

    void markOldItemAlive() {
        oldItemAlive = true;
    }

    bool isOldItemAlive() const {
        return oldItemAlive;
    }

    void markLogicalInsert() {
        logicalInsert = true;
    }

    bool isLogicalInsert() const {
        return logicalInsert;
    }

    void markNewDocReflectedInDiskSize() {
        newDocReflectedInDiskSize = true;
    }

    bool isNewDocReflectedInDiskSize() const {
        return newDocReflectedInDiskSize;
    }

    std::string to_string();

    size_t getDocSize() const {
        return getBodySize() + getRawKeyLen() + getDocMeta().size();
    }

private:
    std::string docMeta;
    value_t docBody;

    // Is there an old item which is alive? i.e. this item is replacing
    // a non-deleted item.
    bool oldItemAlive{false};

    // Is this item a logical insert? (i.e. an insert into a new genereation
    // of a collection but the item previously existed in an old generation).
    // We could achieve the same affect by setting oldItemAlive above to be true
    // but this gives us the ability to detect this case in NexusKVStore and
    // handle it which is important as couchstore does not care about logical
    // inserts due to the differences in item counting.
    bool logicalInsert{false};

    // Were disk size stats updated to reflect the _new_ item size by this
    // request?
    // They may need adjusting if an item is later compressed.
    bool newDocReflectedInDiskSize{false};
};
