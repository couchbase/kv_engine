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
#include "kvstore/kvstore_priv.h"
#include "magma-kvstore_metadata.h"

class BucketLogger;

class MagmaRequest : public IORequest {
public:
    /**
     * Constructor
     *
     * @param item Item instance to be persisted
     * @param callback Persistence Callback
     * @param logger Used for logging
     */
    MagmaRequest(queued_item it, std::shared_ptr<BucketLogger> logger);

    magmakv::MetaData& getDocMeta() {
        return docMeta;
    }

    Vbid getVbID() const {
        return Vbid(docMeta.vbid);
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

    size_t getMetaSize() const {
        return sizeof(magmakv::MetaData);
    }

    void markOldItemExists() {
        itemOldExists = true;
    }

    bool oldItemExists() const {
        return itemOldExists;
    }

    void markOldItemIsDelete() {
        itemOldIsDelete = true;
    }

    bool oldItemIsDelete() const {
        return itemOldIsDelete;
    }

    std::string to_string();

private:
    magmakv::MetaData docMeta;
    value_t docBody;
    bool itemOldExists{false};
    bool itemOldIsDelete{false};
};
