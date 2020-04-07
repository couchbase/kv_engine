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

#include "item.h"
#include "kvstore_priv.h"
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

    void markRequestFailed() {
        reqFailed = true;
    }

    bool requestFailed() const {
        return reqFailed;
    }

    std::string to_string();

private:
    magmakv::MetaData docMeta;
    value_t docBody;
    bool itemOldExists{false};
    bool itemOldIsDelete{false};
    bool reqFailed{false};
};
