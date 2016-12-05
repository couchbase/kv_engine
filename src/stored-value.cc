/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "config.h"
#include <platform/cb_malloc.h>

#include "stored-value.h"

#include "hash_table.h"

double StoredValue::mutation_mem_threshold = 0.9;
const int64_t StoredValue::state_deleted_key = -3;
const int64_t StoredValue::state_non_existent_key = -4;
const int64_t StoredValue::state_temp_init = -5;

bool StoredValue::ejectValue(HashTable &ht, item_eviction_policy_t policy) {
    if (eligibleForEviction(policy)) {
        reduceCacheSize(ht, value->length());
        markNotResident();
        value = NULL;
        return true;
    }
    return false;
}

void StoredValue::referenced() {
    if (nru > MIN_NRU_VALUE) {
        --nru;
    }
}

void StoredValue::setNRUValue(uint8_t nru_val) {
    if (nru_val <= MAX_NRU_VALUE) {
        nru = nru_val;
    }
}

uint8_t StoredValue::incrNRUValue() {
    uint8_t ret = MAX_NRU_VALUE;
    if (nru < MAX_NRU_VALUE) {
        ret = ++nru;
    }
    return ret;
}

uint8_t StoredValue::getNRUValue() {
    return nru;
}

bool StoredValue::unlocked_restoreValue(Item *itm, HashTable &ht) {
    if (isResident() || isDeleted()) {
        return false;
    }

    if (isTempInitialItem()) { // Regular item with the full eviction
        --ht.numTempItems;
        ++ht.numItems;
        newCacheItem = false; // set it back to false as we created a temp item
                              // by setting it to true when bg fetch is
                              // scheduled (full eviction mode).
    } else {
        ht.decrNumNonResidentItems();
    }

    if (isTempInitialItem()) {
        cas = itm->getCas();
        flags = itm->getFlags();
        exptime = itm->getExptime();
        revSeqno = itm->getRevSeqno();
        bySeqno = itm->getBySeqno();
        nru = INITIAL_NRU_VALUE;
    }
    deleted = false;
    value = itm->getValue();
    increaseCacheSize(ht, value->length());
    return true;
}

bool StoredValue::unlocked_restoreMeta(Item *itm, ENGINE_ERROR_CODE status,
                                       HashTable &ht) {
    if (!isTempInitialItem()) {
        return true;
    }

    switch(status) {
    case ENGINE_SUCCESS:
        cas = itm->getCas();
        flags = itm->getFlags();
        exptime = itm->getExptime();
        revSeqno = itm->getRevSeqno();
        if (itm->isDeleted()) {
            setDeleted();
        } else { // Regular item with the full eviction
            --ht.numTempItems;
            ++ht.numItems;
            ++ht.numNonResidentItems;
            bySeqno = itm->getBySeqno();
            newCacheItem = false; // set it back to false as we created a temp
                                  // item by setting it to true when bg fetch is
                                  // scheduled (full eviction mode).
        }
        if (nru == MAX_NRU_VALUE) {
            nru = INITIAL_NRU_VALUE;
        }
        return true;
    case ENGINE_KEY_ENOENT:
        setNonExistent();
        return true;
    default:
        LOG(EXTENSION_LOG_WARNING,
            "The underlying storage returned error %d for get_meta\n", status);
        return false;
    }
}

void StoredValue::setMutationMemoryThreshold(double memThreshold) {
    if (memThreshold > 0.0 && memThreshold <= 1.0) {
        mutation_mem_threshold = memThreshold;
    }
}

void StoredValue::increaseCacheSize(HashTable &ht, size_t by) {
    ht.cacheSize.fetch_add(by);
    ht.memSize.fetch_add(by);
}

void StoredValue::reduceCacheSize(HashTable &ht, size_t by) {
    ht.cacheSize.fetch_sub(by);
    ht.memSize.fetch_sub(by);
}

void StoredValue::increaseMetaDataSize(HashTable &ht, EPStats &st, size_t by) {
    ht.metaDataMemory.fetch_add(by);
    st.currentSize.fetch_add(by);
}

void StoredValue::reduceMetaDataSize(HashTable &ht, EPStats &st, size_t by) {
    ht.metaDataMemory.fetch_sub(by);
    st.currentSize.fetch_sub(by);
}

/**
 * Is there enough space for this thing?
 */
bool StoredValue::hasAvailableSpace(EPStats &st, const Item &itm,
                                    bool isReplication) {
    double newSize = static_cast<double>(st.getTotalMemoryUsed() +
                                         sizeof(StoredValue) + itm.getKey().size());
    double maxSize = static_cast<double>(st.getMaxDataSize());
    if (isReplication) {
        return newSize <= (maxSize * st.replicationThrottleThreshold);
    } else {
        return newSize <= (maxSize * mutation_mem_threshold);
    }
}

Item* StoredValue::toItem(bool lck, uint16_t vbucket) const {
    Item* itm = new Item(getDocKey(), getFlags(), getExptime(), value,
                         lck ? static_cast<uint64_t>(-1) : getCas(),
                         bySeqno, vbucket, getRevSeqno());

    itm->setNRUValue(nru);

    if (deleted) {
        itm->setDeleted();
    }

    return itm;
}

Item* StoredValue::toValuelessItem(uint16_t vbucket) const {
    return new Item(getDocKey(), getFlags(),
                    getExptime(), NULL /* valuePtr */, 0 /* valuelen */,
                    NULL /* ext_meta*/, 0 /* ext_len */, getCas(),
                    getBySeqno(), vbucket, getRevSeqno());
}

void StoredValue::reallocate() {
    // Allocate a new Blob for this stored value; copy the existing Blob to
    // the new one and free the old.
    value_t new_val(Blob::Copy(*value));
    value.reset(new_val);
}
