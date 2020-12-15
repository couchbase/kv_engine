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

#include "hash_table.h"

#include "ep_time.h"
#include "item.h"
#include "stats.h"
#include "stored_value_factories.h"

#include <folly/lang/Assume.h>
#include <phosphor/phosphor.h>
#include <platform/compress.h>

#include <logtags.h>
#include <nlohmann/json.hpp>
#include <cstring>

static const ssize_t prime_size_table[] = {
    3, 7, 13, 23, 47, 97, 193, 383, 769, 1531, 3079, 6143, 12289, 24571, 49157,
    98299, 196613, 393209, 786433, 1572869, 3145721, 6291449, 12582917,
    25165813, 50331653, 100663291, 201326611, 402653189, 805306357,
    1610612741, -1
};

/**
 * Define the increment factor for the ProbabilisticCounter being used for
 * the frequency counter.  The value is set such that it allows an 8-bit
 * ProbabilisticCounter to mimic a uint16 counter.
 *
 * The value was reached by running the following code using a variety of
 * incFactor values.
 *
 * ProbabilisticCounter<uint8_t> probabilisticCounter(incFactor);
 * uint64_t iterationCount{0};
 * uint8_t counter{0};
 *     while (counter != std::numeric_limits<uint8_t>::max()) {
 *         counter = probabilisticCounter.generateValue(counter);
 *         iterationCount++;
 *     }
 * std::cerr << "iterationCount=" <<  iterationCount << std::endl;
 *
 * To mimic a uint16 counter, iterationCount needs to be ~65000 (the maximum
 * value of a uint16_t is 65,536).  Through experimentation this was found to be
 * achieved with an incFactor of 0.012.
 */
static const double freqCounterIncFactor = 0.012;

std::string to_string(MutationStatus status) {
    switch (status) {
    case MutationStatus::NotFound:
        return "NotFound";
    case MutationStatus::InvalidCas:
        return "InvalidCas";
    case MutationStatus::WasClean:
        return "WasClean";
    case MutationStatus::WasDirty:
        return "WasDirty";
    case MutationStatus::IsLocked:
        return "IsLocked";
    case MutationStatus::NoMem:
        return "NoMem";
    case MutationStatus::NeedBgFetch:
        return "NeedBgFetch";
    case MutationStatus::IsPendingSyncWrite:
        return "IsPendingSyncWrite";
    }
    return "<invalid>(" + std::to_string(int(status)) + ")";
}

std::ostream& operator<<(std::ostream& os, const HashTable::Position& pos) {
    os << "{lock:" << pos.lock << " bucket:" << pos.hash_bucket << "/" << pos.ht_size << "}";
    return os;
}

HashTable::StoredValueProxy::StoredValueProxy(HashBucketLock&& hbl,
                                              StoredValue* sv,
                                              Statistics& stats)
    : lock(std::move(hbl)),
      value(sv),
      valueStats(stats),
      pre(valueStats.get().prologue(sv)) {
}

HashTable::StoredValueProxy::~StoredValueProxy() {
    if (value) {
        valueStats.get().epilogue(pre, value);
    }
}

void HashTable::StoredValueProxy::setCommitted(CommittedState state) {
    value->setCommitted(state);
    value->markDirty();
    value->setCompletedOrDeletedTime(ep_real_time());
}

StoredValue* HashTable::StoredValueProxy::release() {
    auto* tmp = value;
    value = nullptr;
    return tmp;
}

HashTable::FindUpdateResult::FindUpdateResult(
        HashTable::StoredValueProxy&& prepare,
        StoredValue* committed,
        HashTable& ht)
    : pending(std::move(prepare)), committed(committed), ht(ht) {
}

StoredValue* HashTable::FindUpdateResult::selectSVToModify(bool durability) {
    if (durability) {
        if (pending) {
            return pending.getSV();
        } else {
            return committed;
        }
    } else {
        if (pending && !pending->isCompleted()) {
            return pending.getSV();
        } else {
            return committed;
        }
    }
}

StoredValue* HashTable::FindUpdateResult::selectSVToModify(const Item& itm) {
    return selectSVToModify(itm.isPending());
}

StoredValue* HashTable::FindUpdateResult::selectSVForRead(
        TrackReference trackReference,
        WantsDeleted wantsDeleted,
        ForGetReplicaOp forGetReplicaOp) {
    return ht.selectSVForRead(trackReference,
                              wantsDeleted,
                              forGetReplicaOp,
                              getHBL(),
                              committed,
                              pending.getSV());
}

HashTable::HashTable(EPStats& st,
                     std::unique_ptr<AbstractStoredValueFactory> svFactory,
                     size_t initialSize,
                     size_t locks)
    : initialSize(initialSize),
      size(initialSize),
      mutexes(locks),
      stats(st),
      valFact(std::move(svFactory)),
      visitors(0),
      valueStats(stats),
      numEjects(0),
      numResizes(0),
      maxDeletedRevSeqno(0),
      probabilisticCounter(freqCounterIncFactor) {
    values.resize(size);
    activeState = true;
}

HashTable::~HashTable() {
    Expects(visitors == 0);

    // Use unlocked clear for the destructor, avoids lock inversions on VBucket
    // delete
    clear_UNLOCKED(true);
}

void HashTable::cleanupIfTemporaryItem(const HashBucketLock& hbl,
                                       StoredValue& v) {
    if (v.isTempDeletedItem() || v.isTempNonExistentItem()) {
        unlocked_del(hbl, &v);
    }
}

void HashTable::clear(bool deactivate) {
    if (!deactivate) {
        // If not deactivating, assert we're already active.
        if (!isActive()) {
            throw std::logic_error("HashTable::clear: Cannot call on a "
                    "non-active object");
        }
    }
    MultiLockHolder mlh(mutexes);
    clear_UNLOCKED(deactivate);
}

void HashTable::clear_UNLOCKED(bool deactivate) {
    if (deactivate) {
        setActiveState(false);
    }
    size_t clearedMemSize = 0;
    size_t clearedValSize = 0;
    for (int i = 0; i < (int)size; i++) {
        while (values[i]) {
            // Take ownership of the StoredValue from the vector, update
            // statistics and release it.
            auto v = std::move(values[i]);
            clearedMemSize += v->size();
            clearedValSize += v->valuelen();
            values[i] = std::move(v->getNext());
        }
    }

    stats.coreLocal.get()->currentSize.fetch_sub(clearedMemSize -
                                                 clearedValSize);

    valueStats.reset();
}

static size_t distance(size_t a, size_t b) {
    return std::max(a, b) - std::min(a, b);
}

static size_t nearest(size_t n, size_t a, size_t b) {
    return (distance(n, a) < distance(b, n)) ? a : b;
}

static bool isCurrently(size_t size, ssize_t a, ssize_t b) {
    auto current(static_cast<ssize_t>(size));
    return (current == a || current == b);
}

void HashTable::resize() {
    size_t ni = getNumInMemoryItems();
    int i(0);
    size_t new_size(0);

    // Figure out where in the prime table we are.
    auto target(static_cast<ssize_t>(ni));
    for (i = 0; prime_size_table[i] > 0 && prime_size_table[i] < target; ++i) {
        // Just looking...
    }

    if (prime_size_table[i] == -1) {
        // We're at the end, take the biggest
        new_size = prime_size_table[i-1];
    } else if (prime_size_table[i] < static_cast<ssize_t>(initialSize)) {
        // Was going to be smaller than the initial size.
        new_size = initialSize;
    } else if (0 == i) {
        new_size = prime_size_table[i];
    }else if (isCurrently(size, prime_size_table[i-1], prime_size_table[i])) {
        // If one of the candidate sizes is the current size, maintain
        // the current size in order to remain stable.
        new_size = size;
    } else {
        // Somewhere in the middle, use the one we're closer to.
        new_size = nearest(ni, prime_size_table[i-1], prime_size_table[i]);
    }

    resize(new_size);
}

void HashTable::resize(size_t newSize) {
    if (!isActive()) {
        throw std::logic_error("HashTable::resize: Cannot call on a "
                "non-active object");
    }

    // Due to the way hashing works, we can't fit anything larger than
    // an int.
    if (newSize > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return;
    }

    // Don't resize to the same size, either.
    if (newSize == size) {
        return;
    }

    TRACE_EVENT2(
            "HashTable", "resize", "size", size.load(), "newSize", newSize);

    MultiLockHolder mlh(mutexes);
    if (visitors.load() > 0) {
        // Do not allow a resize while any visitors are actually
        // processing.  The next attempt will have to pick it up.  New
        // visitors cannot start doing meaningful work (we own all
        // locks at this point).
        return;
    }

    // Get a place for the new items.
    table_type newValues(newSize);

    stats.coreLocal.get()->memOverhead.fetch_sub(memorySize());
    ++numResizes;

    // Set the new size so all the hashy stuff works.
    size_t oldSize = size;
    size.store(newSize);

    // Move existing records into the new space.
    for (size_t i = 0; i < oldSize; i++) {
        while (values[i]) {
            // unlink the front element from the hash chain at values[i].
            auto v = std::move(values[i]);
            values[i] = std::move(v->getNext());

            // And re-link it into the correct place in newValues.
            int newBucket = getBucketForHash(v->getKey().hash());
            v->setNext(std::move(newValues[newBucket]));
            newValues[newBucket] = std::move(v);
        }
    }

    // Finally assign the new table to values.
    values = std::move(newValues);

    stats.coreLocal.get()->memOverhead.fetch_add(memorySize());
}

HashTable::FindInnerResult HashTable::findInner(const DocKey& key) {
    if (!isActive()) {
        throw std::logic_error(
                "HashTable::find: Cannot call on a "
                "non-active object");
    }
    HashBucketLock hbl = getLockedBucket(key);
    // Scan through all elements in the hash bucket chain looking for Committed
    // and Pending items with the same key.
    StoredValue* foundCmt = nullptr;
    StoredValue* foundPend = nullptr;
    for (StoredValue* v = values[hbl.getBucketNum()].get().get(); v;
         v = v->getNext().get().get()) {
        if (v->hasKey(key)) {
            if (v->isPending() || v->isCompleted()) {
                Expects(!foundPend);
                foundPend = v;
            } else {
                Expects(!foundCmt);
                foundCmt = v;
            }
        }
    }

    return {std::move(hbl), foundCmt, foundPend};
}

std::unique_ptr<Item> HashTable::getRandomKey(CollectionID cid, long rnd) {
    /* Try to locate a partition */
    size_t start = rnd % size;
    size_t curr = start;
    std::unique_ptr<Item> ret;

    do {
        ret = getRandomKeyFromSlot(cid, curr++);
        if (curr == size) {
            curr = 0;
        }
    } while (ret == nullptr && curr != start);

    return ret;
}

MutationStatus HashTable::set(const Item& val) {
    auto htRes = findForWrite(val.getKey());
    if (htRes.storedValue) {
        return unlocked_updateStoredValue(htRes.lock, *htRes.storedValue, val)
                .status;
    } else {
        unlocked_addNewStoredValue(htRes.lock, val);
        return MutationStatus::WasClean;
    }
}

void HashTable::rollbackItem(const Item& item) {
    auto htRes = findItem(item);
    if (htRes.storedValue) {
        unlocked_updateStoredValue(htRes.lock, *htRes.storedValue, item);
    } else {
        unlocked_addNewStoredValue(htRes.lock, item);
    }
}

HashTable::UpdateResult HashTable::unlocked_updateStoredValue(
        const HashBucketLock& hbl, StoredValue& v, const Item& itm) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_updateStoredValue: htLock "
                "not held");
    }

    if (!isActive()) {
        throw std::logic_error(
                "HashTable::unlocked_updateStoredValue: Cannot "
                "call on a non-active HT object");
    }

    // Can directly replace the existing SV.
    MutationStatus status =
            v.isDirty() ? MutationStatus::WasDirty : MutationStatus::WasClean;

    const auto preProps = valueStats.prologue(&v);

    /* setValue() will mark v as undeleted if required */
    v.setValue(itm);
    updateFreqCounter(v);

    valueStats.epilogue(preProps, &v);

    return {status, &v};
}

HashTable::UpdateResult HashTable::unlocked_replaceValueAndDatatype(
        const HashBucketLock& hbl,
        StoredValue& v,
        std::unique_ptr<Blob> newValue,
        protocol_binary_datatype_t newDT) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_replaceValueAndDatatype: htLock "
                "not held");
    }

    if (!isActive()) {
        throw std::logic_error(
                "HashTable::unlocked_replaceValueAndDatatype: Cannot "
                "call on a non-active HT object");
    }

    const auto preStatus =
            v.isDirty() ? MutationStatus::WasDirty : MutationStatus::WasClean;

    const auto preProps = valueStats.prologue(&v);

    v.setDatatype(newDT);
    v.replaceValue(std::move(newValue));

    valueStats.epilogue(preProps, &v);

    return {preStatus, &v};
}

StoredValue* HashTable::unlocked_addNewStoredValue(const HashBucketLock& hbl,
                                                   const Item& itm) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_addNewStoredValue: htLock "
                "not held");
    }

    if (!isActive()) {
        throw std::invalid_argument(
                "HashTable::unlocked_addNewStoredValue: Cannot "
                "call on a non-active HT object");
    }

    const auto emptyProperties = valueStats.prologue(nullptr);

    // Create a new StoredValue and link it into the head of the bucket chain.
    auto v = (*valFact)(itm, std::move(values[hbl.getBucketNum()]));

    valueStats.epilogue(emptyProperties, v.get().get());

    values[hbl.getBucketNum()] = std::move(v);
    return values[hbl.getBucketNum()].get().get();
}

HashTable::Statistics::StoredValueProperties::StoredValueProperties(
        const StoredValue* sv) {
    // If no previous StoredValue exists; return default constructed object.
    if (sv == nullptr) {
        return;
    }

    // Record all properties of the stored value which statistics require.
    isValid = true;
    size = sv->size();
    metaDataSize = sv->metaDataSize();
    datatype = sv->getDatatype();
    uncompressedSize = sv->uncompressedSize();
    isResident = sv->isResident();
    isDeleted = sv->isDeleted();
    isTempItem = sv->isTempItem();
    isSystemItem = sv->getKey().isInSystemCollection();
    isPreparedSyncWrite = sv->isPending() || sv->isCompleted();
    cid = sv->getKey().getCollectionID();
}

HashTable::Statistics::StoredValueProperties HashTable::Statistics::prologue(
        const StoredValue* v) const {
    return StoredValueProperties(v);
}

struct HashTable::Statistics::CacheLocalStatistics {
    // Many of these stats should logically be NonNegativeCounters,
    // but as they are used inside a LastLevelCacheStore there are
    // multiple copies of each stat; one copy _may_ go negative
    // even though the sum across cores remains non-negative.

    /// Count of alive & deleted, in-memory non-resident and resident
    /// items. Excludes temporary and prepared items.
    std::atomic<ssize_t> numItems;

    /// Count of alive, non-resident items.
    std::atomic<ssize_t> numNonResidentItems;

    /// Count of deleted items.
    std::atomic<ssize_t> numDeletedItems;

    /// Count of items where StoredValue::isTempItem() is true.
    std::atomic<ssize_t> numTempItems;

    /// Count of items where StoredValue resides in system namespace
    std::atomic<ssize_t> numSystemItems;

    /// Count of items where StoredValue is a prepared SyncWrite.
    std::atomic<ssize_t> numPreparedSyncWrites;

    /**
     * Number of documents of a given datatype. Includes alive
     * (non-deleted), committed documents in the HashTable.
     * (Prepared documents are not counted).
     * For value eviction includes resident & non-resident items (as the
     * datatype is part of the metadata), for full-eviction will only
     * include resident items.
     */
    AtomicDatatypeCombo datatypeCounts = {};

    //! Cache size (fixed-length fields in StoredValue + keylen +
    //! valuelen).
    std::atomic<ssize_t> cacheSize = {};

    //! Meta-data size (fixed-length fields in StoredValue + keylen).
    std::atomic<ssize_t> metaDataMemory = {};

    //! Memory consumed by items in this hashtable.
    std::atomic<ssize_t> memSize = {};

    /// Memory consumed if the items were uncompressed.
    std::atomic<ssize_t> uncompressedMemSize = {};
};

HashTable::Statistics::Statistics(EPStats& epStats) : epStats(epStats) {
}

size_t HashTable::Statistics::getNumItems() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.numItems;
    }
    return result;
}

size_t HashTable::Statistics::getNumNonResidentItems() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.numNonResidentItems;
    }
    return result;
}

size_t HashTable::Statistics::getNumDeletedItems() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.numDeletedItems;
    }
    return result;
}

size_t HashTable::Statistics::getNumTempItems() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.numTempItems;
    }
    return result;
}

size_t HashTable::Statistics::getNumSystemItems() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.numSystemItems;
    }
    return result;
}

size_t HashTable::Statistics::getNumPreparedSyncWrites() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.numPreparedSyncWrites;
    }
    return result;
}

HashTable::DatatypeCombo HashTable::Statistics::getDatatypeCounts() const {
    DatatypeCombo result{{0}};
    for (const auto& stripe : llcLocal) {
        for (size_t i = 0; i < stripe.datatypeCounts.size(); ++i) {
            result[i] += stripe.datatypeCounts[i];
        }
    }
    return result;
}

size_t HashTable::Statistics::getCacheSize() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.cacheSize;
    }
    return result;
}

size_t HashTable::Statistics::getMetaDataMemory() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.metaDataMemory;
    }
    return result;
}

size_t HashTable::Statistics::getMemSize() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.memSize;
    }
    return result;
}

size_t HashTable::Statistics::getUncompressedMemSize() const {
    size_t result = 0;
    for (const auto& stripe : llcLocal) {
        result += stripe.uncompressedMemSize;
    }
    return result;
}

void HashTable::Statistics::epilogue(StoredValueProperties pre,
                                     const StoredValue* v) {
    // After performing updates to sv; compare with the previous properties and
    // update all statistics for all properties which have changed.

    const auto post = StoredValueProperties(v);

    auto& local = llcLocal.get();

    // Update size, metadataSize & uncompressed size if pre/post differ.
    if (pre.size != post.size) {
        auto sizeDelta = post.size - pre.size;
        // update per-collection stats
        if (pre.isValid || post.isValid) {
            // either of pre or post may be invalid, but if either is
            // valid use the collection id from that.
            auto cid = pre.isValid ? pre.cid : post.cid;
            auto collectionMemUsed =
                    epStats.coreLocal.get()->collectionMemUsed.lock();
            auto itr = collectionMemUsed->find(cid);
            if (itr != collectionMemUsed->end()) {
                itr->second += sizeDelta;
            }
        }

        local.cacheSize.fetch_add(sizeDelta);
        local.memSize.fetch_add(sizeDelta);
        memChangedCallback(sizeDelta);
    }
    if (pre.metaDataSize != post.metaDataSize) {
        local.metaDataMemory.fetch_add(post.metaDataSize - pre.metaDataSize);
        epStats.coreLocal.get()->currentSize.fetch_add(post.metaDataSize -
                                                       pre.metaDataSize);
    }
    if (pre.uncompressedSize != post.uncompressedSize) {
        local.uncompressedMemSize.fetch_add(post.uncompressedSize -
                                            pre.uncompressedSize);
    }

    // Determine if valid, non resident; and update numNonResidentItems if
    // differ.
    bool preNonResident = pre.isValid && (!pre.isResident && !pre.isDeleted &&
                                          !pre.isTempItem);
    bool postNonResident =
            post.isValid &&
            (!post.isResident && !post.isDeleted && !post.isTempItem);
    if (preNonResident != postNonResident) {
        local.numNonResidentItems.fetch_add(postNonResident - preNonResident);
    }

    if (pre.isTempItem != post.isTempItem) {
        local.numTempItems.fetch_add(post.isTempItem - pre.isTempItem);
    }

    // nonItems only considers valid; non-temporary items:
    bool preNonTemp = pre.isValid && !pre.isTempItem;
    bool postNonTemp = post.isValid && !post.isTempItem;
    if (preNonTemp != postNonTemp) {
        local.numItems.fetch_add(postNonTemp - preNonTemp);
    }

    if (pre.isSystemItem != post.isSystemItem) {
        local.numSystemItems.fetch_add(post.isSystemItem - pre.isSystemItem);
    }

    // numPreparedItems counts valid, prepared (not yet committed) items.
    const bool prePrepared = pre.isValid && pre.isPreparedSyncWrite;
    const bool postPrepared = post.isValid && post.isPreparedSyncWrite;
    if (prePrepared != postPrepared) {
        local.numPreparedSyncWrites.fetch_add(postPrepared - prePrepared);
    }

    // Don't include system items in the deleted count, numSystemItems will
    // count both types (a marked deleted system event still has purpose)
    // Don't include prepared items in the deleted count - they haven't (yet)
    // been deleted.
    const bool preDeleted =
            pre.isDeleted && !pre.isSystemItem && !pre.isPreparedSyncWrite;
    const bool postDeleted =
            post.isDeleted && !post.isSystemItem && !post.isPreparedSyncWrite;
    if (preDeleted != postDeleted) {
        local.numDeletedItems.fetch_add(postDeleted - preDeleted);
    }

    // Update datatypes. These are only tracked for non-temp, non-deleted,
    // committed items.
    if (preNonTemp && !pre.isDeleted && !pre.isPreparedSyncWrite) {
        --local.datatypeCounts[pre.datatype];
    }
    if (postNonTemp && !post.isDeleted && !post.isPreparedSyncWrite) {
        ++local.datatypeCounts[post.datatype];
    }
}

void HashTable::Statistics::setMemChangedCallback(
        std::function<void(int64_t delta)> callback) {
    memChangedCallback = std::move(callback);
}

const std::function<void(int64_t delta)>&
HashTable::Statistics::getMemChangedCallback() const {
    return memChangedCallback;
}

void HashTable::Statistics::reset() {
    for (auto& core : llcLocal) {
        for (auto& entry: core.datatypeCounts) {
            entry = 0;
        }
        core.numItems.store(0);
        core.numTempItems.store(0);
        core.numNonResidentItems.store(0);
        core.memSize.store(0);
        core.cacheSize.store(0);
        core.uncompressedMemSize.store(0);
    }
}

std::pair<StoredValue*, StoredValue::UniquePtr>
HashTable::unlocked_replaceByCopy(const HashBucketLock& hbl,
                                  StoredValue& vToCopy) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_replaceByCopy: htLock "
                "not held");
    }

    if (!isActive()) {
        throw std::invalid_argument(
                "HashTable::unlocked_replaceByCopy: Cannot "
                "call on a non-active HT object");
    }

    /* Release (remove) the StoredValue from the hash table */
    auto releasedSv = unlocked_release(hbl, &vToCopy);

    /* Copy the StoredValue and link it into the head of the bucket chain. */
    auto newSv = valFact->copyStoredValue(
            vToCopy, std::move(values[hbl.getBucketNum()]));

    // Adding a new item into the HashTable; update stats.
    const auto emptyProperties = valueStats.prologue(nullptr);
    valueStats.epilogue(emptyProperties, newSv.get().get());

    values[hbl.getBucketNum()] = std::move(newSv);
    return {values[hbl.getBucketNum()].get().get(), std::move(releasedSv)};
}

HashTable::DeleteResult HashTable::unlocked_softDelete(
        const HashBucketLock& hbl,
        StoredValue& v,
        bool onlyMarkDeleted,
        DeleteSource delSource) {
    switch (v.getCommitted()) {
    case CommittedState::PrepareAborted:
    case CommittedState::PrepareCommitted:
        // We shouldn't be trying to use PrepareCompleted states yet
        throw std::logic_error(
                "HashTable::unlocked_softDelete attempting"
                " to delete a completed prepare");
    case CommittedState::Pending:
    case CommittedState::PreparedMaybeVisible:
    case CommittedState::CommittedViaMutation:
    case CommittedState::CommittedViaPrepare:
        const auto preProps = valueStats.prologue(&v);

        if (onlyMarkDeleted) {
            v.markDeleted(delSource);
        } else {
            v.del(delSource);
            // As part of deleting, set committedState to CommittedViaMutation -
            // this is necessary so when we later queue this SV into
            // CheckpoitnManager, if if was previously CommittedViaPrepare it
            // isn't mis-interpreted for a SyncDelete.
            v.setCommitted(CommittedState::CommittedViaMutation);
        }

        valueStats.epilogue(preProps, &v);
        return {DeletionStatus::Success, &v};
    }
    folly::assume_unreachable();
}

HashTable::DeleteResult HashTable::unlocked_abortPrepare(
        const HashTable::HashBucketLock& hbl, StoredValue& v) {
    const auto preProps = valueStats.prologue(&v);
    // We consider a prepare that is non-resident to be a completed abort.
    v.setCommitted(CommittedState::PrepareAborted);

    // Set the completed time so we don't prematurely purge the SV
    v.setCompletedOrDeletedTime(ep_real_time());
    valueStats.epilogue(preProps, &v);
    return {DeletionStatus::Success, &v};
}

StoredValue::UniquePtr HashTable::unlocked_createSyncDeletePrepare(
        const HashTable::HashBucketLock& hbl,
        const StoredValue& v,
        DeleteSource delSource) {
    auto pendingDel = valFact->copyStoredValue(v, nullptr /*next chain ptr*/);
    pendingDel->setCommitted(CommittedState::Pending);
    pendingDel->del(delSource);
    return pendingDel;
}

StoredValue* HashTable::selectSVForRead(TrackReference trackReference,
                                        WantsDeleted wantsDeleted,
                                        ForGetReplicaOp forGetReplica,
                                        HashTable::HashBucketLock& hbl,
                                        StoredValue* committed,
                                        StoredValue* pending) {
    /// Reading normally uses the Committed StoredValue - however if a
    /// pendingSV is found we must check if it's marked as MaybeVisible -
    /// which will block reading.
    /// However if this request is for a GET_REPLICA then we should only
    /// return committed items
    if (forGetReplica == ForGetReplicaOp::No && pending &&
        pending->isPreparedMaybeVisible()) {
        // Return the pending one as an indication the caller cannot read it.
        return pending;
    }
    auto* sv = committed;

    if (!sv) {
        // No item found - return null.
        return nullptr;
    }

    if (committed->isDeleted()) {
        // Deleted items should only be returned if caller asked for them,
        // and we don't update ref-counts for them.
        return (wantsDeleted == WantsDeleted::Yes) ? sv : nullptr;
    }

    // Found a non-deleted item. Now check if we should update ref-count.
    if (trackReference == TrackReference::Yes) {
        updateFreqCounter(*sv);
    }

    return sv;
}

HashTable::FindROResult HashTable::findForRead(
        const DocKey& key,
        TrackReference trackReference,
        WantsDeleted wantsDeleted,
        const ForGetReplicaOp fetchRequestedForReplicaItem) {
    auto result = findInner(key);
    auto* sv = selectSVForRead(trackReference,
                               wantsDeleted,
                               fetchRequestedForReplicaItem,
                               result.lock,
                               result.committedSV,
                               result.pendingSV);

    return {sv, std::move(result.lock)};
}

HashTable::FindResult HashTable::findForWrite(const DocKey& key,
                                              WantsDeleted wantsDeleted) {
    auto result = findInner(key);

    // We found a prepare. It may have been completed (Ephemeral) though. If it
    // has been completed we will return the committed StoredValue.
    if (result.pendingSV && !result.pendingSV->isCompleted()) {
        // Early return if we found a prepare. We should always return prepares
        // regardless of whether or not they are deleted or the caller has asked
        // for deleted SVs. For example, consider searching for a SyncDelete, we
        // should always return the deleted prepare.
        return {result.pendingSV, std::move(result.lock)};
    }

    /// Writing using the Pending StoredValue (if found), else committed.
    if (!result.committedSV) {
        // No item found - return null.
        return {nullptr, std::move(result.lock)};
    }

    if (result.committedSV->isDeleted() && wantsDeleted == WantsDeleted::No) {
        // Deleted items should only be returned if caller asked for them -
        // otherwise return null.
        return {nullptr, std::move(result.lock)};
    }
    return {result.committedSV, std::move(result.lock)};
}

HashTable::FindResult HashTable::findForSyncWrite(const DocKey& key) {
    auto result = findInner(key);

    if (result.pendingSV) {
        // Early return if we found a prepare. We should always return
        // prepares regardless of whether or not they are deleted or the caller
        // has asked for deleted SVs. For example, consider searching for a
        // SyncDelete, we should always return the deleted prepare. Also,
        // we always return completed prepares.
        return {result.pendingSV, std::move(result.lock)};
    }

    if (!result.committedSV) {
        // No item found - return null.
        return {nullptr, std::move(result.lock)};
    }

    if (result.committedSV->isDeleted()) {
        // Deleted items should only be returned if caller asked for them -
        // otherwise return null.
        return {nullptr, std::move(result.lock)};
    }
    return {result.committedSV, std::move(result.lock)};
}

HashTable::FindResult HashTable::findForSyncReplace(const DocKey& key) {
    auto result = findInner(key);

    if (result.pendingSV) {
        // For the replace case, we should return ENGINE_KEY_ENOENT if no
        // document exists for the given key. For the case where we abort a
        // SyncWrite then attempt to do another we would find the AbortedPrepare
        // (in the Ephemeral case) which we would then use to do another
        // SyncWrite if we called the findForSyncWrite function. So, if we find
        // a completed SyncWrite but the committed StoredValue does not exist,
        // then return nullptr as logically a replace is not possible.
        if (result.pendingSV->isCompleted() && !result.committedSV) {
            return {nullptr, std::move(result.lock)};
        }
        // Otherwise, return the prepare so that we can re-use it.
        return {result.pendingSV, std::move(result.lock)};
    }

    if (!result.committedSV) {
        // No item found - return null.
        return {nullptr, std::move(result.lock)};
    }

    if (result.committedSV->isDeleted()) {
        // Deleted items should only be returned if caller asked for them -
        // otherwise return null.
        return {nullptr, std::move(result.lock)};
    }
    return {result.committedSV, std::move(result.lock)};
}

HashTable::StoredValueProxy HashTable::findForWrite(StoredValueProxy::RetSVPTag,
                                                    const DocKey& key,
                                                    WantsDeleted wantsDeleted) {
    auto result = findForWrite(key, wantsDeleted);
    return StoredValueProxy(
            std::move(result.lock), result.storedValue, valueStats);
}

HashTable::FindUpdateResult HashTable::findForUpdate(const DocKey& key) {
    auto result = findInner(key);

    StoredValueProxy prepare{
            std::move(result.lock), result.pendingSV, valueStats};
    return {std::move(prepare), result.committedSV, *this};
}

HashTable::FindResult HashTable::findOnlyCommitted(const DocKey& key) {
    auto result = findInner(key);
    return {result.committedSV, std::move(result.lock)};
}

HashTable::FindResult HashTable::findOnlyPrepared(const DocKey& key) {
    auto result = findInner(key);
    return {result.pendingSV, std::move(result.lock)};
}

HashTable::FindResult HashTable::findItem(const Item& item) {
    auto result = findInner(item.getKey());
    auto preparedNamespace = item.isPending() || item.isAbort();
    return {preparedNamespace ? result.pendingSV : result.committedSV,
            std::move(result.lock)};
}

void HashTable::unlocked_del(const HashBucketLock& hbl, StoredValue* value) {
    unlocked_release(hbl, value).reset();
}

StoredValue::UniquePtr HashTable::unlocked_release(
        const HashBucketLock& hbl,
        StoredValue* valueToRelease) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_release_base: htLock not held");
    }

    if (!isActive()) {
        throw std::logic_error(
                "HashTable::unlocked_release_base: Cannot call on a "
                "non-active object");
    }
    // Remove the first (should only be one) StoredValue matching the given
    // pointer
    auto released = hashChainRemoveFirst(
            values[hbl.getBucketNum()], [valueToRelease](const StoredValue* v) {
                return v == valueToRelease;
            });

    if (!released) {
        /* We shouldn't reach here, we must delete the StoredValue in the
           HashTable */
        throw std::logic_error(
                "HashTable::unlocked_release_base: StoredValue to be released "
                "not found in HashTable; possibly HashTable leak");
    }

    // Update statistics for the item which is now gone.
    const auto preProps = valueStats.prologue(released.get().get());
    valueStats.epilogue(preProps, nullptr);

    return released;
}

MutationStatus HashTable::insertFromWarmup(const Item& itm,
                                           bool eject,
                                           bool keyMetaDataOnly,
                                           EvictionPolicy evictionPolicy) {
    auto htRes = findInner(itm.getKey());
    auto* v = (itm.isCommitted() ? htRes.committedSV : htRes.pendingSV);
    auto& hbl = htRes.lock;

    if (v == nullptr) {
        v = unlocked_addNewStoredValue(hbl, itm);

        // TODO: Would be faster if we just skipped creating the value in the
        // first place instead of adding it to the Item and then discarding it
        // in markNotResident.
        if (keyMetaDataOnly) {
            const auto preProps = valueStats.prologue(v);
            v->markNotResident();
            valueStats.epilogue(preProps, v);
        }
    } else {
        if (keyMetaDataOnly) {
            // We don't have a better error code ;)
            return MutationStatus::InvalidCas;
        }

        // Existing item found. This should only occur if:
        // a) The existing item is temporary (i.e. result of a front-end
        //    thread attempting to read and triggered a bgFetch); or
        // b) The existing item is non-temporary and was loaded as the result of
        //    a previous BGfetch (and has the same CAS).
        //
        // Verify that the CAS isn't changed
        if (v->getCas() != itm.getCas()) {
            if (v->getCas() == 0) {
                v->setCas(itm.getCas());
                v->setFlags(itm.getFlags());
                v->setExptime(itm.getExptime());
                v->setRevSeqno(itm.getRevSeqno());
            } else {
                return MutationStatus::InvalidCas;
            }
        }

        // CAS is equal - exact same item. Update the SV if it's not already
        // resident.
        if (!v->isResident()) {
            Expects(unlocked_restoreValue(hbl.getHTLock(), itm, *v));
        }
    }

    v->markClean();

    if (eject && !keyMetaDataOnly) {
        unlocked_ejectItem(hbl, v, evictionPolicy);
    }

    return MutationStatus::NotFound;
}

bool HashTable::reallocateStoredValue(StoredValue&& sv) {
    // Search the chain and reallocate
    for (StoredValue::UniquePtr* curr =
                 &values[getBucketForHash(sv.getKey().hash())];
         curr->get().get();
         curr = &curr->get()->getNext()) {
        if (&sv == curr->get().get()) {
            auto newSv = valFact->copyStoredValue(sv, std::move(sv.getNext()));
            curr->swap(newSv);
            return true;
        }
    }
    return false;
}

void HashTable::dump() const {
    std::cerr << *this << std::endl;
}

nlohmann::json HashTable::dumpStoredValuesAsJson() const {
    MultiLockHolder mlh(mutexes);
    auto obj = nlohmann::json::array();
    for (const auto& chain : values) {
        if (chain) {
            for (StoredValue* sv = chain.get().get(); sv != nullptr;
                 sv = sv->getNext().get().get()) {
                std::stringstream ss;
                ss << sv->getKey();
                obj.push_back(*sv);
            }
        }
    }

    return obj;
}

void HashTable::storeCompressedBuffer(std::string_view buf, StoredValue& v) {
    const auto preProps = valueStats.prologue(&v);

    v.storeCompressedBuffer(buf);

    valueStats.epilogue(preProps, &v);
}

void HashTable::visit(HashTableVisitor& visitor) {
    HashTable::Position ht_pos;
    while (ht_pos != endPosition()) {
        ht_pos = pauseResumeVisit(visitor, ht_pos);
    }
}

void HashTable::visitDepth(HashTableDepthVisitor &visitor) {
    if (valueStats.getNumItems() == 0 || !isActive()) {
        return;
    }
    size_t visited = 0;
    // Acquire one (any) of the mutexes before incrementing {visitors}, this
    // prevents any race between this visitor and the HashTable resizer.
    // See comments in pauseResumeVisit() for further details.
    std::unique_lock<std::mutex> lh(mutexes[0]);
    VisitorTracker vt(&visitors);
    lh.unlock();

    for (int l = 0; l < static_cast<int>(mutexes.size()); l++) {
        for (int i = l; i < static_cast<int>(size); i+= mutexes.size()) {
            // (re)acquire mutex on each HashBucket, to minimise any impact
            // on front-end threads.
            LockHolder lh(mutexes[l]);

            size_t depth = 0;
            StoredValue* p = values[i].get().get();
            if (p) {
                // TODO: Perf: This check seems costly - do we think it's still
                // worth keeping?
                auto hashbucket = getBucketForHash(p->getKey().hash());
                if (i != hashbucket) {
                    throw std::logic_error("HashTable::visit: inconsistency "
                            "between StoredValue's calculated hashbucket "
                            "(which is " + std::to_string(hashbucket) +
                            ") and bucket it is located in (which is " +
                            std::to_string(i) + ")");
                }
            }
            size_t mem(0);
            while (p) {
                depth++;
                mem += p->size();
                p = p->getNext().get().get();
            }
            visitor.visit(i, depth, mem);
            ++visited;
        }
    }
}

HashTable::Position HashTable::pauseResumeVisit(HashTableVisitor& visitor,
                                                Position& start_pos) {
    if ((valueStats.getNumItems() + valueStats.getNumTempItems()) == 0 ||
        !isActive()) {
        // Nothing to visit
        return endPosition();
    }

    bool paused = false;

    // To attempt to minimize the impact the visitor has on normal frontend
    // operations, we deliberately acquire (and release) the mutex between
    // each hash_bucket - see `lh` in the inner for() loop below. This means we
    // hold a given mutex for a large number of short durations, instead of just
    // one single, long duration.
    // *However*, there is a potential race with this approach - the {size} of
    // the HashTable may be changed (by the Resizer task) between us first
    // reading it to calculate the starting hash_bucket, and then reading it
    // inside the inner for() loop. To prevent this race, we explicitly acquire
    // (any) mutex, increment {visitors} and then release the mutex. This
    //avoids the race as if visitors >0 then Resizer will not attempt to resize.
    std::unique_lock<std::mutex> lh(mutexes[0]);
    VisitorTracker vt(&visitors);
    lh.unlock();

    // Start from the requested lock number if in range.
    size_t lock = (start_pos.lock < mutexes.size()) ? start_pos.lock : 0;
    size_t hash_bucket = 0;

    for (; isActive() && !paused && lock < mutexes.size(); lock++) {

        // If the bucket position is *this* lock, then start from the
        // recorded bucket (as long as we haven't resized).
        hash_bucket = lock;
        if (start_pos.lock == lock &&
            start_pos.ht_size == size &&
            start_pos.hash_bucket < size) {
            hash_bucket = start_pos.hash_bucket;
        }

        // Iterate across all values in the hash buckets owned by this lock.
        // Note: we don't record how far into the bucket linked-list we
        // pause at; so any restart will begin from the next bucket.
        for (; !paused && hash_bucket < size; hash_bucket += mutexes.size()) {
            visitor.setUpHashBucketVisit();

            // HashBucketLock scope. If a visitor needs additional locking
            // around the HashBucket visit then we need to release it before
            // tearDownHashBucketVisit() is called.
            {
                HashBucketLock lh(hash_bucket, mutexes[lock]);

                StoredValue* v = values[hash_bucket].get().get();
                while (!paused && v) {
                    StoredValue* tmp = v->getNext().get().get();
                    paused = !visitor.visit(lh, *v);
                    v = tmp;
                }
            }

            visitor.tearDownHashBucketVisit();
        }

        // If the visitor paused us before we visited all hash buckets owned
        // by this lock, we don't want to skip the remaining hash buckets, so
        // stop the outer for loop from advancing to the next lock.
        if (paused && hash_bucket < size) {
            break;
        }

        // Finished all buckets owned by this lock. Set hash_bucket to 'size'
        // to give a consistent marker for "end of lock".
        hash_bucket = size;
    }

    // Return the *next* location that should be visited.
    return HashTable::Position(size, lock, hash_bucket);
}

HashTable::Position HashTable::endPosition() const  {
    return HashTable::Position(size, mutexes.size(), size);
}

bool HashTable::unlocked_ejectItem(const HashTable::HashBucketLock&,
                                   StoredValue*& vptr,
                                   EvictionPolicy policy) {
    if (vptr == nullptr) {
        throw std::invalid_argument("HashTable::unlocked_ejectItem: "
                "Unable to delete NULL StoredValue");
    }

    if (!vptr->eligibleForEviction(policy)) {
        ++stats.numFailedEjects;
        return false;
    }

    const auto preProps = valueStats.prologue(vptr);

    switch (policy) {
    case EvictionPolicy::Value: {
        vptr->ejectValue();
        ++stats.numValueEjects;
        valueStats.epilogue(preProps, vptr);
        break;
    }
    case EvictionPolicy::Full: {
        // Remove the item from the hash table.
        int bucket_num = getBucketForHash(vptr->getKey().hash());
        auto removed = hashChainRemoveFirst(
                values[bucket_num],
                [vptr](const StoredValue* v) { return v == vptr; });

        if (removed->isResident()) {
            ++stats.numValueEjects;
        }
        valueStats.epilogue(preProps, nullptr);

        updateMaxDeletedRevSeqno(vptr->getRevSeqno());
        break;
    }
    }

    ++numEjects;
    return true;
}

std::unique_ptr<Item> HashTable::getRandomKeyFromSlot(CollectionID cid,
                                                      int slot) {
    auto lh = getLockedBucket(slot);
    for (StoredValue* v = values[slot].get().get(); v;
            v = v->getNext().get().get()) {
        if (!v->isTempItem() && !v->isDeleted() && v->isResident() &&
            v->isCommitted() && v->getKey().getCollectionID() == cid) {
            return v->toItem(Vbid(0));
        }
    }

    return nullptr;
}

bool HashTable::unlocked_restoreValue(
        const std::unique_lock<std::mutex>& htLock,
        const Item& itm,
        StoredValue& v) {
    if (!htLock || !isActive() || v.isResident()) {
        return false;
    }

    const auto preProps = valueStats.prologue(&v);

    v.restoreValue(itm);

    valueStats.epilogue(preProps, &v);

    return true;
}

void HashTable::unlocked_restoreMeta(const std::unique_lock<std::mutex>& htLock,
                                     const Item& itm,
                                     StoredValue& v) {
    if (!htLock) {
        throw std::invalid_argument(
                "HashTable::unlocked_restoreMeta: htLock "
                "not held");
    }

    if (!isActive()) {
        throw std::logic_error(
                "HashTable::unlocked_restoreMeta: Cannot "
                "call on a non-active HT object");
    }

    const auto preProps = valueStats.prologue(&v);

    v.restoreMeta(itm);

    valueStats.epilogue(preProps, &v);
}

uint8_t HashTable::generateFreqValue(uint8_t counter) {
    return probabilisticCounter.generateValue(counter);
}

void HashTable::updateFreqCounter(StoredValue& v) {
    // Attempt to increment the storedValue frequency counter
    // value.  Because a probabilistic counter is used the new
    // value will either be the same or an increment of the
    // current value.
    auto updatedFreqCounterValue = generateFreqValue(v.getFreqCounterValue());
    v.setFreqCounterValue(updatedFreqCounterValue);

    if (updatedFreqCounterValue == std::numeric_limits<uint8_t>::max()) {
        // Invoke the registered callback function which
        // wakeups the ItemFreqDecayer task.
        frequencyCounterSaturated();
    }
}

std::ostream& operator<<(std::ostream& os, const HashTable& ht) {
    os << "HashTable[" << &ht << "] with"
       << " numItems:" << ht.getNumItems()
       << " numInMemory:" << ht.getNumInMemoryItems()
       << " numDeleted:" << ht.getNumDeletedItems()
       << " numNonResident:" << ht.getNumInMemoryNonResItems()
       << " numTemp:" << ht.getNumTempItems()
       << " numSystemItems:" << ht.getNumSystemItems()
       << " numPreparedSW:" << ht.getNumPreparedSyncWrites()
       << " values: " << std::endl;
    for (const auto& chain : ht.values) {
        if (chain) {
            for (StoredValue* sv = chain.get().get(); sv != nullptr;
                 sv = sv->getNext().get().get()) {
                os << "    " << *sv << std::endl;
            }
        }
    }
    return os;
}
