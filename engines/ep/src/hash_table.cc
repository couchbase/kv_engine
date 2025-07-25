/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "hash_table.h"

#include "ep_time.h"
#include "item.h"
#include "multi_lock_holder.h"
#include "stats.h"
#include "stored_value_factories.h"
#include <folly/lang/Assume.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <utilities/logtags.h>
#include <chrono>
#include <cstring>
#include <utility>

static const std::array<uint32_t, 30> prime_size_table{
        {3,        7,         13,        23,        47,        97,
         193,      383,       769,       1531,      3079,      6143,
         12289,    24571,     49157,     98299,     196613,    393209,
         786433,   1572869,   3145721,   6291449,   12582917,  25165813,
         50331653, 100663291, 201326611, 402653189, 805306357, 1610612741}};

static size_t hashToBucket(uint32_t hash, size_t tableSize) {
    return hash % tableSize;
}

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
    os << (pos.table == HashTable::WhichTable::Primary
                   ? "{table:primary,lock:"
                   : "{table:temporary,lock:")
       << pos.lock << ",bucket:" << pos.hash_bucket << '/' << pos.ht_size
       << '}';
    return os;
}

std::string to_string(const HashTable::Position& pos) {
    std::ostringstream oss;
    oss << pos;
    return oss.str();
}

HashTable::StoredValueProxy::StoredValueProxy(HashBucketLock&& hbl,
                                              StoredValue* sv)
    : lock(std::move(hbl)), value(sv) {
    if (!lock.getHTLock()) {
        throw std::invalid_argument(
                "StoredValueProxy::StoredValueProxy: htLock not held");
    }
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
        }
        return committed;
    }
    if (pending && !pending->isPrepareCompleted()) {
        return pending.getSV();
    }
    return committed;
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

uint8_t HashTable::defaultGetInitialMFU() {
    return Item::initialFreqCount;
}

HashTable::HashTable(EPStats& st,
                     std::unique_ptr<AbstractStoredValueFactory> svFactory,
                     size_t initialSize,
                     size_t locks,
                     double freqCounterIncFactor,
                     size_t defaultTempItemsAllowedPercent,
                     std::function<uint8_t()> getInitialMFU,
                     ShouldTrackMFUCallback shouldTrackMfuCallback)
    : minimumSize([initialSize]() { return initialSize; }),
      tempItemsAllowedPercent([defaultTempItemsAllowedPercent]() {
          return defaultTempItemsAllowedPercent;
      }),
      valuesSize(initialSize),
      values(initialSize),
      mutexes(locks),
      resizingMutexes(locks),
      stats(st),
      valFact(std::move(svFactory)),
      valueStats(stats, std::move(shouldTrackMfuCallback)),
      numEjects(0),
      numResizes(0),
      maxDeletedRevSeqno(0),
      probabilisticCounter(freqCounterIncFactor),
      getInitialMFU(std::move(getInitialMFU)) {
    activeState = true;
    stats.coreLocal.get()->memOverhead += getMemoryOverhead();
}

HashTable::~HashTable() {
    std::unique_lock lh(visitorMutex, std::try_to_lock);
    Expects(lh.owns_lock());

    stats.coreLocal.get()->memOverhead -= getMemoryOverhead();

    // Use unlocked clear for the destructor, avoids lock inversions on VBucket
    // delete
    clear_UNLOCKED(true);
}

bool HashTable::cleanupIfTemporaryItem(const HashBucketLock& hbl,
                                       StoredValue& v) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::cleanupIfTemporaryItem: htLock not held");
    }
    if (v.isTempDeletedItem() || v.isTempNonExistentItem()) {
        unlocked_del(hbl, v);
        return true;
    }
    return false;
}

void HashTable::clear(bool deactivate) {
    if (!deactivate) {
        // If not deactivating, assert we're already active.
        if (!isActive()) {
            throw std::logic_error("HashTable::clear: Cannot call on a "
                    "non-active object");
        }
    }
    MultiLockHolder mlh1(mutexes);
    MultiLockHolder mlh2(resizingMutexes);
    clear_UNLOCKED(deactivate);
}

void HashTable::clear_UNLOCKED(bool deactivate) {
    if (deactivate) {
        setActiveState(false);
    }

    // Account collection sizes so we can adjust the collection mem_used
    std::unordered_map<CollectionID, size_t> memUsedAdjustment;
    for (auto* table : {&values, &resizingTemporaryValues}) {
        for (auto& chain : *table) {
            if (chain) {
                for (StoredValue* sv = chain.get().get(); sv != nullptr;
                     sv = sv->getNext().get().get()) {
                    auto [itr, emplaced] = memUsedAdjustment.try_emplace(
                            sv->getKey().getCollectionID(), sv->size());
                    if (!emplaced) {
                        itr->second += sv->size();
                    }
                }
            }

            chain.reset();
        }
    }

    for (const auto& [cid, size] : memUsedAdjustment) {
        // Note: can't capture a structured binding, but can if we explicitly
        // copy, these are just u32 and u64 types so copy is fine. c++20 fixes
        stats.coreLocal.get()->collectionMemUsed.withLock(
                [cid = cid, size = size](auto& map) {
                    auto itr = map.find(cid);
                    if (itr != map.end()) {
                        itr->second -= size;
                    }
                });
    }

    const auto metadataMemory = valueStats.getMetaDataMemory();
    stats.coreLocal.get()->currentSize -= metadataMemory;
    valueStats.reset();
}

static size_t distance(size_t a, size_t b) {
    return std::max(a, b) - std::min(a, b);
}

static size_t nearest(size_t n, size_t a, size_t b) {
    return (distance(n, a) < distance(b, n)) ? a : b;
}

size_t HashTable::getPreferredSize(
        cb::time::steady_clock::duration delay) const {
    const size_t minSize = minimumSize();
    const size_t numItems = getNumInMemoryItems();
    const size_t currSize = getSize();

    // Figure out where in the prime table we are.
    const auto candidate = std::ranges::find_if(
            prime_size_table,
            [numItems](auto prime) { return prime >= numItems; });

    size_t newSize;

    if (candidate == prime_size_table.end()) {
        // We're at the end, take the biggest
        return prime_size_table.back();
    }
    if (*candidate < minSize) {
        // Was going to be smaller than the minimum size.
        newSize = minSize;
    } else if (candidate == prime_size_table.begin()) {
        newSize = *candidate;
    } else if (currSize == *(candidate - 1) || currSize == *candidate) {
        // If one of the candidate sizes is the current size, maintain
        // the current size in order to remain stable.
        return currSize;
    } else {
        // Somewhere in the middle, use the one we're closer to.
        newSize = nearest(numItems, *(candidate - 1), *candidate);
    }

    if (newSize < currSize) {
        auto duration = cb::time::steady_clock::now() - getLastResizeTime();
        // Don't resize down if the delay interval has not passed since last
        // resize.
        if (duration < delay) {
            return currSize;
        }
    }

    return newSize;
}

NeedsRevisit HashTable::resizeInOneStep(size_t newSize) {
    if (!isActive()) {
        throw std::logic_error(
                "HashTable::resizeInOneStep: Cannot call on a "
                "non-active object");
    }

    // Due to the way hashing works, we can't fit anything larger than
    // an int.
    if (newSize == 0 ||
        newSize > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw std::invalid_argument("HashTable::resizeInOneStep: newSize:" +
                                    std::to_string(newSize));
    }

    // ht_temp_items_allowed_percent could have changed & we would want that
    // to take effect always even if the size hasn't changed.
    auto updateTempItemsAllowedOnExit =
            folly::makeGuard([this]() { updateNumTempItemsAllowed(); });

    // Don't resize to the same size, either.
    if (newSize == getSize()) {
        return NeedsRevisit::No;
    }

    std::unique_lock visitorLH(visitorMutex, std::try_to_lock);
    if (!visitorLH.owns_lock()) {
        // Do not allow a resize while any visitors are currently
        // processing. The next attempt will have to pick it up.
        return NeedsRevisit::YesLater;
    }
    if (getResizeInProgress() != ResizeAlgo::None) {
        return NeedsRevisit::No;
    }
    resizeInProgress = ResizeAlgo::OneStep;

    TRACE_EVENT2("HashTable",
                 "resizeInOneStep",
                 "size",
                 getSize(),
                 "newSize",
                 newSize);
    MultiLockHolder mlh(mutexes);

    // Get a place for the new items.
    table_type newValues(newSize);

    stats.coreLocal.get()->memOverhead +=
            static_cast<int64_t>(getMemoryOverhead(newSize)) -
            static_cast<int64_t>(getMemoryOverhead());
    ++numResizes;

    // Set the new size so all the hashy stuff works.
    size_t oldSize = getSize();
    valuesSize.store(newSize);

    // Move existing records into the new space.
    for (size_t i = 0; i < oldSize; i++) {
        while (values[i]) {
            // unlink the front element from the hash chain at values[i].
            auto v = std::move(values[i]);
            values[i] = std::move(v->getNext());

            // And re-link it into the correct place in newValues.
            const auto newBucket = hashToBucket(v->getKey().hash(), newSize);
            v->setNext(std::move(newValues[newBucket]));
            newValues[newBucket] = std::move(v);
        }
    }

    // Finally assign the new table to values.
    values = std::move(newValues);

    resizeInProgress.store(ResizeAlgo::None, std::memory_order_release);

    // Record resize completion time
    lastResizeTime = cb::time::steady_clock::now();

    return NeedsRevisit::No;
}

NeedsRevisit HashTable::beginIncrementalResize(size_t newSize) {
    if (!isActive()) {
        throw std::logic_error(
                "HashTable::beginIncrementalResize: Cannot call on a "
                "non-active object");
    }

    // Due to the way hashing works, we can't fit anything larger than
    // an int.
    if (newSize == 0 ||
        newSize > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw std::invalid_argument(
                "HashTable::beginIncrementalResize: newSize:" +
                std::to_string(newSize));
    }

    // ht_temp_items_allowed_percent could have changed & we would want that
    // to take effect always even if the size hasn't changed.
    auto updateTempItemsAllowedOnExit =
            folly::makeGuard([this]() { updateNumTempItemsAllowed(); });

    // Don't resize to the same size, either.
    if (newSize == getSize()) {
        return NeedsRevisit::No;
    }

    std::unique_lock visitorLH(visitorMutex, std::try_to_lock);
    if (!visitorLH.owns_lock()) {
        // Do not allow a resize while any visitors are currently
        // processing. The next attempt will have to pick it up.
        return NeedsRevisit::YesLater;
    }
    if (getResizeInProgress() != ResizeAlgo::None) {
        return NeedsRevisit::No;
    }
    // memory_order_acquire to pair with memory_order_release
    Expects(0 == locksMovedForResizing.load(std::memory_order_acquire));

    resizingTemporaryValues = table_type(newSize);
    stats.coreLocal.get()->memOverhead +=
            static_cast<int64_t>(getMemoryOverhead(newSize)) -
            static_cast<int64_t>(getMemoryOverhead());
    ++numResizes;
    nextSize.store(newSize);
    // Other value updates should not overtake resize-in-progress indication
    resizeInProgress.store(ResizeAlgo::Incremental, std::memory_order_release);
    return NeedsRevisit::YesNow;
}

NeedsRevisit HashTable::continueIncrementalResize() {
    if (!isActive()) {
        throw std::logic_error(
                "HashTable::continueResize: Cannot call on a "
                "non-active object");
    }
    // ht_temp_items_allowed_percent could have changed & we would want that
    // to take effect always even if the size hasn't changed.
    auto updateTempItemsAllowedOnExit =
            folly::makeGuard([this]() { updateNumTempItemsAllowed(); });
    // Declared here to deallocate outside the critical section
    table_type sortedByLock;

    std::unique_lock visitorLH(visitorMutex, std::try_to_lock);
    if (!visitorLH.owns_lock()) {
        return NeedsRevisit::YesLater;
    }
    switch (getResizeInProgress()) {
    case ResizeAlgo::None:
        return NeedsRevisit::No;
    case ResizeAlgo::OneStep:
        return NeedsRevisit::No;
    case ResizeAlgo::Incremental:
        break;
    }

    // memory_order_relaxed as we are holding a unique lock
    const size_t currSize = getSize();
    const size_t nextSize = this->nextSize.load();
    const size_t lock = locksMovedForResizing.load(std::memory_order_relaxed);
    Expects(nextSize != 0);

    TRACE_EVENT2(
            "HashTable", "continueResize", "newSize", nextSize, "lock", lock);

    if (lock == mutexes.size()) {
        // All buckets moved. Acquire all locks and replace the Primary table
        // with the ResizingTemporary where the items were moved to.

        MultiLockHolder mlh1(mutexes);
        MultiLockHolder mlh2(resizingMutexes);

        values.swap(resizingTemporaryValues);

        valuesSize.store(nextSize);
        this->nextSize.store(0);
        // All locks held, so we can relax the memory order
        locksMovedForResizing.store(0, std::memory_order_release);
        // Other value updates should not overtake resize-in-progress indication
        resizeInProgress.store(ResizeAlgo::None, std::memory_order_release);

        // Move to deallocate outside the critical section
        resizingTemporaryValues.swap(sortedByLock);

        // Record resize completion time
        lastResizeTime = cb::time::steady_clock::now();

        return NeedsRevisit::No;
    }

    Expects(lock < mutexes.size());
    sortedByLock.resize(mutexes.size());
    std::lock_guard primaryLH(mutexes[lock]);

    // Take items from the buckets of the Primary table that are under the
    // current lock
    for (size_t bucket = lock; bucket < currSize; bucket += mutexes.size()) {
        auto& head = values[bucket];
        while (head) {
            // Unlink the front element from the hash chain
            auto v = std::move(head);
            head = std::move(v->getNext());

            // Insert into sortedByLock
            const auto newBucket = hashToBucket(v->getKey().hash(), nextSize);
            auto& sortedHead = sortedByLock[getMutexForBucket(newBucket)];
            v->setNext(std::move(sortedHead));
            sortedHead = std::move(v);
        }
    }

    // We sorted the items taken from the primary table according to which lock
    // needs to be held to insert them into the temporary table. We will insert
    // them one lock at a time, and keep holding the lock until all the
    // corresponding items are moved, hence avoiding re-locking for each item.

    // Insert back items into the ResizingTemporary table
    for (size_t tempLock = 0; tempLock < sortedByLock.size(); ++tempLock) {
        std::lock_guard tempLH(resizingMutexes[tempLock]);

        auto& head = sortedByLock[tempLock];
        while (head) {
            // Unlink the front element from the hash chain
            auto v = std::move(head);
            head = std::move(v->getNext());

            // Insert into temporary table
            const auto newBucket = hashToBucket(v->getKey().hash(), nextSize);
            Expects(getMutexForBucket(newBucket) == tempLock);
            auto& nextHead = resizingTemporaryValues[newBucket];
            v->setNext(std::move(nextHead));
            nextHead = std::move(v);
        }
    }

    // `locksMovedForResizing` determines which table we look in. This also
    // determines which lock will be acquired. Hence, when updated, a thread may
    // acquire a different lock from the one held here. We need to make sure
    // that all previous writes are visible to that thread before the update.
    locksMovedForResizing.store(lock + 1, std::memory_order_release);
    return NeedsRevisit::YesNow;
}

size_t HashTable::getMutexForBucket(size_t bucketNum) const {
    if (!isActive()) {
        throw std::logic_error(
                "HashTable::getMutexForBucket: "
                "Cannot call on a non-active object");
    }
    return static_cast<uint32_t>(bucketNum) %
           static_cast<uint32_t>(mutexes.size());
}

HashTable::Position HashTable::getPositionForHash(uint32_t hash) const {
    // memory_order_relaxed as the value needs to be checked under lock
    const size_t currSize = getSize();
    const size_t primaryBucket = hashToBucket(hash, currSize);
    const size_t primaryMutex = getMutexForBucket(primaryBucket);
    // While size is mutated only when _all_ locks are held, locks moved may be
    // mutated under a lock other than the one we computed. On some relaxed
    // memory models this may mean that we compute the right bucket and mutex
    // index, acquire a lock, but because it's different from the one held
    // while updating the threshold, we don't see all the updates.
    // On x86, load acquire and store release are implemented as normal reads
    // and writes due to TSO.
    if (primaryMutex >= locksMovedForResizing.load(std::memory_order_acquire)) {
        return {WhichTable::Primary, currSize, primaryMutex, primaryBucket};
    }
    // Min 1 to avoid mod by 0. If actual value is 0 then we are not resizing.
    const size_t nextSize = std::max(size_t(1), this->nextSize.load());
    const size_t tempBucket = hashToBucket(hash, nextSize);
    const size_t tempMutex = getMutexForBucket(tempBucket);
    return {WhichTable::ResizingTemporary, nextSize, tempMutex, tempBucket};
}

HashTable::HashBucketLock HashTable::getLockedBucket(size_t idx) {
    Position position(
            WhichTable::Primary, getSize(), getMutexForBucket(idx), idx);
    return {position, mutexes[position.lock]};
}

HashTable::HashBucketLock HashTable::getLockedBucketForHash(uint32_t hash) {
    for (;;) {
        const auto position = getPositionForHash(hash);
        HashBucketLock hbl(position,
                           (position.table == WhichTable::Primary)
                                   ? mutexes[position.lock]
                                   : resizingMutexes[position.lock]);
        if (position == getPositionForHash(hash)) {
            return hbl;
        }
    }
}

const HashTable::table_type::value_type& HashTable::unlocked_getBucket(
        const Position& position) const {
    switch (position.table) {
    case WhichTable::Primary:
        return values[position.hash_bucket];
    case WhichTable::ResizingTemporary:
        return resizingTemporaryValues[position.hash_bucket];
    }
    throw std::logic_error(
            "HashTable::unlocked_getBucket: invalid WhichTable:" +
            std::to_string(int(position.table)));
}

HashTable::FindInnerResult HashTable::findInner(const DocKeyView& key) {
    if (!isActive()) {
        throw std::logic_error(
                "HashTable::find: Cannot call on a "
                "non-active object");
    }
    HashBucketLock hbl = getLockedBucket(key);
    auto rv = unlocked_find(hbl, key);
    return {std::move(hbl), rv.committedSV, rv.pendingSV};
}

HashTable::UnlockedFindResult HashTable::unlocked_find(
        const HashBucketLock& hbl, const DocKeyView& key) const {
    // Scan through all elements in the hash bucket chain looking for Committed
    // and Pending items with the same key.
    StoredValue* foundCmt = nullptr;
    StoredValue* foundPend = nullptr;
    for (StoredValue* v = unlocked_getBucket(hbl).get().get(); v;
         v = v->getNext().get().get()) {
        if (v->hasKey(key)) {
            if (v->isPending() || v->isPrepareCompleted()) {
                Expects(!foundPend);
                foundPend = v;
            } else {
                Expects(!foundCmt);
                foundCmt = v;
            }
        }
    }

    return {foundCmt, foundPend};
}

HashTable::RandomKeyVisitor::RandomKeyVisitor(size_t size, uint32_t random)
    : random(random) {
    setup(size);
}

size_t HashTable::RandomKeyVisitor::getNextBucket() {
    if (currentBucket == currentSize) {
        currentBucket = 0;
    }
    ++bucketsVisited;
    return currentBucket++;
}

bool HashTable::RandomKeyVisitor::visitComplete() const {
    return bucketsVisited >= currentSize;
}

bool HashTable::RandomKeyVisitor::maybeReset(size_t size) {
    if (size != currentSize) {
        setup(size);
        return true;
    }
    return false;
}

void HashTable::RandomKeyVisitor::setup(size_t size) {
    if (size == 0) {
        throw std::invalid_argument(
                "HashTable::RandomKeyVisitor::setup size must not be 0");
    }

    currentSize = size;
    currentBucket = random % static_cast<uint32_t>(currentSize);
    bucketsVisited = 0;
}

std::unique_ptr<Item> HashTable::getRandomKey(CollectionID cid, uint32_t rnd) {
    return getRandomKey(cid, RandomKeyVisitor{getSize(), rnd});
}

std::unique_ptr<Item> HashTable::getRandomKey(CollectionID cid,
                                              RandomKeyVisitor visitor) {
    std::unique_ptr<Item> ret;

    do {
        auto lh = getLockedBucket(visitor.getNextBucket());

        // Now we have a lock, ask the visitor if it needs to reset, if it does
        // we must skip the call to getRandomKey
        if (visitor.maybeReset(getSize())) {
            continue;
        }

        ret = getRandomKey(cid, lh);
    } while (ret == nullptr && !visitor.visitComplete());

    return ret;
}

MutationStatus HashTable::set(const Item& val) {
    auto htRes = findForWrite(val.getKey());
    if (htRes.storedValue) {
        return unlocked_updateStoredValue(htRes.lock, *htRes.storedValue, val)
                .status;
    }
    unlocked_addNewStoredValue(htRes.lock, val);
    return MutationStatus::WasClean;
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

    const auto preProps = valueStats.prologue(hbl, &v);

    /* setValue() will mark v as undeleted if required */
    v.setValue(itm);
    updateFreqCounter(hbl, v);

    valueStats.epilogue(hbl, preProps, &v);

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

    const auto preProps = valueStats.prologue(hbl, &v);

    v.setDatatype(newDT);
    v.replaceValue(std::move(newValue));

    valueStats.epilogue(hbl, preProps, &v);

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

    const auto emptyProperties = valueStats.prologue(hbl, nullptr);

    // Create a new StoredValue and link it into the head of the bucket chain.
    auto v = (*valFact)(itm, std::move(unlocked_getBucket(hbl)));

    if (auto initialMFU = itm.getFreqCounterValue()) {
        v->setFreqCounterValue(*initialMFU);
    } else {
        // initialise the MFU to a value derived from the distribution of the
        // MFU of existing items
        v->setFreqCounterValue(getInitialMFU());
    }

    valueStats.epilogue(hbl, emptyProperties, v.get().get());

    auto& ret = unlocked_getBucket(hbl);
    ret = std::move(v);
    return ret.get().get();
}

HashTable::Statistics::StoredValueProperties::StoredValueProperties(
        const HashTable::HashBucketLock& lh,
        const StoredValue* sv,
        const ShouldTrackMFUCallback& shouldTrackMfuCallback) {
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
    isSystemItem = sv->getKey().isInSystemEventCollection();
    isPreparedSyncWrite = sv->isPending() || sv->isPrepareCompleted();
    cid = sv->getKey().getCollectionID();
    shouldTrackMFU = shouldTrackMfuCallback(lh, *sv);
    freqCounter = sv->getFreqCounterValue();
}

HashTable::Statistics::StoredValueProperties HashTable::Statistics::prologue(
        const HashTable::HashBucketLock& hbl, const StoredValue* v) const {
    return StoredValueProperties(hbl, v, shouldTrackMfuCallback);
}

HashTable::Statistics::Statistics(EPStats& epStats,
                                  ShouldTrackMFUCallback callback)
    : shouldTrackMfuCallback(std::move(callback)), epStats(epStats) {
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

void HashTable::Statistics::recordMFU(uint8_t mfu) {
    evictableMFUHist.add(mfu);
}

void HashTable::Statistics::removeMFU(uint8_t mfu) {
    evictableMFUHist.remove(mfu);
}

void HashTable::Statistics::epilogue(const HashTable::HashBucketLock& hbl,
                                     StoredValueProperties pre,
                                     const StoredValue* v) {
    // After performing updates to sv; compare with the previous properties and
    // update all statistics for all properties which have changed.

    const auto post = StoredValueProperties(hbl, v, shouldTrackMfuCallback);

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

        local.memSize.fetch_add(sizeDelta);
        memChangedCallback(sizeDelta);
    }
    if (pre.metaDataSize != post.metaDataSize) {
        local.metaDataMemory.fetch_add(post.metaDataSize - pre.metaDataSize);
        epStats.coreLocal.get()->currentSize +=
                post.metaDataSize - pre.metaDataSize;
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

    // the evictability of the value may have changed (e.g., if the value
    // has been made dirty or non-resident), or the MFU itself may have
    // changed.
    if ((pre.shouldTrackMFU != post.shouldTrackMFU) ||
        (pre.freqCounter != post.freqCounter)) {
        if (pre.shouldTrackMFU) {
            // remove the old MFU from the MFU histogram
            evictableMFUHist.remove(pre.freqCounter);
        }
        if (post.shouldTrackMFU) {
            // add the new MFU to the MFU histogram
            evictableMFUHist.add(post.freqCounter);
        }
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

bool HashTable::Statistics::shouldTrackMFU(const HashTable::HashBucketLock& lh,
                                           const StoredValue& v) const {
    return shouldTrackMfuCallback(lh, v);
}

void HashTable::Statistics::reset() {
    // We are about to reset stats to 0, memory usage should be changing
    // accordingly, so call the mem changed callback with the new amount.
    getMemChangedCallback()(-getMemSize());

    for (auto& core : llcLocal) {
        core = {};
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
    auto releasedSv = unlocked_release(hbl, vToCopy);

    /* Copy the StoredValue and link it into the head of the bucket chain. */
    auto newSv = valFact->copyStoredValue(vToCopy,
                                          std::move(unlocked_getBucket(hbl)));

    // Adding a new item into the HashTable; update stats.
    const auto emptyProperties = valueStats.prologue(hbl, nullptr);
    valueStats.epilogue(hbl, emptyProperties, newSv.get().get());

    auto& ret = unlocked_getBucket(hbl);
    ret = std::move(newSv);
    return {ret.get().get(), std::move(releasedSv)};
}

HashTable::DeleteResult HashTable::unlocked_softDelete(
        const HashBucketLock& hbl,
        StoredValue& v,
        bool onlyMarkDeleted,
        DeleteSource delSource) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_softDelete: htLock not held");
    }
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
        const auto preProps = valueStats.prologue(hbl, &v);

        if (onlyMarkDeleted) {
            v.markDeleted(delSource);
        } else {
            v.del(delSource);
        }

        // As part of deleting, set committedState to CommittedViaMutation -
        // this is necessary so when we later queue this SV into
        // CheckpoitnManager, if if was previously CommittedViaPrepare it
        // isn't mis-interpreted for a SyncDelete.
        v.setCommitted(CommittedState::CommittedViaMutation);

        valueStats.epilogue(hbl, preProps, &v);
        return {DeletionStatus::Success, &v};
    }
    folly::assume_unreachable();
}

HashTable::DeleteResult HashTable::unlocked_abortPrepare(
        const HashTable::HashBucketLock& hbl, StoredValue& v) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_abortPrepare: htLock not held");
    }
    const auto preProps = valueStats.prologue(hbl, &v);
    // We consider a prepare that is non-resident to be a completed abort.
    v.setCommitted(CommittedState::PrepareAborted);

    // Set the completed time so we don't prematurely purge the SV
    v.setCompletedOrDeletedTime(ep_real_time());
    valueStats.epilogue(hbl, preProps, &v);
    return {DeletionStatus::Success, &v};
}

StoredValue::UniquePtr HashTable::unlocked_createSyncDeletePrepare(
        const HashTable::HashBucketLock& hbl,
        const StoredValue& v,
        DeleteSource delSource) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_createSyncDeletePrepare: htLock not held");
    }
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
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::selectSVForRead: htLock not held");
    }
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
        updateFreqCounter(hbl, *sv);
    }

    return sv;
}

HashTable::FindROResult HashTable::findForRead(
        const DocKeyView& key,
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

HashTable::FindResult HashTable::findForWrite(const DocKeyView& key,
                                              WantsDeleted wantsDeleted) {
    auto result = findInner(key);

    // We found a prepare. It may have been completed (Ephemeral) though. If it
    // has been completed we will return the committed StoredValue.
    if (result.pendingSV && !result.pendingSV->isPrepareCompleted()) {
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

HashTable::FindResult HashTable::findForSyncWrite(const DocKeyView& key) {
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

HashTable::FindResult HashTable::findForSyncReplace(const DocKeyView& key) {
    auto result = findInner(key);

    if (result.pendingSV) {
        // For the replace case, we should return cb::engine_errc::no_such_key
        // if no document exists for the given key. For the case where we abort
        // a SyncWrite then attempt to do another we would find the
        // AbortedPrepare (in the Ephemeral case) which we would then use to do
        // another SyncWrite if we called the findForSyncWrite function. So, if
        // we find a completed SyncWrite but the committed StoredValue does not
        // exist, then return nullptr as logically a replace is not possible.
        if (result.pendingSV->isPrepareCompleted() && !result.committedSV) {
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

HashTable::FindUpdateResult HashTable::findForUpdate(const DocKeyView& key) {
    auto result = findInner(key);

    StoredValueProxy prepare{std::move(result.lock), result.pendingSV};
    return {std::move(prepare), result.committedSV, *this};
}

HashTable::FindResult HashTable::findOnlyCommitted(const DocKeyView& key) {
    auto result = findInner(key);
    return {result.committedSV, std::move(result.lock)};
}

HashTable::FindResult HashTable::findOnlyPrepared(const DocKeyView& key) {
    auto result = findInner(key);
    return {result.pendingSV, std::move(result.lock)};
}

HashTable::FindResult HashTable::findItem(const Item& item) {
    auto result = findInner(item.getKey());
    auto preparedNamespace = item.isPending() || item.isAbort();
    return {preparedNamespace ? result.pendingSV : result.committedSV,
            std::move(result.lock)};
}

void HashTable::unlocked_del(const HashBucketLock& hbl,
                             const StoredValue& value) {
    unlocked_release(hbl, value).reset();
}

StoredValue::UniquePtr HashTable::unlocked_release(
        const HashBucketLock& hbl, const StoredValue& valueToRelease) {
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
    auto released = hashChainRemove(unlocked_getBucket(hbl), valueToRelease);

    if (!released) {
        /* We shouldn't reach here, we must delete the StoredValue in the
           HashTable */
        throw std::logic_error(
                "HashTable::unlocked_release_base: StoredValue to be released "
                "not found in HashTable; possibly HashTable leak");
    }

    // Update statistics for the item which is now gone.
    const auto preProps = valueStats.prologue(hbl, released.get().get());
    valueStats.epilogue(hbl, preProps, nullptr);

    return released;
}

MutationStatus HashTable::upsertItem(const Item& itm,
                                     bool eject,
                                     bool keyMetaDataOnly,
                                     EvictionPolicy evictionPolicy) {
    auto htRes = findInner(itm.getKey());
    auto* v = (itm.isCommitted() ? htRes.committedSV : htRes.pendingSV);
    auto& hbl = htRes.lock;

    if (v == nullptr) {
        v = unlocked_addNewStoredValue(hbl, itm);
        v->markClean();

        // TODO: Would be faster if we just skipped creating the value in the
        // first place instead of adding it to the Item and then discarding it
        // in markNotResident.
        if (keyMetaDataOnly) {
            const auto preProps = valueStats.prologue(hbl, v);
            v->markNotResident();
            valueStats.epilogue(hbl, preProps, v);
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
            Expects(unlocked_restoreValue(hbl, itm, *v));
            v->markClean();
        }
    }

    if (eject && !keyMetaDataOnly) {
        unlocked_ejectItem(hbl, v, evictionPolicy, false);
    }

    return MutationStatus::NotFound;
}

bool HashTable::reallocateStoredValue(const HashBucketLock& hbl,
                                      StoredValue&& sv) {
    // Search the chain and reallocate
    for (StoredValue::UniquePtr* curr = &unlocked_getBucket(hbl);
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
    MultiLockHolder mlh1(mutexes);
    MultiLockHolder mlh2(resizingMutexes);
    auto obj = nlohmann::json::array();
    for (const auto* table : {&values, &resizingTemporaryValues}) {
        for (const auto& chain : *table) {
            if (chain) {
                for (StoredValue* sv = chain.get().get(); sv != nullptr;
                     sv = sv->getNext().get().get()) {
                    obj.push_back(*sv);
                }
            }
        }
    }
    return obj;
}

void HashTable::storeCompressedBuffer(const HashBucketLock& hbl,
                                      std::string_view buf,
                                      StoredValue& v) {
    const auto preProps = valueStats.prologue(hbl, &v);

    v.storeCompressedBuffer(buf);

    valueStats.epilogue(hbl, preProps, &v);
}

void HashTable::visit(HashTableVisitor& visitor) {
    HashTable::Position currPos;
    while (currPos != endPosition()) {
        const auto nextPos = pauseResumeVisit(visitor, currPos);
        if (nextPos == currPos) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        currPos = nextPos;
    }
}

void HashTable::visitDepth(HashTableDepthVisitor &visitor) {
    if (valueStats.getNumItems() == 0 || !isActive()) {
        return;
    }
    std::shared_lock visitorLH(visitorMutex, std::defer_lock);
    while (getResizeInProgress() != ResizeAlgo::None || !visitorLH.try_lock()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    size_t currSize = getSize();
    for (size_t lock = 0; lock < mutexes.size(); ++lock) {
        for (size_t bucket = lock; bucket < currSize;
             bucket += mutexes.size()) {
            // (re)acquire mutex on each HashBucket, to minimise any impact
            // on front-end threads.
            std::lock_guard<std::mutex> lh(mutexes[lock]);

            size_t depth = 0;
            StoredValue* p = values[bucket].get().get();
            if (p) {
                // TODO: Perf: This check seems costly - do we think it's still
                // worth keeping?
                auto hashbucket =
                        getPositionForHash(p->getKey().hash()).hash_bucket;
                if (bucket != hashbucket) {
                    throw std::logic_error(
                            "HashTable::visit: inconsistency "
                            "between StoredValue's calculated hashbucket "
                            "(which is " + std::to_string(hashbucket) +
                            ") and bucket it is located in (which is " +
                            std::to_string(bucket) + ')');
                }
            }
            size_t mem(0);
            while (p) {
                ++depth;
                mem += p->size();
                p = p->getNext().get().get();
            }
            visitor.visit(bucket, depth, mem);
        }
    }
}

HashTable::Position HashTable::pauseResumeVisit(HashTableVisitor& visitor,
                                                const Position& start_pos) {
    if ((valueStats.getNumItems() + valueStats.getNumTempItems()) == 0 ||
        !isActive()) {
        // Nothing to visit
        return endPosition();
    }

    if (getResizeInProgress() != ResizeAlgo::None) {
        return start_pos;
    }

    bool paused = false;

    // To attempt to minimize the impact the visitor has on normal frontend
    // operations, we deliberately acquire (and release) the mutex between
    // each hash_bucket - see `lh` in the inner for() loop below. This means we
    // hold a given mutex for a large number of short durations, instead of just
    // one single, long duration.
    // *However*, there is a potential race with this approach - the HashTable
    // may be resized. When we fail to acquire a shared_lock the visit is
    // paused, as the unique_lock holder (resizing task) is likely to block us
    // for a long time.
    std::shared_lock lh(visitorMutex, std::try_to_lock);
    if (!lh.owns_lock() || getResizeInProgress() != ResizeAlgo::None) {
        return start_pos;
    }

    // Start from the requested lock number if in range.
    size_t lock = (start_pos.lock < mutexes.size()) ? start_pos.lock : 0;
    size_t hash_bucket = 0;
    const size_t size = getSize();

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
            if (!visitor.setUpHashBucketVisit()) {
                visitor.tearDownHashBucketVisit();
                break;
            }

            // HashBucketLock scope. If a visitor needs additional locking
            // around the HashBucket visit then we need to release it before
            // tearDownHashBucketVisit() is called.
            {
                const auto hbl = getLockedBucket(hash_bucket);
                if (hbl.getPosition().lock != lock) {
                    throw std::logic_error(
                            "HashTable::pauseResumeVisit: wrong lock");
                }

                StoredValue* v = values[hash_bucket].get().get();
                while (!paused && v) {
                    StoredValue* tmp = v->getNext().get().get();
                    paused = !visitor.visit(hbl, *v);
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
    return {WhichTable::Primary, size, lock, hash_bucket};
}

HashTable::Position HashTable::endPosition() const  {
    const auto size = getSize();
    return {WhichTable::Primary, size, mutexes.size(), size};
}

bool HashTable::unlocked_ejectItem(const HashTable::HashBucketLock& hbl,
                                   StoredValue*& vptr,
                                   EvictionPolicy policy,
                                   bool keepMetadata) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_ejectItem: htLock not held");
    }
    if (vptr == nullptr) {
        throw std::invalid_argument("HashTable::unlocked_ejectItem: "
                "Unable to delete NULL StoredValue");
    }

    if (!vptr->eligibleForEviction(policy, keepMetadata)) {
        ++stats.numFailedEjects;
        return false;
    }

    const auto preProps = valueStats.prologue(hbl, vptr);

    // If keepMetadata is set, only the value is evicted.
    // Deleted items may be entirely removed from memory even in value eviction,
    // but not if keepMetadata is set. Locked items are not entirely removed
    // from memory as the locked status is not persisted to disk.
    bool shouldKeepMetadata =
            keepMetadata ||
            (!vptr->isDeleted() && (policy == EvictionPolicy::Value ||
                                    vptr->isLocked(ep_current_time())));

    if (shouldKeepMetadata) {
        // Just eject the value.
        vptr->ejectValue();
        ++stats.numValueEjects;
        valueStats.epilogue(hbl, preProps, vptr);
    } else {
        // Remove the item from the hash table.
        auto removed = hashChainRemove(unlocked_getBucket(hbl), *vptr);
        Expects(removed);

        if (removed->isResident()) {
            ++stats.numValueEjects;
        }
        valueStats.epilogue(hbl, preProps, nullptr);

        updateMaxDeletedRevSeqno(vptr->getRevSeqno());
    }

    ++numEjects;
    return true;
}

std::unique_ptr<Item> HashTable::getRandomKey(CollectionID cid,
                                              const HashBucketLock& hbl) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument("HashTable::getRandomKey: htLock not held");
    }
    for (StoredValue* v = unlocked_getBucket(hbl).get().get(); v;
         v = v->getNext().get().get()) {
        if (!v->isTempItem() && !v->isDeleted() && v->isResident() &&
            !v->isPending() && !v->isPrepareCompleted() &&
            v->getKey().getCollectionID() == cid) {
            return v->toItem(Vbid(0));
        }
    }

    return nullptr;
}

bool HashTable::unlocked_restoreValue(const HashBucketLock& hbl,
                                      const Item& itm,
                                      StoredValue& v) {
    if (!hbl.getHTLock() || !isActive() || v.isResident()) {
        return false;
    }

    const auto preProps = valueStats.prologue(hbl, &v);

    v.restoreValue(itm);
    // Set the initial MFU so that the item is not immediately eligible for
    // eviction.
    v.setFreqCounterValue(getInitialMFU());

    valueStats.epilogue(hbl, preProps, &v);

    return true;
}

void HashTable::unlocked_restoreMeta(const HashBucketLock& hbl,
                                     const Item& itm,
                                     StoredValue& v) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_restoreMeta: htLock "
                "not held");
    }

    if (!isActive()) {
        throw std::logic_error(
                "HashTable::unlocked_restoreMeta: Cannot "
                "call on a non-active HT object");
    }

    const auto preProps = valueStats.prologue(hbl, &v);

    v.restoreMeta(itm);
    // Set the initial MFU so that the item is not immediately eligible for
    // eviction.
    v.setFreqCounterValue(getInitialMFU());

    valueStats.epilogue(hbl, preProps, &v);
}

void HashTable::unlocked_setCommitted(const HashTable::HashBucketLock& hbl,
                                      StoredValue& value,
                                      CommittedState state) {
    if (!hbl.getHTLock()) {
        throw std::invalid_argument(
                "HashTable::unlocked_setCommitted: htLock not held");
    }
    const auto preProps = valueStats.prologue(hbl, &value);
    value.setCommitted(state);
    value.markDirty();
    value.setCompletedOrDeletedTime(ep_real_time());
    valueStats.epilogue(hbl, preProps, &value);
}

uint8_t HashTable::generateFreqValue(uint8_t counter) {
    return probabilisticCounter.generateValue(counter);
}

void HashTable::updateFreqCounter(const HashBucketLock& lh, StoredValue& v) {
    // Attempt to increment the storedValue frequency counter
    // value.  Because a probabilistic counter is used the new
    // value will either be the same or an increment of the
    // current value.
    auto origFreqCounterValue = v.getFreqCounterValue();
    auto updatedFreqCounterValue = generateFreqValue(origFreqCounterValue);
    setSVFreqCounter(lh, v, updatedFreqCounterValue);

    if (updatedFreqCounterValue == std::numeric_limits<uint8_t>::max()) {
        // Invoke the registered callback function which
        // wakeups the ItemFreqDecayer task.
        frequencyCounterSaturated();
    }
}

void HashTable::setSVFreqCounter(const HashBucketLock& lh,
                                 StoredValue& v,
                                 uint8_t newFreqCounter) {
    // changing the MFU counter of a value will not change whether it is
    // evictable, but if the value already _is_ evictable the MFU histogram
    // needs to be updated to reflect this.
    auto origFreqCounter = v.getFreqCounterValue();
    // update the SV itself
    v.setFreqCounterValue(newFreqCounter);

    bool isEvictable = valueStats.shouldTrackMFU(lh, v);
    if (isEvictable) {
        valueStats.removeMFU(origFreqCounter);
        valueStats.recordMFU(newFreqCounter);
    }
}

void HashTable::markSVClean(const HashBucketLock& lh, StoredValue& v) {
    // Marking a value as clean _may_ change it from non-evictable to evictable.
    // Doing so does not change the MFU of the item, and will never transition
    // the item from evictable to non-evictable.
    // Therefore, the only action which may need to be taken would be to add the
    // value's current MFU to the evictable histogram.

    auto wasEvictable = valueStats.shouldTrackMFU(lh, v);
    // update the SV itself
    v.markClean();

    auto isEvictable = valueStats.shouldTrackMFU(lh, v);
    if (isEvictable && !wasEvictable) {
        valueStats.recordMFU(v.getFreqCounterValue());
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
       << " numPreparedSW:" << ht.getNumPreparedSyncWrites() << std::endl;
    for (const auto& [title, table] :
         {std::make_pair(" values:", &ht.values),
          std::make_pair(" resizingTemporaryValues:",
                         &ht.resizingTemporaryValues)}) {
        os << title << std::endl;
        for (const auto& chain : *table) {
            if (chain) {
                for (StoredValue* sv = chain.get().get(); sv != nullptr;
                     sv = sv->getNext().get().get()) {
                    os << "    " << *sv << std::endl;
                }
            }
        }
    }
    return os;
}
