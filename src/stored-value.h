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

#ifndef SRC_STORED_VALUE_H_
#define SRC_STORED_VALUE_H_ 1

#include "config.h"

#include "item.h"
#include "item_pager.h"
#include "utility.h"

#include <platform/cb_malloc.h>

// Forward declaration for StoredValue
class HashTable;
class StoredValueFactory;

/**
 * In-memory storage for an item.
 */
class StoredValue {
public:

    void operator delete(void* p) {
        ::operator delete(p);
    }

    uint8_t getNRUValue() const;

    void setNRUValue(uint8_t nru_val);

    uint8_t incrNRUValue();

    void referenced();

    /**
     * Mark this item as needing to be persisted.
     */
    void markDirty() {
        _isDirty = 1;
    }

    /**
     * Mark this item as clean.
     */
    void markClean() {
        _isDirty = 0;
    }

    /**
     * True if this object is dirty.
     */
    bool isDirty() const {
        return _isDirty;
    }

    bool eligibleForEviction(item_eviction_policy_t policy) {
        if (policy == VALUE_ONLY) {
            return isResident() && !isDirty() && !isDeleted();
        } else {
            return !isDirty() && !isDeleted();
        }
    }

    /**
     * Check if this item is expired or not.
     *
     * @param asOf the time to be compared with this item's expiry time
     * @return true if this item's expiry time < asOf
     */
    bool isExpired(time_t asOf) const {
        if (getExptime() != 0 && getExptime() < asOf) {
            return true;
        }
        return false;
    }

    /**
     * True if this item is for the given key.
     *
     * @param k the key we're checking
     * @return true if this item's key is equal to k
     */
    bool hasKey(const DocKey& k) const {
        return key == k;
    }

    /**
     * Get this item's key.
     */
    const SerialisedDocKey& getKey() const {
        return key;
    }

    /**
     * Get this item's value.
     */
    const value_t &getValue() const {
        return value;
    }

    /**
     * Get the expiration time of this item.
     *
     * @return the expiration time for feature items, 0 for small items
     */
    time_t getExptime() const {
        return exptime;
    }

    void setExptime(time_t tim) {
        exptime = tim;
        markDirty();
    }

    /**
     * Get the client-defined flags of this item.
     *
     * @return the flags for feature items, 0 for small items
     */
    uint32_t getFlags() const {
        return flags;
    }

    /**
     * Set the client-defined flags for this item.
     */
    void setFlags(uint32_t fl) {
        flags = fl;
    }

    /**
     * Set a new value for this item.
     *
     * @param itm the item with a new value
     * @param ht the hashtable that contains this StoredValue instance
     */
    void setValue(const Item& itm, HashTable& ht) {
        size_t currSize = size();
        reduceCacheSize(ht, currSize);
        value = itm.getValue();
        deleted = itm.isDeleted();
        flags = itm.getFlags();
        bySeqno = itm.getBySeqno();

        cas = itm.getCas();
        exptime = itm.getExptime();
        revSeqno = itm.getRevSeqno();

        nru = itm.getNRUValue();

        if (isTempInitialItem()) {
            markClean();
        } else {
            markDirty();
        }

        if (isTempItem()) {
            markNotResident();
        }

        size_t newSize = size();
        increaseCacheSize(ht, newSize);
    }

    void markDeleted() {
        deleted = true;
        markDirty();
    }

    /**
     * Reset the value of this item.
     */
    void resetValue() {
        if (isDeleted()) {
            throw std::logic_error("StoredValue::resetValue: Not possible to "
                    "reset the value of a deleted item");
        }
        markNotResident();
        // item no longer resident once reset the value
        deleted = true;
    }

    /**
     * Eject an item value from memory.
     * @param ht the hashtable that contains this StoredValue instance
     */
    bool ejectValue(HashTable &ht, item_eviction_policy_t policy);

    /**
     * Restore the value for this item.
     *
     * @param itm the item to be restored
     */
    void restoreValue(const Item& itm);

    /**
     * Restore the metadata of of a temporary item upon completion of a
     * background fetch assuming the hashtable bucket is locked.
     *
     * @param itm the Item whose metadata is being restored
     */
    void restoreMeta(const Item& itm);

    /**
     * Get this item's CAS identifier.
     *
     * @return the cas ID for feature items, 0 for small items
     */
    uint64_t getCas() const {
        return cas;
    }

    /**
     * Set a new CAS ID.
     *
     * This is a NOOP for small item types.
     */
    void setCas(uint64_t c) {
        cas = c;
    }

    /**
     * Lock this item until the given time.
     *
     * This is a NOOP for small item types.
     */
    void lock(rel_time_t expiry) {
        lock_expiry = expiry;
    }

    /**
     * Unlock this item.
     */
    void unlock() {
        lock_expiry = 0;
    }

    /**
     * True if this item has an ID.
     *
     * An item always has an ID after it's been persisted.
     */
    bool hasBySeqno() {
        return bySeqno > 0;
    }

    /**
     * Get this item's ID.
     *
     * @return the ID for the item; 0 if the item has no ID
     */
    int64_t getBySeqno() const {
        return bySeqno;
    }

    /**
     * Set the ID for this item.
     *
     * This is used by the persistene layer.
     *
     * It is an error to set an ID on an item that already has one.
     */
    void setBySeqno(int64_t to) {
        if (to <= 0) {
            throw std::invalid_argument("StoredValue::setBySeqno: to "
                    "(which is " + std::to_string(to) + ") must be positive");
        }
        bySeqno = to;
    }

    // Marks the stored item as deleted.
    void setDeleted()
    {
        bySeqno = state_deleted_key;
    }

    // Marks the stored item as non-existent.
    void setNonExistent()
    {
        bySeqno = state_non_existent_key;
    }

    /**
     * Is this a temporary item created for processing a get-meta request?
     */
     bool isTempItem() {
         return(isTempNonExistentItem() || isTempDeletedItem() || isTempInitialItem());

     }

    /**
     * Is this an initial temporary item?
     */
    bool isTempInitialItem() {
        return bySeqno == state_temp_init;
    }

    /**
     * Is this a temporary item created for a non-existent key?
     */
    bool isTempNonExistentItem() const {
         return bySeqno == state_non_existent_key;
    }

    /**
     * Is this a temporary item created for a deleted key?
     */
    bool isTempDeletedItem() {
         return bySeqno == state_deleted_key;

     }

    size_t valuelen() const {
        if (isDeleted() || !isResident()) {
            return 0;
        }
        return value->length();
    }

    /**
     * Get the total size of this item.
     *
     * @return the amount of memory used by this item.
     */
    size_t size() {
        return sizeof(StoredValue) + key.size() + valuelen();
    }

    size_t metaDataSize() {
        return sizeof(StoredValue) + key.size();
    }

    /**
     * Return true if this item is locked as of the given timestamp.
     *
     * @param curtime lock expiration marker (usually the current time)
     * @return true if the item is locked
     */
    bool isLocked(rel_time_t curtime) {
        if (lock_expiry == 0 || (curtime > lock_expiry)) {
            lock_expiry = 0;
            return false;
        }
        return true;
    }

    /**
     * True if this value is resident in memory currently.
     */
    bool isResident() const {
        return value.get() != NULL;
    }

    void markNotResident() {
        value.reset();
    }

    /**
     * True if this object is logically deleted.
     */
    bool isDeleted() const {
        return deleted;
    }

    /**
     * Logically delete this object.
     */
    void del(HashTable &ht) {
        if (isDeleted()) {
            return;
        }

        reduceCacheSize(ht, valuelen());
        resetValue();
        markDirty();
    }


    uint64_t getRevSeqno() const {
        return revSeqno;
    }

    /**
     * Set a new revision sequence number.
     */
    void setRevSeqno(uint64_t s) {
        revSeqno = s;
    }

    /**
     * Return true if this is a new cache item.
     */
    bool isNewCacheItem(void) {
        return newCacheItem;
    }

    /**
     * Set / reset a new cache item flag.
     */
    void setNewCacheItem(bool newitem) {
        newCacheItem = newitem;
    }

    /**
     * Generate a new Item out of this object.
     *
     * @param lck if true, the new item will return a locked CAS ID.
     * @param vbucket the vbucket containing this item.
     */
    Item *toItem(bool lck, uint16_t vbucket) const;

    /**
     * Set the memory threshold on the current bucket quota for accepting a new mutation
     */
    static void setMutationMemoryThreshold(double memThreshold);

    /**
     * Return the memory threshold for accepting a new mutation
     */
    static double getMutationMemThreshold() {
        return mutation_mem_threshold;
    }

    /*
     * Values of the bySeqno attribute used by temporarily created StoredValue
     * objects.
     * state_deleted_key: represents an item that's deleted from memory but
     *                    present in the persistent store.
     * state_non_existent_key: represents a non existent item
     * state_collection_open: a special value used by collections to help
     *  represent a collections life-time in sequence-numbers (start to end).
     *  If a collection has no end, it's termed open and has an end
     *  sequence-number of StoredValue::state_collection_open. We do not
     *  actually assign this value to StoredValue objects, but it's here in
     *  this "number space" of special sequence numbers to help ensure it's
     *  different to the other special sequence numbers we define.
     *
     */
    static const int64_t state_deleted_key;
    static const int64_t state_non_existent_key;
    static const int64_t state_temp_init;
    static const int64_t state_collection_open;

    ~StoredValue() {
        ObjectRegistry::onDeleteStoredValue(this);
    }

    size_t getObjectSize() const {
        return sizeof(*this) + key.getObjectSize();
    }

    /**
     * Reallocates the dynamic members of StoredValue. Used as part of
     * defragmentation.
     */
    void reallocate();

    /* [TBD] : Move this function out of StoredValue class */
    static bool hasAvailableSpace(EPStats&,
                                  const Item& item,
                                  bool isReplication = false);

private:
    StoredValue(const Item& itm, StoredValue* n, EPStats& stats, HashTable& ht)
        : value(itm.getValue()),
          next(n),
          cas(itm.getCas()),
          revSeqno(itm.getRevSeqno()),
          bySeqno(itm.getBySeqno()),
          lock_expiry(0),
          exptime(itm.getExptime()),
          flags(itm.getFlags()),
          deleted(false),
          newCacheItem(true),
          nru(itm.getNRUValue()),
          key(itm.getKey()) {
        if (isTempInitialItem()) {
            markClean();
        } else {
            markDirty();
        }

        if (isTempItem()) {
            markNotResident();
        }

        increaseMetaDataSize(ht, stats, metaDataSize());
        increaseCacheSize(ht, size());

        ObjectRegistry::onCreateStoredValue(this);
        static_assert(offsetof(StoredValue, key) ==
                      (sizeof(StoredValue) - sizeof(key)),
                      "key must be the final member of StoredValue");
    }

    /*
     * Return how many bytes are need to store Item as a StoredValue
     */
    static size_t getRequiredStorage(const Item& item) {
        return sizeof(StoredValue) + SerialisedDocKey::getObjectSize(item.getKey().size());
    }

    friend class HashTable;
    friend class StoredValueFactory;

    value_t            value;          // 8 bytes
    StoredValue        *next;          // 8 bytes
    uint64_t           cas;            //!< CAS identifier.
    uint64_t           revSeqno;       //!< Revision id sequence number
    int64_t            bySeqno;        //!< By sequence id number
    rel_time_t         lock_expiry;    //!< getl lock expiration
    uint32_t           exptime;        //!< Expiration time of this item.
    uint32_t           flags;          // 4 bytes
    bool               _isDirty  :  1; // 1 bit
    bool               deleted   :  1;
    bool               newCacheItem : 1;
    uint8_t            nru       :  2; //!< True if referenced since last sweep
    SerialisedDocKey key; //!< The key itself.

    static void increaseMetaDataSize(HashTable &ht, EPStats &st, size_t by);
    static void reduceMetaDataSize(HashTable &ht, EPStats &st, size_t by);
    static void increaseCacheSize(HashTable &ht, size_t by);
    static void reduceCacheSize(HashTable &ht, size_t by);
    static double mutation_mem_threshold;

    DISALLOW_COPY_AND_ASSIGN(StoredValue);
};

/**
 * Creator of StoredValue instances.
 */
class StoredValueFactory {
public:

    /**
     * Create a new StoredValueFactory of the given type.
     */
    StoredValueFactory(EPStats &s) : stats(&s) { }

    /**
     * Create a new StoredValue with the given item.
     *
     * @param itm the item the StoredValue should contain
     * @param n the the top of the hash bucket into which this will be inserted
     * @param ht the hashtable that will contain the StoredValue instance
     *           created
     */
    StoredValue* operator()(const Item& itm, StoredValue* n, HashTable& ht) {
        return newStoredValue(itm, n, ht);
    }

private:
    StoredValue* newStoredValue(const Item& itm,
                                StoredValue* n,
                                HashTable& ht) {
        // Allocate a buffer to store the StoredValue and any trailing bytes
        // that maybe required.
        StoredValue* t =
                new (::operator new(StoredValue::getRequiredStorage(itm)))
                        StoredValue(itm, n, *stats, ht);
        return t;
    }

    EPStats                *stats;
};

#endif  // SRC_STORED_VALUE_H_
