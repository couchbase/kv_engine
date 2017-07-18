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

#pragma once

#include "config.h"

#include "blob.h"
#include "item_pager.h"
#include "storeddockey.h"
#include "utility.h"

#include <boost/intrusive/list.hpp>

class Item;
class OrderedStoredValue;

/**
 * In-memory storage for an item.
 *
 * This class represents a single document which is present in the HashTable -
 * essentially this is value_type used by HashTable.
 *
 * It contains the documents' key, related metadata (CAS, rev, seqno, ...).
 * It also has a pointer to the documents' value - which may be null if the
 * value of the item is not currently resident (for example it's been evicted to
 * save memory) or if the item has no value (deleted item has value as null)
 * Additionally it contains flags to help HashTable manage the state of the
 * item - such as dirty flag, NRU bits, and a `next` pointer to support
 * chaining of StoredValues which hash to the same hash bucket.
 *
 * The key of the item is of variable length (from 1 to ~256 bytes). As an
 * optimization, we allocate the key directly after the fixed size of
 * StoredValue, so StoredValue and its key are contiguous in memory. This saves
 * us the cost of an indirection compared to storing the key out-of-line, and
 * the space of a pointer in StoredValue to point to the out-of-line
 * allocation. It does, however complicate the management of StoredValue
 * objects as they are now variable-sized - they must be created using a
 * factory method (StoredValueFactory) and must be heap-allocated, managed
 * using a unique_ptr (StoredValue::UniquePtr).
 *
 * Graphically the looks like:
 *
 *              StoredValue::UniquePtr
 *                          |
 *                          V
 *               .-------------------.
 *               | StoredValue       |
 *               +-------------------+
 *           {   | value [ptr]       | ======> Blob (nullptr if evicted)
 *           {   | next  [ptr]       | ======> StoredValue (next in hash chain).
 *     fixed {   | CAS               |
 *    length {   | revSeqno          |
 *           {   | ...               |
 *           {   | datatype          |
 *           {   | internal flags: isDirty, deleted, isOrderedStoredValue ...
 *               + - - - - - - - - - +
 *  variable {   | key[]             |
 *   length  {   | ...               |
 *               +-------------------+
 *
 * OrderedStoredValue is a "subclass" of StoredValue, which is used by
 * Ephemeral buckets as it supports maintaining a seqno ordering of items in
 * memory (for Persistent buckets this ordering is maintained on-disk).
 *
 * The implementation of OrderedStoredValue is tightly coupled to StoredValue
 * so it will be described here:
 *
 * OrderedStoredValue has the fixed length members of StoredValue, then it's
 * own fixed length fields (seqno list), followed finally by the variable
 * length key (again, allocated contiguously):
 *
 *              StoredValue::UniquePtr
 *                          |
 *                          V
 *               .--------------------.
 *               | OrderedStoredValue |
 *               +--------------------+
 *           {   | value [ptr]        | ======> Blob (nullptr if evicted)
 *           {   | next  [ptr]        | ======> StoredValue (next in hash chain).
 *     fixed {   | StoredValue fixed ...
 *    length {   + - - - - - - - - - -+
 *           {   | seqno next [ptr]   |
 *           {   | seqno prev [ptr]   |
 *               + - - - - - - - - - -+
 *  variable {   | key[]              |
 *   length  {   | ...                |
 *               +--------------------+
 *
 * To support dynamic dispatch (for example to lookup the key, whose location
 * varies depending if it's StoredValue or OrderedStoredValue), we choose to
 * use a manual flag-based dispatching (as opposed to a normal vTable based
 * approach) as the per-object costs are much cheaper - 1 bit for the flag vs.
 * 8 bytes for a vTable ptr.
 * StoredValue::isOrderedStoredValue is set to false for StoredValue objects,
 * and true for OrderedStoredValue objects, and then any methods
 * needing dynamic dispatch read the value of the flag. Note this means that
 * the 'base' class (StoredValue) needs to know about all possible subclasses
 * (only one currently) and what class-specific code to call.
 * Similary, deletion of OrderedStoredValue objects is delicate - we cannot
 * safely delete via the base-class pointer directly, as that would only run
 * ~StoredValue and not the members of the derived class. Instead a custom
 * deleter is associated with StoredValue::UniquePtr, which checks the flag
 * and dispatches to the correct destructor.
 */
class StoredValue {
public:
    // Custom deleter for StoredValue objects.
    struct Deleter {
        void operator()(StoredValue* val);
    };

    // Owning pointer type for StoredValue objects.
    using UniquePtr = std::unique_ptr<StoredValue, Deleter>;

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
        return getKey() == k;
    }

    /**
     * Get this item's key.
     */
    const SerialisedDocKey& getKey() const {
        return *const_cast<const SerialisedDocKey*>(
                const_cast<StoredValue&>(*this).key());
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
     * @return the expiration time.
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
     * @return the flags.
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
     * get the items datatype
     */
    protocol_binary_datatype_t getDatatype() const {
        return datatype;
    }

    /**
     * Set the items datatype
     */
    void setDatatype(protocol_binary_datatype_t type) {
        datatype = type;
    }

    /**
     * Set a new value for this item.
     *
     * @param itm the item with a new value
     */
    void setValue(const Item& itm);

    void markDeleted() {
        deleted = true;
        markDirty();
    }

    /**
     * Eject an item value from memory.
     */
    void ejectValue();

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
     * @return the cas ID
     */
    uint64_t getCas() const {
        return cas;
    }

    /**
     * Set a new CAS ID.
     */
    void setCas(uint64_t c) {
        cas = c;
    }

    /**
     * Lock this item until the given time.
     */
    void lock(rel_time_t expiry) {
        if (isDeleted()) {
            // Cannot lock Deleted items.
            throw std::logic_error(
                    "StoredValue::lock: Called on Deleted item");
        }
        lock_expiry_or_delete_time = expiry;
    }

    /**
     * Unlock this item.
     */
    void unlock() {
        if (isDeleted()) {
            // Deleted items are not locked - just skip.
            return;
        }
        lock_expiry_or_delete_time = 0;
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

    // Marks the stored item as temporarily deleted
    void setTempDeleted()
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
    bool isTempItem() const {
        return (isTempNonExistentItem() || isTempDeletedItem() ||
                isTempInitialItem());

     }

    /**
     * Is this an initial temporary item?
     */
     bool isTempInitialItem() const {
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
    bool isTempDeletedItem() const {
        return bySeqno == state_deleted_key;

     }

    size_t valuelen() const {
        if (!value) {
            return 0;
        }
        return value->length();
    }

    /**
     * Get the total size of this item.
     *
     * @return the amount of memory used by this item.
     */
    size_t size() const {
        return getObjectSize() + valuelen();
    }

    size_t metaDataSize() const {
        return getObjectSize();
    }

    /**
     * Return true if this item is locked as of the given timestamp.
     *
     * @param curtime lock expiration marker (usually the current time)
     * @return true if the item is locked
     */
    bool isLocked(rel_time_t curtime) const {
        if (isDeleted()) {
            // Deleted items cannot be locked.
            return false;
        }

        if (lock_expiry_or_delete_time == 0 ||
            (curtime > lock_expiry_or_delete_time)) {
            return false;
        }
        return true;
    }

    /**
     * True if this value is resident in memory currently.
     */
    bool isResident() const {
        return resident;
    }

    void markNotResident() {
        resetValue();
        resident = false;
    }

    /// Discard the value from this document.
    void resetValue() {
        value.reset();
    }

    /**
     * True if this object is logically deleted.
     */
    bool isDeleted() const {
        return deleted;
    }

    /**
     * Logically delete this object
     * @return true if the item was deleted
     */
    bool del();

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
    bool isNewCacheItem() const {
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
    std::unique_ptr<Item> toItem(bool lck, uint16_t vbucket) const;

    /**
     * Generate a new Item with only key and metadata out of this object.
     * The item generated will not contain value
     *
     * @param vbucket the vbucket containing this item.
     */
    std::unique_ptr<Item> toItemKeyOnly(uint16_t vbucket) const;

    /**
     * Get an item_info from the StoredValue
     *
     * @param vbuuid a VB UUID to set in to the item_info
     * @returns item_info populated with the StoredValue's state if the
     *                    StoredValue is not a temporary item (!::isTempItem()).
     *                    If the object is a temporary item the optional is not
     *                    initialised.
     */
    boost::optional<item_info> getItemInfo(uint64_t vbuuid) const;

    void setNext(UniquePtr&& nextSv) {
        if (stale) {
            throw std::logic_error(
                    "StoredValue::setNext: StoredValue is stale,"
                    "cannot set chain next value");
        }
        chain_next_or_replacement = std::move(nextSv);
    }

    UniquePtr& getNext() {
        if (stale) {
            throw std::logic_error(
                    "StoredValue::getNext: StoredValue is stale,"
                    "cannot get chain next value");
        }
        return chain_next_or_replacement;
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

    /**
     * Return the size in byte of this object; both the fixed fields and the
     * variable-length key. Doesn't include value size (allocated externally).
     */
    inline size_t getObjectSize() const;

    /**
     * Reallocates the dynamic members of StoredValue. Used as part of
     * defragmentation.
     */
    void reallocate();

    /**
     * Returns pointer to the subclass OrderedStoredValue if it the object is
     * of the type, if not throws a bad_cast.
     *
     * Equivalent to dynamic cast, but done manually as we wanted to avoid
     * vptr per object.
     */
    OrderedStoredValue* toOrderedStoredValue();
    const OrderedStoredValue* toOrderedStoredValue() const;

    /**
     * Check if the contents of the StoredValue is same as that of the other
     * one. Does not consider the intrusive hash bucket link.
     *
     * @param other The StoredValue to be compared with
     */
    bool operator==(const StoredValue& other) const;

    /// Return how many bytes are need to store Item as a StoredValue
    static size_t getRequiredStorage(const Item& item);

protected:
    /**
     * Constructor - protected as allocation needs to be done via
     * StoredValueFactory.
     *
     * @param itm Item to base this StoredValue on.
     * @param n The StoredValue which will follow the new stored value in
     *           the hash bucket chain, which this new item will take
     *           ownership of. (Typically the top of the hash bucket into
     *           which the new item is being inserted).
     * @param stats EPStats to update for this new StoredValue
     * @param isOrdered Are we constructing an OrderedStoredValue?
     */
    StoredValue(const Item& itm,
                UniquePtr n,
                EPStats& stats,
                bool isOrdered);

    // Destructor. protected, as needs to be carefully deleted (via
    // StoredValue::Destructor) depending on the value of isOrdered flag.
    ~StoredValue();

    /**
     * Copy constructor - protected as allocation needs to be done via
     * StoredValueFactory.
     *
     * @param other StoredValue being copied
     * @param n The StoredValue which will follow the new stored value in
     *           the hash bucket chain, which this new item will take
     *           ownership of. (Typically the top of the hash bucket into
     *           which the new item is being inserted).
     * @param stats EPStats to update for this new StoredValue
     */
    StoredValue(const StoredValue& other,
                UniquePtr n,
                EPStats& stats);

    /* Do not allow assignment */
    StoredValue& operator=(const StoredValue& other) = delete;

    /**
     * Get the address of item's key .
     */
    inline SerialisedDocKey* key();

    /**
     * Logically mark this SV as deleted.
     * Implementation for StoredValue instances (dispatched to by del() based
     * on isOrdered==false).
     */
    bool deleteImpl();

    /* Update the value for this SV from the given item.
     * Implementation for StoredValue instances (dispatched to by setValue()).
     */
    void setValueImpl(const Item& itm);

    friend class StoredValueFactory;

    value_t            value;          // 8 bytes

    // Serves two purposes -
    // 1. Used to implement HashTable chaining (for elements hashing to the same
    // bucket).
    // 2. Once the stored value has been marked stale, this is used to point at
    // the replacement stored value. In this case, *we do not have ownership*,
    // so we release the ptr in the destructor. The replacement is needed to
    // determine if it would also appear in a given rangeRead - we should return
    // only the newer version if so.
    UniquePtr chain_next_or_replacement; // 8 bytes
    uint64_t           cas;            //!< CAS identifier.
    uint64_t           revSeqno;       //!< Revision id sequence number
    int64_t            bySeqno;        //!< By sequence id number
    /// For alive items: GETL lock expiration. For deleted items: delete time.
    rel_time_t         lock_expiry_or_delete_time;
    uint32_t           exptime;        //!< Expiration time of this item.
    uint32_t           flags;          // 4 bytes
    protocol_binary_datatype_t datatype; // 1 byte
    bool               _isDirty  :  1; // 1 bit
    bool               deleted   :  1;
    bool               newCacheItem : 1;
    const bool isOrdered : 1; //!< Is this an instance of OrderedStoredValue?
    uint8_t            nru       :  2; //!< True if referenced since last sweep
    bool               resident :  1;
    bool unused : 1; // Unused bits in first byte of bitfields.

    // Indicates if a newer instance of the item is added. Logically part of
    // OSV, but is physically located in SV as there are spare bytes here.
    // Guarded by the SequenceList's writeLock.
    // NOTE: As this is guarded by a different lock to the rest of the SV,
    // it *must* be in a different byte than any other data not guarded by
    // writeLock (Hence why this isn't in the same byte as _isDirty, deleted,
    // newCacheItem etc). To achieve this std::atomic is used to ensure accesses
    // are not "optimized" and merged with the previous byte. The thread-safety
    // of std::atomic is not actually used/needed; we just need the no-merge
    // guarantee.
    // Note (2): Only 1 bit of this is currently used; rest is "spare".
    std::atomic<bool> stale;

    friend std::ostream& operator<<(std::ostream& os, const StoredValue& sv);
};

std::ostream& operator<<(std::ostream& os, const StoredValue& sv);

/**
 * Subclass of StoredValue which additionally supports sequence number ordering.
 *
 * See StoredValue for implementation details.
 */
class OrderedStoredValue : public StoredValue {
public:
    // Intrusive linked-list for sequence number ordering.
    // Guarded by the SequenceList's writeLock.
    boost::intrusive::list_member_hook<> seqno_hook;

    ~OrderedStoredValue() {
        if (stale) {
            // This points to the replacement OSV which we do not actually own.
            // We are reusing a unique_ptr so we explicitly release it in this
            // case. We /do/ own the chain_next if we are not stale.
            chain_next_or_replacement.release();
        }
    }

    /**
     * True if a newer version of the same key exists in the HashTable.
     * Note: Only true for OrderedStoredValues which are no longer in the
     *       HashTable (and only live in SequenceList)
     * @param writeGuard The locked SeqList writeLock which guards the stale
     * param.
     */
    bool isStale(std::lock_guard<std::mutex>& writeGuard) const {
        return stale;
    }

    /**
     * Marks that newer instance of this item is added in the HashTable
     * @param writeLock The SeqList writeLock which guards the stale param.
     */
    void markStale(std::lock_guard<std::mutex>& writeGuard,
                   StoredValue* newSv) {
        // next is a UniquePtr which is up to this point was used for chaining
        // in the HashTable. Now this item is stale, we are reusing this to
        // point to the updated version of this StoredValue. _BUT_ we do not
        // own the new SV. At destruction, we must release this ptr if
        // we are stale.
        chain_next_or_replacement.reset(newSv);
        stale = true;
    }

    StoredValue* getReplacementIfStale(
            std::lock_guard<std::mutex>& writeGuard) const {
        if (!stale) {
            return nullptr;
        }

        return chain_next_or_replacement.get();
    }

    /**
     * Check if the contents of the StoredValue is same as that of the other
     * one. Does not consider the intrusive hash bucket link.
     *
     * @param other The StoredValue to be compared with
     */
    bool operator==(const OrderedStoredValue& other) const;

    /// Return how many bytes are need to store Item as an OrderedStoredValue
    static size_t getRequiredStorage(const Item& item);

    /**
     * Return the time the item was deleted. Only valid for deleted items.
     */
    rel_time_t getDeletedTime() const;

protected:
    SerialisedDocKey* key() {
        return reinterpret_cast<SerialisedDocKey*>(this + 1);
    }

    /**
     * Logically mark this OSV as deleted. Implementation for
     * OrderedStoredValue instances (dispatched to by del() based on
     * isOrdered==true).
     */
    bool deleteImpl();

    /* Update the value for this OSV from the given item.
     * Implementation for OrderedStoredValue instances (dispatched to by
     *  setValue()).
     */
    void setValueImpl(const Item& itm);

    /**
     * Set the time the item was deleted to the specified time.
     */
    inline void setDeletedTime(rel_time_t time);

private:
    // Constructor. Private, as needs to be carefully created via
    // OrderedStoredValueFactory.
    OrderedStoredValue(const Item& itm,
                       UniquePtr n,
                       EPStats& stats)
        : StoredValue(itm, std::move(n), stats, /*isOrdered*/ true) {
    }

    // Copy Constructor. Private, as needs to be carefully created via
    // OrderedStoredValueFactory.
    //
    // Only StoredValue part (Hash Chain included) is copied. Hence the copied
    // StoredValue will be in the HashTable, but not in the ordered
    // data structure.
    OrderedStoredValue(const StoredValue& other,
                       UniquePtr n,
                       EPStats& stats)
        : StoredValue(other, std::move(n), stats) {
    }

    /* Do not allow assignment */
    OrderedStoredValue& operator=(const OrderedStoredValue& other) = delete;

    // Grant friendship so our factory can call our (private) constructor.
    friend class OrderedStoredValueFactory;

    // Grant friendship to base class so it can perform flag dispatch to our
    // overridden protected methods.
    friend class StoredValue;
};

SerialisedDocKey* StoredValue::key() {
    // key is located immediately following the object.
    if (isOrdered) {
        return static_cast<OrderedStoredValue*>(this)->key();
    } else {
        return reinterpret_cast<SerialisedDocKey*>(this + 1);
    }
}

size_t StoredValue::getObjectSize() const {
    // Size of fixed part of OrderedStoredValue or StoredValue, plus size of
    // (variable) key.
    if (isOrdered) {
        return sizeof(OrderedStoredValue) + getKey().getObjectSize();
    }
    return sizeof(*this) + getKey().getObjectSize();
}
