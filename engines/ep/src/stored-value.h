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
#include "tagged_ptr.h"
#include "utility.h"

#include <mcbp/protocol/datatype.h>
#include <memcached/3rd_party/folly/AtomicBitSet.h>
#include <memcached/types.h>
#include <platform/n_byte_integer.h>

#include <boost/intrusive/list.hpp>
#include <relaxed_atomic.h>

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
    /**
     * C++14 will call the sized delete version, but we
     * allocate the object by using the new operator with a custom
     * size (the key is packed after the object). We need to use
     * the non-sized delete variant as the runtime don't know
     * the size of the allocated object.
     */
    static void operator delete(void* ptr) {
        ::operator delete(ptr);
    }

    /**
     * Compress the value part of stored value. If the compressed document
     * ends up being bigger than the original, then the method leaves the
     * document inflated
     *
     * return true, if the compression was successful or if the compressed
     *        document ends up being bigger than the original
     *        false, otherwise
     */
    bool compressValue();

    /**
     * Replace the existing value with the given compressed buffer.
     *
     * @param deflated the input buffer holding compressed data
     */
    void storeCompressedBuffer(cb::const_char_buffer deflated);

    // Custom deleter for StoredValue objects.
    struct Deleter {
        void operator()(StoredValue* val);
    };

    // Owning pointer type for StoredValue objects.
    using UniquePtr = std::unique_ptr<StoredValue,
            TaggedPtrDeleter<StoredValue, Deleter>>;

    uint8_t getNRUValue() const;

    void setNRUValue(uint8_t nru_val);

    uint8_t incrNRUValue();

    // Sets the top 16-bits of the chain_next_or_replacement pointer to the
    // u16int input value.
    void setChainTag(uint16_t v) {
        chain_next_or_replacement.get().setTag(v);
    }

    // Gets the top 16-bits of the chain_next_or_replacement pointer and
    // convert to a uint16 value.
    uint16_t getChainTag() const {
        return chain_next_or_replacement.get().getTag();
    }

    // Set the frequency counter value to the input value
    void setFreqCounterValue(uint16_t newValue) {
        value.unsafeGetPointer().setTag(newValue);
    }

    // Gets the frequency counter value
    uint16_t getFreqCounterValue() const {
        return value.get().getTag();
    }

    void referenced();

    /**
     * Mark this item as needing to be persisted.
     */
    void markDirty() {
        bits.set(dirtyIndex, true);
    }

    /**
     * Mark this item as clean.
     */
    void markClean() {
        bits.set(dirtyIndex, false);
    }

    /**
     * True if this object is dirty.
     */
    bool isDirty() const {
        return bits.test(dirtyIndex);
    }

    /**
     * Check if the value is compressible
     *
     * @return true if the data is compressible
     *         false if data is already compressed
     *                  value doesn't exist
     *                  value exists but has zero length
     */
    bool isCompressible() {
        if (mcbp::datatype::is_snappy(datatype) || !valuelen()) {
            return false;
        }
        return value->isCompressible();
    }

    bool eligibleForEviction(item_eviction_policy_t policy) const {
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

    void setUncompressible() {
        if (value) {
            value->setUncompressible();
        }
    }

    /**
     * Set a new value for this item.
     *
     * @param itm the item with a new value
     */
    void setValue(const Item& itm);

    void markDeleted(DeleteSource delSource) {
        setDeletedPriv(true);
        setDeletionSource(delSource);
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
     * Marks that the item's sequence number is pending (valid, non-temp; but
     * final value not yet known.
     */
    void setPendingSeqno() {
        bySeqno = state_pending_seqno;
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
        return value->valueSize();
    }

    /**
     * Returns the uncompressed value length (if resident); else zero.
     * For uncompressed values this is the same as valuelen().
     */
    size_t uncompressedValuelen() const;

    /**
     * Get the total size of this item.
     *
     * @return the amount of memory used by this item.
     */
    size_t size() const {
        return getObjectSize() + valuelen();
    }

    /**
     * Get the total uncompressed size of this item.
     *
     * @returns the amount of memory which would be used by this item if is it
     * was uncompressed.
     * For uncompressed items this is the same as size().
     */
    size_t uncompressedSize() const {
        return getObjectSize() + uncompressedValuelen();
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
        return bits.test(residentIndex);
    }

    void markNotResident() {
        resetValue();
        setResident(false);
    }

    /// Discard the value from this document.
    void resetValue() {
        value.reset();
    }

    /// Replace the existing value with new data.
    void replaceValue(TaggedPtr<Blob> data) {
        // Maintain the frequency count for the storedValue.
        auto freqCount = getFreqCounterValue();
        value.reset(data);
        setFreqCounterValue(freqCount);
    }

    /**
     * True if this object is logically deleted.
     */
    bool isDeleted() const {
        return bits.test(deletedIndex);
    }

    /**
     * Logically delete this object
     * @param delSource The source of the deletion
     * @return true if the item was deleted
     */
    bool del(DeleteSource delSource);

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
        return bits.test(newCacheItemIndex);
    }

    /**
     * Set / reset a new cache item flag.
     */
    void setNewCacheItem(bool newitem) {
        bits.set(newCacheItemIndex, newitem);
    }

    /**
     * Generate a new Item out of this object.
     *
     * @param lck if true, the new item will return a locked CAS ID.
     * @param vbucket the vbucket containing this item.
     */
    std::unique_ptr<Item> toItem(bool lck, Vbid vbucket) const;

    /**
     * Generate a new Item with only key and metadata out of this object.
     * The item generated will not contain value
     *
     * @param vbucket the vbucket containing this item.
     */
    std::unique_ptr<Item> toItemKeyOnly(Vbid vbucket) const;

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
        if (isStalePriv()) {
            throw std::logic_error(
                    "StoredValue::setNext: StoredValue is stale,"
                    "cannot set chain next value");
        }
        chain_next_or_replacement = std::move(nextSv);
    }

    UniquePtr& getNext() {
        if (isStalePriv()) {
            throw std::logic_error(
                    "StoredValue::getNext: StoredValue is stale,"
                    "cannot get chain next value");
        }
        return chain_next_or_replacement;
    }

    /*
     * Values of the bySeqno attribute used by temporarily created StoredValue
     * objects.
     */

    /**
     * Represents the state when a StoredValue is in the process of having its
     * seqno updated (and so _will be_ non-temporary; but we don't yet have the
     * new sequence number. This is unfortunately required due to limitations
     * in sequence number generation; where we need delete an item in the
     * HashTable before we know what its sequence number is going to be.
     */
    static const int64_t state_pending_seqno;

    /// Represents an item that's deleted from memory but present on disk.
    static const int64_t state_deleted_key;

    /// Represents a non existent item
    static const int64_t state_non_existent_key;

    /**
     * Represents a placeholder item created to mark that a bg fetch operation
     * is pending.
     */
    static const int64_t state_temp_init;

    /**
     * A special value used by collections to help represent a collections
     * life-time in sequence-numbers (start to end). If a collection has no
     * end, it's termed open and has an end sequence-number of
     * StoredValue::state_collection_open. We do not actually assign this
     * value to StoredValue objects, but it's here in this "number space" of
     * special sequence numbers to help ensure it's different to the other
     * special sequence numbers we define.
     */
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

    /// Return how many bytes are need to store item given key as a StoredValue
    static size_t getRequiredStorage(const DocKey& key);

    /**
     * @return the deletion source of the stored value
     */
    DeleteSource getDeletionSource() const {
        if (!isDeleted()) {
            throw std::logic_error(
                    "StoredValue::getDeletionSource: Called on a non-Deleted "
                    "item");
        }
        uint8_t delTest = bits2.test(deletionSource);
        return static_cast<DeleteSource>(delTest);
    }

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
     * @param delSource The source of the deletion.
     */
    bool deleteImpl(DeleteSource delSource);

    /* Update the value for this SV from the given item.
     * Implementation for StoredValue instances (dispatched to by setValue()).
     */
    void setValueImpl(const Item& itm);

    // name clash with public OSV isStale
    bool isStalePriv() const {
        return bits.test(staleIndex);
    }

    void setStale(bool value) {
        bits.set(staleIndex, value);
    }

    bool isOrdered() const {
        return bits.test(orderedIndex);
    }

    void setOrdered(bool value) {
        bits.set(orderedIndex, value);
    }

    void setDeletedPriv(bool value) {
        bits.set(deletedIndex, value);
    }

    void setResident(bool value) {
        bits.set(residentIndex, value);
    }

    void setDirty(bool value) {
        bits.set(dirtyIndex, value);
    }

    void setNru(uint8_t nru) {
        bits.set(nruIndex1, nru & 1);
        bits.set(nruIndex2, (nru & 2) >> 1);
    }

    uint8_t getNru() const {
        // Not atomic across the two bits, recommend holding the HBL
        return uint8_t(bits.test(nruIndex1)) |
               (uint8_t(bits.test(nruIndex2)) << 1);
    }

    void setDeletionSource(DeleteSource delSource) {
        bits2.set(deletionSource, static_cast<uint8_t>(delSource));
    }

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
    // bySeqno is atomic primarily for TSAN, which would flag that we write/read
    // this in ephemeral backfills with different locks (which is true, but the
    // access is we believe actually safe)
    Couchbase::RelaxedAtomic<int64_t> bySeqno; //!< By sequence id number
    /// For alive items: GETL lock expiration. For deleted items: delete time.
    rel_time_t         lock_expiry_or_delete_time;
    uint32_t           exptime;        //!< Expiration time of this item.
    uint32_t           flags;          // 4 bytes
    cb::uint48_t revSeqno; //!< Revision id sequence number
    protocol_binary_datatype_t datatype; // 1 byte

    /**
     * Compressed members live in the AtomicBitSet old comments for some members
     * ordered := true if this is an instance of OrderedStoredValue
     * stale := Indicates if a newer instance of the item is added. Logically
     * part of OSV, but is physically located in SV as there are spare bits
     * here. Guarded by the SequenceList's writeLock.
     */
    static constexpr size_t dirtyIndex = 0;
    static constexpr size_t deletedIndex = 1;
    static constexpr size_t newCacheItemIndex = 2;
    static constexpr size_t orderedIndex = 3;
    // 2 bit nru managed via setNru/getNru
    static constexpr size_t nruIndex1 = 4;
    static constexpr size_t nruIndex2 = 5;
    static constexpr size_t residentIndex = 6;
    static constexpr size_t staleIndex = 7;

    folly::AtomicBitSet<sizeof(uint8_t)> bits;

    // If the stored value is deleted, this stores the source of its deletion.
    static constexpr size_t deletionSource = 0;

    /**
     * Much like bits, bits2 consists of compressed members inside an
     * AtomicBitSet and is stored in StoredValue due to spare bits. Currently,
     * only 1 of the 8 available bits is used.
     */
    folly::AtomicBitSet<sizeof(uint8_t)> bits2;

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
    /**
     * C++14 will call the sized delete version, but we
     * allocate the object by using the new operator with a custom
     * size (the key is packed after the object). We need to use
     * the non-sized delete variant as the runtime don't know
     * the size of the allocated object.
     */
    static void operator delete(void* ptr) {
        ::operator delete(ptr);
    }

    // Intrusive linked-list for sequence number ordering.
    // Guarded by the SequenceList's writeLock.
    boost::intrusive::list_member_hook<> seqno_hook;

    ~OrderedStoredValue() {
        if (isStalePriv()) {
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
        return isStalePriv();
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
        setStale(true);
    }

    StoredValue* getReplacementIfStale(
            std::lock_guard<std::mutex>& writeGuard) const {
        if (!isStalePriv()) {
            return nullptr;
        }

        return chain_next_or_replacement.get().get();
    }

    /**
     * Check if the contents of the StoredValue is same as that of the other
     * one. Does not consider the intrusive hash bucket link.
     *
     * @param other The StoredValue to be compared with
     */
    bool operator==(const OrderedStoredValue& other) const;

    /// Return how many bytes are need to store item with given key as an
    /// OrderedStoredValue
    static size_t getRequiredStorage(const DocKey& key);

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
    bool deleteImpl(DeleteSource delSource);

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
    if (isOrdered()) {
        return static_cast<OrderedStoredValue*>(this)->key();
    } else {
        return reinterpret_cast<SerialisedDocKey*>(this + 1);
    }
}

size_t StoredValue::getObjectSize() const {
    // Size of fixed part of OrderedStoredValue or StoredValue, plus size of
    // (variable) key.
    if (isOrdered()) {
        return sizeof(OrderedStoredValue) + getKey().getObjectSize();
    }
    return sizeof(*this) + getKey().getObjectSize();
}
