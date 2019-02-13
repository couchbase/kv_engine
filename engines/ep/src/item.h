/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "atomic.h"
#include "blob.h"
#include "dcp/dcp-types.h"
#include "queue_op.h"
#include "storeddockey.h"
#include <mcbp/protocol/datatype.h>
#include <memcached/durability_spec.h>
#include <memcached/types.h>
#include <platform/n_byte_integer.h>
#include <string>

/// Return a string representation of queue_op.
std::string to_string(queue_op op);

// Max Value for NRU bits
const uint8_t MAX_NRU_VALUE = 3;
// Initial value for NRU bits
const uint8_t INITIAL_NRU_VALUE = 2;
//Min value for NRU bits
const uint8_t MIN_NRU_VALUE = 0;


const uint64_t DEFAULT_REV_SEQ_NUM = 1;

/**
 * The ItemMetaData structure is used to pass meta data information of
 * an Item.
 */
class ItemMetaData {
public:
    ItemMetaData() :
        cas(0), revSeqno(DEFAULT_REV_SEQ_NUM), flags(0), exptime(0) {
    }

    ItemMetaData(uint64_t c, uint64_t s, uint32_t f, time_t e) :
        cas(c), revSeqno(s == 0 ? DEFAULT_REV_SEQ_NUM : s), flags(f),
        exptime(e) {
    }

    uint64_t cas;
    cb::uint48_t revSeqno;
    uint32_t flags;
    time_t exptime;
};

bool operator==(const ItemMetaData& lhs, const ItemMetaData& rhs);
std::ostream& operator<<(std::ostream& os, const ItemMetaData& md);

item_info to_item_info(const ItemMetaData& itemMeta,
                       uint8_t datatype,
                       uint32_t deleted);

/**
 * The Item structure we use to pass information between the memcached
 * core and the backend. Please note that the kvstore don't store these
 * objects, so we do have an extra layer of memory copying :(
 */
class Item : public RCValue {
public:

    /* Constructor (existing value_t).
     * Used when a value already exists, and the Item should refer to that
     * value.
     */
    Item(const DocKey& k,
         const uint32_t fl,
         const time_t exp,
         const value_t& val,
         protocol_binary_datatype_t dtype = PROTOCOL_BINARY_RAW_BYTES,
         uint64_t theCas = 0,
         int64_t i = -1,
         Vbid vbid = Vbid(0),
         uint64_t sno = 1);

    /* Constructor (new value).
     * k         specify the item's DocKey.
     * fl        Item flags.
     * exp       Item expiry.
     * {dta, nb} specify the item's value. nb specifies how much memory will be
     *           allocated for the value. If dta is non-NULL then the value
     *           is set from the memory pointed to by dta. If dta is NULL,
     *           then no data is copied in.
     *  The remaining arguments specify various optional attributes.
     */
    Item(const DocKey& k,
         const uint32_t fl,
         const time_t exp,
         const void* dta,
         const size_t nb,
         protocol_binary_datatype_t dtype = PROTOCOL_BINARY_RAW_BYTES,
         uint64_t theCas = 0,
         int64_t i = -1,
         Vbid vbid = Vbid(0),
         uint64_t sno = 1,
         uint8_t nru = INITIAL_NRU_VALUE,
         uint16_t freqCount = initialFreqCount);

    Item(const DocKey& k,
         const Vbid vb,
         queue_op o,
         const uint64_t revSeq,
         const int64_t bySeq);

    /* Copy constructor */
    Item(const Item& other);

    ~Item();

    static Item* makeDeletedItem(
            DeleteSource cause,
            const DocKey& k,
            const uint32_t fl,
            const time_t exp,
            const void* dta,
            const size_t nb,
            protocol_binary_datatype_t dtype = PROTOCOL_BINARY_RAW_BYTES,
            uint64_t theCas = 0,
            int64_t i = -1,
            Vbid vbid = Vbid(0),
            uint64_t sno = 1,
            uint8_t nru = INITIAL_NRU_VALUE,
            uint16_t freqCount = initialFreqCount) {
        auto ret = new Item(k,
                            fl,
                            exp,
                            dta,
                            nb,
                            dtype,
                            theCas,
                            i,
                            vbid,
                            sno,
                            nru,
                            freqCount);
        ret->setDeleted(cause);
        return ret;
    }

    /* Snappy compress value and update datatype */
    bool compressValue();

    /* Snappy uncompress value and update datatype */
    bool decompressValue();

    const char *getData() const {
        return value ? value->getData() : NULL;
    }

    const value_t &getValue() const {
        return value;
    }

    const StoredDocKey& getKey() const {
        return key;
    }

    int64_t getBySeqno() const {
        return bySeqno.load();
    }

    void setBySeqno(int64_t to) {
        bySeqno.store(to);
    }

    uint32_t getNBytes() const {
        return value ? static_cast<uint32_t>(value->valueSize()) : 0;
    }

    size_t getValMemSize() const {
        return value ? value->getSize() : 0;
    }

    time_t getExptime() const {
        return metaData.exptime;
    }

    time_t getDeleteTime() const {
        if (!isDeleted()) {
            throw std::logic_error("Item::getDeleteTime called on a mutation");
        }
        // exptime stores the delete-time (but only for deleted items)
        return metaData.exptime;
    }

    uint32_t getFlags() const {
        return metaData.flags;
    }

    uint64_t getCas() const {
        return metaData.cas;
    }

    protocol_binary_datatype_t getDataType() const {
        return datatype;
    }

    void setDataType(protocol_binary_datatype_t datatype_) {
        datatype = datatype_;
    }

    void setCas() {
        metaData.cas = nextCas();
    }

    void setCas(uint64_t ncas) {
        metaData.cas = ncas;
    }

    /// Replace the existing value with new data.
    void replaceValue(TaggedPtr<Blob> data) {
        // Maintain the frequency count for the Item.
        auto freqCount = getFreqCounterValue();
        value.reset(data);
        setFreqCounterValue(freqCount);
    }

    void setFlags(uint32_t f) {
        metaData.flags = f;
    }

    void setExpTime(time_t exp_time) {
        metaData.exptime = exp_time;
    }

    Vbid getVBucketId() const {
        return vbucketId;
    }

    void setVBucketId(Vbid to) {
        vbucketId = to;
    }

    size_t size(void) const {
        return sizeof(Item) + key.size() + getValMemSize();
    }

    uint64_t getRevSeqno() const {
        return metaData.revSeqno;
    }

    void setRevSeqno(uint64_t to) {
        if (to == 0) {
            to = DEFAULT_REV_SEQ_NUM;
        }
        metaData.revSeqno = to;
    }

    const ItemMetaData& getMetaData() const {
        return metaData;
    }

    bool isDeleted() const {
        return deleted;
    }

    /**
     * setDeleted controls the item's deleted flag.
     * @param cause Denotes the source of the deletion.
     */
    void setDeleted(DeleteSource cause = DeleteSource::Explicit);

    // Returns the cause of the item's deletion (Explicit or TTL [aka expiry])
    DeleteSource deletionSource() const {
        if (!isDeleted()) {
            throw std::logic_error(
                    "Item::deletionSource cannot be called on "
                    "an item that hasn't been deleted ");
        }
        return static_cast<DeleteSource>(deletionCause);
    }

    uint32_t getQueuedTime(void) const { return queuedTime; }

    queue_op getOperation(void) const {
        return op;
    }

    /*
     * Should this item be persisted?
     */
    bool shouldPersist() const {
        switch (op) {
        case queue_op::mutation:
        case queue_op::commit_sync_write:
        case queue_op::pending_sync_write:
        case queue_op::system_event:
        case queue_op::set_vbucket_state:
            return true;
        case queue_op::flush:
        case queue_op::empty:
        case queue_op::checkpoint_start:
        case queue_op::checkpoint_end:
            return false;
        }
        // Silence GCC warning
        return false;
    }

    /*
     * Should this item be replicated by DCP?
     * @param supportsSyncReplication true if the DCP stream supports
     *        synchronous replication.
     */
    bool shouldReplicate(bool supportsSyncReplication) const {
        const bool nonMetaItem = !isCheckPointMetaItem();
        if (supportsSyncReplication) {
            return nonMetaItem;
        }
        return nonMetaItem && op != queue_op::pending_sync_write;
    }

    bool isCheckPointMetaItem() const {
        switch (op) {
        case queue_op::mutation:
        case queue_op::pending_sync_write:
        case queue_op::commit_sync_write:
        case queue_op::system_event:
            return false;
        case queue_op::flush:
        case queue_op::empty:
        case queue_op::checkpoint_start:
        case queue_op::checkpoint_end:
        case queue_op::set_vbucket_state:
            return true;
        }
        // Silence GCC warning
        return false;
    }

    /// Returns true if this Item is a meta item, excluding queue_op::empty.
    bool isNonEmptyCheckpointMetaItem() const {
        return isCheckPointMetaItem() && (op != queue_op::empty);
    }

    void setNRUValue(uint8_t nru_value) {
        nru = nru_value;
    }

    uint8_t getNRUValue() const {
        return nru;
    }

    /// Set the frequency counter value to the input value
    void setFreqCounterValue(uint16_t newValue) {
        value.unsafeGetPointer().setTag(newValue);
    }

    /// Gets the frequency counter value
    uint16_t getFreqCounterValue() const {
        return value.get().getTag();
    }

    static uint64_t nextCas();

    /* Returns true if the specified CAS is valid */
    static bool isValidCas(const uint64_t& itmCas) {
        if (itmCas == 0 || itmCas == static_cast<uint64_t>(-1)) {
            return false;
        }
        return true;
    }

    /**
     * Sets the item as being a pendingSyncWrite with the specified durability
     * requirements.
     */
    void setPendingSyncWrite(cb::durability::Requirements requirements);

    /// Sets the item as being a Commited via Pending SyncWrite.
    void setCommittedviaPrepareSyncWrite();

    /// Is this Item Committed (via Mutation or Prepare), or Pending Sync Write?
    CommittedState getCommitted() const {
        switch (op) {
        case queue_op::pending_sync_write:
            return CommittedState::Pending;
        case queue_op::commit_sync_write:
            return CommittedState::CommittedViaPrepare;
        case queue_op::mutation:
        case queue_op::system_event:
            return CommittedState::CommittedViaMutation;
        default:
            throw std::logic_error(
                    "Item::getCommitted(): Called on Item with unexpected "
                    "queue_op:" +
                    to_string(op));
        }
    }

    /// Returns if this is a Pending SyncWrite
    bool isPending() const {
        return op == queue_op::pending_sync_write;
    }

    /**
     * @return the durability requirements for this Item. If the item is not
     * pending, returns requirements of Level::None.
     */
    cb::durability::Requirements getDurabilityReqs() const {
        return durabilityReqs;
    }

    /* Retrieve item_info for this item instance
     *
     * @param vb_uuid the UUID of the associated vbucket
     * @param hlcEpoch item's with seqno > hlcEpoch have a HLC CAS
     * return item_info structure with populate item
     *        information
     */
    item_info toItemInfo(uint64_t vb_uuid, int64_t hlcEpoch) const;

    /**
     * Removes the value and / or the xattributes from the item if they
     * are not to be sent over the wire to the consumer.
     *
     * @param includeVal states whether the item should include value, or not
     * @param includeXattrs states whether the item should include xattrs or not
     **/
    void pruneValueAndOrXattrs(IncludeValue includeVal,
                               IncludeXattrs includeXattrs);

    /// Returns if this item is a system event
    bool isSystemEvent() const {
        return op == queue_op::system_event;
    }

    /**
     * Each Item has a frequency counter that is used by the hifi_mfu hash
     * table eviction policy.  The counter is initialised to "initialFreqCount"
     * when first added to the hash table.  It is not 0, as we want to ensure
     * that we do not immediately evict items that we have just added.
     */
    static const uint8_t initialFreqCount = 4;

private:
    /**
     * Set the item's data. This is only used by constructors, so we
     * make it private.
     */
    void setData(const char* dta, const size_t nb) {
        Blob *data;
        if (dta == nullptr) {
            data = Blob::New(nb);
        } else {
            data = Blob::New(dta, nb);
        }
        replaceValue(data);
    }

    ItemMetaData metaData;
    value_t value;
    StoredDocKey key;

    // bySeqno is atomic because it (rarely) needs to be changed after
    // the item has been added to a Checkpoint - for meta-items in
    // checkpoints when updating a the open checkpointID - see
    // CheckpointManager::setOpenCheckpointId_UNLOCKED
    std::atomic<int64_t> bySeqno;
    uint32_t queuedTime;
    Vbid vbucketId;
    queue_op op;
    uint8_t nru  : 2;
    uint8_t deleted : 1;
    // If deleted, deletionCause stores the cause of the deletion.
    uint8_t deletionCause : 1;

    // Keep a cached version of the datatype. It allows for using
    // "partial" items created from from the hashtable. Every time the
    // caller tries to get / set the datatype we first try to use the
    // real value in the actual blob. If the blob isn't there we use
    // this cached version.
    mutable protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;

    /**
     * Mutations' durability requirements. For non-synchronous mutations has
     * Level==None; for SyncWrites specifies what conditions need to be met
     * before the item is considered durable.
     */
    cb::durability::Requirements durabilityReqs =
            cb::durability::NoRequirements;

    static std::atomic<uint64_t> casCounter;
    DISALLOW_ASSIGN(Item);

    friend bool operator==(const Item& lhs, const Item& rhs);
    friend std::ostream& operator<<(std::ostream& os, const Item& i);
};

bool operator==(const Item& lhs, const Item& rhs);
std::ostream& operator<<(std::ostream& os, const Item& item);

typedef SingleThreadedRCPtr<Item> queued_item;
using UniqueItemPtr = std::unique_ptr<Item>;

// If you're reading this because this assert has failed because you've
// increased Item, ask yourself do you really need to? Can you use padding or
// bit-fields to reduce the size?
// If you've reduced Item size, thanks! Please update the assert with the new
// size.
// Note the assert is written as we see std::string (member of the StoredDocKey)
// differing. This totals 96 or 104 (string being 24 or 32).
static_assert(sizeof(Item) == sizeof(std::string) + 72,
              "sizeof Item may have an effect on run-time memory consumption, "
              "please avoid increasing it");

/**
 * Order QueuedItem objects by their keys and by sequence numbers.
 */
class CompareQueuedItemsBySeqnoAndKey {
public:
    CompareQueuedItemsBySeqnoAndKey() {}
    bool operator()(const queued_item& i1, const queued_item& i2);
};
