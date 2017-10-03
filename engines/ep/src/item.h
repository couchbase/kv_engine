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

#include <memcached/engine.h>
#include <stdio.h>
#include <string.h>
#include <utility>

#include <cstring>
#include <string>

#include "atomic.h"
#include "blob.h"
#include "dcp/dcp-types.h"
#include "storeddockey.h"

/// The set of possible operations which can be queued into a checkpoint.
enum class queue_op : uint8_t {
    /// Set a document key to a given value. Sets to the same key can (and
    /// typically are) de-duplicated - only the most recent queue_op::set in a
    /// checkpoint will be kept. This means that there's no guarantee that
    /// clients will see all intermediate values of a key.
    set,

    /// Delete a key. Deletes can be de-duplicated with respect to queue_op::set -
    /// set(key) followed by del(key) will result in just del(key).
    del,

    /// (meta item) Testing only op, used to mark the end of a test.
    /// TODO: Remove this, it shouldn't be necessary / included just to support
    /// testing.
    flush,

    /// (meta item) Dummy op added to the start of checkpoints to simplify
    /// checkpoint logic.
    /// This is because our Checkpoints are structured such that
    /// CheckpointCursors are advanced before dereferencing them, not after -
    /// see Checkpoint documentation for details. As such we need to have an
    /// empty/dummy element at the start of each Checkpoint, so after the first
    /// advance the cursor is pointing at the 'real' first element (normally
    /// checkpoint_start).
    ///
    /// Unlike other operations, queue_op::empty is ignored for the purposes of
    /// CheckpointManager::numItems - due to it only existing as a placeholder.
    empty,

    /// (meta item) Marker for the start of a checkpoint.
    /// All checkpoints (open or closed) will start with an item of this type.
    /// Like all meta items, this doens't directly match user operations, but
    /// is used to delineate the start of a checkpoint.
    checkpoint_start,

    /// (meta item) Marker for the end of a checkpoint. Only exists in closed
    /// checkpoints, where it is always the last item in the checkpoint.
    checkpoint_end,

    /// (meta item) Marker to persist the VBucket's state (vbucket_state) to
    /// disk. No data (value) associated with it, simply acts as a marker
    /// to ensure a non-empty persistence queue.
    set_vbucket_state,

    /// System events are created by the code to represent something triggered
    /// by the system, possibly orginating from a user's action.
    /// The flags field of system_event describes the detail along with the key
    /// which must at least be unique per event type.
    system_event
};

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
    uint64_t revSeqno;
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
         uint16_t vbid = 0,
         uint64_t sno = 1,
         uint8_t nru_value = INITIAL_NRU_VALUE);

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
         uint16_t vbid = 0,
         uint64_t sno = 1,
         uint8_t nru_value = INITIAL_NRU_VALUE);

    Item(const DocKey& k,
         const uint16_t vb,
         queue_op o,
         const uint64_t revSeq,
         const int64_t bySeq,
         uint8_t nru_value = INITIAL_NRU_VALUE);

    /* Copy constructor */
    Item(const Item& other);

    ~Item();

    /* Snappy compress value and update datatype */
    bool compressValue();

    /* Snappy uncompress value and update datatype */
    bool decompressValue();

    const char *getData() const {
        return value.get() ? value->getData() : NULL;
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
        return value.get() ? static_cast<uint32_t>(value->valueSize()) : 0;
    }

    size_t getValMemSize() const {
        return value.get() ? value->getSize() : 0;
    }

    time_t getExptime() const {
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

    void setValue(const value_t &v) {
        value.reset(v);
    }

    void setFlags(uint32_t f) {
        metaData.flags = f;
    }

    void setExpTime(time_t exp_time) {
        metaData.exptime = exp_time;
    }

    uint16_t getVBucketId(void) const {
        return vbucketId;
    }

    void setVBucketId(uint16_t to) {
        vbucketId = to;
    }

    /**
     * Check if this item is expired or not.
     *
     * @param asOf the time to be compared with this item's expiry time
     * @return true if this item's expiry time < asOf
     */
    bool isExpired(time_t asOf) const {
        if (metaData.exptime != 0 && metaData.exptime < asOf) {
            return true;
        }
        return false;
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

    static uint32_t getNMetaBytes() {
        return metaDataSize;
    }

    const ItemMetaData& getMetaData() const {
        return metaData;
    }

    bool isDeleted() const {
        return op == queue_op::del;
    }

    void setDeleted() {
        op = queue_op::del;
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
            case queue_op::set:
            case queue_op::del:
                return true;
            case queue_op::flush:
            case queue_op::empty:
            case queue_op::checkpoint_start:
            case queue_op::checkpoint_end:
                return false;
            case queue_op::system_event:
            case queue_op::set_vbucket_state:
                return true;
        }
        // Silence GCC warning
        return false;
    }

    /*
     * Should this item be replicated (e.g. by DCP)
     */
    bool shouldReplicate() const {
        return !isCheckPointMetaItem();
    }

    void setOperation(queue_op o) {
        op = o;
    }

    bool isCheckPointMetaItem() const {
        switch (op) {
            case queue_op::set:
            case queue_op::del:
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

    static uint64_t nextCas(void) {
        return gethrtime() + (++casCounter);
    }

    /* Returns true if the specified CAS is valid */
    static bool isValidCas(const uint64_t& itmCas) {
        if (itmCas == 0 || itmCas == static_cast<uint64_t>(-1)) {
            return false;
        }
        return true;
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
        setValue(data);
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
    uint16_t vbucketId;
    queue_op op;
    uint8_t nru  : 2;

    // Keep a cached version of the datatype. It allows for using
    // "partial" items created from from the hashtable. Every time the
    // caller tries to get / set the datatype we first try to use the
    // real value in the actual blob. If the blob isn't there we use
    // this cached version.
    mutable protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;

    static std::atomic<uint64_t> casCounter;
    static const uint32_t metaDataSize;
    DISALLOW_ASSIGN(Item);

    friend bool operator==(const Item& lhs, const Item& rhs);
    friend std::ostream& operator<<(std::ostream& os, const Item& i);
};

bool operator==(const Item& lhs, const Item& rhs);
std::ostream& operator<<(std::ostream& os, const Item& item);

typedef SingleThreadedRCPtr<Item> queued_item;
using UniqueItemPtr = std::unique_ptr<Item>;

/**
 * Order QueuedItem objects by their keys and by sequence numbers.
 */
class CompareQueuedItemsBySeqnoAndKey {
public:
    CompareQueuedItemsBySeqnoAndKey() {}
    bool operator()(const queued_item &i1, const queued_item &i2) {
        return i1->getKey() == i2->getKey()
            ? i1->getBySeqno() > i2->getBySeqno()
            : i1->getKey() < i2->getKey();
    }
};
