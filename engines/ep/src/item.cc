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

#include "ep_time.h"
#include "item.h"
#include "item_eviction.h"
#include "objectregistry.h"

#include <folly/lang/Assume.h>
#include <platform/compress.h>
#include <xattr/utils.h>
#include <chrono>

#include  <iomanip>

std::atomic<uint64_t> Item::casCounter(1);

Item::Item(const DocKey& k,
           const uint32_t fl,
           const time_t exp,
           const value_t& val,
           protocol_binary_datatype_t dtype,
           uint64_t theCas,
           int64_t i,
           Vbid vbid,
           uint64_t sno)
    : metaData(theCas, sno, fl, exp),
      value(TaggedPtr<Blob>(val.get().get(), initialFreqCount)),
      key(k),
      bySeqno(i),
      vbucketId(vbid),
      op(k.getCollectionID() == CollectionID::System ? queue_op::system_event
                                                     : queue_op::mutation),
      deleted(0), // false
      maybeVisible(0),
      preserveTtl(0),
      datatype(dtype) {
    if (bySeqno == 0) {
        throw std::invalid_argument("Item(): bySeqno must be non-zero");
    }

    ObjectRegistry::onCreateItem(this);
}

Item::Item(const DocKey& k,
           const uint32_t fl,
           const time_t exp,
           const void* dta,
           const size_t nb,
           protocol_binary_datatype_t dtype,
           uint64_t theCas,
           int64_t i,
           Vbid vbid,
           uint64_t sno,
           uint8_t freqCount)
    : metaData(theCas, sno, fl, exp),
      value(TaggedPtr<Blob>(nullptr, initialFreqCount)),
      key(k),
      bySeqno(i),
      vbucketId(vbid),
      op(k.getCollectionID() == CollectionID::System ? queue_op::system_event
                                                     : queue_op::mutation),
      deleted(0), // false
      maybeVisible(0),
      preserveTtl(0),
      datatype(dtype) {
    if (bySeqno == 0) {
        throw std::invalid_argument("Item(): bySeqno must be non-zero");
    }
    setData(static_cast<const char*>(dta), nb);
    setFreqCounterValue(freqCount);
    ObjectRegistry::onCreateItem(this);
}

Item::Item(const DocKey& k,
           const Vbid vb,
           queue_op o,
           const uint64_t revSeq,
           const int64_t bySeq)
    : metaData(),
      value(TaggedPtr<Blob>(nullptr, ItemEviction::initialFreqCount)),
      key(k),
      bySeqno(bySeq),
      vbucketId(vb),
      op(o),
      deleted(0), // false
      maybeVisible(0),
      preserveTtl(0) {
    if (bySeqno < 0) {
        throw std::invalid_argument("Item(): bySeqno must be non-negative");
    }
    metaData.revSeqno = revSeq;
    ObjectRegistry::onCreateItem(this);
}

Item::Item(const Item& other)
    : metaData(other.metaData),
      value(other.value), // Implicitly also copies the frequency counter
      key(other.key),
      bySeqno(other.bySeqno.load()),
      prepareSeqno(other.prepareSeqno),
      vbucketId(other.vbucketId),
      op(other.op),
      deleted(other.deleted),
      deletionCause(other.deletionCause),
      maybeVisible(other.maybeVisible),
      preserveTtl(other.preserveTtl),
      datatype(other.datatype),
      durabilityReqs(other.durabilityReqs),
      queuedTime(other.queuedTime) {
    ObjectRegistry::onCreateItem(this);
}

Item::~Item() {
    ObjectRegistry::onDeleteItem(this);
}

std::string to_string(queue_op op) {
    switch(op) {
    case queue_op::mutation:
        return "mutation";
    case queue_op::pending_sync_write:
        return "pending_sync_write";
    case queue_op::commit_sync_write:
        return "commit_sync_write";
    case queue_op::abort_sync_write:
        return "abort_sync_write";
    case queue_op::flush:
        return "flush";
    case queue_op::empty:
        return "empty";
    case queue_op::checkpoint_start:
        return "checkpoint_start";
    case queue_op::checkpoint_end:
        return "checkpoint_end";
    case queue_op::set_vbucket_state:
        return "set_vbucket_state";
    case queue_op::system_event:
        return "system_event";
    }
    return "<" +
            std::to_string(static_cast<std::underlying_type<queue_op>::type>(op)) +
            ">";

}

std::ostream& operator<<(std::ostream& os, const queue_op& op) {
    return os << to_string(op);
}

bool operator==(const Item& lhs, const Item& rhs) {
    return (lhs.metaData == rhs.metaData) && (*lhs.value == *rhs.value) &&
           (lhs.key == rhs.key) && (lhs.bySeqno == rhs.bySeqno) &&
           // Note: queuedTime is *not* compared. The rationale is it is
           // simply used for stats (measureing queue duration) and hence can
           // be ignored from an "equivilence" pov.
           // (lhs.queuedTime == rhs.queuedTime) &&
           (lhs.vbucketId == rhs.vbucketId) && (lhs.op == rhs.op) &&
           (lhs.deleted == rhs.deleted) &&
           // Note: deletionCause is only checked if the item is deleted
           ((lhs.deleted && lhs.deletionCause) ==
            (rhs.deleted && rhs.deletionCause)) &&
           (lhs.durabilityReqs == rhs.durabilityReqs) &&
           lhs.maybeVisible == rhs.maybeVisible &&
           lhs.preserveTtl == rhs.preserveTtl;
}

std::ostream& operator<<(std::ostream& os, const Item& i) {
    os << "Item[" << &i << "] with"
       << " key:" << i.key << "\n";
    if (i.value.get()) {
        os << "\tvalue:" << *i.value << "\n";
    } else {
        os << "\tvalue:nullptr\n";
    }
    os << "\tmetadata:" << i.metaData << "\n"
       << "\tbySeqno:" << i.bySeqno << " queuedTime:"
       << std::chrono::duration_cast<std::chrono::milliseconds>(
                  i.queuedTime.time_since_epoch())
                    .count()
       << " " << i.vbucketId << " op:" << to_string(i.op);
    if (i.maybeVisible) {
        os << "(maybeVisible)";
    }
    os << " datatype:" << int(i.getDataType());

    if (i.isDeleted()) {
        os << " deleted:true(" << to_string(i.deletionSource()) << ")";
    } else {
        os << " deleted:false";
    }
    return os;
}

bool operator==(const ItemMetaData& lhs, const ItemMetaData& rhs) {
    return (lhs.cas == rhs.cas) &&
           (lhs.revSeqno == rhs.revSeqno) &&
           (lhs.flags == rhs.flags) &&
           (lhs.exptime == rhs.exptime);
}

std::ostream& operator<<(std::ostream& os, const ItemMetaData& md) {
    os << "ItemMetaData[" << &md << "] with"
       << " cas:" << md.cas
       << " revSeqno:" << md.revSeqno
       << " flags:" << md.flags
       << " exptime:" << md.exptime;
    return os;
}

bool operator==(const Blob& lhs, const Blob& rhs) {
    return (lhs.size == rhs.size) &&
           (lhs.age == rhs.age) &&
           (memcmp(lhs.data, rhs.data, lhs.size) == 0);
}

std::ostream& operator<<(std::ostream& os, const Blob& b) {
    os << "Blob[" << &b << "] with"
       << " size:" << b.size
       << " age:" << int(b.age)
       << " data: <" << std::hex;
    // Print at most 40 bytes of the body.
    auto bytes_to_print = std::min(uint32_t(40), b.size.load());
    for (size_t ii = 0; ii < bytes_to_print; ii++) {
        if (ii != 0) {
            os << ' ';
        }
        if (isprint(b.data[ii])) {
            os << b.data[ii];
        } else {
            os << std::setfill('0') << std::setw(2) << int(uint8_t(b.data[ii]));
        }
    }
    os << std::dec << '>';
    return os;
}

bool Item::compressValue(bool force) {
    auto datatype = getDataType();
    if (!mcbp::datatype::is_snappy(datatype)) {
        // Attempt compression only if datatype indicates
        // that the value is not compressed already.
        cb::compression::Buffer deflated;
        if (cb::compression::deflate(cb::compression::Algorithm::Snappy,
                                     {getData(), getNBytes()}, deflated)) {
            if (deflated.size() > getNBytes() && !force) {
                //No point doing the compression if the deflated length
                //is greater than the original length
                return true;
            }
            setData(deflated.data(), deflated.size());

            datatype |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
            setDataType(datatype);
        } else {
            return false;
        }
    }
    return true;
}

bool Item::decompressValue() {
    uint8_t datatype = getDataType();
    if (mcbp::datatype::is_snappy(datatype)) {
        // Attempt decompression only if datatype indicates
        // that the value is compressed.
        cb::compression::Buffer inflated;
        if (cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                     {getData(), getNBytes()}, inflated)) {
            setData(inflated.data(), inflated.size());
            datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
            setDataType(datatype);
        } else {
            return false;
        }
    }
    return true;
}

void Item::setDeleted(DeleteSource cause) {
    switch (op) {
    case queue_op::mutation:
    case queue_op::pending_sync_write:
    case queue_op::commit_sync_write:
    case queue_op::abort_sync_write:
        deleted = 1; // true
        deletionCause = static_cast<uint8_t>(cause);
        break;
    case queue_op::system_event:
        if (cause == DeleteSource::TTL) {
            throw std::logic_error(
                    "Item::setDeleted should not expire a system_event");
        }
        deleted = 1; // true
        deletionCause = static_cast<uint8_t>(cause);
        break;
    case queue_op::flush:
    case queue_op::empty:
    case queue_op::checkpoint_start:
    case queue_op::checkpoint_end:
    case queue_op::set_vbucket_state:
        throw std::logic_error("Item::setDeleted cannot delete " +
                               to_string(op));
    }
}

uint64_t Item::nextCas() {
    return std::chrono::steady_clock::now().time_since_epoch().count() +
           (++casCounter);
}

void Item::setPendingSyncWrite(cb::durability::Requirements requirements) {
    if (!requirements.isValid()) {
        throw std::invalid_argument(
                "Item::setPendingSyncWrite: specified requirements are "
                "invalid: " +
                to_string(requirements));
    }
    durabilityReqs = requirements;
    op = queue_op::pending_sync_write;
}

void Item::setPreparedMaybeVisible() {
    Expects(op == queue_op::pending_sync_write);
    maybeVisible = 1;
}

void Item::setCommittedviaPrepareSyncWrite() {
    op = queue_op::commit_sync_write;
}

void Item::setAbortSyncWrite() {
    op = queue_op::abort_sync_write;
}

bool Item::isAnySyncWriteOp() const {
    switch (op) {
        case queue_op::pending_sync_write:
        case queue_op::commit_sync_write:
        case queue_op::abort_sync_write:
            return true;
        case queue_op::mutation:
        case queue_op::system_event:
        case queue_op::flush:
        case queue_op::empty:
        case queue_op::checkpoint_start:
        case queue_op::checkpoint_end:
        case queue_op::set_vbucket_state:
            return false;
    }
    folly::assume_unreachable();
}

item_info Item::toItemInfo(uint64_t vb_uuid, int64_t hlcEpoch) const {
    item_info info;
    info.cas = getCas();
    info.vbucket_uuid = vb_uuid;
    info.seqno = getBySeqno();
    info.revid = getRevSeqno();
    info.exptime = getExptime();
    info.nbytes = getNBytes();
    info.flags = getFlags();
    info.datatype = getDataType();

    if (isDeleted()) {
        info.document_state = DocumentState::Deleted;
    } else {
        info.document_state = DocumentState::Alive;
    }
    info.value[0].iov_base = const_cast<char*>(getData());
    info.value[0].iov_len = getNBytes();

    info.cas_is_hlc = hlcEpoch > HlcCasSeqnoUninitialised &&
                      int64_t(info.seqno) >= hlcEpoch;

    info.key = getKey();
    return info;
}

void Item::pruneValueAndOrXattrs(IncludeValue includeVal,
                                 IncludeXattrs includeXattrs) {
    if (!value) {
        // If the item does not have value (i.e. data and/or xattrs) then no
        // pruning is required.
        return;
    }

    if (includeVal == IncludeValue::Yes) {
        if ((includeXattrs == IncludeXattrs::Yes) ||
                !(mcbp::datatype::is_xattr(getDataType()))) {
            // If we want to include the value and either, we want to include
            // the xattrs or there are no xattrs, then no pruning is required
            // and we can just return.
            return;
        }
    }

    const auto originalDatatype = getDataType();

    if (includeXattrs == IncludeXattrs::No &&
        ((includeVal == IncludeValue::No) ||
         (includeVal == IncludeValue::NoWithUnderlyingDatatype))) {
        // Don't want the xattributes or value, so just send the key
        setData(nullptr, 0);
        setDataType(PROTOCOL_BINARY_RAW_BYTES);
    } else {
        // Call decompress before working on the value (a no-op for non-snappy)
        decompressValue();

        auto root = reinterpret_cast<const char*>(value->getData());
        const cb::const_char_buffer buffer{root, value->valueSize()};

        if (includeXattrs == IncludeXattrs::Yes) {
            if (mcbp::datatype::is_xattr(getDataType())) {
                // Want just the xattributes
                setData(value->getData(), cb::xattr::get_body_offset(buffer));
                // Remove all other datatype flags as we're only sending the
                // xattrs
                setDataType(PROTOCOL_BINARY_DATATYPE_XATTR);
            } else {
                // We don't want the value and there are no xattributes,
                // so just send the key
                setData(nullptr, 0);
                setDataType(PROTOCOL_BINARY_RAW_BYTES);
            }
        } else if (includeVal == IncludeValue::Yes) {
            // Want just the value, so remove xattributes if there are any
            if (mcbp::datatype::is_xattr(getDataType())) {
                const auto sz = cb::xattr::get_body_offset(buffer);
                setData(value->getData() + sz, value->valueSize() - sz);
                // Clear the xattr datatype
                setDataType(getDataType() & ~PROTOCOL_BINARY_DATATYPE_XATTR);
            }
        }
    }

    // MB-31967: Restore the complete datatype if
    // IncludeValue::NoWithUnderlyingDatatype was specified (less disruptive
    // to above logic to do this once at the end).
    if (includeVal == IncludeValue::NoWithUnderlyingDatatype) {
        setDataType(originalDatatype);
    }
}

item_info to_item_info(const ItemMetaData& itemMeta,
                       uint8_t datatype,
                       uint32_t deleted) {
    item_info info;
    info.cas = itemMeta.cas;
    info.datatype = datatype;
    info.exptime = itemMeta.exptime;
    info.flags = itemMeta.flags;
    info.seqno = itemMeta.revSeqno;
    info.document_state =
            deleted ? DocumentState::Deleted : DocumentState::Alive;

    return info;
}

bool OrderItemsForDeDuplication::operator()(const queued_item& i1,
                                            const queued_item& i2) {
    // First compare keys - if they differ then that's sufficient to
    // distinguish them.
    const auto comp = i1->getKey().compare(i2->getKey());
    if (comp < 0) {
        return true;
    }
    if (comp > 0) {
        return false;
    }

    // Same key - compare namespaces (committed items don't de-duplicate
    // prepared ones and vice-versa).
    if (i1->isCommitted() < i2->isCommitted()) {
        return true;
    }
    if (i1->isCommitted() > i2->isCommitted()) {
        return false;
    }

    // Keys and namespace equal - need to check seqno.
    return i1->getBySeqno() > i2->getBySeqno();
}
