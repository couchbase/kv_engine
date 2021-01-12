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

#include "item.h"
#include "ep_time.h"
#include "item_eviction.h"
#include "logtags.h"
#include "objectregistry.h"

#include <folly/lang/Assume.h>
#include <platform/compress.h>
#include <xattr/blob.h>
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
      op(k.isInSystemCollection() ? queue_op::system_event
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
      op(k.isInSystemCollection() ? queue_op::system_event
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
    os << "Item[" << &i << "] with key:";

    if (i.useDocKeyDump()) {
        os << i.key << "\n";
    } else {
        // Just use the raw c_str for these operation types - e.g "set_vb_state"
        os << i.key.c_str() << "\n";
    }

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

    // Just do compression if the caller requires so, or decide based on the
    // datatype.
    //
    // MB-40493: Currently force==true is expected only for handling the case
    // where the value has (1) no Body, (2) an uncompressed Xattr chunk and (2)
    // datatype is still Snappy. That may happen in ActiveStream when the
    // connection is set to IncludeValue::NoWithUnderlyingDatatype.

    if (mcbp::datatype::is_snappy(datatype) && !force) {
        return true;
    }

    cb::compression::Buffer deflated;
    if (!cb::compression::deflate(cb::compression::Algorithm::Snappy,
                                  {getData(), getNBytes()},
                                  deflated)) {
        // Compression failed
        return false;
    }

    if (deflated.size() > getNBytes()) {
        // No point doing the compression if the deflated length is greater
        // than the original length - but this is still considered "success".
        return true;
    }
    setData(deflated.data(), deflated.size());

    datatype |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
    setDataType(datatype);
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

void Item::increaseDurabilityLevel(cb::durability::Level newLevel) {
    const auto level = durabilityReqs.getLevel();
    if (level < newLevel) {
        durabilityReqs.setLevel(newLevel);

        // Transitioning from NormalWrite to SyncWrite?
        if (level == cb::durability::Level::None) {
            Expects(op == queue_op::mutation);
            op = queue_op::pending_sync_write;
        }
    }
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

    // Note: An abort has the same life-cycle as a deletion. If we are building
    // an item via the KVStore or a StoredValue it should already be deleted.
    if (!deleted) {
        // Cause doesn't matter here, aborts will get sent as aborts not
        // deletions or expiries.
        setDeleted();
    }
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

Item::WasValueInflated Item::removeBody() {
    if (!value) {
        // No value, nothing to do
        return WasValueInflated::No;
    }

    if (!mcbp::datatype::is_xattr(getDataType())) {
        // We don't want the body and there are no xattrs, just set empty value
        setData(nullptr, 0);
        setDataType(PROTOCOL_BINARY_RAW_BYTES);
        return WasValueInflated::No;
    }

    // No-op if already uncompressed
    const auto wasInflated = decompressValue();

    // We want only xattrs.
    // Note: The following is no-op if no Body present.
    std::string_view valBuffer{value->getData(), value->valueSize()};
    setData(valBuffer.data(), cb::xattr::get_body_offset(valBuffer));
    setDataType(PROTOCOL_BINARY_DATATYPE_XATTR);

    return wasInflated ? WasValueInflated::Yes : WasValueInflated::No;
}

Item::WasValueInflated Item::removeXattrs() {
    if (!value) {
        // No value, nothing to do
        return WasValueInflated::No;
    }

    if (!mcbp::datatype::is_xattr(getDataType())) {
        // No Xattrs, nothing to do
        return WasValueInflated::No;
    }

    // No-op if already uncompressed
    const auto wasInflated = decompressValue();

    // We want only the body
    std::string_view valBuffer{value->getData(), value->valueSize()};
    const auto bodyOffset = cb::xattr::get_body_offset(valBuffer);
    valBuffer.remove_prefix(bodyOffset);
    setData(valBuffer.data(), valBuffer.size());
    setDataType(getDataType() & ~PROTOCOL_BINARY_DATATYPE_XATTR);

    if (getNBytes() == 0) {
        // Docs with no body and Xattrs may be created with DATATYPE_JSON to
        // bypass the Subdoc restriction on DATATYPE_RAW | DATATYPE_XATTR, see
        // Subdoc logic for details. Here we have to rectify.
        setDataType(getDataType() & ~PROTOCOL_BINARY_DATATYPE_JSON);
    }

    return wasInflated ? WasValueInflated::Yes : WasValueInflated::No;
}

Item::WasValueInflated Item::removeUserXattrs() {
    if (!value) {
        // No value, nothing to do
        return WasValueInflated::No;
    }

    if (!mcbp::datatype::is_xattr(getDataType())) {
        // No Xattrs, nothing to do
        return WasValueInflated::No;
    }

    // No-op if already uncompressed
    const auto wasInflated = decompressValue();

    // The function currently does not support value with body.
    // That is fine for now as this is introduced for MB-37374, thus is supposed
    // to operate only against deleted items, which don't contain any body.
    Expects(isDeleted());
    const auto valNBytes = value->valueSize();
    cb::char_buffer valBuf{const_cast<char*>(value->getData()), valNBytes};
    const auto bodySize = valNBytes - cb::xattr::get_body_offset(
                                              {value->getData(), valNBytes});
    if (bodySize > 0) {
        std::stringstream ss;
        ss << *this;
        throw std::logic_error(
                "Item::removeUserXattrs: Unexpected body (size " +
                std::to_string(bodySize) + ") in deletion: " +
                cb::UserDataView(ss.str()).getSanitizedValue());
    }

    // Operate on a copy
    const cb::xattr::Blob originalBlob(valBuf, false /*compressed*/);
    auto copy = cb::xattr::Blob(originalBlob);
    copy.prune_user_keys();
    const auto final = copy.finalize();
    setData(final.data(), final.size());

    // We have removed all user-xattrs, clear the xattr dt if no xattr left
    if (copy.get_system_size() == 0) {
        setDataType(getDataType() & ~PROTOCOL_BINARY_DATATYPE_XATTR);
    }

    // Docs with no body and Xattrs may be created with DATATYPE_JSON to bypass
    // the Subdoc restriction on DATATYPE_RAW | DATATYPE_XATTR, see Subdoc logic
    // for details. Here we have to rectify.
    // Note: Doing this unconditionally as we reach this line iff there is no
    // body. We would need to do this conditionally otherwise.
    setDataType(getDataType() & ~PROTOCOL_BINARY_DATATYPE_JSON);

    return wasInflated ? WasValueInflated::Yes : WasValueInflated::No;
}

Item::WasValueInflated Item::removeBodyAndOrXattrs(
        IncludeValue includeVal,
        IncludeXattrs includeXattrs,
        IncludeDeletedUserXattrs includeDeletedUserXattrs) {
    if (!value) {
        // If no value (ie, no body and/or xattrs) then nothing to do
        return WasValueInflated::No;
    }

    // Take a copy of the original datatype before proceeding, any modification
    // to the value may change the datatype.
    const auto originalDatatype = getDataType();

    auto wasInflated = WasValueInflated::No;

    // Note: IncludeValue acts like "include body"
    if (includeVal != IncludeValue::Yes) {
        wasInflated = removeBody();
    }

    if (includeXattrs == IncludeXattrs::No) {
        wasInflated = removeXattrs();
    }

    if (isDeleted() &&
        includeDeletedUserXattrs == IncludeDeletedUserXattrs::No) {
        wasInflated = removeUserXattrs();
    }

    // Datatype for no-value must be RAW
    if (getNBytes() == 0) {
        Expects(datatype == PROTOCOL_BINARY_RAW_BYTES);
    }

    // MB-31967: Restore the complete datatype if requested
    if (includeVal == IncludeValue::NoWithUnderlyingDatatype) {
        setDataType(originalDatatype);
    }

    return wasInflated;
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
