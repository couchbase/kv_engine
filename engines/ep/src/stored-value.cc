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

#include "stored-value.h"

#include "ep_time.h"
#include "item.h"
#include "objectregistry.h"
#include "systemevent_factory.h"

#include <platform/compress.h>

#include <collections/vbucket_manifest.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <nlohmann/json.hpp>
#include <sstream>

const int64_t StoredValue::state_pending_seqno = -2;
const int64_t StoredValue::state_deleted_key = -3;
const int64_t StoredValue::state_non_existent_key = -4;
const int64_t StoredValue::state_temp_init = -5;

StoredValue::StoredValue(const Item& itm, UniquePtr n, bool isOrdered)
    : value(itm.getValue()),
      chain_next_or_replacement(std::move(n)),
      bySeqno(itm.getBySeqno()),
      exptime(itm.getExptime()),
      flags(itm.getFlags()),
      revSeqno(itm.getRevSeqno()),
      datatype(itm.getDataType()),
      ordered(isOrdered),
      deletionSource(0),
      committed(static_cast<uint8_t>(CommittedState::CommittedViaMutation)),
      casIsSeparate(0) {
    // Initialise bit fields
    setDeletedPriv(itm.isDeleted());
    setResident(!isTempItem());
    setStale(false);
    setCommitted(itm.getCommitted());
    setAge(0);
    setFreqCounterValue(
            itm.getFreqCounterValue().value_or(Item::initialFreqCount));
    // dirty initialised below

    setCas(itm.getCas());

    // Placement-new the key which lives in memory directly after this
    // object.
    new (key()) SerialisedDocKey(itm.getKey());

    if (isTempInitialItem()) {
        markClean();
    } else {
        markDirty();
    }

    if (isTempItem()) {
        resetValue();
    }

    if (itm.isDeleted()) {
        setDeletionSource(itm.deletionSource());
    }

    ObjectRegistry::onCreateStoredValue(this);
}

StoredValue::~StoredValue() {
    ObjectRegistry::onDeleteStoredValue(this);
    // If still has a locked CAS, must clear this to delete the CasPair before
    // destroying StoredValue.
    clearLockedCas();
}

StoredValue::StoredValue(const StoredValue& other, UniquePtr n)
    : value(other.value), // Implicitly also copies the frequency counter
      chain_next_or_replacement(std::move(n)),
      bySeqno(other.bySeqno),
      lock_expiry_or_delete_or_complete_time(
              other.lock_expiry_or_delete_or_complete_time),
      exptime(other.exptime),
      flags(other.flags),
      revSeqno(other.revSeqno),
      datatype(other.datatype),
      ordered(other.ordered),
      casIsSeparate(0) {
    setDirty(other.isDirty());
    setDeletedPriv(other.isDeleted());
    setResident(other.isResident());
    setStale(false);
    setCommitted(other.getCommitted());
    setAge(0);
    setCas(other.getCas());
    // Placement-new the key which lives in memory directly after this
    // object.
    StoredDocKey sKey(other.getKey());
    new (key()) SerialisedDocKey(sKey);

    if (isDeleted()) {
        setDeletionSource(other.getDeletionSource());
    }

    ObjectRegistry::onCreateStoredValue(this);
}

bool StoredValue::eligibleForEviction(EvictionPolicy policy,
                                      bool keepSV) const {
    // Always resident:
    // * Dirty SVs
    // * Pending SyncWrites
    if (isDirty() || isPending()) {
        return false;
    }

    // Clean, resident SVs are always eligible for eviction - can evict at least
    // value.
    if (isResident()) {
        return true;
    }

    // SV is non-resident and we want to keep the metadata in memory, so there
    // is nothing else to evict (this applies to deletes).
    if (keepSV) {
        return false;
    }

    // Clean, deleted SVs can be entirely removed in any eviction policy,
    // conditional on keepMetadata.
    if (isDeleted()) {
        return true;
    }

    // In full eviction, a SV may become non-resident when it is evicted
    // while locked. Revisit when it is unlocked.
    if (policy == EvictionPolicy::Full) {
        return !isLocked(ep_current_time());
    }

    // Non-resident SVs in value eviction have already been evicted.
    return false;
}

void StoredValue::setValue(const Item& itm) {
    if (isOrdered()) {
        static_cast<OrderedStoredValue*>(this)->setValueImpl(itm);
        return;
    }
    this->setValueImpl(itm);
}

void StoredValue::ejectValue() {
    markNotResident();
}

void StoredValue::restoreValue(const Item& itm) {
    if (isTempInitialItem() || isTempDeletedItem()) {
        setCas(itm.getCas());
        flags = itm.getFlags();
        exptime = itm.getExptime();
        revSeqno = itm.getRevSeqno();
        bySeqno = itm.getBySeqno();
    }
    datatype = itm.getDataType();
    setDeletedPriv(itm.isDeleted());

    // Use the Item frequency counter but keep our age
    auto freq = itm.getFreqCounterValue();
    auto age = getAge();

    value = itm.getValue();

    setFreqCounterValue(freq.value_or(Item::initialFreqCount));
    setCommitted(itm.getCommitted());
    setAge(age);
    setResident(true);
}

void StoredValue::restoreMeta(const Item& itm) {
    setCas(itm.getCas());
    flags = itm.getFlags();
    datatype = itm.getDataType();
    exptime = itm.getExptime();
    revSeqno = itm.getRevSeqno();
    if (itm.isDeleted()) {
        setTempDeleted();
    } else { /* Regular item with the full eviction */
        bySeqno = itm.getBySeqno();
    }
    setFreqCounterValue(
            itm.getFreqCounterValue().value_or(Item::initialFreqCount));
    setCommitted(itm.getCommitted());
}

void StoredValue::lock(rel_time_t expiry, uint64_t lockedCas) {
    if (isDeleted()) {
        // Cannot lock Deleted items.
        throw std::logic_error("StoredValue::lock: Called on Deleted item");
    }
    setLockedCas(lockedCas);
    lock_expiry_or_delete_or_complete_time.lock_expiry = expiry;
}

void StoredValue::unlock() {
    if (isDeleted()) {
        // Deleted items are not locked - just skip.
        return;
    }
    clearLockedCas();
    lock_expiry_or_delete_or_complete_time.lock_expiry = 0;
}

size_t StoredValue::uncompressedValuelen() const {
    if (!value) {
        return 0;
    }
    if (cb::mcbp::datatype::is_snappy(datatype)) {
        return cb::compression::getUncompressedLengthSnappy(
                {value->getData(), value->valueSize()});
    }
    return valuelen();
}

bool StoredValue::del(DeleteSource delSource) {
    if (isOrdered()) {
        return static_cast<OrderedStoredValue*>(this)->deleteImpl(delSource);
    }
    return this->deleteImpl(delSource);
}

size_t StoredValue::getRequiredStorage(const DocKeyView& key) {
    return sizeof(StoredValue) + SerialisedDocKey::getObjectSize(key.size());
}

std::unique_ptr<Item> StoredValue::toItem(
        Vbid vbid,
        HideLockedCas hideLockedCas,
        IncludeValue includeValue,
        std::optional<cb::durability::Requirements> durabilityReqs) const {
    auto item = toItemBase(vbid, hideLockedCas, includeValue);

    // Set the correct CommittedState and associated properties (durability
    // requirements) on the new item.
    switch (getCommitted()) {
    case CommittedState::Pending:
    case CommittedState::PreparedMaybeVisible:
    case CommittedState::PrepareCommitted:
        if (!durabilityReqs) {
            throw std::logic_error(
                    "StoredValue::toItemImpl: attempted to create Item from "
                    "Pending StoredValue without supplying durabilityReqs");
        }
        item->setPendingSyncWrite(*durabilityReqs);
        if (isPreparedMaybeVisible()) {
            item->setPreparedMaybeVisible();
        }
        break;
    case CommittedState::CommittedViaPrepare:
        item->setCommittedviaPrepareSyncWrite();
        break;
    case CommittedState::CommittedViaMutation:
        // nothing do to.
        break;
    case CommittedState::PrepareAborted:
        item->setAbortSyncWrite();
        break;
    }

    if (isOrdered()) {
        item->setPrepareSeqno(
                static_cast<const OrderedStoredValue*>(this)->prepareSeqno);
    }

    return item;
}

std::unique_ptr<Item> StoredValue::toItemAbort(Vbid vbid) const {
    auto item = toItemBase(vbid, HideLockedCas::No, IncludeValue::No);
    item->setAbortSyncWrite();
    return item;
}

void StoredValue::reallocate() {
    // Allocate a new Blob for this stored value; copy the existing Blob to
    // the new one and free the old.
    replaceValue(std::unique_ptr<Blob>{Blob::Copy(*value)});
}

void StoredValue::Deleter::operator()(StoredValue* val) {
    if (val->isOrdered()) {
        delete static_cast<OrderedStoredValue*>(val);
    } else {
        delete val;
    }
}

OrderedStoredValue* StoredValue::toOrderedStoredValue() {
    if (isOrdered()) {
        return static_cast<OrderedStoredValue*>(this);
    }
    throw std::bad_cast();
}

const OrderedStoredValue* StoredValue::toOrderedStoredValue() const {
    if (isOrdered()) {
        return static_cast<const OrderedStoredValue*>(this);
    }
    throw std::bad_cast();
}

bool StoredValue::operator==(const StoredValue& other) const {
    bool orderedEqual = isOrdered() == other.isOrdered();
    // Actually compare the two OSV members if they are ordered.
    if (isOrdered() && other.isOrdered()) {
        auto& osv = static_cast<const OrderedStoredValue&>(*this);
        auto& otherOsv = static_cast<const OrderedStoredValue&>(other);
        orderedEqual = osv.prepareSeqno == otherOsv.prepareSeqno;
    }
    return (getCas() == other.getCas() && revSeqno == other.revSeqno &&
            bySeqno == other.bySeqno &&
            lock_expiry_or_delete_or_complete_time.lock_expiry ==
                    other.lock_expiry_or_delete_or_complete_time.lock_expiry &&
            exptime == other.exptime && flags == other.flags &&
            isDirty() == other.isDirty() && isDeleted() == other.isDeleted() &&
            // Note: deletionCause is only checked if the item is deleted
            ((deletionSource && isDeleted()) ==
             (other.isDeleted() && other.deletionSource)) &&
            isResident() == other.isResident() && getKey() == other.getKey() &&
            getCommitted() == other.getCommitted() && orderedEqual);
}

bool StoredValue::operator!=(const StoredValue& other) const {
    return !(*this == other);
}

bool StoredValue::deleteImpl(DeleteSource delSource) {
    if (isDeleted() && !getValue()) {
        // SV is already marked as deleted and has no value - no further
        // deletion possible.
        return false;
    }

    resetValue();
    setDatatype(PROTOCOL_BINARY_RAW_BYTES);
    setPendingSeqno();

    setDeletedPriv(true);
    setDeletionSource(delSource);
    setResident(true);
    markDirty();

    return true;
}

std::unique_ptr<Item> StoredValue::toItemBase(Vbid vbid,
                                              HideLockedCas hideLockedCas,
                                              IncludeValue includeValue) const {
    auto item = std::make_unique<Item>(
            getKey(),
            getFlags(),
            getExptime(),
            includeValue == IncludeValue::Yes ? value : value_t{},
            includeValue == IncludeValue::Yes ? datatype
                                              : PROTOCOL_BINARY_RAW_BYTES,
            hideLockedCas == HideLockedCas::Yes ? static_cast<uint64_t>(-1)
                                                : getCas(),
            bySeqno,
            vbid,
            getRevSeqno());

    item->setFreqCounterValue(getFreqCounterValue());

    if (isDeleted()) {
        item->setDeleted(getDeletionSource());
    }

    return item;
}

void StoredValue::setValueImpl(const Item& itm) {
    setDeletedPriv(itm.isDeleted());
    if (itm.isDeleted()) {
        setDeletionSource(itm.deletionSource());
    }

    flags = itm.getFlags();
    datatype = itm.getDataType();
    bySeqno = itm.getBySeqno();
    setCas(itm.getCas());
    lock_expiry_or_delete_or_complete_time.lock_expiry = 0;
    exptime = itm.getExptime();
    revSeqno = itm.getRevSeqno();

    if (isTempInitialItem()) {
        markClean();
    } else {
        markDirty();
    }

    if (isTempItem()) {
        setResident(false);
    } else {
        setResident(true);
        replaceValue(itm.getValue());
    }
    setCommitted(itm.getCommitted());
}

StoredValue::CasPair StoredValue::getCasPair() const {
    switch (getCasEncoding()) {
    case CasEncoding::InlineSingle:
        // `cas' encodes the original CAS value, locked CAS is empty.
        CasPair pair;
        pair.originalCAS = cas.single;
        return pair;
    case CasEncoding::SeparateDouble:
        // 'cas' points to a CasPair holding two CAS values:
        return *cas.pair;
    }
    folly::assume_unreachable();
}

void StoredValue::setLockedCas(uint64_t lockedCas) {
    // Switch to SeparateDouble encoding (if not already)
    switch (getCasEncoding()) {
    case CasEncoding::InlineSingle: {
        // Create separate CasPair.
        const auto originalCas = cas.single;
        cas.pair = new CasPair{originalCas, lockedCas};
        casIsSeparate = 1;
        return;
    }
    case CasEncoding::SeparateDouble:
        // Format already correct, just update lockedCas.
        cas.pair->lockedCAS = lockedCas;
        return;
    }
    folly::assume_unreachable();
}

void StoredValue::clearLockedCas() {
    // Switch to InlineSingle encoding (if not already)
    switch (getCasEncoding()) {
    case CasEncoding::InlineSingle:
        // Format already correct (double unlock?) -
        // for robustness allow this.
        return;
    case CasEncoding::SeparateDouble: {
        // Free separate pair, put originalCas back.
        const auto originalCas = cas.pair->originalCAS;
        delete cas.pair;
        cas.single = originalCas;
        casIsSeparate = 0;
        return;
    }
    }
    folly::assume_unreachable();
}

void StoredValue::storeCompressedBuffer(std::string_view deflated) {
    std::unique_ptr<Blob> data(Blob::New(deflated.data(), deflated.size()));
    datatype |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
    replaceValue(std::move(data));
}

/**
 * Get an item_info from the StoredValue
 */
std::optional<item_info> StoredValue::getItemInfo(uint64_t vbuuid) const {
    if (isTempItem()) {
        return std::nullopt;
    }

    item_info info;
    info.cas = getCas();
    info.vbucket_uuid = vbuuid;
    info.seqno = bySeqno;
    info.exptime = exptime;
    info.nbytes = 0;
    info.flags = flags;
    info.datatype = datatype;
    info.document_state =
            isDeleted() ? DocumentState::Deleted : DocumentState::Alive;
    if (getValue()) {
        info.value[0].iov_base = const_cast<char*>(getValue()->getData());
        info.value[0].iov_len = getValue()->valueSize();
    }
    info.key = getKey();
    return info;
}

uint8_t StoredValue::getAge() const {
    return getValueTag().fields.age;
}

void StoredValue::setAge(uint8_t age) {
    auto tag = getValueTag();
    tag.fields.age = age;
    setValueTag(tag);
}

void StoredValue::incrementAge() {
    auto age = getAge();
    if (age < std::numeric_limits<uint8_t>::max()) {
        age++;
        setAge(age);
    }
}

void StoredValue::setCompletedOrDeletedTime(time_t time) {
    if (isOrdered()) {
        // Only applicable to OSV
        static_cast<OrderedStoredValue*>(this)->setCompletedOrDeletedTime(time);
    }
}

void to_json(nlohmann::json& json, const StoredValue& sv) {
    // Add any additional OrderedStoredValue data if required.
    if (sv.isOrdered()) {
        auto& osv = static_cast<const OrderedStoredValue&>(sv);
        json["prepareSeqno"] = static_cast<uint64_t>(osv.prepareSeqno);
    }

    json["cas"] = sv.getCas();
    json["rev"] = static_cast<uint64_t>(sv.revSeqno);
    json["seqno"] = sv.bySeqno.load();
    json["l/e/d/c time"] =
            sv.lock_expiry_or_delete_or_complete_time.lock_expiry;
    json["exp time"] = sv.exptime;
    json["flags"] = sv.flags;
    json["dirty"] = sv.isDirty();
    json["deleted"] = sv.isDeleted();
    if (sv.isDeleted()) {
        json["delSource"] = sv.deletionSource;
    }
    json["ordered"] = sv.isOrdered();
    json["resident"] = sv.isResident();
    std::stringstream ss;
    ss << sv.getKey();
    json["key"] = ss.str();
    json["committed"] = static_cast<int>(sv.getCommitted());
}

static std::string getSystemEventsValueFromStoredValue(const StoredValue& sv) {
    using namespace Collections::VB;

    if (!sv.getKey().isInSystemEventCollection()) {
        throw std::invalid_argument(
                "getSystemEventsValueFromStoredValue(): StoredValue must be a "
                "SystemEvent");
    }

    // If a system event do not attempt to decode when snappy/xattr
    if (cb::mcbp::datatype::is_xattr(sv.getDatatype()) ||
        cb::mcbp::datatype::is_snappy(sv.getDatatype())) {
        return "xattr/snappy";
    }

    auto systemEventType =
            SystemEventFactory::getSystemEventType(sv.getKey()).first;
    std::string_view itemValue{sv.getValue()->getData(),
                               sv.getValue()->getSize()};

    switch (systemEventType) {
    case SystemEvent::Scope: {
        if (sv.isDeleted()) {
            auto eventData = Manifest::getDropScopeEventData(itemValue);
            return to_string(eventData);
        }
        auto eventData = Manifest::getCreateScopeEventData(itemValue);
        return to_string(eventData);
    }
    case SystemEvent::Collection:
    case SystemEvent::ModifyCollection: {
        if (sv.isDeleted()) {
            return to_string(Manifest::getDropEventData(itemValue));
        }
        return to_string(Manifest::getCollectionEventData(itemValue));
    }
    }

    throw std::invalid_argument(
            "getSystemEventsValueFromStoredValue(): StoredValue must be a "
            "SystemEvent for a collection or scope");
}

std::ostream& operator<<(std::ostream& os, const StoredValue& sv) {
    const auto now = ep_current_time();
    // type, address
    os << (sv.isOrdered() ? "OSV @" : " SV @") << &sv << " ";

    // datatype: XCJ
    os << (cb::mcbp::datatype::is_xattr(sv.getDatatype()) ? 'X' : '.');
    os << (cb::mcbp::datatype::is_snappy(sv.getDatatype()) ? 'C' : '.');
    os << (cb::mcbp::datatype::is_json(sv.getDatatype()) ? 'J' : '.');
    os << ' ';

    // dirty (Written), deleted, new, locked
    os << (sv.isDirty() ? 'W' : '.');
    os << (sv.isDeleted() ? 'D' : '.');
    os << (sv.isResident() ? 'R' : '.');
    os << (sv.isLocked(now) ? 'L' : '.');
    switch (sv.getCommitted()) {
    case CommittedState::CommittedViaMutation:
        os << "Cm";
        break;
    case CommittedState::CommittedViaPrepare:
        os << "Cp";
        break;
    case CommittedState::Pending:
        os << "Pe";
        break;
    case CommittedState::PreparedMaybeVisible:
        os << "Pv";
        break;
    case CommittedState::PrepareAborted:
        os << "Pa";
        break;
    case CommittedState::PrepareCommitted:
        os << "Pc";
        break;
    }

    if (sv.isOrdered()) {
        const auto* osv = sv.toOrderedStoredValue();
        os << (osv->isStalePriv() ? 'S' : '.');
    }

    if (sv.isDeleted() && sv.getDeletionSource() == DeleteSource::TTL) {
        os << "TTL";
    }

    os << ' ';

    // Temporary states
    os << "temp:"
       << (sv.isTempInitialItem() ? 'I' : ' ')
       << (sv.isTempDeletedItem() ? 'D' : ' ')
       << (sv.isTempNonExistentItem() ? 'N' : ' ')
       << ' ';

    // seqno, revid, expiry / purge time
    os << std::dec << "seq:" << uint64_t(sv.getBySeqno())
       << " rev:" << sv.getRevSeqno();
    os << " cas:" << sv.getCas();
    if (sv.getCasEncoding() == StoredValue::CasEncoding::SeparateDouble) {
        os << " locked_cas:" << sv.getCasPair().lockedCAS;
    }
    os << " key:\"" << sv.getKey() << "\"";
    if (sv.isOrdered() && sv.isDeleted()) {
        os << " del_time:"
           << sv.toOrderedStoredValue()->getCompletedOrDeletedTime();
    } else {
        os << " exp:" << sv.getExptime();
    }
    os << " age:" << uint32_t(sv.getAge());
    os << " fc:" << uint32_t(sv.getFreqCounterValue());

    os << " vallen:" << sv.valuelen();
    if (sv.getValue().get()) {
        os << " val age:" << uint32_t(sv.getValue()->getAge()) << " :\"";
        if (sv.valuelen() > 0) {
            if (sv.getKey().isInSystemEventCollection()) {
                os << getSystemEventsValueFromStoredValue(sv);
            } else {
                const char* data = sv.getValue()->getData();
                // print up to first 40 bytes of value.
                const size_t limit =
                        std::min(size_t(40), sv.getValue()->valueSize());
                os << std::string_view{data, limit};
                if (limit < sv.getValue()->valueSize()) {
                    os << " <cut>";
                }
            }
        }
        os << "\"";
    }

    if (sv.isOrdered()) {
        auto& osv = static_cast<const OrderedStoredValue&>(sv);
        os << " prepareSeqno: " << osv.prepareSeqno;
    }
    return os;
}

std::string format_as(const StoredValue& sv) {
    std::stringstream ss;
    ss << sv;
    return ss.str();
}

bool OrderedStoredValue::operator==(const OrderedStoredValue& other) const {
    return StoredValue::operator==(other);
}

size_t OrderedStoredValue::getRequiredStorage(const DocKeyView& key) {
    return sizeof(OrderedStoredValue) + SerialisedDocKey::getObjectSize(key);
}

/**
 * Return the time the item was deleted. Only valid for completed or deleted
 * items.
 */
time_t OrderedStoredValue::getCompletedOrDeletedTime() const {
    if (isDeleted() || isPrepareCompleted()) {
        return lock_expiry_or_delete_or_complete_time.delete_or_complete_time;
    }
    throw std::logic_error(
            "OrderedStoredValue::getDeletedItem: Called on Alive item");
}

bool OrderedStoredValue::deleteImpl(DeleteSource delSource) {
    if (StoredValue::deleteImpl(delSource)) {
        // Need to record the time when an item is deleted for subsequent
        //purging (ephemeral_metadata_purge_age).
        setCompletedOrDeletedTime(ep_real_time());
        return true;
    }
    return false;
}

void OrderedStoredValue::setValueImpl(const Item& itm) {
    StoredValue::setValueImpl(itm);

    // Update the deleted time (note - even if it was already deleted we should
    // refresh this).
    if (isDeleted()) {
        setCompletedOrDeletedTime(ep_real_time());
    }
}

void OrderedStoredValue::setCompletedOrDeletedTime(time_t time) {
    if (!(isDeleted() || isPrepareCompleted())) {
        throw std::logic_error(
                "OrderedStoredValue::setCompletedOrDeletedTime: Called on "
                "Alive item");
    }
    lock_expiry_or_delete_or_complete_time.delete_or_complete_time = time;
}
