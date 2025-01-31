/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "magma-kvstore_metadata.h"

#include <folly/lang/Assume.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/unsigned_leb128.h>
#include <nlohmann/json.hpp>

namespace magmakv {

MetaData::MetaData(std::string_view buf) {
    // Read version first, that should always exist
    std::tie(version, buf) = VersionStorage::decode(buf);

    switch (getVersion()) {
    case Version::V0:
        // v0 receives the bits
        std::tie(allMeta.v0.bits.all, buf) = allMeta.v0.decode(buf);
        break;
    case Version::V1:
        // v0 receives the bits
        std::tie(allMeta.v0.bits.all, buf) = allMeta.v0.decode(buf);
        buf = allMeta.v1.decode(buf);
        break;
    case Version::V2:
        // deocde the v0, but v2 receives the bits
        std::tie(allMeta.v2.bits.all, buf) = allMeta.v0.decode(buf);
        if (!buf.empty()) {
            buf = allMeta.v1.decode(buf);
            allMeta.v2.containsV1Meta = true;
        }
        break;
    }

    Expects(buf.empty());
}

std::pair<MetaData::VersionStorage, std::string_view>
MetaData::VersionStorage::decode(std::string_view buf) {
    Expects(buf.size() >= sizeof(VersionStorage));

    VersionStorage v;
    std::memcpy(&v, buf.data(), sizeof(VersionStorage));

    buf.remove_prefix(sizeof(VersionStorage));
    return {v, buf};
}

uint8_t MetaData::getDurabilityLevel() const {
    Expects(isDurabilityDefined());
    return allMeta.v1.durabilityDetails.pending.level;
}

cb::uint48_t MetaData::getPrepareSeqno() const {
    Expects(isDurabilityDefined());
    return allMeta.v1.durabilityDetails.completed.prepareSeqno;
}

bool MetaData::isDeleted() const {
    switch (getVersion()) {
    case Version::V0:
    case Version::V1:
        // both v0/v1 use the v0 bits.
        return allMeta.v0.bits.bits.deleted;
    case Version::V2:
        return allMeta.v2.bits.bits.deleted;
    }
    folly::assume_unreachable();
}

void MetaData::setDeleted(bool deleted, DeleteSource deleteSource) {
    switch (getVersion()) {
    case Version::V0:
    case Version::V1:
        // both v0/v1 use the v0 bits.
        allMeta.v0.bits.bits.deleted = deleted;
        allMeta.v0.bits.bits.deleteSource = uint8_t(deleteSource);
        break;
    case Version::V2:
        allMeta.v2.bits.bits.deleted = deleted;
        allMeta.v2.bits.bits.deleteSource = uint8_t(deleteSource);
        break;
    }
}

DeleteSource MetaData::getDeleteSource() const {
    switch (getVersion()) {
    case Version::V0:
    case Version::V1:
        // both v0/v1 use the v0 bits.
        return static_cast<DeleteSource>(allMeta.v0.bits.bits.deleteSource);
    case Version::V2:
        return static_cast<DeleteSource>(allMeta.v2.bits.bits.deleteSource);
    }

    folly::assume_unreachable();
}

bool MetaData::isSyncDelete() const {
    Expects(isDurabilityDefined());
    return allMeta.v1.durabilityDetails.pending.isDelete;
}

void MetaData::setDurabilityDetailsForPrepare(bool isDelete, uint8_t level) {
    Expects(getVersion() == Version::V1 || getVersion() == Version::V2);
    allMeta.v1.durabilityDetails.pending.isDelete = isDelete;
    allMeta.v1.durabilityDetails.pending.level = level;

    // Ok to write to v2 if version is v1, it just gets ignored in that case
    allMeta.v2.containsV1Meta = true;
}

void MetaData::setDurabilityDetailsForAbort(cb::uint48_t prepareSeqno) {
    Expects(getVersion() == Version::V1 || getVersion() == Version::V2);
    allMeta.v1.durabilityDetails.completed.prepareSeqno = prepareSeqno;
    allMeta.v2.containsV1Meta = true;
}

bool MetaData::isHistoryEnabled() const {
    switch (getVersion()) {
    case Version::V2:
        return allMeta.v2.bits.bits.history;
    case Version::V0:
    case Version::V1:
        break;
    }
    return false;
}

void MetaData::setHistory(bool value) {
    Expects(isHistoryDefined());
    allMeta.v2.bits.bits.history = value;
}

std::string MetaData::encode() const {
    // Add and encode v...
    auto ret = version.encode();

    switch (getVersion()) {
    case Version::V0:
        ret.append(allMeta.v0.encode(allMeta.v0.bits.all));
        break;
    case Version::V1:
        ret.append(allMeta.v0.encode(allMeta.v0.bits.all));
        ret.append(allMeta.v1.encode());
        break;
    case Version::V2:
        ret.append(allMeta.v0.encode(allMeta.v2.bits.all));
        ret.append(allMeta.v2.encode(allMeta.v1));
        break;
    }

    return ret;
}

std::string MetaData::VersionStorage::encode() const {
    return {(char*)&version, sizeof(version)};
}

std::string MetaData::MetaDataV0::encode(uint8_t bits) const {
    std::string ret;

    constexpr size_t maxSize =
            sizeof(bySeqno) + sizeof(cas) + sizeof(valueSize) +
            sizeof(datatype) + sizeof(bits) +
            sizeof(cb::mcbp::unsigned_leb128<uint64_t>::getMaxSize()) +
            2 * sizeof(cb::mcbp::unsigned_leb128<uint32_t>::getMaxSize());
    ret.reserve(maxSize);

    ret.append(reinterpret_cast<const char*>(&bySeqno), sizeof(bySeqno));
    ret.append(reinterpret_cast<const char*>(&cas), sizeof(cas));
    ret.append(reinterpret_cast<const char*>(&valueSize), sizeof(valueSize));
    ret.append(reinterpret_cast<const char*>(&datatype), sizeof(datatype));
    ret.append(reinterpret_cast<const char*>(&bits), sizeof(bits));

    // Now all the leb128 encoded fields, doing these in one chunk makes the
    // decode slightly simpler as we just need to resize the buffer once after
    auto leb64 = cb::mcbp::unsigned_leb128<uint64_t>(revSeqno);
    ret.append(leb64.begin(), leb64.end());

    auto leb32 = cb::mcbp::unsigned_leb128<uint32_t>(exptime);
    ret.append(leb32.begin(), leb32.end());

    leb32 = cb::mcbp::unsigned_leb128<uint32_t>(flags);
    ret.append(leb32.begin(), leb32.end());

    return ret;
}

std::string MetaData::MetaDataV1::encode() const {
    std::string ret;

    // leb128 encoded as prepares will only use 1 byte of the 6
    auto leb = cb::mcbp::unsigned_leb128<uint64_t>(durabilityDetails.raw);
    ret.append(leb.begin(), leb.end());

    return ret;
}

std::string MetaData::MetaDataV2::encode(MetaDataV1 v1) const {
    std::string ret;

    if (containsV1Meta) {
        ret.append(v1.encode());
    }

    return ret;
}

std::pair<uint8_t, std::string_view> MetaData::MetaDataV0::decode(
        std::string_view buf) {
    std::memcpy(&bySeqno, buf.data(), sizeof(bySeqno));
    buf.remove_prefix(sizeof(bySeqno));

    std::memcpy(&cas, buf.data(), sizeof(cas));
    buf.remove_prefix(sizeof(cas));

    std::memcpy(&valueSize, buf.data(), sizeof(valueSize));
    buf.remove_prefix(sizeof(valueSize));

    std::memcpy(&datatype, buf.data(), sizeof(datatype));
    buf.remove_prefix(sizeof(datatype));

    uint8_t allBits{0};
    std::memcpy(&allBits, buf.data(), sizeof(allBits));
    buf.remove_prefix(sizeof(allBits));

    std::pair<uint64_t, cb::const_byte_buffer> decoded = {
            0,
            {reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())),
             buf.size()}};
    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    revSeqno = decoded.first;

    decoded = cb::mcbp::unsigned_leb128<uint32_t>::decode(decoded.second);
    exptime = decoded.first;

    decoded = cb::mcbp::unsigned_leb128<uint32_t>::decode(decoded.second);
    flags = decoded.first;

    buf.remove_prefix(buf.size() - decoded.second.size());

    return {allBits, buf};
}

std::string_view MetaData::MetaDataV1::decode(std::string_view buf) {
    std::pair<uint64_t, cb::const_byte_buffer> decoded = {
            0,
            {reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())),
             buf.size()}};
    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    durabilityDetails.raw = decoded.first;

    buf.remove_prefix(buf.size() - decoded.second.size());

    return buf;
}

bool MetaData::operator==(const MetaData& other) const {
    auto v = getVersion();
    if (v != other.getVersion()) {
        return false;
    }

    switch (v) {
    case Version::V0:
        return allMeta.v0 == other.allMeta.v0;
    case Version::V1:
        return allMeta.v0 == other.allMeta.v0 && allMeta.v1 == other.allMeta.v1;
    case Version::V2:
        return allMeta.v0 == other.allMeta.v0 &&
               allMeta.v1 == other.allMeta.v1 && allMeta.v2 == other.allMeta.v2;
    }
    folly::assume_unreachable();
}

bool MetaData::MetaDataV0::operator==(const MetaData::MetaDataV0& other) const {
    return bySeqno == other.bySeqno && cas == other.cas &&
           revSeqno == other.revSeqno && exptime == other.exptime &&
           flags == other.flags && valueSize == other.valueSize &&
           datatype == other.datatype && bits.all == other.bits.all;
}

bool MetaData::MetaDataV1::operator==(const MetaData::MetaDataV1& other) const {
    return durabilityDetails.raw == other.durabilityDetails.raw;
}

bool MetaData::MetaDataV2::operator==(const MetaData::MetaDataV2& other) const {
    return bits.all == other.bits.all && containsV1Meta == other.containsV1Meta;
}

void to_json(nlohmann::json& json, const MetaData& meta) {
    json = nlohmann::json{
            {"bySeqno", std::to_string(meta.getBySeqno())},
            {"cas", std::to_string(meta.getCas())},
            {"revSeqno", std::to_string(uint64_t(meta.getRevSeqno()))},
            {"flags", std::to_string(meta.getFlags())},
            {"valueSize", std::to_string(meta.getValueSize())},
            {"deleteSource", to_string(meta.getDeleteSource())},
            {"version",
             std::to_string(static_cast<uint8_t>(meta.getVersion()))},
            {"datatype", std::to_string(meta.getDatatype())},
            {"deleted", std::to_string(meta.isDeleted())},
            {"history_seconds",
             std::to_string(meta.getHistoryTimeStamp().count())}};

    if (meta.isDeleted()) {
        json["delete_time"] = std::to_string(meta.getExptime());
    } else {
        json["exptime"] = std::to_string(meta.getExptime());
    }

    if (meta.isDurabilityDefined()) {
        if (meta.isDeleted()) {
            json["durabilityOperation"] = "abort";
            json["prepareSeqno"] = std::to_string(meta.getPrepareSeqno());
        } else {
            json["durabilityOperation"] = "prepare";
            json["syncDelete"] = meta.isSyncDelete();
            json["level"] = meta.getDurabilityLevel();
        }
    }

    if (meta.isHistoryDefined()) {
        json["history"] = meta.isHistoryEnabled();
    }
}
} // namespace magmakv