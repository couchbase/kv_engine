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
        std::tie(allMeta.v0, buf) = MetaDataV0::decode(buf);
        break;
    case Version::V1:
        std::tie(allMeta.v0, buf) = MetaDataV0::decode(buf);
        std::tie(allMeta.v1, buf) = MetaDataV1::decode(buf);
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
    Expects(getVersion() == Version::V1);
    return allMeta.v1.durabilityDetails.pending.level;
}

void MetaData::setDurabilityDetailsForPrepare(bool isDelete, uint8_t level) {
    Expects(getVersion() == Version::V1);
    allMeta.v1.durabilityDetails.pending.isDelete = isDelete;
    allMeta.v1.durabilityDetails.pending.level = level;
}

cb::uint48_t MetaData::getPrepareSeqno() const {
    Expects(getVersion() == Version::V1);
    return allMeta.v1.durabilityDetails.completed.prepareSeqno;
}

void MetaData::setDurabilityDetailsForAbort(cb::uint48_t prepareSeqno) {
    Expects(getVersion() == Version::V1);
    allMeta.v1.durabilityDetails.completed.prepareSeqno = prepareSeqno;
}

bool MetaData::isSyncDelete() const {
    Expects(getVersion() == Version::V1);
    return allMeta.v1.durabilityDetails.pending.isDelete;
}

std::string MetaData::encode() const {
    // Add and encode v...
    auto ret = version.encode();

    switch (getVersion()) {
    case Version::V0:
        ret.append(allMeta.v0.encode());
        break;
    case Version::V1:
        ret.append(allMeta.v0.encode());
        ret.append(allMeta.v1.encode());
        break;
    }

    return ret;
}

std::string MetaData::VersionStorage::encode() const {
    return std::string((char*)&version, sizeof(version));
}

std::string MetaData::MetaDataV0::encode() const {
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

std::pair<MetaData::MetaDataV0, std::string_view> MetaData::MetaDataV0::decode(
        std::string_view buf) {
    MetaDataV0 v;

    std::memcpy(&v.bySeqno, buf.data(), sizeof(bySeqno));
    buf.remove_prefix(sizeof(bySeqno));

    std::memcpy(&v.cas, buf.data(), sizeof(cas));
    buf.remove_prefix(sizeof(cas));

    std::memcpy(&v.valueSize, buf.data(), sizeof(valueSize));
    buf.remove_prefix(sizeof(valueSize));

    std::memcpy(&v.datatype, buf.data(), sizeof(datatype));
    buf.remove_prefix(sizeof(datatype));

    std::memcpy(&v.bits, buf.data(), sizeof(bits));
    buf.remove_prefix(sizeof(bits));

    std::pair<uint64_t, cb::const_byte_buffer> decoded = {
            0,
            {reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())),
             buf.size()}};
    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    v.revSeqno = decoded.first;

    decoded = cb::mcbp::unsigned_leb128<uint32_t>::decode(decoded.second);
    v.exptime = decoded.first;

    decoded = cb::mcbp::unsigned_leb128<uint32_t>::decode(decoded.second);
    v.flags = decoded.first;

    buf.remove_prefix(buf.size() - decoded.second.size());

    return {v, buf};
}

std::pair<MetaData::MetaDataV1, std::string_view> MetaData::MetaDataV1::decode(
        std::string_view buf) {
    MetaDataV1 v;

    std::pair<uint64_t, cb::const_byte_buffer> decoded = {
            0,
            {reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())),
             buf.size()}};
    decoded = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded.second);
    v.durabilityDetails.raw = decoded.first;

    buf.remove_prefix(buf.size() - decoded.second.size());

    return {v, buf};
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
    }
    folly::assume_unreachable();
}

bool MetaData::MetaDataV0::operator==(const MetaData::MetaDataV0& other) const {
    return bySeqno == other.bySeqno && cas == other.cas &&
           revSeqno == other.revSeqno && exptime == other.exptime &&
           flags == other.flags && valueSize == other.valueSize &&
           datatype == other.datatype && bits.deleted == other.bits.deleted &&
           bits.deleteSource == other.bits.deleteSource;
}

bool MetaData::MetaDataV1::operator==(const MetaData::MetaDataV1& other) const {
    return durabilityDetails.raw == other.durabilityDetails.raw;
}

void to_json(nlohmann::json& json, const MetaData& meta) {
    json = nlohmann::json{
            {"bySeqno", std::to_string(meta.getBySeqno())},
            {"cas", std::to_string(meta.getCas())},
            {"revSeqno", std::to_string(uint64_t(meta.getRevSeqno()))},
            {"exptime", std::to_string(meta.getExptime())},
            {"flags", std::to_string(meta.getFlags())},
            {"valueSize", std::to_string(meta.getValueSize())},
            {"deleteSource", std::to_string(meta.getDeleteSource())},
            {"version",
             std::to_string(static_cast<uint8_t>(meta.getVersion()))},
            {"datatype", std::to_string(meta.getDatatype())},
            {"deleted", std::to_string(meta.isDeleted())}};

    if (meta.getVersion() == MetaData::Version::V1) {
        if (meta.isDeleted()) {
            json["durabilityOperation"] = "abort";
            json["prepareSeqno"] = std::to_string(meta.getPrepareSeqno());
        } else {
            json["durabilityOperation"] = "prepare";
            json["syncDelete"] = meta.isSyncDelete();
            json["level"] = meta.getDurabilityLevel();
        }
    }
}

void from_json(const nlohmann::json& j, MetaData& meta) {
    meta.setBySeqno(std::stoull(j.at("bySeqno").get<std::string>()));
    meta.setCas(std::stoull(j.at("cas").get<std::string>()));
    meta.setRevSeqno(std::stoull(j.at("revSeqno").get<std::string>()));
    meta.setExptime(std::stoul(j.at("exptime").get<std::string>()));
    meta.setFlags(std::stoul(j.at("flags").get<std::string>()));
    meta.setValueSize(std::stoul(j.at("valueSize").get<std::string>()));
    meta.setVersion(MetaData::Version(static_cast<uint8_t>(
            std::stoul(j.at("version").get<std::string>()))));
    meta.setDataType(static_cast<uint8_t>(
            std::stoul(j.at("datatype").get<std::string>())));

    meta.setDeleted(
            static_cast<bool>(std::stoul(j.at("deleted").get<std::string>())),
            static_cast<bool>(
                    std::stoul(j.at("deleteSource").get<std::string>())));

    if (meta.getVersion() == MetaData::Version::V1) {
        if (meta.isDeleted()) {
            // Abort
            auto prepareSeqno =
                    std::stoull(j.at("prepareSeqno").get<std::string>());
            meta.setDurabilityDetailsForAbort(prepareSeqno);
        } else {
            // Prepare
            auto syncDelete = j.at("syncDelete").get<bool>();
            auto level = static_cast<uint8_t>(
                    std::stoul(j.at("level").get<std::string>()));
            meta.setDurabilityDetailsForPrepare(syncDelete, level);
        }
    }
}

} // namespace magmakv