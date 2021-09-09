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
#include <nlohmann/json.hpp>

namespace magmakv {

MetaData::MetaData(std::string_view buf) {
    // Read version first, that should always exist
    std::tie(version, buf) = VersionStorage::parse(buf);

    switch (getVersion()) {
    case Version::V0:
        std::tie(allMeta.v0, buf) = MetaDataV0::parse(buf);
        break;
    case Version::V1:
        std::tie(allMeta.v0, buf) = MetaDataV0::parse(buf);
        std::tie(allMeta.v1, buf) = MetaDataV1::parse(buf);
        break;
    }

    Expects(buf.empty());
}

std::pair<MetaData::VersionStorage, std::string_view>
MetaData::VersionStorage::parse(std::string_view buf) {
    Expects(buf.size() >= sizeof(VersionStorage));

    VersionStorage v;
    std::memcpy(&v, buf.data(), sizeof(VersionStorage));

    buf.remove_prefix(sizeof(VersionStorage));
    return {v, buf};
}

std::pair<MetaData::MetaDataV0, std::string_view> MetaData::MetaDataV0::parse(
        std::string_view buf) {
    Expects(buf.size() >= sizeof(MetaDataV0));

    MetaDataV0 v;
    std::memcpy(&v, buf.data(), sizeof(MetaDataV0));

    buf.remove_prefix(sizeof(MetaDataV0));
    return {v, buf};
}

std::pair<MetaData::MetaDataV1, std::string_view> MetaData::MetaDataV1::parse(
        std::string_view buf) {
    Expects(buf.size() >= sizeof(MetaDataV1));

    MetaDataV1 v;
    std::memcpy(&v, buf.data(), sizeof(MetaDataV1));

    buf.remove_prefix(sizeof(MetaDataV1));
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

size_t MetaData::getLength() const {
    auto versionSize = sizeof(uint8_t);

    switch (getVersion()) {
    case Version::V0:
        return versionSize + sizeof(MetaDataV0);
    case Version::V1:
        return versionSize + sizeof(MetaDataV0) + sizeof(MetaDataV1);
    }

    folly::assume_unreachable();
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