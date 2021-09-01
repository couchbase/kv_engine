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

#include "magma-kvstore_magma_db_stats.h"

#include <nlohmann/json.hpp>

void to_json(nlohmann::json& json, const MagmaDbStats& dbStats) {
    json = nlohmann::json{{"docCount", std::to_string(dbStats.docCount)},
                          {"purgeSeqno", std::to_string(dbStats.purgeSeqno)}};
}

void from_json(const nlohmann::json& j, MagmaDbStats& dbStats) {
    dbStats.docCount = std::stoull(j.at("docCount").get<std::string>());
    dbStats.purgeSeqno.reset(
        std::stoull(j.at("purgeSeqno").get<std::string>()));
}

void MagmaDbStats::Merge(const UserStats& other) {
    auto otherStats = dynamic_cast<const MagmaDbStats*>(&other);
    if (!otherStats) {
        throw std::invalid_argument("MagmaDbStats::Merge: Bad cast of other");
    }

    docCount += otherStats->docCount;
    if (otherStats->purgeSeqno > purgeSeqno) {
        purgeSeqno = otherStats->purgeSeqno;
    }
}

std::unique_ptr<magma::UserStats> MagmaDbStats::Clone() {
    auto cloned = std::make_unique<MagmaDbStats>();
    cloned->reset(*this);
    return cloned;
}

std::string MagmaDbStats::Marshal() {
    nlohmann::json j = *this;
    return j.dump();
}

magma::Status MagmaDbStats::Unmarshal(const std::string& encoded) {
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(encoded);
    } catch (const nlohmann::json::exception& e) {
        throw std::logic_error("MagmaDbStats::Unmarshal cannot decode json:" +
            encoded + " " + e.what());
    }

    try {
        reset(j);
    } catch (const nlohmann::json::exception& e) {
        throw std::logic_error(
            "MagmaDbStats::Unmarshal cannot construct MagmaDbStats from "
            "json:" +
                encoded + " " + e.what());
    }

    return magma::Status::OK();
}
