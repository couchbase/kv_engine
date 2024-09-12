/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "backup.h"
#include "backup/backup_generated.h"
#include "flatbuffers/flatbuffers.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <iterator>

namespace Backup {

std::string encodeBackupMetadata(
        uint64_t maxCas, const std::vector<vbucket_failover_t>& failoverTable) {
    flatbuffers::FlatBufferBuilder builder;

    std::vector<flatbuffers::Offset<Flatbuffers::FailoverEntry>>
            failoverEntries;
    std::transform(failoverTable.begin(),
                   failoverTable.end(),
                   std::back_inserter(failoverEntries),
                   [&builder](const vbucket_failover_t& entry) {
                       return Flatbuffers::CreateFailoverEntry(
                               builder, entry.uuid, entry.seqno);
                   });
    auto failoverEntriesFb = builder.CreateVector(failoverEntries);

    auto toWrite =
            Flatbuffers::CreateMetadata(builder, maxCas, failoverEntriesFb);
    builder.Finish(toWrite);

    auto fb = builder.Release();
    return {reinterpret_cast<const char*>(fb.data()), fb.size()};
}

nlohmann::json decodeBackupMetadata(std::string_view data) {
    flatbuffers::Verifier v(reinterpret_cast<const uint8_t*>(data.data()),
                            data.size());
    if (!v.VerifyBuffer<Flatbuffers::Metadata>(nullptr)) {
        throw std::runtime_error(fmt::format(
                "decodeBackupMetadata(): invalid data, ptr:{}, size:{}",
                fmt::ptr(data.data()),
                data.size()));
    }

    const auto* fbData =
            flatbuffers::GetRoot<Flatbuffers::Metadata>(data.data());

    auto failovers = nlohmann::json::array();
    for (const auto& entry : *fbData->failovers()) {
        failovers.push_back({{"id", entry->id()}, {"seq", entry->seq()}});
    }

    return {
            {"maxCas", fbData->maxCas()},
            {"failovers", std::move(failovers)},
    };
}

} // namespace Backup
