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
#include "collections/kvstore.h"
#include "collections/kvstore_generated.h"
#include "flatbuffers/flatbuffers.h"
#include "memcached/dockey_view.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <chrono>
#include <iterator>

namespace Backup {

flatbuffers::Offset<Collections::KVStore::OpenCollections>
reEncodeOpenCollections(flatbuffers::FlatBufferBuilder& builder,
                        const std::vector<Collections::KVStore::OpenCollection>&
                                openCollections) {
    using namespace Collections::KVStore;
    std::vector<flatbuffers::Offset<Collections::KVStore::Collection>>
            collectionFbs;
    for (const auto& open : openCollections) {
        auto collectionFb = Collections::KVStore::CreateCollection(
                builder,
                open.startSeqno,
                ScopeIDType(open.metaData.sid),
                CollectionIDType(open.metaData.cid),
                open.metaData.maxTtl.has_value(),
                open.metaData.maxTtl.value_or(std::chrono::seconds(0)).count(),
                builder.CreateString(open.metaData.name),
                getHistoryFromCanDeduplicate(open.metaData.canDeduplicate),
                getMeteredFromEnum(open.metaData.metered),
                open.metaData.flushUid);
        collectionFbs.push_back(collectionFb);
    }
    return CreateOpenCollections(builder, builder.CreateVector(collectionFbs));
}

flatbuffers::Offset<Collections::KVStore::Scopes> reEncodeScopes(
        flatbuffers::FlatBufferBuilder& builder,
        const std::vector<Collections::KVStore::OpenScope>& scopes) {
    using namespace Collections::KVStore;

    std::vector<flatbuffers::Offset<Collections::KVStore::Scope>> scopeFbs;
    for (const auto& scope : scopes) {
        auto scopeFb = Collections::KVStore::CreateScope(
                builder,
                scope.startSeqno,
                ScopeIDType(scope.metaData.sid),
                builder.CreateString(scope.metaData.name));
        scopeFbs.push_back(scopeFb);
    }
    return CreateScopes(builder, builder.CreateVector(scopeFbs));
}

std::string encodeBackupMetadata(
        uint64_t maxCas,
        const std::vector<vbucket_failover_t>& failoverTable,
        const Collections::KVStore::Manifest& manifest) {
    using namespace Collections::KVStore;

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

    auto manifestFb = Collections::KVStore::CreateCommittedManifest(
            builder, manifest.manifestUid);

    auto openCollectionFb =
            reEncodeOpenCollections(builder, manifest.collections);
    auto scopesFb = reEncodeScopes(builder, manifest.scopes);

    auto toWrite = Flatbuffers::CreateMetadata(builder,
                                               maxCas,
                                               failoverEntriesFb,
                                               manifestFb,
                                               openCollectionFb,
                                               scopesFb);
    builder.Finish(toWrite);

    auto fb = builder.Release();
    return {reinterpret_cast<const char*>(fb.data()), fb.size()};
}

const Flatbuffers::Metadata& decodeBackupMetadata(std::string_view data) {
    flatbuffers::Verifier v(reinterpret_cast<const uint8_t*>(data.data()),
                            data.size());
    if (!v.VerifyBuffer<Flatbuffers::Metadata>(nullptr)) {
        throw std::runtime_error(fmt::format(
                "decodeBackupMetadata: data invalid, ptr:{}, size:{}",
                fmt::ptr(data.data()),
                data.size()));
    }

    const auto* fbData =
            flatbuffers::GetRoot<Flatbuffers::Metadata>(data.data());

    return *fbData;
}

} // namespace Backup
