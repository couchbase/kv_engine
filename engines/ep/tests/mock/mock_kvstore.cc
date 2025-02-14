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

#include "mock_kvstore.h"
#include "kv_bucket.h"
#include "vb_commit.h"
#include <mcbp/protocol/request.h>

using namespace ::testing;

MockKVStore::MockKVStore(std::unique_ptr<KVStoreIface> real)
    : realKVS(std::move(real)) {
    if (realKVS) {
        // If we have a real KVStore, delegate some common methods to it
        // to aid in mocking.
        // Note: this could probably be expanded to the entire interface,
        // however thus far only methods needed by unit tests using the mock
        // have been implemented.
        ON_CALL(*this, initBySeqnoScanContext(_, _, _, _, _, _, _, _))
                .WillByDefault(
                        [this](auto cb,
                               auto cl,
                               Vbid vbid,
                               uint64_t startSeqno,
                               DocumentFilter options,
                               ValueFilter valOptions,
                               SnapshotSource source,
                               std::unique_ptr<KVFileHandle> fileHandle) {
                            return this->realKVS->initBySeqnoScanContext(
                                    std::move(cb),
                                    std::move(cl),
                                    vbid,
                                    startSeqno,
                                    options,
                                    valOptions,
                                    source,
                                    std::move(fileHandle));
                        });
        ON_CALL(*this, getCachedVBucketState(_))
                .WillByDefault([this](Vbid vbid) {
                    return this->realKVS->getCachedVBucketState(vbid);
                });
        ON_CALL(*this, getAggrDbFileInfo()).WillByDefault([this]() {
            return this->realKVS->getAggrDbFileInfo();
        });
        ON_CALL(*this, getConfig()).WillByDefault([this]() -> const auto& {
            return this->realKVS->getConfig();
        });
        ON_CALL(*this, snapshotVBucket(_, _))
                .WillByDefault([this](Vbid vbid, const VB::Commit& meta) {
                    return this->realKVS->snapshotVBucket(vbid, meta);
                });
        ON_CALL(*this, begin(_, _))
                .WillByDefault(
                        [this](Vbid vbid,
                               std::unique_ptr<PersistenceCallback> pcb) {
                            return this->realKVS->begin(vbid, std::move(pcb));
                        });
        ON_CALL(*this, set(_, _))
                .WillByDefault(
                        [this](TransactionContext& txnCtx, queued_item item) {
                            this->realKVS->set(txnCtx, item);
                        });
        ON_CALL(*this, del(_, _))
                .WillByDefault(
                        [this](TransactionContext& txnCtx, queued_item item) {
                            this->realKVS->del(txnCtx, item);
                        });
        ON_CALL(*this, getStorageProperties()).WillByDefault([this]() {
            return this->realKVS->getStorageProperties();
        });
        ON_CALL(*this, compactDB(_, _))
                .WillByDefault([this](std::unique_lock<std::mutex>& vbLock,
                                      std::shared_ptr<CompactionContext> c) {
                    return this->realKVS->compactDB(vbLock, c);
                });
        ON_CALL(*this, snapshotStats(_))
                .WillByDefault([this](const nlohmann::json& stats) {
                    return this->realKVS->snapshotStats(stats);
                });
        ON_CALL(*this, commit(_, _))
                .WillByDefault([this](auto ctx, auto& commit) {
                    return this->realKVS->commit(std::move(ctx), commit);
                });
        ON_CALL(*this, prepareToDeleteImpl(_)).WillByDefault([this](Vbid vbid) {
            return this->realKVS->prepareToDeleteImpl(vbid);
        });
        ON_CALL(*this, prepareToCreateImpl(_)).WillByDefault([this](Vbid vbid) {
            this->realKVS->prepareToCreateImpl(vbid);
        });
        ON_CALL(*this, getCollectionsManifest(_))
                .WillByDefault([this](Vbid vbid) {
                    return this->realKVS->getCollectionsManifest(vbid);
                });
        ON_CALL(*this, getItemCount(_)).WillByDefault([this](Vbid vbid) {
            return this->realKVS->getItemCount(vbid);
        });
        ON_CALL(*this, delVBucket(_, _))
                .WillByDefault([this](auto vbid, auto rev) {
                    return this->realKVS->delVBucket(vbid, std::move(rev));
                });
        ON_CALL(*this, prepareToCreate(_)).WillByDefault([this](Vbid vbid) {
            this->realKVS->prepareToCreate(vbid);
        });
        ON_CALL(*this, prepareToDelete(_)).WillByDefault([this](Vbid vbid) {
            return this->realKVS->prepareToDelete(vbid);
        });
        ON_CALL(*this, rollback(_, _, _))
                .WillByDefault(
                        [this](auto vbid, auto rollbackSeqno, auto rollbackCB) {
                            return this->realKVS->rollback(
                                    vbid, rollbackSeqno, std::move(rollbackCB));
                        });
        ON_CALL(*this, getPersistedVBucketState(_))
                .WillByDefault([this](auto vbid) {
                    return this->realKVS->getPersistedVBucketState(vbid);
                });
        ON_CALL(*this,
                getCollectionStats(An<const KVFileHandle&>(),
                                   An<CollectionID>()))
                .WillByDefault(
                        [this](const auto& kvFileHandle, auto collection) {
                            return this->realKVS->getCollectionStats(
                                    kvFileHandle, collection);
                        });
        ON_CALL(*this, getCollectionStats(An<Vbid>(), An<CollectionID>()))
                .WillByDefault([this](auto vbid, auto collection) {
                    return this->realKVS->getCollectionStats(vbid, collection);
                });
        ON_CALL(*this, keyMayExist(_, _))
                .WillByDefault([this](auto vbid, auto key) {
                    return this->realKVS->keyMayExist(vbid, std::move(key));
                });
        ON_CALL(*this, scan(An<ByIdScanContext&>()))
                .WillByDefault([this](auto& scanCtx) {
                    return this->realKVS->scan(scanCtx);
                });
        ON_CALL(*this, scan(An<BySeqnoScanContext&>()))
                .WillByDefault([this](auto& scanCtx) {
                    return this->realKVS->scan(scanCtx);
                });
    }
}

MockKVStore::~MockKVStore() = default;

MockKVStore& MockKVStore::replaceRWKVStoreWithMock(KVBucket& bucket,
                                                   size_t shardId) {
    auto rw = bucket.takeRW(shardId);
    auto mockRw = std::make_unique<NiceMock<MockKVStore>>(std::move(rw));
    auto& mockKVStore = dynamic_cast<MockKVStore&>(*mockRw);
    bucket.setRW(shardId, std::move(mockRw));
    return mockKVStore;
}

std::unique_ptr<MockKVStore> MockKVStore::restoreOriginalRWKVStore(
        KVBucket& bucket) {
    auto rw = bucket.takeRW(0);
    // Sanity check - read-write from bucket should be an instance of
    // MockKVStore
    if (!dynamic_cast<MockKVStore*>(rw.get())) {
        throw std::logic_error(
                "MockKVStore::restoreOriginalRWKVStore: Bucket's read-write "
                "KVS is not an instance of MockKVStore");
    }
    // Take ownership of the MockKVStore from the bucket.
    auto ownedMockKVS = std::unique_ptr<MockKVStore>(
            dynamic_cast<MockKVStore*>(rw.release()));
    // Put real KVStore back into bucket, return mock.
    bucket.setRW(0, std::move(ownedMockKVS->realKVS));
    return ownedMockKVS;
}
