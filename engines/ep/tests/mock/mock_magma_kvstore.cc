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

#include "mock_magma_kvstore.h"
#include "kvstore/magma-kvstore/magma-memory-tracking-proxy.h"

MockMagmaKVStore::MockMagmaKVStore(MagmaKVStoreConfig& config)
    : MagmaKVStore(config) {
}

KVStoreIface::ReadVBStateResult MockMagmaKVStore::readVBStateFromDisk(
        Vbid vbid) {
    return MagmaKVStore::readVBStateFromDisk(vbid);
}

MagmaKVStore::DiskState MockMagmaKVStore::readVBStateFromDisk(
        Vbid vbid, magma::Magma::Snapshot& snapshot) const {
    readVBStateFromDiskHook();

    return MagmaKVStore::readVBStateFromDisk(vbid, snapshot);
}

int MockMagmaKVStore::saveDocs(MagmaKVStoreTransactionContext& txnCtx,
                               VB::Commit& commitData,
                               kvstats_ctx& kvctx) {
    if (saveDocsErrorInjector) {
        return saveDocsErrorInjector(commitData, kvctx);
    }

    return MagmaKVStore::saveDocs(txnCtx, commitData, kvctx);
}

bool MockMagmaKVStore::snapshotVBucket(Vbid vbid,
                                       const vbucket_state& newVBState) {
    if (snapshotVBucketErrorInjector) {
        return snapshotVBucketErrorInjector();
    }

    return MagmaKVStore::snapshotVBucket(vbid, newVBState);
}

magma::Status MockMagmaKVStore::addLocalDoc(Vbid vbid,
                                            std::string_view key,
                                            std::string value) {
    WriteOps writeOps;
    LocalDbReqs localDbReqs;
    localDbReqs.emplace_back(MagmaLocalReq(key, std::move(value)));

    addLocalDbReqs(localDbReqs, writeOps);
    auto ret = magma->WriteDocs(
            vbid.get(), writeOps, kvstoreRevList[getCacheSlot(vbid)]);

    magma->Sync(true);

    return ret;
}

magma::Status MockMagmaKVStore::deleteLocalDoc(Vbid vbid,
                                               std::string_view key) {
    WriteOps writeOps;
    LocalDbReqs localDbReqs;
    localDbReqs.emplace_back(MagmaLocalReq::makeDeleted(key));

    addLocalDbReqs(localDbReqs, writeOps);
    auto ret = magma->WriteDocs(
            vbid.get(), writeOps, kvstoreRevList[getCacheSlot(vbid)]);

    magma->Sync(true);

    return ret;
}

magma::Status MockMagmaKVStore::runImplicitCompactKVStore(Vbid vbid) {
    return magma->RunImplicitCompactKVStore(vbid.get());
}

magma::Status MockMagmaKVStore::newCheckpoint(Vbid vbid) {
    return magma->NewCheckpoint(vbid.get());
}
