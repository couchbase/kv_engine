/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#ifdef EP_USE_MAGMA

#include "magma-kvstore/magma-kvstore.h"

class MockMagmaKVStore : public MagmaKVStore {
public:
    explicit MockMagmaKVStore(MagmaKVStoreConfig& config)
        : MagmaKVStore(config) {
    }

    MagmaKVStore::DiskState readVBStateFromDisk(Vbid vbid) {
        return MagmaKVStore::readVBStateFromDisk(vbid);
    }

    MagmaKVStore::DiskState readVBStateFromDisk(
            Vbid vbid, magma::Magma::Snapshot& snapshot) override {
        readVBStateFromDiskHook();

        return MagmaKVStore::readVBStateFromDisk(vbid, snapshot);
    }

    int saveDocs(VB::Commit& commitData, kvstats_ctx& kvctx) override {
        if (saveDocsErrorInjector) {
            return saveDocsErrorInjector(commitData, kvctx);
        }

        return MagmaKVStore::saveDocs(commitData, kvctx);
    }

    magma::Status addLocalDoc(Vbid vbid,
                              std::string_view key,
                              std::string value) {
        WriteOps writeOps;
        LocalDbReqs localDbReqs;
        localDbReqs.emplace_back(MagmaLocalReq(key, std::move(value)));

        addLocalDbReqs(localDbReqs, writeOps);
        auto ret = magma->WriteDocs(
                vbid.get(), writeOps, kvstoreRevList[vbid.get()]);

        magma->Sync(true);

        return ret;
    }

    magma::Status deleteLocalDoc(Vbid vbid, std::string_view key) {
        WriteOps writeOps;
        LocalDbReqs localDbReqs;
        localDbReqs.emplace_back(MagmaLocalReq::makeDeleted(key));

        addLocalDbReqs(localDbReqs, writeOps);
        auto ret = magma->WriteDocs(
                vbid.get(), writeOps, kvstoreRevList[vbid.get()]);

        magma->Sync(true);

        return ret;
    }

    TestingHook<> readVBStateFromDiskHook;

    std::function<int(VB::Commit&, kvstats_ctx&)> saveDocsErrorInjector;
};

#endif
