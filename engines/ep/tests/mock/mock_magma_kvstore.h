/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
        if (readVBStateFromDiskHook) {
            readVBStateFromDiskHook();
        }

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

    std::function<void()> readVBStateFromDiskHook;

    std::function<int(VB::Commit&, kvstats_ctx&)> saveDocsErrorInjector;
};

#endif
