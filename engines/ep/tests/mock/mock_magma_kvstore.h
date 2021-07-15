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

#include "kvstore/magma-kvstore/magma-kvstore.h"

class MockMagmaKVStore : public MagmaKVStore {
public:
    explicit MockMagmaKVStore(MagmaKVStoreConfig& config);

    MagmaKVStore::DiskState readVBStateFromDisk(Vbid vbid);

    MagmaKVStore::DiskState readVBStateFromDisk(
            Vbid vbid, magma::Magma::Snapshot& snapshot) const override;

    int saveDocs(MagmaKVStoreTransactionContext& txnCtx,
                 VB::Commit& commitData,
                 kvstats_ctx& kvctx) override;

    magma::Status addLocalDoc(Vbid vbid,
                              std::string_view key,
                              std::string value);

    magma::Status deleteLocalDoc(Vbid vbid, std::string_view key);

    TestingHook<> readVBStateFromDiskHook;

    std::function<int(VB::Commit&, kvstats_ctx&)> saveDocsErrorInjector;
};

#endif
