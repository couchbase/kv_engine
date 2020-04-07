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

    int saveDocs(VB::Commit& commitData, kvstats_ctx& kvctx) override {
        if (saveDocsErrorInjector) {
            return saveDocsErrorInjector(commitData, kvctx);
        }

        return MagmaKVStore::saveDocs(commitData, kvctx);
    }

    std::function<int(VB::Commit&, kvstats_ctx&)> saveDocsErrorInjector;
};

#endif
