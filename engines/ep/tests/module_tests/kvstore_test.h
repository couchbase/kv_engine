/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "collections/flush.h"
#include "collections/vbucket_manifest.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

class KVStoreConfig;
class KVStore;

/**
 * Test fixture for KVStore tests.
 */
class KVStoreTest : public ::testing::Test {
public:
    KVStoreTest();

protected:
    void SetUp() override;

    void TearDown() override;

    std::string data_dir;
    Collections::VB::Manifest manifest;
    Collections::VB::Flush flush;
};

// Test fixture for tests which run on all KVStore implementations (Couchstore
// and RocksDB).
// The string parameter represents the KVStore implementation that each test
// of this class will use (e.g., "couchdb" and "rocksdb").
class KVStoreParamTest : public KVStoreTest,
                         public ::testing::WithParamInterface<std::string> {
protected:
    void SetUp() override;

    void TearDown() override;

    std::unique_ptr<KVStoreConfig> kvstoreConfig;
    std::unique_ptr<KVStore> kvstore;
};