/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "stats.h"
#include <folly/portability/GTest.h>

class AbstractStoredValueFactory;
struct DocKey;
class HashTable;

extern EPStats global_stats;

// Test fixture for HashTable tests.
class HashTableTest : public ::testing::Test {
public:
    /**
     * Delete the item with the given key.
     *
     * @param key the storage key of the value to delete
     * @return true if the item existed before this call
     */
    static bool del(HashTable& ht, const DocKey& key);

protected:
    HashTableTest();

    static std::unique_ptr<AbstractStoredValueFactory> makeFactory(
            bool isOrdered = false);

    const size_t defaultHtSize;
};
