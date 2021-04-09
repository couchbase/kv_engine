/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
