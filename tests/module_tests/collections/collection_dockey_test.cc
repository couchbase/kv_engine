/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/collections_dockey.h"
#include "tests/module_tests/makestoreddockey.h"

#include <gtest/gtest.h>

TEST(CollectionDocKeyTest, make) {
    auto key1 = makeStoredDocKey("beer::bud", DocNamespace::Collections);
    auto key2 = makeStoredDocKey("beerbud", DocNamespace::Collections);

    EXPECT_EQ(strlen("beer"),
              Collections::DocKey::make(key1, "::").getCollectionLen());

    EXPECT_EQ(0, Collections::DocKey::make(key2, "::").getCollectionLen());

    EXPECT_EQ(0, Collections::DocKey::make(key1, "##").getCollectionLen());

    // This case is if the separator and the key are the same thing
    // The collection len is 0
    EXPECT_EQ(0,
              Collections::DocKey::make(key1, "beer::bud").getCollectionLen());

    // If a key is beer::brewery and the separtor is brewery then the collection
    // is beer::
    EXPECT_EQ(strlen("beer::"),
              Collections::DocKey::make(key1, "bud").getCollectionLen());

    EXPECT_EQ(0,
              Collections::DocKey::make(key1, "longerthanthekey")
                      .getCollectionLen());
}