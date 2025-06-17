/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "memcached/storeddockey.h"
#include <folly/portability/GTest.h>
#include <fuzztest/fuzztest.h>

/**
 * Fuzz test for the DocKey class.
 *
 * This fuzz test is used to test the DocKey constructor and the to_string
 * method.
 */
static void createAndFormat(std::string key, CollectionIDType collectionId) {
    if (collectionId >= CollectionID::SystemEvent &&
        collectionId <= CollectionID::Reserved7) {
        // These collection IDs are reserved for system use, so we skip them.
        return;
    }
    StoredDocKey docKey(key, CollectionID(collectionId));
    docKey.to_string();
}

// Initialise the createAndFormat fuzz test.
FUZZ_TEST(DocKeyFuzzTest, createAndFormat)
        // Initialise with the default domains. This could be omitted in this
        // instance, but it makes the fuzz test more readable.
        .WithDomains(fuzztest::String(),
                     fuzztest::Positive<CollectionIDType>());
