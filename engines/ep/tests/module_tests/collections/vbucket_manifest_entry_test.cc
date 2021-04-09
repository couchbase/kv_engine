/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/vbucket_manifest_entry.h"
#include "test_manifest.h"

#include <folly/portability/GTest.h>

using namespace std::chrono_literals;

// Basic ManifestEntry construction checks
TEST(ManifestEntry, test_getters) {
    auto meta = make_STRCPtr<const Collections::VB::CollectionSharedMetaData>(
            "name", ScopeID{101}, cb::ExpiryLimit{5000});

    Collections::VB::ManifestEntry m(meta, 1000);
    EXPECT_EQ(1000, m.getStartSeqno());
    EXPECT_TRUE(m.getMaxTtl());
    EXPECT_TRUE(m.getMaxTtl().has_value());
    EXPECT_EQ(5000s, m.getMaxTtl().value());
    EXPECT_EQ("name", m.getName());
    EXPECT_EQ(ScopeID{101}, m.getScopeID());
}

TEST(ManifestEntry, exceptions) {
    auto meta = make_STRCPtr<const Collections::VB::CollectionSharedMetaData>(
            "name", ScopeID{101}, cb::NoExpiryLimit);

    // Collection starts at seqno 1000, note these test require a name because
    // the ManifestEntry ostream<< will be invoked by the exception path
    Collections::VB::ManifestEntry m(meta, 1000);

    EXPECT_FALSE(m.getMaxTtl().has_value());

    // Now start = 1000 and end = 2000
    // Check we cannot change start to be...
    EXPECT_THROW(m.setStartSeqno(999), std::invalid_argument); // ... smaller
    EXPECT_THROW(m.setStartSeqno(1000), std::invalid_argument); // ... the same

    EXPECT_NO_THROW(m.setStartSeqno(3000)); // bigger is fine
}
