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
