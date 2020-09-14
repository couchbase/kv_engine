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
    Collections::VB::ManifestEntry m(ScopeEntry::defaultS,
                                     5000s, // ttl
                                     1000);
    EXPECT_EQ(1000, m.getStartSeqno());
    EXPECT_TRUE(m.getMaxTtl());
    EXPECT_EQ(5000s, m.getMaxTtl().value());
}

TEST(ManifestEntry, exceptions) {

    // Collection starts at seqno 1000
    Collections::VB::ManifestEntry m(ScopeEntry::defaultS, {}, 1000);

    // Now start = 1000 and end = 2000
    // Check we cannot change start to be...
    EXPECT_THROW(m.setStartSeqno(999), std::invalid_argument); // ... smaller
    EXPECT_THROW(m.setStartSeqno(1000), std::invalid_argument); // ... the same

    EXPECT_NO_THROW(m.setStartSeqno(3000));
}

TEST(ManifestEntry, construct_assign) {
    // Collection starts at seqno 2
    Collections::VB::ManifestEntry entry1(ScopeEntry::defaultS, {}, 2);
    entry1.setHighSeqno(101);
    entry1.setPersistedHighSeqno(99);

    //  Move entry1 to entry2
    Collections::VB::ManifestEntry entry2(std::move(entry1));
    EXPECT_EQ(2, entry2.getStartSeqno());
    EXPECT_EQ(101, entry2.getHighSeqno());
    EXPECT_EQ(99, entry2.getPersistedHighSeqno());

    // Take a copy of entry2
    Collections::VB::ManifestEntry entry3(entry2);
    EXPECT_EQ(2, entry3.getStartSeqno());
    EXPECT_EQ(101, entry3.getHighSeqno());
    EXPECT_EQ(99, entry3.getPersistedHighSeqno());

    // change entry2
    entry2.setStartSeqno(3);

    // Now copy entry2 assign over entry3
    entry3 = entry2;
    EXPECT_EQ(3, entry3.getStartSeqno());
    EXPECT_EQ(101, entry3.getHighSeqno());
    EXPECT_EQ(99, entry3.getPersistedHighSeqno());

    // And move entry3 back to entry1
    entry1 = std::move(entry3);

    EXPECT_EQ(3, entry1.getStartSeqno());
    EXPECT_EQ(101, entry1.getHighSeqno());
    EXPECT_EQ(99, entry1.getPersistedHighSeqno());
}
