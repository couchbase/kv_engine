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

#include "collections/vbucket_filter.h"
#include "collections/vbucket_manifest.h"
#include "configuration.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "stats.h"
#include "tests/module_tests/collections/test_manifest.h"

#include <gtest/gtest.h>

#include <limits>

class CollectionsVBFilterTest : public ::testing::Test {
public:
    /// Dummy callback to replace the flusher callback so we can create VBuckets
    class DummyCB : public Callback<Vbid> {
    public:
        DummyCB() {
        }

        void callback(Vbid& dummy) {
        }
    };

    CollectionsVBFilterTest()
        : vb(Vbid(0),
             vbucket_state_active,
             global_stats,
             checkpoint_config,
             /*kvshard*/ nullptr,
             /*lastSeqno*/ 0,
             /*lastSnapStart*/ 0,
             /*lastSnapEnd*/ 0,
             /*table*/ nullptr,
             std::make_shared<DummyCB>(),
             /*newSeqnoCb*/ nullptr,
             NoopSyncWriteCompleteCb,
             config,
             VALUE_ONLY,
             std::make_unique<Collections::VB::Manifest>()) {
        Collections::Manifest m(cm);
        vbm.wlock().update(vb, m);
    }

    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    EPVBucket vb;
    CollectionsManifest cm;
    Collections::VB::Manifest vbm;
};

/**
 * Test invalid JSON formats as an input
 */
TEST_F(CollectionsVBFilterTest, junk_in) {
    std::vector<std::string> inputs = {"not json",
                                       "{}",
                                       R"({"collections":1})",
                                       R"({"collections:"this"})",
                                       R"({"collections:{"a":1})",
                                       R"({"collection:["a"])",
                                       R"({"collections:[a])"};

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);
        try {
            Collections::VB::Filter f(json, vbm);
            FAIL() << "Should of thrown an exception " << s;
        } catch (const cb::engine_error& e) {
            EXPECT_EQ(cb::engine_errc::invalid_arguments, e.code());
        } catch (...) {
            FAIL() << "Should of thrown cb::engine_error";
        }
    }
}

/**
 * Test invalid JSON formats as an input
 */
TEST_F(CollectionsVBFilterTest, junk_in_scope) {
    std::vector<std::string> inputs = {"not json",
                                       "{}",
                                       R"({"scope":1})",
                                       R"({"scope":"this"})",
                                       R"({"scope":{"a":1})",
                                       R"({"scope":["2"]})",
                                       R"({"scope":[a]})",
                                       R"({"scope":["0", "2"]"})"};

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);
        try {
            Collections::VB::Filter f(json, vbm);
            FAIL() << "Should of thrown an exception " << s;
        } catch (const cb::engine_error& e) {
            EXPECT_EQ(cb::engine_errc::invalid_arguments, e.code());
        } catch (...) {
            FAIL() << "Should of thrown cb::engine_error";
        }
    }
}

/**
 * Test valid inputs to the filter.
 */
TEST_F(CollectionsVBFilterTest, validation1) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::vector<std::string> inputs = {
            R"({"collections":["0"]})",
            R"({"collections":["8"]})",
            R"({"collections":["9", "a"]})"};

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);

        try {
            Collections::VB::Filter f(json, vbm);
        } catch (...) {
            FAIL() << "Exception thrown with input " << s;
        }
    }
}

/**
 * Test valid scope based inputs to the filter
 */
TEST_F(CollectionsVBFilterTest, validation1_scope) {
    cm.add(CollectionEntry::fruit);
    cm.add(ScopeEntry::shop1).add(CollectionEntry::meat, ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::vector<std::string> inputs = {
            R"({"scope":"0"})",
            R"({"scope":"8"})"};

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);

        try {
            Collections::VB::Filter f(json, vbm);
        } catch (...) {
            FAIL() << "Exception thrown with input " << s;
        }
    }
}

/**
 * Test valid JSON formats to the filter, but they contain invalid content
 */
TEST_F(CollectionsVBFilterTest, validation2) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::vector<std::string> inputs = {
            // wrong UID inputs
            R"({"collections":["e"]})", // one unknown CID
            R"({"collections":["8","22"]})" // one known, one unknown
    };

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);
        try {
            Collections::VB::Filter f(json, vbm);
            FAIL() << "Should have thrown an exception with input " << s;
        } catch (const cb::engine_error& e) {
            EXPECT_EQ(cb::engine_errc::unknown_collection, e.code());
        } catch (...) {
            FAIL() << "Should have thrown cb::engine_error";
        }
    }
}

/**
 * Test valid JSON formats to the filter, but they contain invalid content
 */
TEST_F(CollectionsVBFilterTest, validation2_scope) {
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::vector<std::string> inputs = {
            R"({"scope":"9"})" // one unknown SID
    };

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);
        try {
            Collections::VB::Filter f(json, vbm);
            FAIL() << "Should have thrown an exception with input " << s;
        } catch (const cb::engine_error& e) {
            EXPECT_EQ(cb::engine_errc::unknown_scope, e.code());
        } catch (...) {
            FAIL() << "Should have thrown cb::engine_error";
        }
    }
}

/**
 * Test invalid JSON containing both collections and scopes
 */
TEST_F(CollectionsVBFilterTest, validation2_collections_and_scope) {
    cm.add(CollectionEntry::meat);
    cm.add(ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::vector<std::string> inputs = {
            R"({"scope":"8",
                "collections":["2"]})"};

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);
        try {
            Collections::VB::Filter f(json, vbm);
            FAIL() << "Should have thrown an exception";
        } catch (const cb::engine_error& e) {
            EXPECT_EQ(cb::engine_errc::invalid_arguments, e.code());
        } catch (...) {
            FAIL() << "Should have thrown cb::engine_error";
        }
    }
}

TEST_F(CollectionsVBFilterTest, validation2_empty_scope) {
    cm.add(ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string input = R"({"scope":"8"})";
    boost::optional<cb::const_char_buffer> json(input);
    try {
        Collections::VB::Filter f(json, vbm);
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments, e.code());
    } catch (...) {
        FAIL() << "Should have thrown cb::engine_error";
    }
}

/**
 * Test that we cannot create default collection filter when no default
 * collection exists
 */
TEST_F(CollectionsVBFilterTest, validation_no_default) {
    cm.remove(CollectionEntry::defaultC)
            .add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    boost::optional<cb::const_char_buffer> json;
    try {
        Collections::VB::Filter f(json, vbm);
        FAIL() << "Should of thrown an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::unknown_collection, e.code());
    } catch (...) {
        FAIL() << "Should of thrown cb::engine_error";
    }
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter which contains a set of collections
 */
TEST_F(CollectionsVBFilterTest, filter_basic1) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["0", "8", "9"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::VB::Filter f(json, vbm);

    // This is not a passthrough filter
    EXPECT_FALSE(f.isPassthrough());

    // But this filter would send the default
    EXPECT_TRUE(f.allowDefaultCollection());
    // and allow system events
    EXPECT_TRUE(f.allowSystemEvents());

    // The actual filter "list" only stores fruit and meat though, default is
    // special cased via doesDefaultCollectionExist
    EXPECT_EQ(2, f.size());
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter with a scope - which adds a set of collections
 */
TEST_F(CollectionsVBFilterTest, filter_basic1_default_scope) {
    cm.add(CollectionEntry::meat);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"scope":"0"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::VB::Filter f(json, vbm);

    EXPECT_FALSE(f.isPassthrough());
    EXPECT_TRUE(f.allowDefaultCollection());
    EXPECT_TRUE(f.allowSystemEvents());

    // There are two collections (default and meat) in the default scope,
    // however we only include the non-default in size
    EXPECT_EQ(1, f.size());
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter with a scope - which adds a set of collections
 */
TEST_F(CollectionsVBFilterTest, filter_basic1_non_default_scope) {
    cm.add(ScopeEntry::shop1).add(CollectionEntry::meat, ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"scope":"8"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::VB::Filter f(json, vbm);

    EXPECT_FALSE(f.isPassthrough());
    EXPECT_FALSE(f.allowDefaultCollection());
    EXPECT_TRUE(f.allowSystemEvents());

    // There is only the meat collection in this scope
    EXPECT_EQ(1, f.size());
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter which is passthrough
 */
TEST_F(CollectionsVBFilterTest, filter_basic2) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter; // empty string creates a pass through
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::VB::Filter f(json, vbm);

    // This is a passthrough filter
    EXPECT_TRUE(f.isPassthrough());

    // So this filter would send the default
    EXPECT_TRUE(f.allowDefaultCollection());

    // and still allow system events
    EXPECT_TRUE(f.allowSystemEvents());

    // The actual filter "list" stores nothing
    EXPECT_EQ(0, f.size());
}

/**
 * Construct a valid Collections::Filter as if a legacy DCP producer was created
 */
TEST_F(CollectionsVBFilterTest, filter_legacy) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    // No string...
    boost::optional<cb::const_char_buffer> json;
    Collections::VB::Filter f(json, vbm);

    // Not a pass through
    EXPECT_FALSE(f.isPassthrough());

    // Allows the default
    EXPECT_TRUE(f.allowDefaultCollection());

    // Does not allow system events
    EXPECT_FALSE(f.allowSystemEvents());

    // The actual filter "list" stores nothing
    EXPECT_EQ(0, f.size());
}

/**
 * Create a filter with collections and check we allow what should be allowed.
 */
TEST_F(CollectionsVBFilterTest, basic_allow) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["0", "8", "9"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);

    // Yes to these guys
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"anykey", CollectionEntry::defaultC},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"meat:bacon", CollectionEntry::meat},
             0,
             0,
             nullptr,
             0}));

    // No to these keys
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
             0,
             0,
             nullptr,
             0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"vegetable:cabbage", CollectionEntry::vegetable},
             0,
             0,
             nullptr,
             0}));
}

/**
 * Create a filter using a scope and check we allow what should be allowed.
 */
TEST_F(CollectionsVBFilterTest, basic_allow_default_scope) {
    cm.add(CollectionEntry::dairy)
            .add(ScopeEntry::shop1)
            .add(CollectionEntry::dairy2, ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"scope":"0"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);

    // Yes to default and dairy
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"anykey", CollectionEntry::defaultC},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
             0,
             0,
             nullptr,
             0}));
    // No to dairy2
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"dairy:milk", CollectionEntry::dairy2},
             0,
             0,
             nullptr,
             0}));
}

/**
 * Create a filter using a scope and check we allow what should be allowed.
 */
TEST_F(CollectionsVBFilterTest, basic_allow_non_default_scope) {
    cm.add(CollectionEntry::dairy)
            .add(ScopeEntry::shop1)
            .add(CollectionEntry::dairy2, ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"scope":"8"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);

    // Yes to dairy2
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"dairy:milk", CollectionEntry::dairy2},
             0,
             0,
             nullptr,
             0}));

    // No to default and dairy
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"anykey", CollectionEntry::defaultC},
             0,
             0,
             nullptr,
             0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
             0,
             0,
             nullptr,
             0}));
}

/**
 * Create a filter as if a legacy DCP connection would, i.e. the optional
 * JSON filter is not initialised (because DCP open does not send a value).
 */
TEST_F(CollectionsVBFilterTest, legacy_filter) {
    cm.add(CollectionEntry::meat);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    boost::optional<cb::const_char_buffer> json;

    Collections::VB::Filter vbf(json, vbm);
    // Legacy would only allow default
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"anykey", CollectionEntry::defaultC},
             0,
             0,
             nullptr,
             0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
             0,
             0,
             nullptr,
             0}));
}

/**
 * Create a passthrough filter and check it allows anything
 */
TEST_F(CollectionsVBFilterTest, passthrough) {
    cm.add(CollectionEntry::meat);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string filterJson; // empty string
    boost::optional<cb::const_char_buffer> json(filterJson);

    // Everything is allowed (even junk, which isn't the filter's job to police)
    Collections::VB::Filter vbf(json, vbm);
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"anykey", CollectionEntry::defaultC},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"meat:steak", CollectionEntry::meat},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"JUNK!!", CollectionEntry::vegetable},
             0,
             0,
             nullptr,
             0}));
}

/**
 * Create a filter which blocks the default collection
 */
TEST_F(CollectionsVBFilterTest, no_default) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["8", "9"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    // Now filter!
    Collections::VB::Filter vbf(json, vbm);
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"anykey", CollectionEntry::defaultC},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"meat:steak", CollectionEntry::meat},
             0,
             0,
             nullptr,
             0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
             0,
             0,
             nullptr,
             0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"JUNK!!", CollectionEntry::vegetable},
             0,
             0,
             nullptr,
             0}));
}

/**
 * Check we can remove collections from the filter (which live DCP may do) and
 * check ::checkAndUpdate works as expected
 */
TEST_F(CollectionsVBFilterTest, remove1) {
    cm.remove(CollectionEntry::defaultC)
            .add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);

    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["8", "9"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
             0,
             0,
             nullptr,
             0}));

    // Process a deletion of fruit
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::fruit,
            CollectionName::fruit,
            {ScopeUid::defaultS, {}, 0, -6},
            true,
            {});
    auto sz = vbf.size();
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));
    EXPECT_EQ(sz - 1, vbf.size());

    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
             0,
             0,
             nullptr,
             0}));

    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"meat:steak", CollectionEntry::meat},
             0,
             0,
             nullptr,
             0}));

    // Process a deletion of meat
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::meat,
            CollectionName::meat,
            {ScopeUid::defaultS, {}, 0, -6},
            true,
            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));
    EXPECT_TRUE(vbf.empty()); // now empty
    EXPECT_FALSE(vbf.checkAndUpdate(*ev)); // no more meat for you
}

/**
 * Check we can remove collections from the filter (which live DCP may do) and
 * check ::allow works as expected
 * This test includes checking we can remove $default
 */
TEST_F(CollectionsVBFilterTest, remove2) {
    cm.add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);

    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["0", "8"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);
    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"anykey", CollectionEntry::defaultC},
             0,
             0,
             nullptr,
             0}));

    // Process a deletion of $default
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::defaultC,
            CollectionName::defaultC,
            {ScopeUid::defaultS, {}, 0, -6},
            true,
            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"anykey", CollectionEntry::defaultC},
             0,
             0,
             nullptr,
             0}));

    EXPECT_TRUE(vbf.checkAndUpdate(
            {StoredDocKey{"meat:steak", CollectionEntry::meat},
             0,
             0,
             nullptr,
             0}));

    // Process a deletion of meat
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::meat,
            CollectionName::meat,
            {ScopeUid::defaultS, {}, 0, -6},
            true,
            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"meat:apple", CollectionEntry::meat},
             0,
             0,
             nullptr,
             0}));
    EXPECT_TRUE(vbf.empty()); // now empty
    EXPECT_FALSE(vbf.checkAndUpdate(*ev)); // no more meat for you
    EXPECT_FALSE(vbf.checkAndUpdate(
            {StoredDocKey{"meat:steak", CollectionEntry::meat},
             0,
             0,
             nullptr,
             0}));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a passthrough filter so everything is allowed.
 */
TEST_F(CollectionsVBFilterTest, system_events1) {
    cm.add(CollectionEntry::meat).add(CollectionEntry::fruit);

    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter;
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);

    // meat system event is allowed by the meat filter
    EXPECT_TRUE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "meat", {}, {})));

    // $default system event is allowed by the filter
    EXPECT_TRUE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "$default", {}, {})));

    // dairy system event is allowed even though dairy doesn't exist in the
    // manifest, we wouldn't actually create this event as dairy isn't present
    // but this just shows the passthrough interface at work.
    EXPECT_TRUE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "dairy", {}, {})));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a filter where only matching events are allowed
 */
TEST_F(CollectionsVBFilterTest, system_events2) {
    cm.add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    // only events for default and meat are allowed
    std::string jsonFilter = R"({"collections":["0", "8"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);

    // meat system event is allowed by the meat filter
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::meat,
            CollectionName::meat,
            {ScopeUid::defaultS, {}, 0, -6},
            false,
            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));

    // $default system event is allowed by the filter
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::defaultC,
            CollectionName::defaultC,
            {ScopeUid::defaultS, {}, 0, -6},
            false,
            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));

    // dairy system event is not allowed by the filter
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::dairy,
            CollectionName::dairy,
            {ScopeUid::defaultS, {}, 0, -6},
            false,
            {});
    EXPECT_FALSE(vbf.checkAndUpdate(*ev));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a filter where only matching events are allowed
 */
TEST_F(CollectionsVBFilterTest, system_events2_default_scope) {
    cm.add(CollectionEntry::dairy)
            .add(ScopeEntry::shop1)
            .add(CollectionEntry::meat, ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    // Only events for defaultC and dairy are allowed
    std::string jsonFilter = R"({"scope":"0"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);

    // default (default) system events are allowed
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::defaultC,
            CollectionName::defaultC,
            {ScopeUid::defaultS, {}, 0, -6},
            false,
            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));

    // dairy (default) system events are allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::dairy,
            CollectionName::dairy,
            {ScopeUid::defaultS, {}, 0, -6},
            false,
            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));

    // meat (shop1) system events are not allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::meat,
            CollectionName::meat,
            {ScopeUid::shop1, {}, 0, -6},
            false,
            {});
    EXPECT_FALSE(vbf.checkAndUpdate(*ev));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a filter where only matching events are allowed
 */
TEST_F(CollectionsVBFilterTest, system_events2_non_default_scope) {
    cm.add(CollectionEntry::dairy)
            .add(ScopeEntry::shop1)
            .add(CollectionEntry::meat, ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    // Only events for meat are allowed
    std::string jsonFilter = R"({"scope":"8"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::VB::Filter vbf(json, vbm);

    // meat (shop1) system events are allowed
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::meat,
            CollectionName::meat,
            {ScopeUid::shop1, {}, 0, -6},
            false,
            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*ev));

    // default (default) system events are not allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::defaultC,
            CollectionName::defaultC,
            {ScopeUid::defaultS, {}, 0, -6},
            false,
            {});
    EXPECT_FALSE(vbf.checkAndUpdate(*ev));

    // dairy (default) system events are not allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::dairy,
            CollectionName::dairy,
            {ScopeUid::defaultS, {}, 0, -6},
            false,
            {});
    EXPECT_FALSE(vbf.checkAndUpdate(*ev));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a 'legacy' filter, one which an old DCP client would be
 * attached to - no system events at all are allowed.
 */
TEST_F(CollectionsVBFilterTest, system_events3) {
    cm.add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);

    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    boost::optional<cb::const_char_buffer> json;

    Collections::VB::Filter vbf(json, vbm);

    // All system events dropped by this empty/legacy filter
    EXPECT_FALSE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "meat", {}, {})));
    EXPECT_FALSE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "$default", {}, {})));
    EXPECT_FALSE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "dairy", {}, {})));
}

/**
 * Test that a collection is added to a scope filter by creating the correct
 * system events.
 */
TEST_F(CollectionsVBFilterTest, add_collection_to_scope_filter) {
    // Initially shop1 has the meat collection
    cm.add(ScopeEntry::shop1).add(CollectionEntry::meat, ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"scope":"8"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::VB::Filter vbf(json, vbm);

    // Only have meat in this filter
    ASSERT_EQ(vbf.size(), 1);

    // Shop1 system events are allowed
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::dairy,
            CollectionName::dairy,
            {ScopeUid::shop1, {}, 0, -6},
            false,
            {});
    ASSERT_TRUE(vbf.checkAndUpdate(*ev));

    EXPECT_EQ(vbf.size(), 2);
}

/**
 * Test that collections owned by a scope are removed from a scope filter
 */
TEST_F(CollectionsVBFilterTest, remove_collection_from_scope_filter) {
    // Initially shop1 has the meat and dairy collections
    cm.add(ScopeEntry::shop1)
            .add(CollectionEntry::meat, ScopeEntry::shop1)
            .add(CollectionEntry::dairy, ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    // scope 8 is shop1
    std::string jsonFilter = R"({"scope":"8"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::VB::Filter vbf(json, vbm);

    // 2 collections in this filter
    ASSERT_EQ(vbf.size(), 2);

    // Create an event which represents a drop of dairy/shop1
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::dairy,
            CollectionName::dairy,
            {ScopeUid::shop1, {}, 0, -6},
            true,
            {});
    ASSERT_TRUE(vbf.checkAndUpdate(*ev));
    ASSERT_EQ(vbf.size(), 1);

    // Create an event which represents a drop of meat/shop1
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::meat,
            CollectionName::meat,
            {ScopeUid::shop1, {}, 0, -6},
            true,
            {});

    ASSERT_TRUE(vbf.checkAndUpdate(*ev));
    EXPECT_EQ(vbf.size(), 0);
}

/**
 * We should be able to create a scope filter for a scope that has no
 * collections, then add a collection to it
 */
TEST_F(CollectionsVBFilterTest, empty_scope_filter) {
    // Initially shop1 has no collections
    cm.add(ScopeEntry::shop1);
    Collections::Manifest m(cm);
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"scope":"8"})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    // Create the filter
    Collections::VB::Filter vbf(json, vbm);
    EXPECT_EQ(vbf.size(), 0);

    // Now add a new collection
    cm.add(CollectionEntry::meat, ScopeEntry::shop1);
    m = Collections::Manifest(cm);
    vbm.wlock().update(vb, m);

    // Meat system events are allowed
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            0,
            CollectionUid::meat,
            CollectionName::meat,
            {ScopeUid::shop1, {}, 0, -6},
            false,
            {});
    ASSERT_TRUE(vbf.checkAndUpdate(*ev));

    EXPECT_EQ(vbf.size(), 1);
}
