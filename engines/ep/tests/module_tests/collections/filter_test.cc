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

#include "checkpoint_config.h"
#include "collections/events_generated.h"
#include "collections/manager.h"
#include "collections/vbucket_filter.h"
#include "collections/vbucket_manifest.h"
#include "configuration.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "stats.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_test.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include <folly/portability/GTest.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

#include <limits>

class CollectionsVBFilterTest : public CollectionsTest {
public:
    /// Dummy callback to replace the flusher callback so we can create VBuckets
    class DummyCB : public Callback<Vbid> {
    public:
        DummyCB() = default;

        void callback(Vbid& dummy) override {
        }
    };

    void SetUp() override {
        CollectionsTest::SetUp();
        vb = store->getVBucket(vbid);
        cookie = create_mock_cookie();
        setCollections(cookie, cm);
    }

    ~CollectionsVBFilterTest() override {
        manager->dereferenceMeta(CollectionID{},
                                 entryInDefaultScope->takeMeta());
        manager->dereferenceMeta(CollectionID{}, entryInShop1Scope->takeMeta());
        manager->dereferenceMeta(CollectionID{},
                                 entryInSystemScope->takeMeta());
    }

    void TearDown() override {
        destroy_mock_cookie(cookie);
        vb.reset();
        CollectionsTest::TearDown();
    }

    bool checkAndUpdate(Collections::VB::Filter& vbf, const Item& item) {
        Item i = item;
        return vbf.checkAndUpdate(i);
    }

    VBucketPtr vb;
    CollectionsManifest cm;
    std::shared_ptr<Collections::Manager> manager =
            std::make_shared<Collections::Manager>();
    // Tests need entry data which appear in default and 'shop1' scopes
    std::unique_ptr<Collections::VB::ManifestEntry> entryInDefaultScope =
            std::make_unique<Collections::VB::ManifestEntry>(
                    manager->createOrReferenceMeta(
                            CollectionID::Default,
                            Collections::VB::CollectionSharedMetaDataView{
                                    "name1", ScopeID::Default}),
                    0,
                    CanDeduplicate::Yes,
                    cb::NoExpiryLimit,
                    Collections::Metered::No,
                    Collections::ManifestUid{});
    std::unique_ptr<Collections::VB::ManifestEntry> entryInShop1Scope =
            std::make_unique<Collections::VB::ManifestEntry>(
                    manager->createOrReferenceMeta(
                            CollectionID::Default,
                            Collections::VB::CollectionSharedMetaDataView{
                                    "name2", ScopeUid::shop1}),
                    0,
                    CanDeduplicate::Yes,
                    cb::NoExpiryLimit,
                    Collections::Metered::No,
                    Collections::ManifestUid{});

    std::unique_ptr<Collections::VB::ManifestEntry> entryInSystemScope =
            std::make_unique<Collections::VB::ManifestEntry>(
                    manager->createOrReferenceMeta(
                            CollectionID::Default,
                            Collections::VB::CollectionSharedMetaDataView{
                                    "namee", ScopeUid::systemScope}),
                    0,
                    CanDeduplicate::Yes,
                    cb::NoExpiryLimit,
                    Collections::Metered::No,
                    Collections::ManifestUid{});

    CookieIface* cookie = nullptr;
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
                                       R"({"collections:[a])",
                                       R"({"scope":"1", "collections:[a])"};

    for (const auto& s : inputs) {
        std::optional<std::string_view> json(s);
        try {
            Collections::VB::Filter f(
                    json, vb->getManifest(), *cookie, *engine);
            FAIL() << "Should have thrown an exception s:" << s;
        } catch (const cb::engine_error& e) {
            EXPECT_EQ(cb::engine_errc::invalid_arguments, e.code());
        } catch (...) {
            FAIL() << "Should have thrown cb::engine_error s:" << s;
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
        std::optional<std::string_view> json(s);
        try {
            Collections::VB::Filter f(
                    json, vb->getManifest(), *cookie, *engine);
            FAIL() << "Should have thrown an exception " << s;
        } catch (const cb::engine_error& e) {
            EXPECT_EQ(cb::engine_errc::invalid_arguments, e.code());
        } catch (...) {
            FAIL() << "Should have thrown cb::engine_error";
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::vector<std::string> inputs = {R"({"uid":"a"})",
                                       R"({"collections":["0"]})",
                                       R"({"collections":["8"]})",
                                       R"({"collections":["9", "a"]})"};

    for (const auto& s : inputs) {
        std::optional<std::string_view> json(s);

        try {
            Collections::VB::Filter f(
                    json, vb->getManifest(), *cookie, *engine);
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::vector<std::string> inputs = {
            R"({"scope":"0"})",
            R"({"scope":"8"})"};

    for (const auto& s : inputs) {
        std::optional<std::string_view> json(s);

        try {
            Collections::VB::Filter f(
                    json, vb->getManifest(), *cookie, *engine);
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::vector<std::string> inputs = {
            // wrong UID inputs
            R"({"collections":["e"]})", // one unknown CID
            R"({"collections":["8","22"]})" // one known, one unknown
    };

    for (const auto& s : inputs) {
        std::optional<std::string_view> json(s);
        try {
            Collections::VB::Filter f(
                    json, vb->getManifest(), *cookie, *engine);
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::vector<std::string> inputs = {
            R"({"scope":"9"})" // one unknown SID
    };

    for (const auto& s : inputs) {
        std::optional<std::string_view> json(s);
        try {
            Collections::VB::Filter f(
                    json, vb->getManifest(), *cookie, *engine);
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::vector<std::string> inputs = {
            R"({"scope":"8",
                "collections":["2"]})"};

    for (const auto& s : inputs) {
        std::optional<std::string_view> json(s);
        try {
            Collections::VB::Filter f(
                    json, vb->getManifest(), *cookie, *engine);
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string input = R"({"scope":"8"})";
    std::optional<std::string_view> json(input);
    try {
        Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::optional<std::string_view> json;
    try {
        Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
        FAIL() << "Should have thrown an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::unknown_collection, e.code());
    } catch (...) {
        FAIL() << "Should have thrown cb::engine_error";
    }
}

// class exposes some of the internal state flags
class CollectionsTestFilter : public Collections::VB::Filter {
public:
    CollectionsTestFilter(std::optional<std::string_view> jsonFilter,
                          const Collections::VB::Manifest& manifest,
                          CookieIface* cookie,
                          EventuallyPersistentEngine& engine)
        : Filter(jsonFilter, manifest, *cookie, engine) {
    }
    /// @return is this filter a passthrough (allows every collection)
    bool isPassthrough() const {
        return isPassThroughFilter();
    }

    /**
     * This only makes sense for !passthrough and returns if the filter allows
     * the default collection (the cached bool). Given this is test code we
     * aren't checking !passthrough when querying this flag
     */
    bool allowDefaultCollection() const {
        return defaultAllowed;
    }

    /// @return if system-events are allowed (e.g. create collection)
    bool allowSystemEvents() const {
        return !isLegacyFilter();
    }

    ScopeID getFilteredScopeID() const {
        return scopeID;
    }

    Collections::Visibility getFilteredScopeVisibility() const {
        return filteredScopeVisibility;
    }
};

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter which contains a set of collections
 */
TEST_F(CollectionsVBFilterTest, filter_basic1) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"collections":["0", "8", "9"]})";
    std::optional<std::string_view> json(jsonFilter);
    CollectionsTestFilter f(json, vb->getManifest(), cookie, *engine);

    // This is not a passthrough filter
    EXPECT_FALSE(f.isPassthrough());

    // But this filter would send the default
    EXPECT_TRUE(f.allowDefaultCollection());
    // and allow system events
    EXPECT_TRUE(f.allowSystemEvents());

    // Filter set stores all collections
    EXPECT_EQ(3, f.size());
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter with a scope - which adds a set of collections
 */
TEST_F(CollectionsVBFilterTest, filter_basic1_default_scope) {
    cm.add(CollectionEntry::meat);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"scope":"0"})";
    std::optional<std::string_view> json(jsonFilter);
    CollectionsTestFilter f(json, vb->getManifest(), cookie, *engine);

    EXPECT_FALSE(f.isPassthrough());
    EXPECT_TRUE(f.allowDefaultCollection());
    EXPECT_TRUE(f.allowSystemEvents());
    EXPECT_EQ(2, f.size());
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter with a scope - which adds a set of collections
 */
TEST_F(CollectionsVBFilterTest, filter_basic1_non_default_scope) {
    cm.add(ScopeEntry::shop1).add(CollectionEntry::meat, ScopeEntry::shop1);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"scope":"8"})";
    std::optional<std::string_view> json(jsonFilter);
    CollectionsTestFilter f(json, vb->getManifest(), cookie, *engine);

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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter; // empty string creates a pass through
    std::optional<std::string_view> json(jsonFilter);
    CollectionsTestFilter f(json, vb->getManifest(), cookie, *engine);

    // This is a passthrough filter
    EXPECT_TRUE(f.isPassthrough());

    // and still allow system events
    EXPECT_TRUE(f.allowSystemEvents());

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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // No string...
    std::optional<std::string_view> json;
    CollectionsTestFilter f(json, vb->getManifest(), cookie, *engine);

    // Not a pass through
    EXPECT_FALSE(f.isPassthrough());

    // Allows the default
    EXPECT_TRUE(f.allowDefaultCollection());

    // Does not allow system events
    EXPECT_FALSE(f.allowSystemEvents());

    EXPECT_EQ(1, f.size());
}

/**
 * Create a filter with collections and check we allow what should be allowed.
 */
TEST_F(CollectionsVBFilterTest, basic_allow) {
    cm.add(CollectionEntry::vegetable)
            .add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"collections":["0", "8", "9"]})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // Yes to these guys
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"anykey", CollectionEntry::defaultC},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"meat:bacon", CollectionEntry::meat},
                            0,
                            0,
                            nullptr,
                            0}));

    // No to these keys
    EXPECT_FALSE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_FALSE(checkAndUpdate(
            vbf,
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"scope":"0"})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // Yes to default and dairy
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"anykey", CollectionEntry::defaultC},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                            0,
                            0,
                            nullptr,
                            0}));
    // No to dairy2
    EXPECT_FALSE(
            checkAndUpdate(vbf,
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"scope":"8"})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // Yes to dairy2
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"dairy:milk", CollectionEntry::dairy2},
                            0,
                            0,
                            nullptr,
                            0}));

    // No to default and dairy
    EXPECT_FALSE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"anykey", CollectionEntry::defaultC},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_FALSE(
            checkAndUpdate(vbf,
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::optional<std::string_view> json;

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);
    // Legacy would only allow default
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"anykey", CollectionEntry::defaultC},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_FALSE(
            checkAndUpdate(vbf,
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string filterJson; // empty string
    std::optional<std::string_view> json(filterJson);

    // Everything is allowed (even junk, which isn't the filter's job to police)
    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"anykey", CollectionEntry::defaultC},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"meat:steak", CollectionEntry::meat},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
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
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"collections":["8", "9"]})";
    std::optional<std::string_view> json(jsonFilter);

    // Now filter!
    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);
    EXPECT_FALSE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"anykey", CollectionEntry::defaultC},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"meat:steak", CollectionEntry::meat},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_FALSE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_FALSE(
            checkAndUpdate(vbf,
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

    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"collections":["8", "9"]})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
                            0,
                            0,
                            nullptr,
                            0}));

    // Process a deletion of fruit
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::fruit,
            CollectionName::fruit,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::End,
            {});
    auto sz = vbf.size();
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));
    EXPECT_EQ(sz - 1, vbf.size());

    EXPECT_FALSE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
                            0,
                            0,
                            nullptr,
                            0}));

    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"meat:steak", CollectionEntry::meat},
                            0,
                            0,
                            nullptr,
                            0}));

    // Process a deletion of meat
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::meat,
            CollectionName::meat,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::End,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));
    EXPECT_TRUE(vbf.empty()); // now empty
    EXPECT_FALSE(checkAndUpdate(vbf, *ev)); // no more meat for you
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

    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"collections":["0", "8"]})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"anykey", CollectionEntry::defaultC},
                            0,
                            0,
                            nullptr,
                            0}));

    // Process a deletion of $default
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::defaultC,
            CollectionName::defaultC,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::End,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));
    EXPECT_FALSE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"anykey", CollectionEntry::defaultC},
                            0,
                            0,
                            nullptr,
                            0}));

    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"meat:steak", CollectionEntry::meat},
                            0,
                            0,
                            nullptr,
                            0}));

    // Process a deletion of meat
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::meat,
            CollectionName::meat,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::End,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));
    EXPECT_FALSE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"meat:apple", CollectionEntry::meat},
                            0,
                            0,
                            nullptr,
                            0}));
    EXPECT_TRUE(vbf.empty()); // now empty
    EXPECT_FALSE(checkAndUpdate(vbf, *ev)); // no more meat for you
    EXPECT_FALSE(
            checkAndUpdate(vbf,
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

    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter;
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // meat system event is allowed by the meat filter
    EXPECT_TRUE(checkAndUpdate(vbf,
                               *SystemEventFactory::makeCollectionEvent(
                                       CollectionEntry::meat, {}, {})));

    // $default system event is allowed by the filter
    EXPECT_TRUE(checkAndUpdate(vbf,
                               *SystemEventFactory::makeCollectionEvent(
                                       CollectionEntry::defaultC, {}, {})));

    // dairy system event is allowed even though dairy doesn't exist in the
    // manifest, we wouldn't actually create this event as dairy isn't present
    // but this just shows the passthrough interface at work.
    EXPECT_TRUE(checkAndUpdate(vbf,
                               *SystemEventFactory::makeCollectionEvent(
                                       CollectionEntry::dairy, {}, {})));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a filter where only matching events are allowed
 */
TEST_F(CollectionsVBFilterTest, system_events2) {
    cm.add(CollectionEntry::meat)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::dairy);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // only events for default and meat are allowed
    std::string jsonFilter = R"({"collections":["0", "8"]})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // meat system event is allowed by the meat filter
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::meat,
            CollectionName::meat,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));

    // $default system event is allowed by the filter
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::defaultC,
            CollectionName::defaultC,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));

    // Modify event is allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::defaultC,
            CollectionName::defaultC,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Modify,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));

    // dairy system event is not allowed by the filter
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::dairy,
            CollectionName::dairy,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_FALSE(checkAndUpdate(vbf, *ev));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a filter where only matching events are allowed
 */
TEST_F(CollectionsVBFilterTest, system_events2_default_scope) {
    cm.add(CollectionEntry::dairy)
            .add(ScopeEntry::shop1)
            .add(CollectionEntry::meat, ScopeEntry::shop1);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // Only events for defaultC and dairy are allowed
    std::string jsonFilter = R"({"scope":"0"})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // default (default) system events are allowed
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::defaultC,
            CollectionName::defaultC,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));

    // dairy (default) system events are allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::dairy,
            CollectionName::dairy,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));

    // meat (shop1) system events are not allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::meat,
            CollectionName::meat,
            *entryInShop1Scope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_FALSE(checkAndUpdate(vbf, *ev));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a filter where only matching events are allowed
 */
TEST_F(CollectionsVBFilterTest, system_events2_non_default_scope) {
    cm.add(CollectionEntry::dairy)
            .add(ScopeEntry::shop1)
            .add(CollectionEntry::meat, ScopeEntry::shop1);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // Only events for meat are allowed
    std::string jsonFilter = R"({"scope":"8"})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // meat (shop1) system events are allowed
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::meat,
            CollectionName::meat,
            *entryInShop1Scope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));

    // default (default) system events are not allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::defaultC,
            CollectionName::defaultC,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_FALSE(checkAndUpdate(vbf, *ev));

    // dairy (default) system events are not allowed
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::dairy,
            CollectionName::dairy,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_FALSE(checkAndUpdate(vbf, *ev));
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

    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::optional<std::string_view> json;

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // All system events dropped by this empty/legacy filter
    EXPECT_FALSE(checkAndUpdate(vbf,
                                *SystemEventFactory::makeCollectionEvent(
                                        CollectionEntry::meat, {}, {})));
    EXPECT_FALSE(checkAndUpdate(vbf,
                                *SystemEventFactory::makeCollectionEvent(
                                        CollectionEntry::defaultC, {}, {})));
    EXPECT_FALSE(checkAndUpdate(vbf,
                                *SystemEventFactory::makeCollectionEvent(
                                        CollectionEntry::dairy, {}, {})));
}

/**
 * Test that a collection is added to a scope filter by creating the correct
 * system events.
 */
TEST_F(CollectionsVBFilterTest, add_collection_to_scope_filter) {
    // Initially shop1 has the meat collection
    cm.add(ScopeEntry::shop1).add(CollectionEntry::meat, ScopeEntry::shop1);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"scope":"8"})";
    std::optional<std::string_view> json(jsonFilter);
    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // Only have meat in this filter
    ASSERT_EQ(vbf.size(), 1);

    // Shop1 system events are allowed
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::dairy,
            CollectionName::dairy,
            *entryInShop1Scope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    ASSERT_TRUE(checkAndUpdate(vbf, *ev));

    EXPECT_EQ(vbf.size(), 2);

    // Add a system collection (_ prefix) and check we know about it, this
    // does not require that the VB::Manifest know as we deliberately do not
    // look at that object from the filter checkAndUpdate path
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::systemCollection,
            CollectionName::systemCollection,
            *entryInShop1Scope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    ASSERT_TRUE(checkAndUpdate(vbf, *ev));

    EXPECT_EQ(vbf.size(), 3);
    auto itr = vbf.find(CollectionUid::systemCollection);
    EXPECT_NE(itr, vbf.end());
    EXPECT_EQ(Collections::Visibility::System,
              itr->second.collectionVisibility);
}

/**
 * Test that collections owned by a scope are removed from a scope filter
 */
TEST_F(CollectionsVBFilterTest, remove_collection_from_scope_filter) {
    // Initially shop1 has the meat and dairy collections
    cm.add(ScopeEntry::shop1)
            .add(CollectionEntry::meat, ScopeEntry::shop1)
            .add(CollectionEntry::dairy, ScopeEntry::shop1);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // scope 8 is shop1
    std::string jsonFilter = R"({"scope":"8"})";
    std::optional<std::string_view> json(jsonFilter);
    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // 2 collections in this filter
    ASSERT_EQ(vbf.size(), 2);

    // Create an event which represents a drop of dairy/shop1
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::dairy,
            CollectionName::dairy,
            *entryInShop1Scope,
            Collections::VB::Manifest::SystemEventType::End,
            {});
    ASSERT_TRUE(checkAndUpdate(vbf, *ev));
    ASSERT_EQ(vbf.size(), 1);

    // Create an event which represents a drop of meat/shop1
    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::meat,
            CollectionName::meat,
            *entryInShop1Scope,
            Collections::VB::Manifest::SystemEventType::End,
            {});

    ASSERT_TRUE(checkAndUpdate(vbf, *ev));
    EXPECT_EQ(vbf.size(), 0);
}

/**
 * We should be able to create a scope filter for a scope that has no
 * collections, then add a collection to it
 */
TEST_F(CollectionsVBFilterTest, empty_scope_filter) {
    // Initially shop1 has no collections
    cm.add(ScopeEntry::shop1);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"scope":"8"})";
    std::optional<std::string_view> json(jsonFilter);

    // Create the filter
    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);
    EXPECT_EQ(vbf.size(), 0);

    // Now add a new collection
    cm.add(CollectionEntry::meat, ScopeEntry::shop1);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // Meat system events are allowed
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::meat,
            CollectionName::meat,
            *entryInShop1Scope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    ASSERT_TRUE(checkAndUpdate(vbf, *ev));

    EXPECT_EQ(vbf.size(), 1);
}

/**
 * Validate a snappy item can update the filter
 */
TEST_F(CollectionsVBFilterTest, snappy_event) {
    cm.remove(CollectionEntry::defaultC).add(CollectionEntry::fruit);

    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string jsonFilter = R"({"collections":["9"]})";
    std::optional<std::string_view> json(jsonFilter);

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);
    EXPECT_TRUE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
                            0,
                            0,
                            nullptr,
                            0}));

    // Process a deletion of fruit
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::fruit,
            CollectionName::fruit,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::End,
            {});
    ev->compressValue();
    auto sz = vbf.size();
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));
    EXPECT_EQ(sz - 1, vbf.size());

    EXPECT_FALSE(
            checkAndUpdate(vbf,
                           {StoredDocKey{"fruit:apple", CollectionEntry::fruit},
                            0,
                            0,
                            nullptr,
                            0}));
}

TEST_F(CollectionsVBFilterTest, size_stats_for_dropped_collection) {
    cm.add(CollectionEntry::fruit);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // Filter by fruit collection.
    std::string filterJson = R"({"collections":["9"]})";
    std::optional<std::string_view> filterManifest{filterJson};
    Collections::VB::Filter filter(
            filterManifest, vb->getManifest(), *cookie, *engine);

    cm.remove(CollectionEntry::fruit);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // Make sure getSizeStats does not crash due to the collection in the
    // filter having been dropped.
    filter.getSizeStats(vb->getManifest());
}

// If anyone did want to filter on a system scope
TEST_F(CollectionsVBFilterTest, system_scope_filter) {
    cm.add(ScopeEntry::systemScope);
    cm.add(CollectionEntry::systemCollection, ScopeEntry::systemScope);

    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string filterJson = R"({"scope":"10"})";
    std::optional<std::string_view> json{filterJson};

    CollectionsTestFilter vbf(json, vb->getManifest(), cookie, *engine);

    // system collection is allowed by the filter
    auto ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::systemCollection,
            CollectionName::systemCollection,
            *entryInSystemScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    EXPECT_TRUE(checkAndUpdate(vbf, *ev));

    ev = Collections::VB::Manifest::makeCollectionSystemEvent(
            Collections::ManifestUid(0),
            CollectionUid::defaultC,
            CollectionName::defaultC,
            *entryInDefaultScope,
            Collections::VB::Manifest::SystemEventType::Begin,
            {});
    // $default system event is not allowed by the filter
    EXPECT_FALSE(checkAndUpdate(vbf, *ev));

    // Check some state
    ASSERT_TRUE(vbf.isScopeFilter());
    EXPECT_EQ(ScopeUid::systemScope, vbf.getFilteredScopeID());
    EXPECT_EQ(Collections::Visibility::System,
              vbf.getFilteredScopeVisibility());
}

class CollectionsVBFilterAccessControlTest : public CollectionsVBFilterTest {
    void TearDown() override {
        MockCookie::setCheckPrivilegeFunction({});
        CollectionsVBFilterTest::TearDown();
    }
};

TEST_F(CollectionsVBFilterAccessControlTest, no_privilege_for_passthrough) {
    MockCookie::setCheckPrivilegeFunction(
            [](const CookieIface&,
               cb::rbac::Privilege priv,
               std::optional<ScopeID> sid,
               std::optional<CollectionID> cid) -> cb::rbac::PrivilegeAccess {
                EXPECT_FALSE(cid);
                EXPECT_FALSE(sid);

                if (priv == cb::rbac::Privilege::DcpStream) {
                    return cb::rbac::PrivilegeAccessFail;
                }
                return cb::rbac::PrivilegeAccessOk;
            });

    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));
    std::string input;
    std::optional<std::string_view> json(input);

    try {
        Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
        FAIL() << "Should have thrown an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::no_access, e.code());
    } catch (...) {
        FAIL() << "Should have thrown cb::engine_error";
    }
}

TEST_F(CollectionsVBFilterAccessControlTest, privilege_for_passthrough) {
    MockCookie::setCheckPrivilegeFunction(
            [](const CookieIface&,
               cb::rbac::Privilege priv,
               std::optional<ScopeID> sid,
               std::optional<CollectionID> cid) -> cb::rbac::PrivilegeAccess {
                EXPECT_FALSE(cid);
                EXPECT_FALSE(sid);

                if (priv == cb::rbac::Privilege::DcpStream ||
                    priv == cb::rbac::Privilege::SystemCollectionLookup) {
                    return cb::rbac::PrivilegeAccessOk;
                }
                return cb::rbac::PrivilegeAccessFail;
            });

    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));
    std::string input;
    std::optional<std::string_view> json(input);
    Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
}

// Test that 1) we cannot create a filter for a collection (9) when we don't
// have access to it.
// Test that 2) we can create a filter for a collection (c) when we do have
// the DcpStream privilege for it.
TEST_F(CollectionsVBFilterAccessControlTest, privilege_check_for_collection) {
    // Make check bucket fail, looks like the connection has no bucket.DcpStream
    // privilege.
    CollectionID noAccessTo = CollectionEntry::fruit.getId();

    MockCookie::setCheckPrivilegeFunction(
            [noAccessTo](const CookieIface&,
                         cb::rbac::Privilege priv,
                         std::optional<ScopeID> sid,
                         std::optional<CollectionID> cid)
                    -> cb::rbac::PrivilegeAccess {
                if (cid && cid.value() == noAccessTo) {
                    return cb::rbac::PrivilegeAccessFail;
                }
                return cb::rbac::PrivilegeAccessOk;
            });

    cm.add(CollectionEntry::dairy).add(CollectionEntry::fruit);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // No access to fruit
    std::string input = R"({"collections":["9"]})";
    std::optional<std::string_view> json(input);

    // Test 1)
    try {
        Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
        FAIL() << "Should have thrown an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::no_access, e.code());
    } catch (...) {
        FAIL() << "Should have thrown cb::engine_error";
    }

    // Test 2)
    input = R"({"collections":["c"]})";
    Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
}

// Similar to the above test, but here we try to build a multi-collection filter
// where we don't have access to all requested collections.
TEST_F(CollectionsVBFilterAccessControlTest, privilege_check_for_collections) {
    // Make check bucket fail, looks like the connection has no bucket.DcpStream
    // privilege.
    CollectionID noAccessTo = CollectionEntry::fruit.getId();
    MockCookie::setCheckPrivilegeFunction(
            [noAccessTo](const CookieIface&,
                         cb::rbac::Privilege priv,
                         std::optional<ScopeID> sid,
                         std::optional<CollectionID> cid)
                    -> cb::rbac::PrivilegeAccess {
                if (cid && cid.value() == noAccessTo) {
                    return cb::rbac::PrivilegeAccessFail;
                }
                return cb::rbac::PrivilegeAccessOk;
            });

    cm.add(CollectionEntry::dairy).add(CollectionEntry::fruit);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    // Multi collection request, but no access to fruit
    std::string input = R"({"collections":["c", "9"]})";
    std::optional<std::string_view> json(input);

    try {
        Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
        FAIL() << "Should have thrown an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::no_access, e.code());
    } catch (...) {
        FAIL() << "Should have thrown cb::engine_error";
    }
}

// Test that 1) we cannot create a filter for a scope (9) when we don't
// have access to it.
// Test that 2) we can create a filter for a scope (c) when we do have
// the DcpStream privilege for it.
TEST_F(CollectionsVBFilterAccessControlTest, privilege_check_for_scope) {
    // Make check bucket fail, looks like the connection has no bucket.DcpStream
    // privilege.
    ScopeID noAccessTo = ScopeEntry::shop1.getId();

    MockCookie::setCheckPrivilegeFunction(
            [noAccessTo](const CookieIface&,
                         cb::rbac::Privilege priv,
                         std::optional<ScopeID> sid,
                         std::optional<CollectionID> cid)
                    -> cb::rbac::PrivilegeAccess {
                if (sid && sid.value() == noAccessTo) {
                    return cb::rbac::PrivilegeAccessFail;
                }
                return cb::rbac::PrivilegeAccessOk;
            });

    cm.add(ScopeEntry::shop1).add(ScopeEntry::shop2);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    std::string input = R"({"scope":"8"})";
    std::optional<std::string_view> json(input);

    // Test 1)
    try {
        Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
        FAIL() << "Should have thrown an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::no_access, e.code());
    } catch (...) {
        FAIL() << "Should have thrown cb::engine_error";
    }

    // Test 2)
    input = R"({"scope":"9"})";
    Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
}

// Check that when constructing a bucket filter/passthrough and the caller lacks
// SystemCollectionLookup - a System filter is created
TEST_F(CollectionsVBFilterAccessControlTest, system_filter) {
    MockCookie::setCheckPrivilegeFunction(
            [](const CookieIface&,
               cb::rbac::Privilege priv,
               std::optional<ScopeID> sid,
               std::optional<CollectionID> cid) -> cb::rbac::PrivilegeAccess {
                // Return no for system collection against bucket (no sid/cid)
                if (priv == cb::rbac::Privilege::SystemCollectionLookup &&
                    !sid && !cid) {
                    return cb::rbac::PrivilegeAccessFail;
                }
                return cb::rbac::PrivilegeAccessOk;
            });

    cm.add(CollectionEntry::dairy).add(CollectionEntry::fruit);
    ASSERT_EQ(cb::engine_errc::success, setCollections(cookie, cm));
    std::string input;
    std::optional<std::string_view> json(input);
    Collections::VB::Filter f(json, vb->getManifest(), *cookie, *engine);
    EXPECT_TRUE(f.isUserVisibleFilter());
    // Stores 2 collections + default
    EXPECT_EQ(3, f.size());
    // No oso for the system view - just more to test...
    EXPECT_FALSE(f.isOsoSuitable(f.size() + 1));
    EXPECT_FALSE(f.empty());

    // The check interface will check "normal keys" - it has limited usage in
    // the code.
    EXPECT_TRUE(f.check(StoredDocKey{"yes", CollectionEntry::fruit}));
    EXPECT_FALSE(f.check(StoredDocKey{"no", CollectionEntry::vegetable}));
    EXPECT_FALSE(
            f.check(StoredDocKey{"NO!", CollectionEntry::systemCollection}));

    // But (and as per the usage) always say true for SystemEvent keys
    EXPECT_TRUE(f.check(SystemEventFactory::makeCollectionEventKey(
            CollectionUid::systemCollection, SystemEvent::Collection)));

    // Check and update is different, it will dig into all keys and return and
    // is the function that ultimately decides what DCP sends. This works on
    // an Item& as input. We need a few Item's for the test
    Item fruitMutation{
            StoredDocKey{"yes", CollectionEntry::fruit}, 0, 0, nullptr, 0};
    Item vegetableMutation{
            StoredDocKey{"no", CollectionEntry::vegetable}, 0, 0, nullptr, 0};
    Item systemMutation{StoredDocKey{"NO!", CollectionEntry::systemCollection},
                        0,
                        0,
                        nullptr,
                        0};
    EXPECT_TRUE(f.checkAndUpdate(fruitMutation));
    EXPECT_FALSE(f.checkAndUpdate(vegetableMutation));
    EXPECT_FALSE(f.checkAndUpdate(systemMutation));

    // SystemCollection SystemEvent - not allowed. Some boiler plate needed to
    // build an Item which represents the collection create
    {
        flatbuffers::FlatBufferBuilder builder;
        auto collection = Collections::VB::CreateCollection(
                builder,
                1, // manifest uid
                uint32_t(ScopeUid::systemScope),
                uint32_t(CollectionUid::systemCollection),
                false, // no ttl
                0, // ttl 0
                builder.CreateString(CollectionName::systemCollection),
                true, // history false
                0,
                false /*metered*/);
        builder.Finish(collection);
        EXPECT_FALSE(f.checkAndUpdate(*SystemEventFactory::makeCollectionEvent(
                CollectionUid::systemCollection,
                {builder.GetBufferPointer(), builder.GetSize()},
                1)));
    }

    // Same for scope, boiler plate flatbuffer value
    {
        flatbuffers::FlatBufferBuilder builder;
        auto scope = Collections::VB::CreateScope(
                builder,
                1, // "manifest"
                uint32_t(ScopeUid::systemScope),
                builder.CreateString(ScopeName::systemScope));
        builder.Finish(scope);
        EXPECT_FALSE(f.checkAndUpdate(*SystemEventFactory::makeScopeEvent(
                ScopeUid::systemScope,
                {builder.GetBufferPointer(), builder.GetSize()},
                1)));
    }

    // But test that the system filter allows normal scopes and learns
    // collections

    {
        flatbuffers::FlatBufferBuilder builder;
        auto scope = Collections::VB::CreateScope(
                builder,
                1, // "manifest"
                uint32_t(ScopeUid::shop1),
                builder.CreateString(ScopeName::shop1));
        builder.Finish(scope);
        EXPECT_TRUE(f.checkAndUpdate(*SystemEventFactory::makeScopeEvent(
                ScopeUid::shop1,
                {builder.GetBufferPointer(), builder.GetSize()},
                1)));
    }

    // Process the create of the vegetable collection, the filter will also
    // update the set of valid collections
    {
        flatbuffers::FlatBufferBuilder builder;
        auto collection = Collections::VB::CreateCollection(
                builder,
                1, // manifest uid
                uint32_t(ScopeID::Default),
                uint32_t(CollectionUid::vegetable),
                false, // no ttl
                0, // ttl 0
                builder.CreateString(CollectionName::vegetable),
                true, // history false
                0,
                false /*metered*/);
        builder.Finish(collection);
        EXPECT_TRUE(f.checkAndUpdate(*SystemEventFactory::makeCollectionEvent(
                CollectionUid::vegetable,
                {builder.GetBufferPointer(), builder.GetSize()},
                1)));
    }
    EXPECT_TRUE(f.check(StoredDocKey{"no", CollectionEntry::vegetable}));
    EXPECT_TRUE(f.checkAndUpdate(vegetableMutation));
}
