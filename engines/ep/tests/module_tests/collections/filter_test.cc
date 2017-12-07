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

#include "collections/filter.h"
#include "collections/vbucket_filter.h"
#include "collections/vbucket_manifest.h"
#include "ep_vb.h"
#include "failover-table.h"

#include <gtest/gtest.h>

#include <limits>

class CollectionsFilterTest : public ::testing::Test {
public:
    /// Dummy callback to replace the flusher callback so we can create VBuckets
    class DummyCB : public Callback<uint16_t> {
    public:
        DummyCB() {
        }

        void callback(uint16_t& dummy) {
        }
    };

    CollectionsFilterTest()
        : vb(0,
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
             config,
             VALUE_ONLY) {
    }

    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    EPVBucket vb;
};

/**
 * Test invalid inputs to the filter.
 */
TEST_F(CollectionsFilterTest, junk_in) {
    Collections::Manifest m(
            R"({"separator":":",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"vegetable","uid":"1"}]})");

    std::vector<std::string> inputs = {"{}",
                                       R"({"collections":1})",
                                       R"({"collections:"this"})",
                                       R"({"collections:{"a":1})",
                                       R"({"collection:["a"])",
                                       R"({"collections:[a])"};

    for (const auto& s : inputs) {
        boost::optional<const std::string&> json = s;
        try {
            Collections::Filter f(json, &m);
            FAIL() << "Should of thrown an exception";
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
TEST_F(CollectionsFilterTest, validation1) {
    Collections::Manifest m(
            R"({"separator":":",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");

    std::vector<std::string> inputs = {R"({"collections":["$default"]})",
                                       R"({"collections":["vegetable"]})",
                                       R"({"collections":["fruit", "meat"]})"};

    for (const auto& s : inputs) {
        boost::optional<const std::string&> json = s;

        try {
            Collections::Filter f(json, &m);
        } catch (...) {
            FAIL() << "Exception thrown with input " << s;
        }
    }
}

/**
 * Test valid inputs to the filter, but they are not known collections, so
 * should trigger an exception.
 */
TEST_F(CollectionsFilterTest, validation2) {
    Collections::Manifest m(
            R"({"revision":0,"separator":":",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");

    std::vector<std::string> inputs = {R"({"collections":["cheese"]})",
                                       R"({"collections":["fruit","beer"]})",
                                       R"({"collections":["$dufault"]})"};

    for (const auto& s : inputs) {
        boost::optional<const std::string&> json = s;
        try {
            Collections::Filter f(json, &m);
            FAIL() << "Should of thrown an exception";
        } catch (const cb::engine_error& e) {
            EXPECT_EQ(cb::engine_errc::unknown_collection, e.code());
        } catch (...) {
            FAIL() << "Should of thrown cb::engine_error";
        }
    }
}

/**
 * Test that we cannot create default collection filter when no default
 * collection exists
 */
TEST_F(CollectionsFilterTest, validation_no_default) {
    // m does not include $default
    Collections::Manifest m(
            R"({"separator":":",)"
            R"("collections":[{"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");
    boost::optional<const std::string&> json;
    try {
        Collections::Filter f(json, &m);
        FAIL() << "Should of thrown an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::unknown_collection, e.code());
    } catch (...) {
        FAIL() << "Should of thrown cb::engine_error";
    }
}

/**
 * Test that we cannot create a true filter without a manifest
 */
TEST_F(CollectionsFilterTest, no_manifest) {
    std::string jsonFilter = R"({"collections":["$default", "fruit", "meat"]})";
    boost::optional<const std::string&> json(jsonFilter);

    try {
        Collections::Filter f(json, nullptr);
        FAIL() << "Should of thrown an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::no_collections_manifest, e.code());
    } catch (...) {
        FAIL() << "Should of thrown cb::engine_error";
    }
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter which contains a set of collections
 */
TEST_F(CollectionsFilterTest, filter_basic1) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");

    std::string jsonFilter = R"({"collections":["$default", "fruit", "meat"]})";
    boost::optional<const std::string&> json(jsonFilter);
    Collections::Filter f(json, &m);

    // This is not a passthrough filter
    EXPECT_FALSE(f.isPassthrough());

    // But this filter would send the default
    EXPECT_TRUE(f.allowDefaultCollection());
    // and allow system events
    EXPECT_TRUE(f.allowSystemEvents());

    // The actual filter "list" only stores fruit and meat though, default is
    // special cased via doesDefaultCollectionExist
    EXPECT_EQ(2, f.getFilter().size());

    auto list = f.getFilter();
    EXPECT_TRUE(std::find(std::begin(list), std::end(list), "fruit") !=
                list.end());
    EXPECT_TRUE(std::find(std::begin(list), std::end(list), "meat") !=
                list.end());
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter which is passthrough
 */
TEST_F(CollectionsFilterTest, filter_basic2) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");

    std::string jsonFilter; // empty string creates a pass through
    boost::optional<const std::string&> json(jsonFilter);
    Collections::Filter f(json, &m);

    // This is a passthrough filter
    EXPECT_TRUE(f.isPassthrough());

    // So this filter would send the default
    EXPECT_TRUE(f.allowDefaultCollection());

    // and still allow system events
    EXPECT_TRUE(f.allowSystemEvents());

    // The actual filter "list" stores nothing
    EXPECT_EQ(0, f.getFilter().size());
}

class CollectionsVBFilterTest : public CollectionsFilterTest {};

/**
 * Try and create filter for collections which exist, but have been deleted
 * i.e. they aren't writable so should never feature in a new VB::Filter
 */
TEST_F(CollectionsVBFilterTest, deleted_collection) {
    Collections::Manifest m1(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");
    Collections::Manifest m2(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"meat","uid":"3"},
                              {"name":"dairy","uid":"5"}]})");

    // Create the "producer" level filter so that we in theory produce at least
    // these collections
    std::string jsonFilter = R"({"collections":["vegetable", "fruit"]})";
    boost::optional<const std::string&> json(jsonFilter);
    Collections::Filter f(json, &m1);

    Collections::VB::Manifest vbm({});
    // push creates
    vbm.wlock().update(vb, m1);
    // push deletes, removing both filtered collections
    vbm.wlock().update(vb, m2);

    // Construction will fail as the filter would not match anything valid
    EXPECT_THROW(std::make_unique<Collections::VB::Filter>(f, vbm),
                 Collections::VB::Filter::EmptyException);
}

/**
 * Create a filter with collections and check we allow what should be allowed.
 */
TEST_F(CollectionsVBFilterTest, basic_allow) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");

    std::string jsonFilter = R"({"collections":["$default", "fruit", "meat"]})";
    boost::optional<const std::string&> json(jsonFilter);
    Collections::Filter f(json, &m);

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    Collections::VB::Filter vbf(f, vbm);

    // Yes to these guys
    EXPECT_TRUE(vbf.allow(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.allow(
            {{"fruit$apple", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.allow(
            {{"meat$bacon", DocNamespace::Collections}, 0, 0, nullptr, 0}));

    // No to these keys
    EXPECT_FALSE(vbf.allow(
            {{"dairy$milk", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.allow({{"vegetable$cabbage", DocNamespace::Collections},
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
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"meat","uid":"3"}]})");

    boost::optional<const std::string&> json;
    Collections::Filter f(json, &m);

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    Collections::VB::Filter vbf(f, vbm);
    // Legacy would only allow default
    EXPECT_TRUE(vbf.allow(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.allow(
            {{"fruit$apple", DocNamespace::Collections}, 0, 0, nullptr, 0}));
}

/**
 * Create a passthrough filter and check it allows anything
 */
TEST_F(CollectionsVBFilterTest, passthrough) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"meat","uid":"3"}]})");
    std::string filterJson; // empty string
    boost::optional<const std::string&> json(filterJson);
    Collections::Filter f(json, &m);

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    // Everything is allowed (even junk, which isn't the filter's job to police)
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_TRUE(vbf.allow(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.allow(
            {{"fruit$apple", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.allow(
            {{"meat$steak", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.allow(
            {{"dairy$milk", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.allow(
            {{"JUNK!!", DocNamespace::Collections}, 0, 0, nullptr, 0}));
}

/**
 * Create a filter which blocks the default collection
 */
TEST_F(CollectionsVBFilterTest, no_default) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");
    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["fruit", "meat"]})";
    boost::optional<const std::string&> json(jsonFilter);
    Collections::Filter f(json, &m);

    // Now filter!
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_FALSE(vbf.allow(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.allow(
            {{"fruit$apple", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.allow(
            {{"meat$steak", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.allow(
            {{"dairy$milk", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.allow(
            {{"JUNK!!", DocNamespace::Collections}, 0, 0, nullptr, 0}));
}

/**
 * Check we can remove collections from the filter (which live DCP may do)and
 * check ::allow works as expected
 */
TEST_F(CollectionsVBFilterTest, remove1) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"vegetable","uid":"1"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");
    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["fruit", "meat"]})";
    boost::optional<const std::string&> json(jsonFilter);

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_TRUE(vbf.allow(
            {{"fruit$apple", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.remove("fruit"));
    EXPECT_FALSE(vbf.allow(
            {{"fruit$apple", DocNamespace::Collections}, 0, 0, nullptr, 0}));

    EXPECT_TRUE(vbf.allow(
            {{"meat$steak", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.remove("meat"));
    EXPECT_FALSE(vbf.allow(
            {{"meat$apple", DocNamespace::Collections}, 0, 0, nullptr, 0}));
}

/**
 * Check we can remove collections from the filter (which live DCP may do) and
 * check ::allow works as expected
 * This test includes checking we can remove $default
 */
TEST_F(CollectionsVBFilterTest, remove2) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");
    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["$default", "meat"]})";
    boost::optional<const std::string&> json(jsonFilter);

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_TRUE(vbf.allow(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.remove("$default"));
    EXPECT_FALSE(vbf.allow(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));

    EXPECT_TRUE(vbf.allow(
            {{"meat$steak", DocNamespace::Collections}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.remove("meat"));
    EXPECT_FALSE(vbf.allow(
            {{"meat$apple", DocNamespace::Collections}, 0, 0, nullptr, 0}));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a passthrough filter so everything is allowed.
 */
TEST_F(CollectionsVBFilterTest, system_events1) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"}]})");
    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    std::string jsonFilter;
    boost::optional<const std::string&> json(jsonFilter);

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);

    // meat system event is allowed by the meat filter
    EXPECT_TRUE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "meat", 0, {})));

    // $default system event is allowed by the filter
    EXPECT_TRUE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "$default", 0, {})));

    // dairy system event is allowed even though dairy doesn't exist in the
    // manifest, we wouldn't actually create this event as dairy isn't present
    // but this just shows the passthrough interface at work.
    EXPECT_TRUE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "dairy", 0, {})));

    // A change of separator is also allowed
    EXPECT_TRUE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::CollectionsSeparatorChanged, "$", "::", 0, {})));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a filter where only matching events are allowed
 */
TEST_F(CollectionsVBFilterTest, system_events2) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");
    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    // only events for default and meat are allowed
    std::string jsonFilter = R"({"collections":["$default", "meat"]})";
    boost::optional<const std::string&> json(jsonFilter);

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);

    // meat system event is allowed by the meat filter
    EXPECT_TRUE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "meat", 0, {})));

    // $default system event is allowed by the filter
    EXPECT_TRUE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "$default", 0, {})));

    // dairy system event is not allowed by the filter
    EXPECT_FALSE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "dairy", 0, {})));

    // A change of separator is also allowed
    EXPECT_TRUE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::CollectionsSeparatorChanged, "$", "::", 0, {})));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a 'legacy' filter, one which an old DCP client would be
 * attached to - no system events at all are allowed.
 */
TEST_F(CollectionsVBFilterTest, system_events3) {
    Collections::Manifest m(
            R"({"separator":"$",)"
            R"("collections":[{"name":"$default","uid":"0"},
                              {"name":"meat","uid":"3"},
                              {"name":"fruit", "uid":"4"},
                              {"name":"dairy","uid":"5"}]})");
    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    boost::optional<const std::string&> json;

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);

    // All system events dropped by this empty/legacy filter
    EXPECT_FALSE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "meat", 0, {})));
    EXPECT_FALSE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "$default", 0, {})));
    EXPECT_FALSE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::Collection, "$", "dairy", 0, {})));
    EXPECT_FALSE(vbf.allow(*SystemEventFactory::make(
            SystemEvent::CollectionsSeparatorChanged, "$", "::", 0, {})));
}