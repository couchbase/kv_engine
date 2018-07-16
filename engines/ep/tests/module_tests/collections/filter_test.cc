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
#include "configuration.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "stats.h"
#include "tests/module_tests/collections/test_manifest.h"

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
 * Test invalid JSON formats as an input
 */
TEST_F(CollectionsFilterTest, junk_in) {
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m(cm);

    std::vector<std::string> inputs = {"{}",
                                       R"({"collections":1})",
                                       R"({"collections:"this"})",
                                       R"({"collections:{"a":1})",
                                       R"({"collection:["a"])",
                                       R"({"collections:[a])"};

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);
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
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m(cm.add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    std::vector<std::string> inputs = {
            R"({"collections":["0"]})",
            R"({"collections":["2"]})",
            R"({"collections":["3", "4"]})"};

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);

        try {
            Collections::Filter f(json, &m);
        } catch (...) {
            FAIL() << "Exception thrown with input " << s;
        }
    }
}

/**
 * Test valid JSON formats to the filter, but they contain invalid content
 */
TEST_F(CollectionsFilterTest, validation2) {
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m(cm.add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    std::vector<std::string> inputs = {
            // wrong UID inputs
            R"({"collections":["8"]})", // one unknown CID
            R"({"collections":["4","22"]})" // one known, one unknown
    };

    for (const auto& s : inputs) {
        boost::optional<cb::const_char_buffer> json(s);
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
    CollectionsManifest cm(NoDefault{});
    Collections::Manifest m(cm.add(CollectionEntry::vegetable)
                                    .add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    boost::optional<cb::const_char_buffer> json;
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
 * Test that we cannot create a filter without a manifest
 */
TEST_F(CollectionsFilterTest, no_manifest) {
    std::string jsonFilter = R"({"collections":["$default", "fruit", "meat"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

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
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m(cm.add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    std::string jsonFilter = R"({"collections":["0", "4", "3"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
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
    // Expect to find collections with CID 4 and 3
    EXPECT_TRUE(list.count(4) > 0);
    EXPECT_TRUE(list.count(3) > 0);
}

/**
 * Construct a valid Collections::Filter and check its public methods
 * This creates a filter which is passthrough
 */
TEST_F(CollectionsFilterTest, filter_basic2) {
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m(cm.add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    std::string jsonFilter; // empty string creates a pass through
    boost::optional<cb::const_char_buffer> json(jsonFilter);
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

/**
 * Construct a valid Collections::Filter as if a legacy DCP producer was created
 */
TEST_F(CollectionsFilterTest, filter_legacy) {
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m(cm.add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    // No string...
    boost::optional<cb::const_char_buffer> json;
    Collections::Filter f(json, &m);

    // Not a pass through
    EXPECT_FALSE(f.isPassthrough());

    // Allows the default
    EXPECT_TRUE(f.allowDefaultCollection());

    // Does not allow system events
    EXPECT_FALSE(f.allowSystemEvents());

    // The actual filter "list" stores nothing
    EXPECT_EQ(0, f.getFilter().size());
}

class CollectionsVBFilterTest : public CollectionsFilterTest {};

/**
 * Try and create filter for collections which exist by name but not with the
 * UID. This represents what could happen if a filtered producer was created
 * successfully, but later when a stream request occurs, the VB's view of
 * collections has shifted.
 */
TEST_F(CollectionsVBFilterTest, uid_mismatch) {
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m1(cm.add(CollectionEntry::dairy));

    CollectionsManifest cm2(CollectionEntry::vegetable2);
    Collections::Manifest m2(cm2.add(CollectionEntry::dairy2));

    // Create the "producer" level filter so that we in theory produce at least
    // these collections
    std::string jsonFilter =
            R"({"collections":["4", "6"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    // At this point the requested collections are valid for m1
    Collections::Filter f(json, &m1);

    Collections::VB::Manifest vbm({});
    // push creates
    vbm.wlock().update(vb, m1);
    // push deletes, removing both filtered collections
    vbm.wlock().update(vb, m2);

    // Construction will now fail as the newly calculated filter doesn't match
    // the collections of vbm
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_TRUE(vbf.empty());
}

/**
 * Try and create filter for collections which exist, but have been deleted
 * i.e. they aren't writable so should never feature in a new VB::Filter
 */
TEST_F(CollectionsVBFilterTest, deleted_collection) {
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m1(cm.add(CollectionEntry::meat)
                                     .add(CollectionEntry::fruit)
                                     .add(CollectionEntry::dairy));
    Collections::Manifest m2(cm.remove(CollectionEntry::vegetable)
                                     .remove(CollectionEntry::fruit));

    // Create the "producer" level filter so that we in theory produce at least
    // these collections
    std::string jsonFilter = R"({"collections":["3", "4"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::Filter f(json, &m1);

    Collections::VB::Manifest vbm({});
    // push creates
    vbm.wlock().update(vb, m1);
    // push deletes, removing both filtered collections
    vbm.wlock().update(vb, m2);

    Collections::VB::Filter vbf(f, vbm);
    EXPECT_TRUE(vbf.empty());
}

/**
 * Create a filter with collections and check we allow what should be allowed.
 */
TEST_F(CollectionsVBFilterTest, basic_allow) {
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m(cm.add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    std::string jsonFilter = R"({"collections":["0", "2", "3"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::Filter f(json, &m);

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    Collections::VB::Filter vbf(f, vbm);

    // Yes to these guys
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"fruit:apple", CollectionEntry::fruit}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"meat:bacon", CollectionEntry::meat}, 0, 0, nullptr, 0}));

    // No to these keys
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"dairy:milk", CollectionEntry::dairy}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"vegetable:cabbage", CollectionEntry::vegetable},
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
    CollectionsManifest cm(CollectionEntry::meat);
    Collections::Manifest m(cm);

    boost::optional<cb::const_char_buffer> json;
    Collections::Filter f(json, &m);

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    Collections::VB::Filter vbf(f, vbm);
    // Legacy would only allow default
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"fruit:apple", CollectionEntry::fruit}, 0, 0, nullptr, 0}));
}

/**
 * Create a passthrough filter and check it allows anything
 */
TEST_F(CollectionsVBFilterTest, passthrough) {
    CollectionsManifest cm(CollectionEntry::meat);
    Collections::Manifest m(cm);

    std::string filterJson; // empty string
    boost::optional<cb::const_char_buffer> json(filterJson);
    Collections::Filter f(json, &m);

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    // Everything is allowed (even junk, which isn't the filter's job to police)
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"fruit:apple", CollectionEntry::fruit}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"meat:steak", CollectionEntry::meat}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"dairy:milk", CollectionEntry::dairy}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"JUNK!!", CollectionEntry::vegetable}, 0, 0, nullptr, 0}));
}

/**
 * Create a filter which blocks the default collection
 */
TEST_F(CollectionsVBFilterTest, no_default) {
    CollectionsManifest cm(CollectionEntry::vegetable);
    Collections::Manifest m(cm.add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["2", "3"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);
    Collections::Filter f(json, &m);

    // Now filter!
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"fruit:apple", CollectionEntry::fruit}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"meat:steak", CollectionEntry::meat}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"dairy:milk", CollectionEntry::dairy}, 0, 0, nullptr, 0}));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"JUNK!!", CollectionEntry::vegetable}, 0, 0, nullptr, 0}));
}

/**
 * Check we can remove collections from the filter (which live DCP may do) and
 * check ::checkAndUpdate works as expected
 */
TEST_F(CollectionsVBFilterTest, remove1) {
    CollectionsManifest cm(NoDefault{});
    Collections::Manifest m(cm.add(CollectionEntry::vegetable)
                                    .add(CollectionEntry::meat)
                                    .add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy));

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["2", "3"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"fruit:apple", CollectionEntry::fruit}, 0, 0, nullptr, 0}));

    // Process a deletion of fruit
    EXPECT_TRUE(
            vbf.checkAndUpdate(*vbm.createSystemEvent(SystemEvent::Collection,
                                                      CollectionEntry::fruit,
                                                      true /*delete*/,
                                                      {})));

    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"fruit:apple", CollectionEntry::fruit}, 0, 0, nullptr, 0}));

    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"meat:steak", CollectionEntry::meat}, 0, 0, nullptr, 0}));

    // Process a deletion of meat
    auto deleteMeat = vbm.createSystemEvent(SystemEvent::Collection,
                                            CollectionEntry::meat,
                                            true /*delete*/,
                                            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*deleteMeat));
    EXPECT_TRUE(vbf.empty()); // now empty
    EXPECT_FALSE(vbf.checkAndUpdate(*deleteMeat)); // no more meat for you
}

/**
 * Check we can remove collections from the filter (which live DCP may do) and
 * check ::allow works as expected
 * This test includes checking we can remove $default
 */
TEST_F(CollectionsVBFilterTest, remove2) {
    CollectionsManifest cm(CollectionEntry::meat);
    Collections::Manifest m(
            cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy));

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    std::string jsonFilter = R"({"collections":["0", "2"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);
    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));
    // Process a deletion of $default
    EXPECT_TRUE(
            vbf.checkAndUpdate(*vbm.createSystemEvent(SystemEvent::Collection,
                                                      CollectionEntry::defaultC,
                                                      true /*delete*/,
                                                      {})));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"anykey", DocNamespace::DefaultCollection}, 0, 0, nullptr, 0}));

    EXPECT_TRUE(vbf.checkAndUpdate(
            {{"meat:steak", CollectionEntry::meat}, 0, 0, nullptr, 0}));
    // Process a deletion of meat
    auto deleteMeat = vbm.createSystemEvent(SystemEvent::Collection,
                                            CollectionEntry::meat,
                                            true /*delete*/,
                                            {});
    EXPECT_TRUE(vbf.checkAndUpdate(*deleteMeat));
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"meat:apple", CollectionEntry::meat}, 0, 0, nullptr, 0}));
    EXPECT_TRUE(vbf.empty()); // now empty
    EXPECT_FALSE(vbf.checkAndUpdate(*deleteMeat)); // no more meat for you
    EXPECT_FALSE(vbf.checkAndUpdate(
            {{"meat:steak", CollectionEntry::meat}, 0, 0, nullptr, 0}));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a passthrough filter so everything is allowed.
 */
TEST_F(CollectionsVBFilterTest, system_events1) {
    CollectionsManifest cm(CollectionEntry::meat);
    Collections::Manifest m(cm.add(CollectionEntry::fruit));

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    std::string jsonFilter;
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);

    // meat system event is allowed by the meat filter
    EXPECT_TRUE(vbf.checkAndUpdate(
            *SystemEventFactory::make(SystemEvent::Collection, "meat", 0, {})));

    // $default system event is allowed by the filter
    EXPECT_TRUE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "$default", 0, {})));

    // dairy system event is allowed even though dairy doesn't exist in the
    // manifest, we wouldn't actually create this event as dairy isn't present
    // but this just shows the passthrough interface at work.
    EXPECT_TRUE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "dairy", 0, {})));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a filter where only matching events are allowed
 */
TEST_F(CollectionsVBFilterTest, system_events2) {
    CollectionsManifest cm(CollectionEntry::meat);
    Collections::Manifest m(
            cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy));

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    // only events for default and meat are allowed
    std::string jsonFilter = R"({"collections":["0", "2"]})";
    boost::optional<cb::const_char_buffer> json(jsonFilter);

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);

    // meat system event is allowed by the meat filter
    EXPECT_TRUE(vbf.checkAndUpdate(*vbm.createSystemEvent(
            SystemEvent::Collection, CollectionEntry::meat, false, {})));

    // $default system event is allowed by the filter
    EXPECT_TRUE(vbf.checkAndUpdate(*vbm.createSystemEvent(
            SystemEvent::Collection, CollectionEntry::defaultC, false, {})));

    // dairy system event is not allowed by the filter
    EXPECT_FALSE(vbf.checkAndUpdate(*vbm.createSystemEvent(
            SystemEvent::Collection, CollectionEntry::dairy, false, {})));
}

/**
 * Test that a filter allows the right system events.
 * This test creates a 'legacy' filter, one which an old DCP client would be
 * attached to - no system events at all are allowed.
 */
TEST_F(CollectionsVBFilterTest, system_events3) {
    CollectionsManifest cm(CollectionEntry::meat);
    Collections::Manifest m(
            cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy));

    Collections::VB::Manifest vbm({});
    vbm.wlock().update(vb, m);

    boost::optional<cb::const_char_buffer> json;

    Collections::Filter f(json, &m);
    Collections::VB::Filter vbf(f, vbm);

    // All system events dropped by this empty/legacy filter
    EXPECT_FALSE(vbf.checkAndUpdate(
            *SystemEventFactory::make(SystemEvent::Collection, "meat", 0, {})));
    EXPECT_FALSE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "$default", 0, {})));
    EXPECT_FALSE(vbf.checkAndUpdate(*SystemEventFactory::make(
            SystemEvent::Collection, "dairy", 0, {})));
}
