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

#include "collections/manifest.h"
#include "collections_test_helpers.h"
#include <utilities/test_manifest.h>

#include <flatbuffers/flatbuffers.h>
#include <folly/portability/GTest.h>
#include <memcached/engine_error.h>

#include <cctype>
#include <limits>
#include <unordered_set>

TEST(ManifestTest, defaultState) {
    Collections::Manifest m;
    EXPECT_TRUE(m.doesDefaultCollectionExist());
    EXPECT_EQ(1, m.getCollectionCount());
    EXPECT_EQ(0, m.getUid());
    auto collection = m.findCollection(CollectionID::Default);
    EXPECT_NE(collection, m.end());
    EXPECT_EQ(ScopeID(ScopeID::Default), collection->second.sid);
    EXPECT_EQ("_default", collection->second.name);
    EXPECT_FALSE(collection->second.maxTtl.has_value());

    collection = m.findCollection("_default", "_default");
    EXPECT_NE(collection, m.end());
    EXPECT_EQ(ScopeID(ScopeID::Default), collection->second.sid);
    EXPECT_EQ("_default", collection->second.name);

    auto scope = m.findScope(ScopeID::Default);
    EXPECT_NE(scope, m.endScopes());
    EXPECT_EQ("_default", scope->second.name);
    EXPECT_EQ(1, scope->second.collections.size());
    EXPECT_EQ(CollectionID::Default, scope->second.collections[0].cid);

    auto oScope = m.getScopeID("_default._default");
    EXPECT_EQ(ScopeID(ScopeID::Default), oScope.value_or(~0));
    auto oCollection = m.getCollectionID(oScope.value(), "_default._default");
    EXPECT_EQ(CollectionID::Default, oCollection.value_or(~0));
}

TEST(ManifestTest, validation) {
    std::vector<std::string> invalidManifests = {
            "", // empty
            "not json", // definitely not json
            R"({"uid"})", // illegal json

            // valid uid, no scopes object
            R"({"uid" : "1"})",

            // valid uid, invalid scopes type
            R"({"uid":"1"
                "scopes" : 0})",

            // valid uid, no scopes
            R"({"uid" : "1",
                "scopes" : []})",

            // valid uid, no default scope
            R"({"uid" : "1",
                "scopes":[{"name":"not_the_default", "uid":"8",
                "collections":[]}]})",

            // default collection not in default scope
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"_default","uid":"0"}]}]})",

            // valid uid, invalid collections type
            R"({"uid" : "1",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[0]}]})",

            // valid uid, valid name, no collection uid
            R"({"uid" : "1",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":"beer"}]}]})",

            // valid uid, valid name, no scope uid
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"scope1",
                                "collections":[]}]})",

            // valid uid, valid collection uid, no collection name
            R"({"uid": "1",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"uid":"8"}]}]})",

            // valid uid, valid scope uid, no scope name
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"uid":"8",
                                "collections":[]}]})",

            // valid name, invalid collection uid (wrong type)
            R"({"uid": "1",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":"beer", "uid":8}]}]})",

            // valid name, invalid scope uid (wrong type)
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"1", "uid":8,
                                "collections":[]}]})",

            // valid name, invalid collection uid (not hex)
            R"({"uid":"0",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":"beer", "uid":"turkey"}]}]})",

            // valid name, invalid scope uid (not hex)
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"1", "uid":"turkey",
                                "collections":[]}]})",

            // invalid collection name (wrong type), valid uid
            R"({"uid" : "1",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":1, "uid":"8"}]}]})",

            // invalid scope name (wrong type), valid uid
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":1, "uid":"8",
                                "collections":[]}]})",

            // duplicate CID
            R"({"uid" : "1",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":"beer", "uid":"8"},
                               {"name":"lager", "uid":"8"}]}]})",

            // duplicate scope id
            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"8","collections":[]},
                    {"name":"brewerB", "uid":"8","collections":[]}]})",

            // duplicate cid across scopes
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"brewery", "uid":"8"},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"brewery", "uid":"8"}]}]})",

            // Invalid manifest UIDs
            // Missing UID
            R"({"scopes":[{"name":"_default", "uid":"0"}]})",

            // UID wrong type
            R"({"uid" : 1,
                "scopes":[{"name":"_default", "uid":"0"}]})",

            // UID cannot be converted to a value
            R"({"uid" : "thisiswrong",
                "scopes":[{"name":"_default", "uid":"0"}]})",

            // UID cannot be converted to a value
            R"({"uid" : "12345678901234567890112111",
                "scopes":[{"name":"_default", "uid":"0}]})",

            // UID cannot be 0x prefixed
            R"({"uid" : "0x101",
                "scopes":[{"name":"_default", "uid":"0"}]})",

            // collection cid cannot be 1
            R"({"uid" : "101",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"1"}]}]})",

            // collection cid cannot be 7 (1-7 reserved)
            R"({"uid" : "101",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"7"}]}]})",

            // scope uid cannot be 1
            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"1","collections":[]}]})",

            // scope uid cannot be 7 (1-7 reserved)
            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"7","collections":[]}]})",

            // scope uid too long
            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"1234567890","collections":[]}]})",

            // collection cid too long
            R"({"uid" : "101",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"1234567890"}]}]})",

            // scope uid too long
            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"1234567890","collections":[]}]})",

            // Invalid collection names, no $ prefix allowed yet and empty
            // also denied
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"$beer", "uid":"8"}]}]})",
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"", "uid":"8"}]}]})",
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"name_is_far_too_long_for_collections________________________________________________________________________________________________________________________________________________________________________________________________________________________",
                "uid":"8"}]}]})",
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"collection.name",
                "uid":"8"}]}]})",

            // Invalid scope names, no $ prefix allowed yet and empty denies
            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"$beer", "uid":"8", "collections":[]}]})",
            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"", "uid":"8", "collections":[]}]})",
            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"name_is_far_too_long_for_collections________________________________________________________________________________________________________________________________________________________________________________________________________________________",
                        "uid":"8",
                        "collections":[]}]})",
            R"({"uid" : "1",
                "scopes":[
                    {"name":"scope.name", "uid":"8", "collections":[]}]})",

            // maxTTL invalid cases
            // wrong type
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"brewery","uid":"9","maxTTL":"string"}]}]})",
            // negative (doesn't make sense)
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"brewery","uid":"9","maxTTL":-700}]}]})",
            // too big for 32-bit
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"brewery","uid":"9","maxTTL":4294967296}]}]})",
            // Test duplicate scope names
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default","uid":"0"},
                                    {"name":"beer", "uid":"8"},
                                    {"name":"brewery","uid":"9"}]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"beer", "uid":"a"},
                                    {"name":"brewery", "uid":"b"}]},
                          {"name":"brewerA", "uid":"9",
                                "collections":[
                                    {"name":"beer", "uid":"c"},
                                    {"name":"brewery", "uid":"d"}]}]})",
            // Test duplicate collection names within the same scope
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default","uid":"0"},
                                    {"name":"brewery", "uid":"8"},
                                    {"name":"brewery","uid":"9"}]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"beer", "uid":"a"},
                                    {"name":"beer", "uid":"b"}]}]})",
            // Illegal name for default collection
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"Default","uid":"0"}]}]})",
            // Illegal name for default scope
            R"({"uid" : "1",
                "scopes":[{"name":"Default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"}]}]})",
            // Illegal name for default collection
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"Default","uid":"0"}]}]})",
    };

    std::vector<std::string> validManifests = {
            // this is the 'epoch' state of collections
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"}]}]})",

            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[]}]})",

            R"({"uid" : "1",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"8", "collections":[]}]})",

            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"beer", "uid":"8"},
                               {"name":"brewery","uid":"9"}]}]})",

            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default","uid":"0"},
                                    {"name":"beer", "uid":"8"},
                                    {"name":"brewery","uid":"9"}]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"beer", "uid":"a"},
                                    {"name":"brewery", "uid":"b"}]}]})",

            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"beer", "uid":"8"},
                               {"name":"brewery","uid":"9"}]}]})",

            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"8"},
                               {"name":"brewery","uid":"9"}]}]})",

            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"beer", "uid":"8"},
                                    {"name":"brewery","uid":"9"}]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"beer", "uid":"a"},
                                    {"name":"brewery", "uid":"b"}]}]})",

            // Extra keys ignored at the moment
            R"({"extra":"key",
                "uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"af"},
                               {"name":"brewery","uid":"8"}]}]})",

            // lower-case uid is fine
            R"({"uid" : "abcd1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[]}]})",
            // upper-case uid is fine
            R"({"uid" : "ABCD1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[]}]})",
            // mix-case uid is fine
            R"({"uid" : "AbCd1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[]}]})",

            // maxTTL valid cases
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"brewery","uid":"9","maxTTL":0}]}]})",
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"brewery","uid":"9","maxTTL":1}]}]})",
            // max u32int
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"brewery","uid":"9","maxTTL":4294967295}]}]})",

            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"brewery","uid":"8"}]}]})",
    };

    for (auto& manifest : invalidManifests) {
        try {
            Collections::Manifest m(manifest);
            FAIL() << "No exception thrown for invalid manifest:" << manifest
                   << std::endl;
        } catch (std::invalid_argument&) {
        }
    }

    for (auto& manifest : validManifests) {
        try {
            Collections::Manifest m(manifest);

        } catch (std::exception& e) {
            FAIL() << "Exception thrown for valid manifest:" << manifest
                   << std::endl
                   << " what:" << e.what();
        }
    }

    // Following logic requires even number of valid manifests
    ASSERT_EQ(0, validManifests.size() % 2);

    // Make use of the valid manifests to give some coverage on the compare
    // operator.
    auto itr = validManifests.rbegin();
    for (auto& manifest : validManifests) {
        try {
            Collections::Manifest m1(manifest);
            Collections::Manifest m2(manifest);

            auto fb = m1.toFlatbuffer();
            std::string_view view(reinterpret_cast<const char*>(fb.data()),
                                  fb.size());
            Collections::Manifest m3(view,
                                     Collections::Manifest::FlatBuffers{});

            Collections::Manifest m4(*itr);
            EXPECT_EQ(m1, m2);
            EXPECT_EQ(m1, m3);

            EXPECT_NE(m1, m4);
            EXPECT_NE(m3, m4);
        } catch (std::exception& e) {
            EXPECT_TRUE(false)
                    << "Exception thrown for valid manifest:" << manifest
                    << std::endl
                    << " what:" << e.what();
        }
        itr++;
    }
}

TEST(ManifestTest, getUid) {
    std::vector<std::pair<Collections::ManifestUid, std::string>>
            validManifests = {{Collections::ManifestUid(1),
                               R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"8"},
                               {"name":"brewery","uid":"9"}]}]})"},
                              {Collections::ManifestUid(0xabcd),
                               R"({"uid" : "ABCD",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"8"},
                               {"name":"brewery","uid":"9"}]}]})"},
                              {Collections::ManifestUid(0xabcd),
                               R"({"uid" : "abcd",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"8"},
                               {"name":"brewery","uid":"9"}]}]})"},
                              {Collections::ManifestUid(0xabcd),
                               R"({"uid" : "aBcD",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"8"},
                               {"name":"brewery","uid":"9"}]}]})"}};

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest.second);
        EXPECT_EQ(manifest.first, m.getUid());
    }
}

TEST(ManifestTest, findCollection) {
    std::string manifest =
            R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"8"},
                               {"name":"brewery","uid":"9"},
                               {"name":"_default","uid":"0"}]}]})";
    std::vector<CollectionID> collectionT = {0, 8, 9};
    std::vector<CollectionID> collectionF = {0xa, 0xb, 0xc};

    Collections::Manifest m(manifest);

    for (auto& collection : collectionT) {
        EXPECT_NE(m.end(), m.findCollection(collection));
    }

    for (auto& collection : collectionF) {
        EXPECT_EQ(m.end(), m.findCollection(collection));
    }
}

// MB-30547: Initialization of `input` below fails on Clang 7 - temporarily
// skip to fix build breakage.
#if !defined(__clang_major__) || __clang_major__ > 7
// validate we can construct from JSON, call toJSON and get back valid JSON
// containing what went in.
TEST(ManifestTest, toJson) {
    struct TestInput {
        CollectionEntry::Entry collection;
        ScopeEntry::Entry scope;
        cb::ExpiryLimit maxTtl;
    };
    std::vector<std::pair<std::string, std::vector<TestInput>>> input = {
            {"abc0", {}},
            {"abc1",
             {{CollectionEntry::defaultC,
               ScopeEntry::defaultS,
               cb::ExpiryLimit{}},
              {CollectionEntry::fruit, ScopeEntry::defaultS, cb::ExpiryLimit{}},
              {CollectionEntry::vegetable,
               ScopeEntry::defaultS,
               cb::ExpiryLimit{}}}},
            {"abc2",
             {{CollectionEntry::fruit, ScopeEntry::defaultS, cb::ExpiryLimit{}},
              {CollectionEntry::vegetable,
               ScopeEntry::defaultS,
               cb::ExpiryLimit{}}}},
            {"abc3",
             {{CollectionEntry::fruit, ScopeEntry::shop1, cb::ExpiryLimit{}},
              {CollectionEntry::vegetable,
               ScopeEntry::defaultS,
               cb::ExpiryLimit{}}}},
            {"abc4",
             {{CollectionEntry::dairy, ScopeEntry::shop1, cb::ExpiryLimit{}},
              {CollectionEntry::dairy2, ScopeEntry::shop2, cb::ExpiryLimit{}}}},
            {"abc5",
             {{{CollectionEntry::dairy,
                ScopeEntry::shop1,
                std::chrono::seconds(100)},
               {CollectionEntry::dairy2,
                ScopeEntry::shop2,
                std::chrono::seconds(0)}}}}};

    Collections::IsVisibleFunction isVisible =
            [](ScopeID, std::optional<CollectionID>) -> bool { return true; };

    for (auto& manifest : input) {
        CollectionsManifest cm(NoDefault{});
        std::unordered_set<ScopeID> scopesAdded;
        scopesAdded.insert(ScopeID::Default); // always the default scope
        for (auto& collection : manifest.second) {
            if (scopesAdded.count(collection.scope.uid) == 0) {
                cm.add(collection.scope);
                scopesAdded.insert(collection.scope.uid);
            }
            cm.add(collection.collection, collection.maxTtl, collection.scope);
        }
        cm.setUid(manifest.first);

        Collections::Manifest m = makeManifest(cm);

        nlohmann::json input;
        auto output = m.toJson(isVisible);
        std::string s(cm);
        try {
            input = nlohmann::json::parse(s);
        } catch (const nlohmann::json::exception& e) {
            FAIL() << "Cannot nlohmann::json::parse input " << s << " "
                   << e.what();
        }

        EXPECT_EQ(input.size(), output.size());
        EXPECT_EQ(input["uid"].dump(), output["uid"].dump());
        EXPECT_EQ(input["scopes"].size(), output["scopes"].size());
        for (const auto& scope1 : output["scopes"]) {
            auto scope2 = std::find_if(
                    input["scopes"].begin(),
                    input["scopes"].end(),
                    [scope1](const nlohmann::json& scopeEntry) {
                        return scopeEntry["name"] == scope1["name"] &&
                               scopeEntry["uid"] == scope1["uid"];
                    });
            ASSERT_NE(scope2, input["scopes"].end());
            // If we are here we know scope1 and scope2 have name, uid fields
            // and they match, check the overall size, should be 3 fields
            // name, uid and collections
            EXPECT_EQ(3, scope1.size());
            EXPECT_EQ(scope1.size(), scope2->size());

            for (const auto& collection1 : scope1["collections"]) {
                // Find the collection from scope in the output
                auto collection2 = std::find_if(
                        (*scope2)["collections"].begin(),
                        (*scope2)["collections"].end(),
                        [collection1](const nlohmann::json& collectionEntry) {
                            return collectionEntry["name"] ==
                                           collection1["name"] &&
                                   collectionEntry["uid"] == collection1["uid"];
                        });
                ASSERT_NE(collection2, (*scope2)["collections"].end());
                // If we are here we know collection1 and collection2 have
                // matching name and uid fields, check the other fields.
                // maxTTL is optional

                EXPECT_EQ(collection1.size(), collection2->size());

                auto ttl1 = collection1.find("maxTTL");
                if (ttl1 != collection1.end()) {
                    ASSERT_EQ(3, collection1.size());
                    auto ttl2 = collection2->find("maxTTL");
                    ASSERT_NE(ttl2, collection2->end());
                    EXPECT_EQ(*ttl1, *ttl2);
                } else {
                    EXPECT_EQ(2, collection1.size());
                }
            }
        }
    }
}
#endif // !defined(__clang_major__) || __clang_major__ > 7

TEST(ManifestTest, badNames) {
    for (char c = 127; c >= 0; c--) {
        std::string name(1, c);
        CollectionsManifest cm({name, 8});

        if (!(std::isdigit(c) || std::isalpha(c) || c == '_' || c == '-' ||
              c == '%')) {
            try {
                auto m = makeManifest(cm);
                EXPECT_TRUE(false)
                        << "No exception thrown for invalid manifest:" << m
                        << std::endl;
            } catch (std::exception&) {
            }
        } else {
            try {
                auto m = makeManifest(cm);
            } catch (std::exception& e) {
                EXPECT_TRUE(false) << "Exception thrown for valid manifest"
                                   << std::endl
                                   << " what:" << e.what();
            }
        }
    }
}

TEST(ManifestTest, findCollectionByName) {
    std::string manifest = R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default", "uid":"0"},
                                    {"name":"meat", "uid":"8"}]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"beer", "uid":"9"}]}]})";
    Collections::Manifest cm(manifest);

    // We expect to find the collections in the default scope, when we do not
    // specify the scope
    // Test that the uid matches the collection that we are searching for
    EXPECT_EQ(cm.findCollection("_default")->first, 0);
    EXPECT_EQ(cm.findCollection("meat")->first, 8);

    // We do not expect to find collections not in the default scope, when we
    // do not specify the scope
    EXPECT_EQ(cm.findCollection("beer"), cm.end());

    // We expect to find collections when searching by collection and scope name
    // Test that the uid matches the collection that we are searching for
    EXPECT_EQ(cm.findCollection("_default", "_default")->first, 0);
    EXPECT_EQ(cm.findCollection("meat", "_default")->first, 8);
    EXPECT_EQ(cm.findCollection("beer", "brewerA")->first, 9);

    // We do not expect to find collections with incorrect scope that does exist
    EXPECT_EQ(cm.findCollection("_default", "brewerA"), cm.end());
    EXPECT_EQ(cm.findCollection("meat", "brewerA"), cm.end());
    EXPECT_EQ(cm.findCollection("beer", "_default"), cm.end());

    // We do not expect to find collections when we give a scope that does
    // not exist
    EXPECT_EQ(cm.findCollection("_default", "a_scope_name"), cm.end());
    EXPECT_EQ(cm.findCollection("meat", "a_scope_name"), cm.end());
    EXPECT_EQ(cm.findCollection("beer", "a_scope_name"), cm.end());

    // We do not expect to find collections that do not exist in a scope that
    // does
    EXPECT_EQ(cm.findCollection("fruit", "_default"), cm.end());
    EXPECT_EQ(cm.findCollection("fruit", "brewerA"), cm.end());

    // We do not expect to find collections that do not exist in scopes that
    // do not exist
    EXPECT_EQ(cm.findCollection("fruit", "a_scope_name"), cm.end());
}

TEST(ManifestTest, getScopeID) {
    std::string manifest = R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default", "uid":"0"},
                                    {"name":"meat", "uid":"8"}]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"beer", "uid":"9"},
                                    {"name":"meat", "uid":"a"}]}]})";
    Collections::Manifest cm(manifest);

    // getScopeID is called from the context of a get_collection_id or
    // a get_scope_id command, thus the path it accepts can either be
    //    * "scope.collection"
    //    * "scope"
    // the function ignores anything after a .
    // The Manifest::getScopeID expects a valid path, it peforms no validation
    EXPECT_EQ(ScopeID(ScopeID::Default), cm.getScopeID("").value());
    EXPECT_EQ(ScopeID(ScopeID::Default), cm.getScopeID(".").value());
    EXPECT_EQ(ScopeID(ScopeID::Default), cm.getScopeID("_default").value());
    EXPECT_EQ(ScopeID(ScopeID::Default), cm.getScopeID(".ignorethis").value());
    EXPECT_EQ(ScopeID(ScopeID::Default),
              cm.getScopeID("_default.ignorethis").value());
    EXPECT_EQ(ScopeID(8), cm.getScopeID("brewerA").value());
    EXPECT_EQ(ScopeID(8), cm.getScopeID("brewerA.ignorethis").value());

    // valid input to unknown scopes
    EXPECT_FALSE(cm.getScopeID("unknown.ignored"));
    EXPECT_FALSE(cm.getScopeID("unknown"));

    // validation of the path is not done by getScopeID, the following just
    // results in an invalid lookup of 'junk' but no throwing because of the
    // "this.that.what"
    EXPECT_FALSE(cm.getScopeID("junk.this.that.what"));

    // However if the scope name is junk we throw for that
    try {
        cm.getScopeID("j*nk.this.that.what");
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments,
                  cb::engine_errc(e.code().value()));
    }

    // valid path but invalid names
    try {
        cm.getScopeID("invalid***");
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments,
                  cb::engine_errc(e.code().value()));
    }
    try {
        cm.getScopeID("invalid***.ignored");
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments,
                  cb::engine_errc(e.code().value()));
    }
}

TEST(ManifestTest, getCollectionID) {
    std::string manifest = R"({"uid" : "1",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default", "uid":"0"},
                                    {"name":"meat", "uid":"8"}]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"beer", "uid":"9"},
                                    {"name":"meat", "uid":"a"}]}]})";
    Collections::Manifest cm(manifest);

    // Note: usage of getCollectionID assumes getScopeID was called first
    // Note: getCollectionID does not perform validation of the path
    EXPECT_EQ(CollectionID::Default,
              cm.getCollectionID(ScopeID::Default, ".").value());
    EXPECT_EQ(CollectionID::Default,
              cm.getCollectionID(ScopeID::Default, "_default.").value());
    EXPECT_EQ(8, cm.getCollectionID(ScopeID::Default, ".meat").value());
    EXPECT_EQ(8, cm.getCollectionID(ScopeID::Default, "_default.meat").value());
    EXPECT_EQ(9, cm.getCollectionID(ScopeID(8), "brewerA.beer").value());
    EXPECT_EQ(0xa, cm.getCollectionID(ScopeID(8), "brewerA.meat").value());

    EXPECT_FALSE(cm.isForcedUpdate());

    // getCollectionID doesn't care about the scope part, correct usage
    // is always to call getScopeID(path) first which is where scope name errors
    // are caught
    EXPECT_EQ(0xa, cm.getCollectionID(ScopeID(8), "ignored.meat").value());

    // validation of the path formation is not done by getCollectionID the
    // following does end up throwing though because it assumes the collection
    // to lookup is this.that.what, and . is an illegal character
    try {
        cm.getScopeID("junk.this.that.what");
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments,
                  cb::engine_errc(e.code().value()));
    }

    // valid path but invalid collection component
    try {
        cm.getCollectionID(ScopeID::Default, "_default.collection&");
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments,
                  cb::engine_errc(e.code().value()));
    }

    // unknown scope as first param is invalid
    EXPECT_THROW(cm.getCollectionID(ScopeID{22}, "_default.meat"),
                 std::invalid_argument);

    // illegal scope name isn't checked here, getScopeID is expected to have
    // been called first and the given ScopeID is what is used in the lookup
    EXPECT_EQ(0xa, cm.getCollectionID(ScopeID(8), "*#illegal.meat").value());

    // Unknown names
    EXPECT_FALSE(cm.getCollectionID(ScopeID::Default, "unknown.collection"));

    // Unknown scope
    EXPECT_FALSE(cm.getCollectionID(ScopeID::Default, "unknown.beer"));

    // Unknown collection
    EXPECT_FALSE(cm.getCollectionID(ScopeID::Default, "brewerA.ale"));
}

TEST(ManifestTest, isSuccesor) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::dairy, ScopeEntry::shop1);

    Collections::Manifest a;
    Collections::Manifest b1;
    Collections::Manifest b2{std::string{cm}};
    Collections::Manifest c{std::string{cm}};

    // a and b1 are the same and isSuccessor allows
    EXPECT_EQ(cb::engine_errc::success, a.isSuccessor(b1).code());

    // b2 is a successor in valid ways
    EXPECT_EQ(cb::engine_errc::success, a.isSuccessor(b2).code());

    // b2 and c are the same (but not default constructed)
    EXPECT_EQ(cb::engine_errc::success, b2.isSuccessor(c).code());
}

TEST(ManifestTest, isNotSuccesor) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::dairy, ScopeEntry::shop1);
    Collections::Manifest current{std::string{cm}};

    {
        // switch the name of the scope and test
        CollectionsManifest cm2 = cm;
        cm2.rename(ScopeEntry::shop1, "some_shop");
        Collections::Manifest incoming{std::string{cm2}};
        EXPECT_NE(cb::engine_errc::success,
                  current.isSuccessor(incoming).code());
    }
    {
        // switch the name of a collection in _default scope and test
        CollectionsManifest cm2 = cm;
        cm2.rename(CollectionEntry::meat, ScopeEntry::defaultS, "MEAT");
        Collections::Manifest incoming{std::string{cm2}};
        EXPECT_NE(cb::engine_errc::success,
                  current.isSuccessor(incoming).code());
    }
    {
        // switch the name of a collection in a custom scope and test
        CollectionsManifest cm2 = cm;
        cm2.rename(CollectionEntry::dairy, ScopeEntry::shop1, "DAIRY");
        Collections::Manifest incoming{std::string{cm2}};
        EXPECT_NE(cb::engine_errc::success,
                  current.isSuccessor(incoming).code());
    }
    {
        // lower the uid
        CollectionsManifest cm2 = cm;
        Collections::Manifest incoming1{std::string{cm2}};
        // prove that the uid shift results in error
        EXPECT_EQ(cb::engine_errc::success,
                  current.isSuccessor(incoming1).code());
        cm2.updateUid(cm2.getUid() - 1);
        Collections::Manifest incoming2{std::string{cm2}};
        EXPECT_NE(cb::engine_errc::success,
                  current.isSuccessor(incoming2).code());
    }

    {
        // increase the uid
        CollectionsManifest cm2 = cm;
        Collections::Manifest incoming1{std::string{cm2}};
        // prove that the uid shift results in error
        EXPECT_EQ(cb::engine_errc::success,
                  current.isSuccessor(incoming1).code());
        cm2.updateUid(cm2.getUid() + 1);
        Collections::Manifest incoming2{std::string{cm2}};
        EXPECT_NE(cb::engine_errc::success,
                  current.isSuccessor(incoming2).code());
    }

    // Move a collection to a different scope
    cm.remove(CollectionEntry::meat);
    cm.add(CollectionEntry::meat, ScopeEntry::shop1);
    Collections::Manifest incoming3{std::string{cm}};
    EXPECT_NE(cb::engine_errc::success, current.isSuccessor(incoming3).code());
}

TEST(ManifestTest, forcedUpdate) {
    std::string manifest = R"({"uid" : "1",
                "force" : true,
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default", "uid":"0"},
                                    {"name":"meat", "uid":"8"}]},
                          {"name":"brewerA", "uid":"8",
                                "collections":[
                                    {"name":"beer", "uid":"9"},
                                    {"name":"meat", "uid":"a"}]}]})";
    Collections::Manifest cm(manifest);
    EXPECT_TRUE(cm.isForcedUpdate());
}
