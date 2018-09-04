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
#include "tests/module_tests/collections/test_manifest.h"

#include <gtest/gtest.h>

#include <cctype>
#include <limits>

TEST(ManifestTest, validation) {
    std::vector<std::string> invalidManifests = {
            "", // empty
            "not json", // definitely not json
            R"({"uid"})", // illegal json

            // valid uid, no scopes object
            R"({"uid" : "0"})",

            // valid uid, invalid scopes type
            R"({"uid":"0"
                "scopes" : 0})",

            // valid uid, no scopes
            R"({"uid" : "0",
                "scopes" : []})",

            // valid uid, no default scope
            R"({"uid" : "0",
                "scopes":[{"name":"not_the_default", "uid":"2",
                "collections":[]}]})",

            // valid uid, default collection not in default scope
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"brewerA", "uid":"2",
                                "collections":[
                                    {"name":"_default","uid":"0"}]}]})",

            // valid uid, invalid collections type
            R"({"uid" : "0",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[0]}]})",

            // valid uid, valid name, no collection uid
            R"({"uid" : "0",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":"beer"}]}]})",

            // valid uid, valid name, no scope uid
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"1",
                                "collections":[]}]})",

            // valid uid, valid collection uid, no collection name
            R"({"uid":"0",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"uid":"2"}]}]})",

            // valid uid, valid scope uid, no scope name
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"uid":"2",
                                "collections":[]}]})",

            // valid name, invalid collection uid (wrong type)
            R"({"uid":"0",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":"beer", "uid":2}]}]})",

            // valid name, invalid scope uid (wrong type)
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"1", "uid":2,
                                "collections":[]}]})",

            // valid name, invalid collection uid (not hex)
            R"({"uid":"0",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":"beer", "uid":"turkey"}]}]})",

            // valid name, invalid scope uid (not hex)
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"1", "uid":"turkey",
                                "collections":[]}]})",

            // invalid collection name (wrong type), valid uid
            R"({"uid" : "0",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":1, "uid":"2"}]}]})",

            // invalid scope name (wrong type), valid uid
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":1, "uid":"2",
                                "collections":[]}]})",

            // duplicate CID
            R"({"uid" : "0",
                "scopes" : [{"name":"_default", "uid":"0","
                "collections":[{"name":"beer", "uid":"2"},
                               {"name":"lager", "uid":"2"}]}]})",

            // duplicate scope id
            R"({"uid" : "0",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"2","collections":[]},
                    {"name":"brewerB", "uid":"2","collections":[]}]})",

            // duplicate cid across scopes
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"brewery", "uid":"2"},
                          {"name":"brewerA", "uid":"3",
                                "collections":[
                                    {"name":"brewery", "uid":"2"}]}]})",

            // Invalid manifest UIDs
            // Missing UID
            R"({"scopes":[{"name":"_default", "uid":"0"}]})",

            // UID wrong type
            R"({"uid" : 0,
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
                "scopes":[{"name":_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"1"}]}]})",

            // scope uid cannot be 1
            R"({"uid" : "0",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"1","collections":[]}]})",

            // scope uid too long
            R"({"uid" : "0",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"1234567890","collections":[]}]})",

            // collection cid too long
            R"({"uid" : "101",
                "scopes":[{"name":_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"1234567890"}]}]})",

            // scope uid too long
            R"({"uid" : "0",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"1234567890","collections":[]}]})",

            // Invalid collection names, no $ prefix allowed yet and empty
            // also denied
            R"({"uid" : "0",
                "scopes":[{"name":_default", "uid":"0",
                "collections":[{"name":"$beer", "uid":"2"}]}]})",
            R"({"uid" : "0",
                "scopes":[{"name":_default", "uid":"0",
                "collections":[{"name":"", "uid":"2"}]}]})",
            R"({"uid" : "0",
                "scopes":[{"name":_default", "uid":"0",
                "collections":[{"name":"name_is_far_too_long_for_collections",
                "uid":"2"}]}]})",

            // Invalid scope names, no $ prefix allowed yet and empty denies
            R"({"uid" : "0",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"$beer", "uid":"2", "collections":[]}]})",
            R"({"uid" : "0",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"", "uid":"2", "collections":[]}]})",
            R"({"uid" : "0",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"name_is_far_too_long_for_collections", "uid":"2",
                        "collections":[]}]})",
    };

    std::vector<std::string> validManifests = {
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[]}]})",

            R"({"uid" : "0",
                "scopes":[
                    {"name":"_default", "uid":"0", "collections":[]},
                    {"name":"brewerA", "uid":"2", "collections":[]}]})",

            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]}]})",

            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default","uid":"0"},
                                    {"name":"beer", "uid":"3"},
                                    {"name":"brewery","uid":"2"}]},
                          {"name":"brewerA", "uid":"2",
                                "collections":[
                                    {"name":"beer", "uid":"4"},
                                    {"name":"brewery", "uid":"5"}]}]})",

            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"_default","uid":"0"},
                               {"name":"beer", "uid":"2"},
                               {"name":"brewery","uid":"3"}]}]})",

            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]}]})",

            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"beer", "uid":"3"},
                                    {"name":"brewery","uid":"2"}]},
                          {"name":"brewerA", "uid":"2",
                                "collections":[
                                    {"name":"beer", "uid":"4"},
                                    {"name":"brewery", "uid":"5"}]}]})",

            // Extra keys ignored at the moment
            R"({"extra":"key",
                "uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"af"},
                               {"name":"brewery","uid":"2"}]}]})",

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
                "collections":[]}]})"};

    for (auto& manifest : invalidManifests) {
        try {
            Collections::Manifest m(manifest);
            EXPECT_TRUE(false)
                    << "No exception thrown for invalid manifest:" << manifest
                    << std::endl;
        } catch (std::exception&) {
        }
    }

    for (auto& manifest : validManifests) {
        try {
            Collections::Manifest m(manifest);
        } catch (std::exception& e) {
            EXPECT_TRUE(false)
                    << "Exception thrown for valid manifest:" << manifest
                    << std::endl
                    << " what:" << e.what();
        }
    }
}

TEST(ManifestTest, getUid) {
    std::vector<std::pair<Collections::uid_t, std::string>> validManifests = {
            {0,
             R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]}]})"},
            {0xabcd,
             R"({"uid" : "ABCD",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]}]})"},
            {0xabcd,
             R"({"uid" : "abcd",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]}]})"},
            {0xabcd,
             R"({"uid" : "aBcD",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]}]})"}};

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest.second);
        EXPECT_EQ(manifest.first, m.getUid());
    }
}

TEST(ManifestTest, findCollection) {
    std::string manifest =
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"},
                               {"name":"_default","uid":"0"}]}]})";
    std::vector<CollectionID> collectionT = {0, 3, 2};
    std::vector<CollectionID> collectionF = {5, 6, 7};

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
    std::vector<std::pair<std::string, std::vector<CollectionEntry::Entry>>>
            input = {
                    {"abcd", {}},
                    {"abcd",
                     {{CollectionEntry::defaultC},
                      {CollectionEntry::fruit},
                      {CollectionEntry::vegetable}}},
                    {"abcd",
                     {{CollectionEntry::fruit}, {CollectionEntry::vegetable}}},

            };

    for (auto& manifest : input) {
        CollectionsManifest cm(NoDefault{});
        for (auto& collection : manifest.second) {
            cm.add(collection);
        }
        cm.setUid(manifest.first);

        Collections::Manifest m(cm);
        nlohmann::json json;
        try {
            json = nlohmann::json::parse(m.toJson());
        } catch (nlohmann::json::exception& e) {
            // Throw test failure instead of the exception so that we can
            // print the failing JSON
            std::stringstream ss;
            ss << "nlohmann could not parse the manifest: \"";
            ss << m.toJson();
            ss << "\" e.what(): ";
            ss << e.what();
            ASSERT_TRUE(false) << ss.str();
        }
        ASSERT_NE(json.end(), json.find("uid"));
        EXPECT_TRUE(json["uid"].is_string());
        EXPECT_EQ(manifest.first, json["uid"].get<std::string>());

        auto scopes = json.find("scopes");

        if (scopes != json.end()) {
            auto defaultScope = scopes->find("_default");
            if (defaultScope != scopes->end()) {
                auto collections = defaultScope->find("collections");
                if (collections != defaultScope->end()) {
                    for (auto& entry : *collections) {
                        EXPECT_NE(
                                manifest.second.end(),
                                std::find_if(
                                        manifest.second.begin(),
                                        manifest.second.end(),
                                        [entry](const CollectionEntry::Entry&
                                                        e) {
                                            if (e.name == entry["name"]) {
                                                return std::to_string(e.uid) ==
                                                       entry["uid"]
                                                               .get<std::string>();
                                            }
                                            return false;
                                        }));
                    }
                } else {
                    EXPECT_EQ(0, manifest.second.size());
                }
            }
        }
    }
}
#endif // !defined(__clang_major__) || __clang_major__ > 7

TEST(ManifestTest, badNames) {
    for (char c = 127; c >= 0; c--) {
        std::string name(1, c);
        CollectionsManifest cm({name, 2});

        if (!(std::isdigit(c) || std::isalpha(c) || c == '.' || c == '_' ||
              c == '-' || c == '%')) {
            try {
                Collections::Manifest m(cm);
                EXPECT_TRUE(false)
                        << "No exception thrown for invalid manifest:" << m
                        << std::endl;
            } catch (std::exception&) {
            }
        } else {
            try {
                Collections::Manifest m(cm);
            } catch (std::exception& e) {
                EXPECT_TRUE(false) << "Exception thrown for valid manifest"
                                   << std::endl
                                   << " what:" << e.what();
            }
        }
    }
}

TEST(ManifestTest, tooManyCollections) {
    std::vector<std::string> invalidManifests = {
            // Too many collections in the default scope
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]}]})",

            // Too many collections in a non-default scope
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[]},
                          {"name":"brewerA", "uid":"2",
                                "collections":[
                                    {"name":"beer", "uid":"4"},
                                    {"name":"brewery", "uid":"5"}]}]})",

            // Too many collections across all scopes
            R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"beer", "uid":"3"}]},
                          {"name":"brewerA", "uid":"2",
                                "collections":[
                                    {"name":"beer", "uid":"4"}]}]})",
    };

    for (auto& manifest : invalidManifests) {
        EXPECT_THROW(Collections::Manifest cm(manifest, 1),
                     std::invalid_argument)
                << "No exception thrown for manifest "
                   "with too many collections. "
                   "Manifest: "
                << manifest << std::endl;
    }
}

TEST(ManifestTest, findCollectionByName) {
    std::string manifest = R"({"uid" : "0",
                "scopes":[{"name":"_default", "uid":"0",
                                "collections":[
                                    {"name":"_default", "uid":"0"},
                                    {"name":"meat", "uid":"3"}]},
                          {"name":"brewerA", "uid":"2",
                                "collections":[
                                    {"name":"beer", "uid":"4"}]}]})";
    Collections::Manifest cm(manifest);

    // We expect to find the collections in the default scope, when we do not
    // specify the scope
    // Test that the uid matches the collection that we are searching for
    EXPECT_EQ(cm.findCollection("_default")->first, 0);
    EXPECT_EQ(cm.findCollection("meat")->first, 3);

    // We do not expect to find collections not in the default scope, when we
    // do not specify the scope
    EXPECT_EQ(cm.findCollection("beer"), cm.end());

    // We expect to find collections when searching by collection and scope name
    // Test that the uid matches the collection that we are searching for
    EXPECT_EQ(cm.findCollection("_default", "_default")->first, 0);
    EXPECT_EQ(cm.findCollection("meat", "_default")->first, 3);
    EXPECT_EQ(cm.findCollection("beer", "brewerA")->first, 4);

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
