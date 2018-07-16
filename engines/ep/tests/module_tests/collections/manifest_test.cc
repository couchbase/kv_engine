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

#include <limits>

TEST(ManifestTest, validation) {
    std::vector<std::string> invalidManifests = {
            "", // empty
            "not json", // definitely not json
            R"({"uid"})", // illegal json

            R"({"uid":"0"
                "collections" : 0})", // illegal collections type

            // valid uid, no collections
            R"({"uid" : "0"})",

            // valid uid, invalid collections type
            R"({"uid" : "0",
                "collections":[0]})",

            // valid uid valid name, no collection uid
            R"({"uid" : "0",
                "collections":[{"name":"beer"}]})",

            // valid uid, valid collection uid, no collection name
            R"({"uid":"0",
                "collections":[{"uid":"1"}]})",

            // valid name, invalid collection uid (wrong type)
            R"({"uid":"0",
                "collections":[{"name":"beer", "uid":1}]})",

            // valid name, invalid collection uid (not hex)
            R"({"uid":"0",
                "collections":[{"name":"beer", "uid":"turkey"}]})",

            // invalid name (wrong type), valid uid
            R"({"uid" : "0",
                "collections":[{"name":1, "uid":"1"}]})",

            // illegal $ prefixed  name
            R"({"uid" : "0",
             "collections":[{"name":"$beer", "uid":"1"},
                            {"name":"brewery","uid":"2"}]})",

            // illegal _ prefixed  name
            R"({"uid" : "0",
               "collections":[{"name":"_beer", "uid":"1"},
                              {"name":"brewery","uid":"2"}]})",

            // duplicate collections
            R"({"uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"beer", "uid":"2"}]})",

            // Invalid manifest UIDs
            // Missing UID
            R"({"collections":[{"name":"beer", "uid":"1"}]})",

            // UID wrong type
            R"({"uid" : 0,
                "collections":[{"name":"beer", "uid":"1"}]})",

            // UID cannot be converted to a value
            R"({"uid" : "thisiswrong",
                "collections":[{"name":"beer", "uid":"1"}]})",

            // UID cannot be converted to a value
            R"({"uid" : "12345678901234567890112111",
                "collections":[{"name":"beer", "uid":"1"}]})",

            // UID cannot be 0x prefixed
            R"({"uid" : "0x101",
                "collections":[{"name":"beer", "uid":"1"}]})",

            // collection cid cannot be 1
            R"({"uid" : "101",
                "collections":[{"name":"beer", "uid":"1"}]})",

            // collection cid too long
            R"({"uid" : "101",
                "collections":[{"name":"beer", "uid":"1234567890"}]})"};

    std::vector<std::string> validManifests = {
            R"({"uid" : "0", "collections":[]})",

            R"({"uid" : "0",
                "collections":[{"name":"$default","uid":"0"},
                               {"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]})",

            // beer & brewery have same UID, valid
            R"({"uid" : "0",
                "collections":[{"name":"$default","uid":"0"},
                               {"name":"beer", "uid":"2"},
                               {"name":"brewery","uid":"3"}]})",

            R"({"uid" : "0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]})",

            // Extra keys ignored at the moment
            R"({"extra":"key",
                 "uid" : "0",
                "collections":[{"name":"beer", "uid":"af"},
                               {"name":"brewery","uid":"2"}]})",

            // lower-case uid is fine
            R"({"uid" : "abcd1", "collections":[]})",
            // upper-case uid is fine
            R"({"uid" : "ABCD1", "collections":[]})",
            // mix-case uid is fine
            R"({"uid" : "AbCd1", "collections":[]})"};

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
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]})"},
            {0xabcd,
             R"({"uid" : "ABCD",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]})"},
            {0xabcd,
             R"({"uid" : "abcd",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]})"},
            {0xabcd,
             R"({"uid" : "aBcD",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"}]})"}};

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest.second);
        EXPECT_EQ(manifest.first, m.getUid());
    }
}

TEST(ManifestTest, findCollection) {
    std::string manifest =
            R"({"uid" : "0",
                "collections":[{"name":"beer", "uid":"3"},
                               {"name":"brewery","uid":"2"},
                               {"name":"$default","uid":"0"}]})";
    std::vector<CollectionID> collectionT = {0, 3, 2};
    std::vector<CollectionID> collectionF = {5, 6, 7};

    Collections::Manifest m(manifest);

    for (auto& collection : collectionT) {
        EXPECT_NE(m.end(), m.find(collection));
    }

    for (auto& collection : collectionF) {
        EXPECT_EQ(m.end(), m.find(collection));
    }
}

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
        auto json = nlohmann::json::parse(m.toJson());
        ASSERT_NE(json.end(), json.find("uid"));
        EXPECT_TRUE(json["uid"].is_string());
        EXPECT_EQ(manifest.first, json["uid"].get<std::string>());
        if (json.find("collections") != json.end()) {
            for (auto& entry : json["collections"]) {
                EXPECT_NE(
                        manifest.second.end(),
                        std::find_if(
                                manifest.second.begin(),
                                manifest.second.end(),
                                [entry](const CollectionEntry::Entry& e) {
                                    if (e.name == entry["name"]) {
                                        return std::to_string(e.uid) ==
                                               entry["uid"].get<std::string>();
                                    }
                                    return false;
                                }));
            }
        } else {
            EXPECT_EQ(0, manifest.second.size());
        }
    }
}
