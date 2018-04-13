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

#include <gtest/gtest.h>

#include <limits>

TEST(ManifestTest, validation) {
    std::vector<std::string> invalidManifests = {
            "", // empty
            "not json", // definitely not json
            R"({"separator"})", // illegal json
            R"({"uid":"0", "separator":[]})", // illegal separator type

            R"({"separator" : "::", "uid":"0"
                "collections" : 0})", // illegal collections type

            // valid separator, valid uid, no collections
            R"({"separator" : ":", "uid" : "0"})",

            // valid uid, valid collections type, no separator
            R"({"uid":"0", "collections" : []})",

            //  valid separator, valid uid, invalid collections type
            R"({"separator": ":", "uid" : "0",
                "collections":[0]})",

            // valid separator/uid valid name, no collection uid
            R"({"separator" : ":", "uid" : "0",
                "collections":[{"name":"beer"}]})",

            // valid separator/uid, valid collection uid, no collection name
            R"({"separator" : ":","uid":"0",
                "collections":[{"uid":"1"}]})",

            // valid name, invalid collection uid (wrong type)
            R"({"separator": ":","uid":"0",
                "collections":[{"name":"beer", "uid":1}]})",

            // valid name, invalid collection uid (not hex)
            R"({"separator": ":","uid":"0",
                "collections":[{"name":"beer", "uid":"turkey"}]})",

            // invalid name (wrong type), valid uid
            R"({"separator": ":", "uid" : "0",
                "collections":[{"name":1, "uid":"1"}]})",

            // invalid separator (empty)
            R"({"separator" : "", "uid" : "0",
               "collections":[{"name":"beer", "uid":"1"}]})",

            // invalid separator > 16
            R"({"separator": "0123456789abcdef_", "uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})",

            // illegal $ prefixed  name
            R"({"separator": ":", "uid" : "0",
             "collections":[{"name":"$beer", "uid":"1"},
                            {"name":"brewery","uid":"2"}]})",

            // illegal _ prefixed  name
            R"({"separator": ":", "uid" : "0",
               "collections":[{"name":"_beer", "uid":"1"},
                              {"name":"brewery","uid":"2"}]})",

            // duplicate collections
            R"({"separator":":", "uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"beer", "uid":"2"}]})",

            // Invalid manifest UIDs
            // Missing UID
            R"({"separator":":",
                "collections":[{"name":"beer", "uid":"1"}]})",

            // UID wrong type
            R"({"separator":":", "uid" : 0,
                "collections":[{"name":"beer", "uid":"1"}]})",

            // UID cannot be converted to a value
            R"({"separator":":", "uid" : "thisiswrong",
                "collections":[{"name":"beer", "uid":"1"}]})",

            // UID cannot be converted to a value
            R"({"separator":":", "uid" : "12345678901234567890112111",
                "collections":[{"name":"beer", "uid":"1"}]})",

            // UID cannot be 0x prefixed
            R"({"separator":":", "uid" : "0x101",
                "collections":[{"name":"beer", "uid":"1"}]})",

            // separator too large
            R"({"separator":"012345", "uid" : "0", "collections":[]})"};

    std::vector<std::string> validManifests = {
            R"({"separator":":", "uid" : "0", "collections":[]})",

            R"({"separator":":", "uid" : "0",
                "collections":[{"name":"$default","uid":"0"},
                               {"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})",

            // beer & brewery have same UID, valid
            R"({"separator":":", "uid" : "0",
                "collections":[{"name":"$default","uid":"0"},
                               {"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"1"}]})",

            R"({"separator":":", "uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})",

            // Max separator
            R"({"separator":"01234", "uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})",

            // Extra keys ignored at the moment
            R"({"extra":"key",
                "separator":"_", "uid" : "0",
                "collections":[{"name":"beer", "uid":"af"},
                               {"name":"brewery","uid":"2"}]})",

            // lower-case uid is fine
            R"({"separator":":", "uid" : "abcd1", "collections":[]})",
            // upper-case uid is fine
            R"({"separator":":", "uid" : "ABCD1", "collections":[]})",
            // mix-case uid is fine
            R"({"separator":":", "uid" : "AbCd1", "collections":[]})"};

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

TEST(ManifestTest, getSeparator) {
    std::vector<std::pair<std::string, std::string> > validManifests = {
            {"_",
             R"({"separator":"_", "uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"},
            {"01234",
             R"({"separator":"01234", "uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"},
    };

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest.second);
        EXPECT_EQ(manifest.first, m.getSeparator());
    }
}

TEST(ManifestTest, getUid) {
    std::vector<std::pair<Collections::uid_t, std::string> > validManifests = {
            {0,
             R"({"separator":"_", "uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"},
            {0xabcd,
             R"({"separator":"_", "uid" : "ABCD",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"},
            {0xabcd,
             R"({"separator":"_", "uid" : "abcd",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"},
            {0xabcd,
             R"({"separator":"_", "uid" : "aBcD",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"}};

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest.second);
        EXPECT_EQ(manifest.first, m.getUid());
    }
}

TEST(ManifestTest, findCollection) {
    std::string manifest =
            R"({"separator":"_", "uid" : "0",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"},
                               {"name":"$default","uid":"0"}]})";
    std::vector<Collections::Manifest::Identifier> collectionT = {
            {"$default", 0}, {"beer", 1}, {"brewery", 2}};
    std::vector<Collections::Manifest::Identifier> collectionF = {
            {"$Default", 0}, {"cheese", 1}, {"bees", 2}, {"beer", 2}};

    Collections::Manifest m(manifest);

    for (auto& collection : collectionT) {
        EXPECT_NE(m.end(), m.find(collection));
    }

    for (auto& collection : collectionF) {
        EXPECT_EQ(m.end(), m.find(collection));
    }
}

TEST(ManifestTest, toJson) {
    // Inputs for testing are not whitespace formatted as toJson does not format
    std::vector<std::string> validManifests = {
            R"({"separator":":","uid":"abcd","collections":[]})",

            R"({"separator":":","uid":"abcd","collections":[{"name":"$default","uid":"0"},)"
            R"({"name":"beer","uid":"1"},{"name":"brewery","uid":"2"}]})",

            // beer & brewery have same UID, valid
            R"({"separator":":","uid":"abcd","collections":[{"name":"$default","uid":"0"},)"
            R"({"name":"beer","uid":"1"},{"name":"brewery","uid":"1"}]})",

            R"({"separator":":","uid":"abcd","collections":[{"name":"beer","uid":"1"},)"
            R"({"name":"brewery","uid":"2"}]})",

            // Max separator
            R"({"separator":"01234","uid":"abcd","collections":)"
            R"([{"name":"beer","uid":"1"},{"name":"brewery","uid":"2"}]})",

            R"({"separator":"_","uid":"abcd","collections":[{"name":"beer","uid":"af"},)"
            R"({"name":"brewery","uid":"2"}]})"};

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest);
        // What we constructed with should match toJson
        EXPECT_EQ(manifest, m.toJson());
    }
}
