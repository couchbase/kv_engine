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
            "{separator}", // illegal json
            R"({"separator"})", // illegal json
            R"({"separator":[]})", // illegal separator type

            R"({"separator" : "::",
                "collections" : 0})", // illegal collections type

            R"({"separator" : ":"})", // valid separator, no collections
            R"({"collections" : []})", // valid collections type, no separator

            R"({"separator": ":",
                "collections":[0]})", // illegal collection entry type

            R"({"separator" : ":",
                "collections":[{"name":"beer"}]})", // valid name, no uid

            R"({"separator" : ":",
                "collections":[{"uid":"1"}]})", // valid uid, no name

            // valid name, invalid uid
            R"({"separator": ":",
                "collections":[{"name":"beer", "uid":1}]})",

            // valid name, invalid uid
            R"({"separator": ":",
                "collections":[{"name":"beer", "uid":"turkey"}]})",

            // invalid name, valid uid
            R"({"separator": ":",
                "collections":[{"name":1, "uid":"1"}]})",

            // invalid separator (empty)
            R"({"separator" : "",
               "collections":[{"name":"beer", "uid":"1"}]})",

            // invalid separator > 16
            R"({"separator": "0123456789abcdef_",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})",

            // illegal $ prefixed  name
            R"({"separator": ":",
             "collections":[{"name":"$beer", "uid":"1"},
                            {"name":"brewery","uid":"2"}]})",

            // illegal _ prefixed  name
            R"({"separator": ":",
               "collections":[{"name":"_beer", "uid":"1"},
                              {"name":"brewery","uid":"2"}]})",

            // duplicate collections
            R"({"separator":":",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"beer", "uid":"2"}]})",
    };

    std::vector<std::string> validManifests = {
            R"({"separator":":", "collections":[]})",

            R"({"separator":":",
                "collections":[{"name":"$default","uid":"0"},
                               {"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})",

            // beer & brewery have same UID, valid
            R"({"separator":":",
                "collections":[{"name":"$default","uid":"0"},
                               {"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"1"}]})",

            R"({"separator":":",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})",

            // Max separator
            R"({"separator":"0123456789abcdef",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})",

            // Extra keys ignored at the moment
            R"({"extra":"key",
                "separator":"_",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"};

    for (auto& manifest : invalidManifests) {
        try {
            Collections::Manifest m(manifest);
            EXPECT_TRUE(false)
                    << "No exception thrown for invalid manifest:" << manifest
                    << std::endl;
        } catch (std::exception& e) {
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
             R"({"separator":"_",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"},
            {"0123456789abcdef",
             R"({"separator":"0123456789abcdef",
                "collections":[{"name":"beer", "uid":"1"},
                               {"name":"brewery","uid":"2"}]})"},
    };

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest.second);
        EXPECT_EQ(manifest.first, m.getSeparator());
    }
}

TEST(ManifestTest, findCollection) {
    std::string manifest =
            R"({"separator":"_",
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
