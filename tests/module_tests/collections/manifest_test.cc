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
            "not json",
            "{\"revision\"", // short
            "{\"revision\":[]}", // illegal revision type

            "{\"revision\":0," // valid revision
            "\"separator\":0}", // illegal separator type

            "{\"revision\":0," // valid revision
            "\"separator\":\":\"," // valid separator
            "\"collections\":0}", // illegal collections type

            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[0]}", // illegal collection

            "{\"revision\":0,"
            "\"separator\":\"\"," // invalid separator
            "\"collections\":[\"beer\"]}",

            "{\"revision\":0," // no separator
            "\"collections\":[\"beer\"]}",

            "{\"separator\":\"," // no revision
            "\"collections\":[\"beer\"]}",

            "{\"revision\":0,"
            "\"separator\":\":\",", // no collections

            "{\"revision\":2147483648" // to large for int32
            "\"separator\":\":\","
            "\"collections\":[\"$default\",\"beer\",\"brewery\"]}"

            "{\"revision\":0," // separator > 250
            "\"separator\":\"!-------------------------------------------------"
            "------------------------------------------------------------------"
            "------------------------------------------------------------------"
            "------------------------------------------------------------------"
            "---\","
            "\"collections\":[\"beer\",\"brewery\"]}"};

    std::vector<std::string> validManifests = {
            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[]}",

            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[\"$default\",\"beer\",\"brewery\"]}",

            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[\"beer\",\"brewery\"]}",

            "{\"revision\":0,"
            "\"separator\":\"--------------------------------------------------"
            "------------------------------------------------------------------"
            "------------------------------------------------------------------"
            "------------------------------------------------------------------"
            "--\","
            "\"collections\":[\"beer\",\"brewery\"]}"};

    for (auto& manifest : invalidManifests) {
        EXPECT_THROW(Collections::Manifest m(manifest), std::invalid_argument)
                << "Didn't throw exception for an invalid manifest " + manifest;
    }

    for (auto& manifest : validManifests) {
        EXPECT_NO_THROW(Collections::Manifest m(manifest))
                << "Exception thrown for valid manifest " + manifest;
    }
}

TEST(ManifestTest, defaultManifest) {
    // Default construction gives the default manifest
    Collections::Manifest manifest;
    EXPECT_EQ(0, manifest.getRevision());
}

TEST(ManifestTest, getRevision) {
    std::vector<std::pair<int32_t, std::string> > validManifests = {
            {0,
             "{\"revision\":0,"
             "\"separator\":\":\","
             "\"collections\":[\"$default\",\"beer\",\"brewery\"]}"},

            {std::numeric_limits<int32_t>::max(),
             "{\"revision\":2147483647,"
             "\"separator\":\":\","
             "\"collections\":[\"$default\",\"beer\",\"brewery\"]}"},
    };

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest.second);
        EXPECT_EQ(manifest.first, m.getRevision());
    }
}

TEST(ManifestTest, getSeparator) {
    std::vector<std::pair<std::string, std::string> > validManifests = {
            {":",
             "{\"revision\":0,"
             "\"separator\":\":\","
             "\"collections\":[\"$default\",\"beer\",\"brewery\"]}"}};

    for (auto& manifest : validManifests) {
        Collections::Manifest m(manifest.second);
        EXPECT_EQ(manifest.first, m.getSeparator());
    }
}

TEST(ManifestTest, findCollection) {
    std::string manifest =
            "{\"revision\":0,"
            "\"separator\":\":\","
            "\"collections\":[\"$default\",\"beer\",\"brewery\"]}";
    std::vector<std::string> collectionT = {"$default", "beer", "brewery"};
    std::vector<std::string> collectionF = {"$Default", "cheese", "bees"};

    Collections::Manifest m(manifest);

    for (auto& collection : collectionT) {
        EXPECT_NE(m.end(), m.find(collection));
    }

    for (auto& collection : collectionF) {
        EXPECT_EQ(m.end(), m.find(collection));
    }
}
