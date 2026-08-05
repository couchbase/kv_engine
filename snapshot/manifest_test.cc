/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "manifest.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

using cb::snapshot::FileInfo;
using cb::snapshot::Manifest;

TEST(Manifest, Conversion) {
    nlohmann::json blueprint = R"(
{
  "deks": [
    {
      "id": 0,
      "path": "/foo/dek",
      "size": "1234"
    }
  ],
  "files": [
    {
      "id": 0,
      "path": "/foo/bar",
      "size": "1234",
      "sha512": "deadbeef"
    }
  ],
  "uuid": "UUID",
  "vbid": 1
})"_json;

    Manifest manifest{Vbid{1}, "UUID"};
    manifest.files.emplace_back("/foo/bar", 1234, 0, "deadbeef");
    manifest.deks.emplace_back("/foo/dek", 1234, 0);
    nlohmann::json json = manifest;
    EXPECT_EQ(blueprint, json);

    Manifest parsed = json;
    EXPECT_EQ(manifest, parsed);
}

TEST(Manifest, ConversionWithStorage) {
    nlohmann::json blueprint = R"(
{
  "deks": [],
  "files": [
    {
      "id": 0,
      "path": "/foo/bar",
      "size": "1234",
      "sha512": "deadbeef"
    }
  ],
  "storage": {
    "backend": "couchstore",
    "version": 14
  },
  "uuid": "UUID",
  "vbid": 1
})"_json;

    Manifest manifest{Vbid{1}, "UUID"};
    manifest.files.emplace_back("/foo/bar", 1234, 0, "deadbeef");
    manifest.backend = "couchstore";
    manifest.diskFormatVersion = 14;
    nlohmann::json json = manifest;
    EXPECT_EQ(blueprint, json);

    Manifest parsed = json;
    EXPECT_EQ(manifest, parsed);

    // A version of 0 (unknown) isn't included
    manifest.diskFormatVersion = 0;
    json = manifest;
    EXPECT_EQ(nlohmann::json({{"backend", "couchstore"}}), json["storage"]);
    parsed = json;
    EXPECT_EQ(manifest, parsed);
}

/// Manifests created before the storage information was introduced don't
/// contain it; they must still parse (backend empty, version 0 = unknown)
TEST(Manifest, ParseWithoutStorage) {
    nlohmann::json json = R"(
{
  "deks": [],
  "files": [],
  "uuid": "UUID",
  "vbid": 1
})"_json;
    Manifest parsed = json;
    EXPECT_TRUE(parsed.backend.empty());
    EXPECT_EQ(0, parsed.diskFormatVersion);
}

TEST(Manifest, ParseInvalidStorage) {
    nlohmann::json json = R"(
{
  "deks": [],
  "files": [],
  "uuid": "UUID",
  "vbid": 1
})"_json;
    json["storage"] = "couchstore";
    EXPECT_THROW(Manifest{json}, std::invalid_argument);
    json["storage"] = {{"backend", 1}};
    EXPECT_THROW(Manifest{json}, std::invalid_argument);
    json["storage"] = {{"backend", "couchstore"}, {"version", "14"}};
    EXPECT_THROW(Manifest{json}, std::invalid_argument);
}
