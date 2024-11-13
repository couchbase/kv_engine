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
      "size": "1234",
      "crc32c": "31"
    }
  ],
  "files": [
    {
      "id": 0,
      "path": "/foo/bar",
      "size": "1234"
    }
  ],
  "uuid": "UUID",
  "vbid": 1
})"_json;

    Manifest manifest;
    manifest.uuid = "UUID";
    manifest.vbid = Vbid(1);
    manifest.files.emplace_back("/foo/bar", 1234, 0);
    manifest.deks.emplace_back("/foo/dek", 1234, 0, 31);
    nlohmann::json json = manifest;
    EXPECT_EQ(blueprint, json);

    Manifest parsed = json;
    EXPECT_EQ(manifest, parsed);
}
