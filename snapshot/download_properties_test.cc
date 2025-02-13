/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "download_properties.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

using cb::snapshot::DownloadProperties;

TEST(DownloadPropertiesTest, ConversionSimple) {
    nlohmann::json blueprint = R"(
{
  "bucket": "travel-sample",
  "fsync_interval": 52428800,
  "host": "::1",
  "port": 11210
}
)"_json;

    DownloadProperties properties;
    properties.bucket = "travel-sample";
    properties.hostname = "::1";
    properties.port = 11210;

    nlohmann::json json = properties;

    // test to_json methods
    EXPECT_EQ(blueprint, json);

    // test from_json methods
    DownloadProperties parsed = json;
    EXPECT_EQ(properties, parsed);
}

TEST(DownloadPropertiesTest, ConversionSasl) {
    nlohmann::json blueprint = R"(
{
  "bucket": "travel-sample",
  "fsync_interval": 52428800,
  "host": "::1",
  "port": 11210,
  "sasl": {
    "mechanism": "PLAIN",
    "username": "Administrator",
    "password": "asdfasdf"
  }
}
)"_json;

    DownloadProperties properties;
    properties.bucket = "travel-sample";
    properties.hostname = "::1";
    properties.port = 11210;
    properties.sasl = {"PLAIN", "Administrator", "asdfasdf"};

    nlohmann::json json = properties;

    // test to_json methods
    EXPECT_EQ(blueprint, json);

    // test from_json methods
    DownloadProperties parsed = json;
    EXPECT_EQ(properties, parsed);
}

TEST(DownloadPropertiesTest, ConversionTls) {
    nlohmann::json blueprint = R"(
{
  "bucket": "travel-sample",
  "fsync_interval": 52428800,
  "host": "::1",
  "port": 11210,
  "tls": {
    "cert": "/foo/mycert.pem",
    "key": "/foo/mykey.pem",
    "ca_store": "/foo/CA",
    "passphrase": "c2VjcmV0"
  }
}
)"_json;

    DownloadProperties properties;
    properties.bucket = "travel-sample";
    properties.hostname = "::1";
    properties.port = 11210;
    properties.tls = {"/foo/mycert.pem", "/foo/mykey.pem", "/foo/CA", "secret"};

    nlohmann::json json = properties;

    // test to_json methods
    EXPECT_EQ(blueprint, json);

    // test from_json methods
    DownloadProperties parsed = json;
    EXPECT_EQ(properties, parsed);
}

TEST(DownloadPropertiesTest, ConversionFull) {
    nlohmann::json blueprint = R"(
{
  "bucket": "travel-sample",
  "fsync_interval":52428800,
  "host": "::1",
  "port": 11210,
  "sasl": {
    "mechanism": "PLAIN",
    "username": "Administrator",
    "password": "asdfasdf"
  },
  "tls": {
    "cert": "/foo/mycert.pem",
    "key": "/foo/mykey.pem",
    "ca_store": "/foo/CA",
    "passphrase": "c2VjcmV0"
  }
}
)"_json;

    DownloadProperties properties;
    properties.bucket = "travel-sample";
    properties.hostname = "::1";
    properties.port = 11210;
    properties.sasl = {"PLAIN", "Administrator", "asdfasdf"};
    properties.tls = {"/foo/mycert.pem", "/foo/mykey.pem", "/foo/CA", "secret"};

    nlohmann::json json = properties;

    // test to_json methods
    EXPECT_EQ(blueprint, json);

    // test from_json methods
    DownloadProperties parsed = json;
    EXPECT_EQ(properties, parsed);
}
