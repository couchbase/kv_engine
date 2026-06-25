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
  "host": "::1",
  "port": 11210,
  "tls": {
    "cert": "/foo/mycert.pem",
    "key": "/foo/mykey.pem",
    "ca_store": "/foo/CA",
    "ssl_peer_verify": false,
    "passphrase": "c2VjcmV0"
  }
}
)"_json;

    DownloadProperties properties;
    properties.bucket = "travel-sample";
    properties.hostname = "::1";
    properties.port = 11210;
    properties.tls = {"/foo/mycert.pem",
                      "/foo/mykey.pem",
                      "/foo/CA",
                      false,
                      "secret",
                      std::nullopt};

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
    "ssl_peer_verify": true,
    "passphrase": "c2VjcmV0"
  }
}
)"_json;

    DownloadProperties properties;
    properties.bucket = "travel-sample";
    properties.hostname = "::1";
    properties.port = 11210;
    properties.sasl = {"PLAIN", "Administrator", "asdfasdf"};
    properties.tls = {"/foo/mycert.pem",
                      "/foo/mykey.pem",
                      "/foo/CA",
                      true,
                      "secret",
                      std::nullopt};

    nlohmann::json json = properties;

    // test to_json methods
    EXPECT_EQ(blueprint, json);

    // test from_json methods
    DownloadProperties parsed = json;
    EXPECT_EQ(properties, parsed);
}

TEST(DownloadPropertiesTest, FromJson) {
    // tls.ssl_peer_verify defaults to true if not provided
    nlohmann::json json = R"(
{
  "bucket": "travel-sample",
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
    "ca_store": "",
    "passphrase": "c2VjcmV0"
  }
}
)"_json;

    DownloadProperties properties = json;
    EXPECT_EQ("travel-sample", properties.bucket);
    EXPECT_FALSE(properties.fsync_interval.has_value());
    EXPECT_FALSE(properties.write_size.has_value());
    EXPECT_EQ("::1", properties.hostname);
    EXPECT_EQ(11210, properties.port);
    ASSERT_TRUE(properties.sasl.has_value());
    EXPECT_EQ("PLAIN", properties.sasl->mechanism);
    EXPECT_EQ("Administrator", properties.sasl->username);
    EXPECT_EQ("asdfasdf", properties.sasl->password);
    ASSERT_TRUE(properties.tls.has_value());
    EXPECT_EQ("/foo/mycert.pem", properties.tls->cert);
    EXPECT_EQ("/foo/mykey.pem", properties.tls->key);
    EXPECT_EQ("", properties.tls->ca_store);
    EXPECT_TRUE(properties.tls->ssl_peer_verify);
    EXPECT_EQ("secret", properties.tls->passphrase);

    // now set ssl_peer_verify to false and re-parse
    json["tls"]["ssl_peer_verify"] = false;
    properties = json;
    ASSERT_TRUE(properties.tls.has_value());
    EXPECT_FALSE(properties.tls->ssl_peer_verify);
    EXPECT_FALSE(properties.tls->crl_config.has_value());
}

TEST(DownloadPropertiesTest, ConversionTlsWithCrlConfig) {
    nlohmann::json blueprint = R"(
{
  "bucket": "travel-sample",
  "host": "::1",
  "port": 11210,
  "tls": {
    "cert": "/foo/mycert.pem",
    "key": "/foo/mykey.pem",
    "ca_store": "/foo/CA",
    "ssl_peer_verify": true,
    "passphrase": "c2VjcmV0",
    "crl_policies": {
      "node_to_node": "Strict",
      "client_auth": "Require"
    },
    "crl_check_intermediate": true,
    "crl_files": ["/crl/crl1.pem", "/crl/crl2.pem"]
  }
}
)"_json;

    DownloadProperties properties;
    properties.bucket = "travel-sample";
    properties.hostname = "::1";
    properties.port = 11210;
    properties.tls = {"/foo/mycert.pem",
                      "/foo/mykey.pem",
                      "/foo/CA",
                      true,
                      "secret",
                      CrlConfiguration{{CrlPolicy::Strict, CrlPolicy::Require},
                                       {"/crl/crl1.pem", "/crl/crl2.pem"},
                                       true}};

    nlohmann::json json = properties;

    // test to_json methods
    EXPECT_EQ(blueprint, json);

    // test from_json methods
    DownloadProperties parsed = json;
    EXPECT_EQ(properties, parsed);
}

TEST(DownloadPropertiesTest, FromJsonTlsCrlPoliciesOnly) {
    nlohmann::json json = R"(
{
  "bucket": "travel-sample",
  "host": "::1",
  "port": 11210,
  "tls": {
    "cert": "/foo/mycert.pem",
    "key": "/foo/mykey.pem",
    "ca_store": "",
    "crl_policies": {
      "node_to_node": "Permissive",
      "client_auth": "Disabled"
    }
  }
}
)"_json;

    DownloadProperties properties = json;
    ASSERT_TRUE(properties.tls.has_value());
    ASSERT_TRUE(properties.tls->crl_config.has_value());
    EXPECT_EQ(CrlPolicy::Permissive,
              properties.tls->crl_config->policies.nodeToNode);
    EXPECT_EQ(CrlPolicy::Disabled,
              properties.tls->crl_config->policies.clientAuth);
    EXPECT_TRUE(properties.tls->crl_config->files.empty());
    EXPECT_FALSE(properties.tls->crl_config->check_intermediate);
}

TEST(DownloadPropertiesTest, FromJsonTlsCrlFilesOnly) {
    nlohmann::json json = R"(
{
  "bucket": "travel-sample",
  "host": "::1",
  "port": 11210,
  "tls": {
    "cert": "/foo/mycert.pem",
    "key": "/foo/mykey.pem",
    "ca_store": "",
    "crl_files": ["/crl/my.crl"]
  }
}
)"_json;

    DownloadProperties properties = json;
    ASSERT_TRUE(properties.tls.has_value());
    ASSERT_TRUE(properties.tls->crl_config.has_value());
    EXPECT_EQ(CrlPolicy::Disabled,
              properties.tls->crl_config->policies.nodeToNode);
    EXPECT_EQ(CrlPolicy::Disabled,
              properties.tls->crl_config->policies.clientAuth);
    ASSERT_EQ(1u, properties.tls->crl_config->files.size());
    EXPECT_EQ("/crl/my.crl", properties.tls->crl_config->files[0]);
    EXPECT_FALSE(properties.tls->crl_config->check_intermediate);
}

TEST(DownloadPropertiesTest, FromJsonTlsCrlCheckIntermediateOnly) {
    nlohmann::json json = R"(
{
  "bucket": "travel-sample",
  "host": "::1",
  "port": 11210,
  "tls": {
    "cert": "/foo/mycert.pem",
    "key": "/foo/mykey.pem",
    "ca_store": "",
    "crl_check_intermediate": true
  }
}
)"_json;

    DownloadProperties properties = json;
    ASSERT_TRUE(properties.tls.has_value());
    ASSERT_TRUE(properties.tls->crl_config.has_value());
    EXPECT_TRUE(properties.tls->crl_config->check_intermediate);
    EXPECT_TRUE(properties.tls->crl_config->files.empty());
    EXPECT_EQ(CrlPolicy::Disabled,
              properties.tls->crl_config->policies.nodeToNode);
}
