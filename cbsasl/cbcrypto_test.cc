/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * All of the hmac-md5 test cases have be written base on the defined
 * test cases in rfc 2202.
 *   http://tools.ietf.org/html/draft-cheng-hmac-test-cases-00
 */

#include "cbcrypto.h"

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>
#include <stdexcept>
#include <string>

std::vector<uint8_t> string2vector(const std::string& str) {
    std::vector<uint8_t> ret(str.size());
    memcpy(ret.data(), str.data(), ret.size());
    return ret;
}

using namespace Couchbase;
using namespace cb;

/*
 * The following tests is picked from RFC2202 (section 3)
 */
TEST(HMAC_SHA1, Test1) {
    std::vector<uint8_t> key{{0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
                              0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
                              0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b}};
    std::string data{"Hi There"};
    std::vector<uint8_t> digest{{0xb6, 0x17, 0x31, 0x86, 0x55, 0x05, 0x72,
                                 0x64, 0xe2, 0x8b, 0xc0, 0xb6, 0xfb, 0x37,
                                 0x8c, 0x8e, 0xf1, 0x46, 0xbe, 0x00}};

    auto new_digest = crypto::HMAC(
            crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            data);

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(HMAC_SHA1, Test2) {
    std::string key{"Jefe"};
    std::string data{"what do ya want for nothing?"};
    std::vector<uint8_t> digest{{0xef, 0xfc, 0xdf, 0x6a, 0xe5, 0xeb, 0x2f,
                                 0xa2, 0xd2, 0x74, 0x16, 0xd5, 0xf1, 0x84,
                                 0xdf, 0x9c, 0x25, 0x9a, 0x7c, 0x79}};

    auto new_digest = crypto::HMAC(crypto::Algorithm::SHA1, key, data);

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(HMAC_SHA1, Test3) {
    std::vector<uint8_t> key{{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                              0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                              0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}};
    std::vector<uint8_t> data(50);
    memset(data.data(), 0xdd, data.size());
    std::vector<uint8_t> digest{{0x12, 0x5d, 0x73, 0x42, 0xb9, 0xac, 0x11,
                                 0xcd, 0x91, 0xa3, 0x9a, 0xf4, 0x8a, 0xa1,
                                 0x7b, 0x4f, 0x63, 0xf1, 0x75, 0xd3}};

    auto new_digest = crypto::HMAC(
            crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            {reinterpret_cast<const char*>(data.data()), data.size()});

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(HMAC_SHA1, Test4) {
    std::vector<uint8_t> key{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                              0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                              0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
                              0x16, 0x17, 0x18, 0x19}};
    std::vector<uint8_t> data(50);
    memset(data.data(), 0xcd, data.size());
    std::vector<uint8_t> digest{{0x4c, 0x90, 0x07, 0xf4, 0x02, 0x62, 0x50,
                                 0xc6, 0xbc, 0x84, 0x14, 0xf9, 0xbf, 0x50,
                                 0xc8, 0x6c, 0x2d, 0x72, 0x35, 0xda}};

    auto new_digest = crypto::HMAC(
            crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            {reinterpret_cast<const char*>(data.data()), data.size()});

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(HMAC_SHA1, Test5) {
    std::vector<uint8_t> key{{0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c,
                              0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c,
                              0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c}};
    std::string data{"Test With Truncation"};
    std::vector<uint8_t> digest{{0x4c, 0x1a, 0x03, 0x42, 0x4b, 0x55, 0xe0,
                                 0x7f, 0xe7, 0xf2, 0x7b, 0xe1, 0xd5, 0x8b,
                                 0xb9, 0x32, 0x4a, 0x9a, 0x5a, 0x04}};

    auto new_digest = crypto::HMAC(
            crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            data);

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(HMAC_SHA1, Test6) {
    std::vector<uint8_t> key(80);
    memset(key.data(), 0xaa, key.size());
    std::string data{"Test Using Larger Than Block-Size Key - Hash Key First"};
    std::vector<uint8_t> digest{{0xaa, 0x4a, 0xe5, 0xe1, 0x52, 0x72, 0xd0,
                                 0x0e, 0x95, 0x70, 0x56, 0x37, 0xce, 0x8a,
                                 0x3b, 0x55, 0xed, 0x40, 0x21, 0x12}};

    auto new_digest = crypto::HMAC(
            crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            data);

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(HMAC_SHA1, Test7) {
    std::vector<uint8_t> key(80);
    memset(key.data(), 0xaa, key.size());
    std::string data{
            "Test Using Larger Than Block-Size Key and Larger "
            "Than One Block-Size Data"};
    ASSERT_EQ(73, data.size());
    std::vector<uint8_t> digest{{0xe8, 0xe9, 0x9d, 0x0f, 0x45, 0x23, 0x7d,
                                 0x78, 0x6d, 0x6b, 0xba, 0xa7, 0x96, 0x5c,
                                 0x78, 0x08, 0xbb, 0xff, 0x1a, 0x91}};

    auto new_digest = crypto::HMAC(
            crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            data);

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(HMAC_SHA1, Test6_1) {
    std::vector<uint8_t> key(80);
    memset(key.data(), 0xaa, key.size());
    std::string data{
            "Test Using Larger Than Block-Size Key - Hash Key"
            " First"};
    ASSERT_EQ(54, data.size());
    std::vector<uint8_t> digest{{0xaa, 0x4a, 0xe5, 0xe1, 0x52, 0x72, 0xd0,
                                 0x0e, 0x95, 0x70, 0x56, 0x37, 0xce, 0x8a,
                                 0x3b, 0x55, 0xed, 0x40, 0x21, 0x12}};

    auto new_digest = crypto::HMAC(
            crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            data);

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(HMAC_SHA1, Test7_1) {
    std::vector<uint8_t> key(80);
    memset(key.data(), 0xaa, key.size());
    std::string data{
            "Test Using Larger Than Block-Size Key and Larger"
            " Than One Block-Size Data"};
    ASSERT_EQ(73, data.size());
    std::vector<uint8_t> digest{{0xe8, 0xe9, 0x9d, 0x0f, 0x45, 0x23, 0x7d,
                                 0x78, 0x6d, 0x6b, 0xba, 0xa7, 0x96, 0x5c,
                                 0x78, 0x08, 0xbb, 0xff, 0x1a, 0x91}};

    auto new_digest = crypto::HMAC(
            crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(key.data()), key.size()},
            data);

    EXPECT_EQ(digest, string2vector(new_digest));
}

TEST(PBKDF2_HMAC, SHA1) {
    const std::string hash("ujVC+2T7EKQbOJopX5IzPgSx3m0=");
    const std::string salt("ZWglX9gQEpMZqYXlzzlGjs2dqMo=");

    EXPECT_EQ(Base64::decode(hash),
              crypto::PBKDF2_HMAC(crypto::Algorithm::SHA1,
                                  "password",
                                  Base64::decode(salt),
                                  4096));
}

TEST(PBKDF2_HMAC, SHA256) {
    const std::string hash("Gg48JSpr1ACwm2sNNfFqlCII7LzkvFaehBDX920nGvE=");
    const std::string salt("K3WUInsELbeaNOpy9jp8nKE907tshZmZq71uw8ExaDs=");
    EXPECT_EQ(Base64::decode(hash),
              crypto::PBKDF2_HMAC(crypto::Algorithm::SHA256,
                                  "password",
                                  Base64::decode(salt),
                                  4096));
}

TEST(PBKDF2_HMAC, SHA512) {
    const std::string hash(
            "gI8135FS74/RbI+wFpofDCqccxNRCpp4d8oEge+/lrJlnPhHDs"
            "1JWzmI+5GD+K5n57/hreh0el+lPRWRuRotGw==");
    const std::string salt(
            "rOa3n53kC5VnpxvrUBgHUlRQ3BG1YYkXaL1S31OBv7oUj66jTR"
            "cBU9FerGh+SlbS0kjyBes2eOMe8+2Oi3/BMQ==");

    EXPECT_EQ(Base64::decode(hash),
              crypto::PBKDF2_HMAC(crypto::Algorithm::SHA512,
                                  "password",
                                  Base64::decode(salt),
                                  4096));
}

TEST(PBKDF2_HMAC, UnknownAlgorithm) {
    EXPECT_THROW(crypto::PBKDF2_HMAC((crypto::Algorithm)100, "", "", 1),
                 std::invalid_argument);
}

TEST(Digest, SHA1) {
    std::string data;
    data.resize(50);
    std::fill(data.begin(), data.end(), 0xdd);
    EXPECT_EQ("a/eYGUZs797W4yYH3kxoypn+dnQ=",
              Base64::encode(crypto::digest(crypto::Algorithm::SHA1, data)));
}

TEST(Digest, SHA256) {
    std::string data;
    data.resize(50);
    std::fill(data.begin(), data.end(), 0xdd);
    auto digest = crypto::digest(crypto::Algorithm::SHA256, data);
    EXPECT_EQ("XPYYtbbTi9FsLlWO701LbVKChFR/1KCdoqu28JjsYZM=",
              Base64::encode(digest));
}

TEST(Digest, SHA512) {
    std::string data;
    data.resize(50);
    std::fill(data.begin(), data.end(), 0xdd);
    auto digest = crypto::digest(crypto::Algorithm::SHA512, data);
    EXPECT_EQ(
            "ocK90Gck7GOlN3GIBrL76aaf6yUuLl3/HXcSB93FlouYyPN+Dgi+NKIg"
            "Lvr+LtJgKvVDrw2aQ4EXTgOFEvt4MA==",
            Base64::encode(digest));
}

/**
 * Extra test to validate that the way we generate the entries in
 * the password database for plain encoding works the same way as
 * ns_server would do it.
 *
 * All of the input values in the test is verified with ns_server
 */
TEST(Crypt, ns_server_password_encoding) {
    const std::vector<uint8_t> salt = {
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}};
    const std::string password = {"pa33w0rd"};
    const std::vector<uint8_t> hmac = {{31,  112, 31,  99, 18, 35,  227,
                                        52,  96,  252, 20, 53, 183, 65,
                                        140, 137, 190, 11, 93, 234}};

    auto generated_hmac = cb::crypto::HMAC(
            cb::crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(salt.data()), salt.size()},
            password);

    EXPECT_EQ(hmac, string2vector(generated_hmac));

    // The format of the password encoding is that we're appending
    // the generated hmac to the salt (which should be 16 bytes).
    std::vector<uint8_t> pwent = {salt};
    std::copy(generated_hmac.begin(),
              generated_hmac.end(),
              std::back_inserter(pwent));

    // And the password entry is dumped as base64
    EXPECT_EQ("AAECAwQFBgcICQoLDA0ODx9wH2MSI+M0YPwUNbdBjIm+C13q",
              cb::base64::encode(std::string_view{(const char*)pwent.data(),
                                                  pwent.size()}));
}
