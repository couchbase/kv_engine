/*
 *     Copyright 2015 Couchbase, Inc.
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

/**
 * All of the hmac-md5 test cases have be written base on the defined test cases
 * in rfc 2202. http://tools.ietf.org/html/draft-cheng-hmac-test-cases-00
 */

#include <gtest/gtest.h>

#include <platform/platform.h>
#include <string>
#include <array>
#include <openssl/evp.h>
#include <openssl/hmac.h>

const int DIGEST_LENGTH = 16;

TEST(HMAC, Test1) {
    std::array<unsigned char, DIGEST_LENGTH> key{{0x0b, 0x0b, 0x0b, 0x0b,
                                                     0x0b, 0x0b, 0x0b, 0x0b,
                                                     0x0b, 0x0b, 0x0b, 0x0b,
                                                     0x0b, 0x0b, 0x0b, 0x0b
                                                 }};

    std::string data("Hi There");
    std::array<unsigned char, DIGEST_LENGTH> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_md5(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.length(), new_digest.data(), nullptr));

    std::array<unsigned char, DIGEST_LENGTH> digest{{0x92, 0x94, 0x72, 0x7a,
                                                        0x36, 0x38, 0xbb, 0x1c,
                                                        0x13, 0xf4, 0x8e, 0xf8,
                                                        0x15, 0x8b, 0xfc, 0x9d
                                                    }};
    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC, Test2) {
    std::string key("Jefe");
    std::string data("what do ya want for nothing?");

    std::array<unsigned char, DIGEST_LENGTH> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_md5(), key.data(), key.length(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.length(), new_digest.data(), nullptr));

    std::array<unsigned char, DIGEST_LENGTH> digest{{0x75, 0x0c, 0x78, 0x3e,
                                                        0x6a, 0xb0, 0xb5, 0x03,
                                                        0xea, 0xa8, 0x6e, 0x31,
                                                        0x0a, 0x5d, 0xb7, 0x38
                                                    }};
    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC, Test3) {
    std::array<unsigned char, DIGEST_LENGTH> key{{0xaa, 0xaa, 0xaa, 0xaa,
                                                     0xaa, 0xaa, 0xaa, 0xaa,
                                                     0xaa, 0xaa, 0xaa, 0xaa,
                                                     0xaa, 0xaa, 0xaa, 0xaa
                                                 }};
    std::array<unsigned char, 50> data{{0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
                                           0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
                                           0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
                                           0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
                                           0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
                                           0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
                                           0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
                                           0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
                                           0xdd, 0xdd
                                       }};

    std::array<unsigned char, DIGEST_LENGTH> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_md5(), key.data(), key.size(),
                            data.data(), data.size(),
                            new_digest.data(), nullptr));

    std::array<unsigned char, DIGEST_LENGTH> digest{{0x56, 0xbe, 0x34, 0x52,
                                                        0x1d, 0x14, 0x4c, 0x88,
                                                        0xdb, 0xb8, 0xc7, 0x33,
                                                        0xf0, 0xe8, 0xb3, 0xf6
                                                    }};
    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC, Test4) {
    std::array<unsigned char, 25> key{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                                          0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                                          0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12,
                                          0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
                                          0x19}};
    std::array<unsigned char, 50> data{{0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
                                           0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
                                           0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
                                           0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
                                           0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
                                           0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
                                           0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
                                           0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
                                           0xcd, 0xcd}};
    std::array<unsigned char, DIGEST_LENGTH> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_md5(), key.data(), key.size(),
                            data.data(), data.size(),
                            new_digest.data(), nullptr));

    std::array<unsigned char, DIGEST_LENGTH> digest{{0x69, 0x7e, 0xaf, 0x0a,
                                                        0xca, 0x3a, 0x3a, 0xea,
                                                        0x3a, 0x75, 0x16, 0x47,
                                                        0x46, 0xff, 0xaa, 0x79
                                                    }};
    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC, Test5) {
    std::array<unsigned char, DIGEST_LENGTH> key{{0x0c, 0x0c, 0x0c, 0x0c,
                                                     0x0c, 0x0c, 0x0c, 0x0c,
                                                     0x0c, 0x0c, 0x0c, 0x0c,
                                                     0x0c, 0x0c, 0x0c, 0x0c
                                                 }};
    std::string data("Test With Truncation");

    std::array<unsigned char, DIGEST_LENGTH> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_md5(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.length(), new_digest.data(), nullptr));

    std::array<unsigned char, DIGEST_LENGTH> digest{{0x56, 0x46, 0x1e, 0xf2,
                                                        0x34, 0x2e, 0xdc, 0x00,
                                                        0xf9, 0xba, 0xb9, 0x95,
                                                        0x69, 0x0e, 0xfd, 0x4c
                                                    }};
    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC, Test6) {
    std::array<unsigned char, 80> key{{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa
                                      }};
    std::string data("Test Using Larger Than Block-Size Key - Hash Key First");
    std::array<unsigned char, DIGEST_LENGTH> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_md5(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.length(), new_digest.data(), nullptr));

    std::array<unsigned char, DIGEST_LENGTH> digest{{0x6b, 0x1a, 0xb7, 0xfe,
                                                        0x4b, 0xd7, 0xbf, 0x8f,
                                                        0x0b, 0x62, 0xe6, 0xce,
                                                        0x61, 0xb9, 0xd0, 0xcd
                                                    }};
    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC, Test7) {
    std::array<unsigned char, 80> key{{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                          0xaa, 0xaa
                                      }};
    std::string data("Test Using Larger Than Block-Size Key"
                         " and Larger Than One Block-Size Data");
    std::array<unsigned char, DIGEST_LENGTH> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_md5(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.length(), new_digest.data(), nullptr));

    std::array<unsigned char, DIGEST_LENGTH> digest{{0x6f, 0x63, 0x0f, 0xad,
                                                        0x67, 0xcd, 0xa0, 0xee,
                                                        0x1f, 0xb1, 0xf5, 0x62,
                                                        0xdb, 0x3a, 0xa5, 0x3e
                                                    }};
    EXPECT_EQ(digest, new_digest);
}

/*
 * The following tests is picked from RFC2202 (section 3)
 */
TEST(HMAC_SHA1, Test1) {
    std::array<uint8_t, 20> key{{0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
                                    0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
                                    0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
                                    0x0b, 0x0b}};
    std::string data{"Hi There"};
    std::array<uint8_t, 20> digest{{0xb6, 0x17, 0x31, 0x86, 0x55, 0x05,
                                       0x72, 0x64, 0xe2, 0x8b, 0xc0, 0xb6,
                                       0xfb, 0x37, 0x8c, 0x8e, 0xf1, 0x46,
                                       0xbe, 0x00}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.length(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC_SHA1, Test2) {
    std::string key{"Jefe"};
    std::string data{"what do ya want for nothing?"};
    std::array<uint8_t, 20> digest{{0xef, 0xfc, 0xdf, 0x6a, 0xe5, 0xeb,
                                       0x2f, 0xa2, 0xd2, 0x74, 0x16, 0xd5,
                                       0xf1, 0x84, 0xdf, 0x9c, 0x25, 0x9a,
                                       0x7c, 0x79}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.length(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC_SHA1, Test3) {
    std::array<uint8_t, 20> key{{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                    0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                    0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                    0xaa, 0xaa}};
    std::array<uint8_t, 50> data;
    memset(data.data(), 0xdd, data.size());
    std::array<uint8_t, 20> digest{{0x12, 0x5d, 0x73, 0x42, 0xb9, 0xac,
                                       0x11, 0xcd, 0x91, 0xa3, 0x9a, 0xf4,
                                       0x8a, 0xa1, 0x7b, 0x4f, 0x63, 0xf1,
                                       0x75, 0xd3}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.size(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC_SHA1, Test4) {
    std::array<uint8_t, 25> key{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                                    0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                                    0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12,
                                    0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
                                    0x19}};
    std::array<uint8_t, 50> data;
    memset(data.data(), 0xcd, data.size());
    std::array<uint8_t, 20> digest{{0x4c, 0x90, 0x07, 0xf4, 0x02, 0x62,
                                       0x50, 0xc6, 0xbc, 0x84, 0x14, 0xf9,
                                       0xbf, 0x50, 0xc8, 0x6c, 0x2d, 0x72,
                                       0x35, 0xda}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.size(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC_SHA1, Test5) {
    std::array<uint8_t, 20> key{{0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c,
                                    0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c,
                                    0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c,
                                    0x0c, 0x0c}};
    std::string data{{"Test With Truncation"}};
    std::array<uint8_t, 20> digest{{0x4c, 0x1a, 0x03, 0x42, 0x4b, 0x55,
                                       0xe0, 0x7f, 0xe7, 0xf2, 0x7b, 0xe1,
                                       0xd5, 0x8b, 0xb9, 0x32, 0x4a, 0x9a,
                                       0x5a, 0x04}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.size(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC_SHA1, Test6) {
    std::array<uint8_t, 80> key;
    memset(key.data(), 0xaa, key.size());
    std::string data{{"Test Using Larger Than Block-Size Key - Hash Key First"}};
    std::array<uint8_t, 20> digest{{0xaa, 0x4a, 0xe5, 0xe1, 0x52, 0x72,
                                       0xd0, 0x0e, 0x95, 0x70, 0x56, 0x37,
                                       0xce, 0x8a, 0x3b, 0x55, 0xed, 0x40,
                                       0x21, 0x12}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.size(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC_SHA1, Test7) {
    std::array<uint8_t, 80> key;
    memset(key.data(), 0xaa, key.size());
    std::string data{{"Test Using Larger Than Block-Size Key and Larger "
                          "Than One Block-Size Data"}};
    ASSERT_EQ(73, data.size());
    std::array<uint8_t, 20> digest{{0xe8, 0xe9, 0x9d, 0x0f, 0x45, 0x23,
                                       0x7d, 0x78, 0x6d, 0x6b, 0xba, 0xa7,
                                       0x96, 0x5c, 0x78, 0x08, 0xbb, 0xff,
                                       0x1a, 0x91}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.size(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}

TEST(HMAC_SHA1, Test6_1) {
    std::array<uint8_t, 80> key;
    memset(key.data(), 0xaa, key.size());
    std::string data{{"Test Using Larger Than Block-Size Key - Hash Key"
                          " First"}};
    ASSERT_EQ(54, data.size());
    std::array<uint8_t, 20> digest{{0xaa, 0x4a, 0xe5, 0xe1, 0x52, 0x72,
                                       0xd0, 0x0e, 0x95, 0x70, 0x56, 0x37,
                                       0xce, 0x8a, 0x3b, 0x55, 0xed, 0x40,
                                       0x21, 0x12}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.size(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}


TEST(HMAC_SHA1, Test7_1) {
    std::array<uint8_t, 80> key;
    memset(key.data(), 0xaa, key.size());
    std::string data{{"Test Using Larger Than Block-Size Key and Larger"
                          " Than One Block-Size Data"}};
    ASSERT_EQ(73, data.size());
    std::array<uint8_t, 20> digest{{0xe8, 0xe9, 0x9d, 0x0f, 0x45, 0x23,
                                       0x7d, 0x78, 0x6d, 0x6b, 0xba, 0xa7,
                                       0x96, 0x5c, 0x78, 0x08, 0xbb, 0xff,
                                       0x1a, 0x91}};

    std::array<unsigned char, 20> new_digest;
    ASSERT_NE(nullptr, HMAC(EVP_sha1(), key.data(), key.size(),
                            reinterpret_cast<const unsigned char*>(data.data()),
                            data.size(), new_digest.data(), nullptr));

    EXPECT_EQ(digest, new_digest);
}
