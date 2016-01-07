/*
 *     Copyright 2016 Couchbase, Inc.
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
 * The intention of this test program is to get a sense of the execution
 * speed for generating the various hashes depending on the iteration
 * input
 */
#include <gtest/gtest.h>
#include <platform/random.h>
#include <platform/base64.h>
#include <openssl/evp.h>

const int Sha1DigestSize = 20;
#ifndef __APPLE__
const int Sha256DigestSize = 32;
const int Sha512DigestSize = 64;
#endif


class Pbkdf2Test : public ::testing::Test {
protected:
    void generateSalt(std::vector<uint8_t>& bytes, std::string& salt) {
        Couchbase::RandomGenerator randomGenerator(true);

        if (!randomGenerator.getBytes(bytes.data(), bytes.size())) {
            throw std::runtime_error("Failed to get random bytes");
        }

        using Couchbase::Base64::encode;

        salt = encode(std::string(reinterpret_cast<char*>(bytes.data()),
                                  bytes.size()));
    }

    void generateSha1Password(int iterationCount) {
        std::vector<uint8_t> salt(Sha1DigestSize);
        std::string sha1salt;
        std::string passwd("password");
        generateSalt(salt, sha1salt);
        std::vector<uint8_t> saltedSha1Password(Sha1DigestSize);

        for (int ii = 0; ii < 1000; ++ii) {
            if (PKCS5_PBKDF2_HMAC_SHA1(passwd.data(), int(passwd.size()),
                                       salt.data(), int(salt.size()),
                                       iterationCount,
                                       Sha1DigestSize,
                                       saltedSha1Password.data()) != 1) {
                throw std::runtime_error("PKCS5_PBKDF2_HMAC_SHA1 failed");
            }
        }
    }

#ifndef __APPLE__
    void generateSha256Password(int iterationCount) {
        std::vector<uint8_t> salt(Sha256DigestSize);
        std::string sha256salt;
        std::string passwd("password");
        generateSalt(salt, sha256salt);
        std::vector<uint8_t> saltedSha256Password(Sha256DigestSize);

        for (int ii = 0; ii < 1000; ++ii) {
            if (PKCS5_PBKDF2_HMAC(passwd.data(), int(passwd.size()),
                                  salt.data(), int(salt.size()),
                                  iterationCount,
                                  EVP_sha256(),
                                  Sha256DigestSize,
                                  saltedSha256Password.data()) != 1) {
                throw std::runtime_error("PKCS5_PBKDF2_HMAC SHA256 failed");
            }
        }
    }

    void generateSha512Password(int iterationCount) {
        std::vector<uint8_t> salt(Sha512DigestSize);
        std::string sha512salt;
        std::string passwd("password");
        generateSalt(salt, sha512salt);
        std::vector<uint8_t> saltedSha512Password(Sha512DigestSize);

        for (int ii = 0; ii < 1000; ++ii) {
            if (PKCS5_PBKDF2_HMAC(passwd.data(), int(passwd.size()),
                                  salt.data(), int(salt.size()),
                                  iterationCount,
                                  EVP_sha512(),
                                  Sha512DigestSize,
                                  saltedSha512Password.data()) != 1) {
                throw std::runtime_error("PKCS5_PBKDF2_HMAC SHA512 failed");
            }
        }
    }
#endif
};

TEST_F(Pbkdf2Test, GenerateSha1Salt) {
    std::vector<uint8_t> bytes(Sha1DigestSize);
    std::string salt;

    for (int ii = 0; ii < 1000; ++ii) {
        generateSalt(bytes, salt);
    }
}

TEST_F(Pbkdf2Test, GenerateSha1I4k) {
    generateSha1Password(4096);
}

TEST_F(Pbkdf2Test, GenerateSha1I10) {
    generateSha1Password(10);
}

#ifndef __APPLE__
TEST_F(Pbkdf2Test, GenerateSha256Salt) {
    std::vector<uint8_t> bytes(Sha256DigestSize);
    std::string salt;

    for (int ii = 0; ii < 1000; ++ii) {
        generateSalt(bytes, salt);
    }
}

TEST_F(Pbkdf2Test, GenerateSha256I4k) {
    generateSha1Password(4096);
}

TEST_F(Pbkdf2Test, GenerateSha256I10) {
    generateSha1Password(10);
}

TEST_F(Pbkdf2Test, GenerateSha512Salt) {
    std::vector<uint8_t> bytes(Sha512DigestSize);
    std::string salt;

    for (int ii = 0; ii < 1000; ++ii) {
        generateSalt(bytes, salt);
    }
}

TEST_F(Pbkdf2Test, GenerateSha512I4k) {
    generateSha1Password(4096);
}

TEST_F(Pbkdf2Test, GenerateSha512I10) {
    generateSha1Password(10);
}
#endif
