/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "config.h"
#include "cbcrypto.h"

#include <stdexcept>

#ifdef __APPLE__

#include <CommonCrypto/CommonDigest.h>
#include <CommonCrypto/CommonHMAC.h>
#include <CommonCrypto/CommonKeyDerivation.h>

#else

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/md5.h>

#endif

#ifdef __APPLE__

static std::vector<uint8_t> HMAC_MD5(const std::vector<uint8_t>& key,
                                     const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::MD5_DIGEST_SIZE);
    CCHmac(kCCHmacAlgMD5, key.data(), key.size(), data.data(), data.size(),
           ret.data());
    return ret;
}

static std::vector<uint8_t> HMAC_SHA1(const std::vector<uint8_t>& key,
                                      const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA1_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA1, key.data(), key.size(), data.data(), data.size(),
           ret.data());
    return ret;
}

static std::vector<uint8_t> HMAC_SHA256(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA256_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA256, key.data(), key.size(), data.data(), data.size(),
           ret.data());
    return ret;
}

static std::vector<uint8_t> HMAC_SHA512(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA512_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA512, key.data(), key.size(), data.data(), data.size(),
           ret.data());
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA1(const std::string& pass,
                                             const std::vector<uint8_t>& salt,
                                             unsigned int iterationCount) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA1_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(kCCPBKDF2,
                                    pass.data(), pass.size(),
                                    salt.data(), salt.size(),
                                    kCCPRFHmacAlgSHA1, iterationCount,
                                    ret.data(), ret.size());

    if (err != 0) {
        throw std::runtime_error(
            "Couchbase::Crypto::PBKDF2_HMAC(SHA1): CCKeyDerivationPBKDF failed: " +
            std::to_string(err));
    }
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA256(const std::string& pass,
                                               const std::vector<uint8_t>& salt,
                                               unsigned int iterationCount) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA256_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(kCCPBKDF2,
                                    pass.data(), pass.size(),
                                    salt.data(), salt.size(),
                                    kCCPRFHmacAlgSHA256, iterationCount,
                                    ret.data(), ret.size());
    if (err != 0) {
        throw std::runtime_error(
            "Couchbase::Crypto::PBKDF2_HMAC(SHA256): CCKeyDerivationPBKDF failed: " +
            std::to_string(err));
    }
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA512(const std::string& pass,
                                               const std::vector<uint8_t>& salt,
                                               unsigned int iterationCount) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA512_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(kCCPBKDF2,
                                    pass.data(), pass.size(),
                                    salt.data(), salt.size(),
                                    kCCPRFHmacAlgSHA512, iterationCount,
                                    ret.data(), ret.size());
    if (err != 0) {
        throw std::runtime_error(
            "Couchbase::Crypto::PBKDF2_HMAC(SHA512): CCKeyDerivationPBKDF failed: " +
            std::to_string(err));
    }
    return ret;
}

static std::vector<uint8_t> digest_md5(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::MD5_DIGEST_SIZE);
    CC_MD5(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha1(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA1_DIGEST_SIZE);
    CC_SHA1(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha256(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA256_DIGEST_SIZE);
    CC_SHA256(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha512(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA512_DIGEST_SIZE);
    CC_SHA512(data.data(), data.size(), ret.data());
    return ret;
}

#else

// OpenSSL

static std::vector<uint8_t> HMAC_MD5(const std::vector<uint8_t>& key,
                                     const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::MD5_DIGEST_SIZE);
    if (HMAC(EVP_md5(), key.data(), key.size(), data.data(), data.size(),
             ret.data(), nullptr) == nullptr) {
        throw std::runtime_error("Couchbase::Crypto::HMAC(MD5): HMAC failed");
    }
    return ret;
}

static std::vector<uint8_t> HMAC_SHA1(const std::vector<uint8_t>& key,
                                      const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA1_DIGEST_SIZE);
    if (HMAC(EVP_sha1(), key.data(), key.size(), data.data(), data.size(),
             ret.data(), nullptr) == nullptr) {
        throw std::runtime_error("Couchbase::Crypto::HMAC(SHA1): HMAC failed");
    }
    return ret;
}

static std::vector<uint8_t> HMAC_SHA256(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA256_DIGEST_SIZE);
    if (HMAC(EVP_sha256(), key.data(), key.size(), data.data(), data.size(),
             ret.data(), nullptr) == nullptr) {
        throw std::runtime_error(
            "Couchbase::Crypto::HMAC(SHA256): HMAC failed");
    }
    return ret;
}

static std::vector<uint8_t> HMAC_SHA512(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA512_DIGEST_SIZE);
    if (HMAC(EVP_sha512(), key.data(), key.size(), data.data(), data.size(),
             ret.data(), nullptr) == nullptr) {
        throw std::runtime_error(
            "Couchbase::Crypto::HMAC(SHA512): HMAC failed");
    }
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA1(const std::string& pass,
                                             const std::vector<uint8_t>& salt,
                                             unsigned int iterationCount) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA1_DIGEST_SIZE);
#if defined(HAVE_PKCS5_PBKDF2_HMAC)
    auto err = PKCS5_PBKDF2_HMAC(pass.data(), int(pass.size()),
                                 salt.data(), int(salt.size()),
                                 iterationCount,
                                 EVP_sha1(),
                                 Couchbase::Crypto::SHA1_DIGEST_SIZE,
                                 ret.data());

    if (err != 1) {
        throw std::runtime_error(
            "Couchbase::Crypto::PBKDF2_HMAC(SHA1): PKCS5_PBKDF2_HMAC_SHA1 failed: " +
            std::to_string(err));
    }
#elif defined(HAVE_PKCS5_PBKDF2_HMAC_SHA1)
    auto err = PKCS5_PBKDF2_HMAC_SHA1(pass.data(), int(pass.size()),
                                      salt.data(), int(salt.size()),
                                      iterationCount,
                                      Couchbase::Crypto::SHA1_DIGEST_SIZE,
                                      ret.data());
    if (err != 1) {
        throw std::runtime_error(
            "Couchbase::Crypto::PBKDF2_HMAC(SHA1): PKCS5_PBKDF2_HMAC_SHA1 failed" +
            std::to_string(err));
    }
#else
    throw std::runtime_error("Couchbase::Crypto::PBKDF2_HMAC(SHA1): Not supported");
#endif

    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA256(const std::string& pass,
                                               const std::vector<uint8_t>& salt,
                                               unsigned int iterationCount) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA256_DIGEST_SIZE);
#if defined(HAVE_PKCS5_PBKDF2_HMAC)
    auto err = PKCS5_PBKDF2_HMAC(pass.data(), int(pass.size()),
                                 salt.data(), int(salt.size()),
                                 iterationCount,
                                 EVP_sha256(),
                                 Couchbase::Crypto::SHA256_DIGEST_SIZE,
                                 ret.data());
    if (err != 1) {
        throw std::runtime_error(
            "Couchbase::Crypto::PBKDF2_HMAC(SHA256): PKCS5_PBKDF2_HMAC failed" +
            std::to_string(err));
    }
#else
    throw std::runtime_error(
        "Couchbase::Crypto::PBKDF2_HMAC(SHA256): Not supported");
#endif
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA512(const std::string& pass,
                                               const std::vector<uint8_t>& salt,
                                               unsigned int iterationCount) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA512_DIGEST_SIZE);
#if defined(HAVE_PKCS5_PBKDF2_HMAC)
    auto err = PKCS5_PBKDF2_HMAC(pass.data(), int(pass.size()),
                                 salt.data(), int(salt.size()),
                                 iterationCount,
                                 EVP_sha512(),
                                 Couchbase::Crypto::SHA512_DIGEST_SIZE,
                                 ret.data());
     if (err != 1) {
        throw std::runtime_error(
            "Couchbase::Crypto::PBKDF2_HMAC(SHA512): PKCS5_PBKDF2_HMAC failed" +
            std::to_string(err));
    }
#else
    throw std::runtime_error(
        "Couchbase::Crypto::PBKDF2_HMAC(SHA512): Not supported");
#endif
    return ret;
}

static std::vector<uint8_t> digest_md5(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::MD5_DIGEST_SIZE);
    MD5(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha1(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA1_DIGEST_SIZE);
    SHA1(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha256(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA256_DIGEST_SIZE);
    SHA256(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha512(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(Couchbase::Crypto::SHA512_DIGEST_SIZE);
    SHA512(data.data(), data.size(), ret.data());
    return ret;
}

#endif

std::vector<uint8_t> Couchbase::Crypto::HMAC(const Algorithm algorithm,
                                             const std::vector<uint8_t>& key,
                                             const std::vector<uint8_t>& data) {
    switch (algorithm) {
    case Algorithm::MD5:
        return HMAC_MD5(key, data);
    case Algorithm::SHA1:
        return HMAC_SHA1(key, data);
    case Algorithm::SHA256:
        return HMAC_SHA256(key, data);
    case Algorithm::SHA512:
        return HMAC_SHA512(key, data);
    }

    throw std::invalid_argument("Couchbase::Crypto::HMAC: Unknown Algorithm: " +
                                std::to_string((int)algorithm));
}

std::vector<uint8_t> Couchbase::Crypto::PBKDF2_HMAC(const Algorithm algorithm,
                                                    const std::string& pass,
                                                    const std::vector<uint8_t>& salt,
                                                    unsigned int iterationCount) {
    switch (algorithm) {
    case Algorithm::MD5:
        throw std::invalid_argument(
            "Couchbase::Crypto::PBKDF2_HMAC: Can't use MD5");
    case Algorithm::SHA1:
        return PBKDF2_HMAC_SHA1(pass, salt, iterationCount);
    case Algorithm::SHA256:
        return PBKDF2_HMAC_SHA256(pass, salt, iterationCount);
    case Algorithm::SHA512:
        return PBKDF2_HMAC_SHA512(pass, salt, iterationCount);
    }

    throw std::invalid_argument(
        "Couchbase::Crypto::PBKDF2_HMAC: Unknown Algorithm: " +
        std::to_string((int)algorithm));
}

static inline void verifyLegalAlgorithm(const Couchbase::Crypto::Algorithm al) {
    switch (al) {
    case Couchbase::Crypto::Algorithm::MD5:
    case Couchbase::Crypto::Algorithm::SHA1:
    case Couchbase::Crypto::Algorithm::SHA256:
    case Couchbase::Crypto::Algorithm::SHA512:
        return;
    }
    throw std::invalid_argument(
        "verifyLegalAlgorithm: Unknown Algorithm: " + std::to_string((int)al));
}

bool Couchbase::Crypto::isSupported(const Algorithm algorithm) {
    verifyLegalAlgorithm(algorithm);

#if defined(__APPLE__) || defined(HAVE_PKCS5_PBKDF2_HMAC)
    return true;
#elif defined(HAVE_PKCS5_PBKDF2_HMAC_SHA1)
    switch (algorithm) {
    case Algorithm::MD5:
    case Algorithm::SHA1:
        return true;
    default:
        return false;
    }
#else
    return algorithm == Algorithm::MD5;
#endif
}

std::vector<uint8_t> Couchbase::Crypto::digest(const Algorithm algorithm,
                                               const std::vector<uint8_t>& data) {
    switch (algorithm) {
    case Algorithm::MD5:
        return digest_md5(data);
    case Algorithm::SHA1:
        return digest_sha1(data);
    case Algorithm::SHA256:
        return digest_sha256(data);
    case Algorithm::SHA512:
        return digest_sha512(data);
    }

    throw std::invalid_argument(
        "Couchbase::Crypto::digest: Unknown Algorithm" +
        std::to_string((int)algorithm));
}
