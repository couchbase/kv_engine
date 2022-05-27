/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "cbcrypto.h"

#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <memory>
#include <stdexcept>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

namespace internal {

static std::string HMAC_SHA1(std::string_view key, std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA1_DIGEST_SIZE);
    if (HMAC(EVP_sha1(),
             key.data(),
             key.size(),
             reinterpret_cast<const uint8_t*>(data.data()),
             data.size(),
             reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())),
             nullptr) == nullptr) {
        throw std::runtime_error("cb::crypto::HMAC(SHA1): HMAC failed");
    }
    return ret;
}

static std::string HMAC_SHA256(std::string_view key, std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA256_DIGEST_SIZE);
    if (HMAC(EVP_sha256(),
             key.data(),
             key.size(),
             reinterpret_cast<const uint8_t*>(data.data()),
             data.size(),
             reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())),
             nullptr) == nullptr) {
        throw std::runtime_error("cb::crypto::HMAC(SHA256): HMAC failed");
    }
    return ret;
}

static std::string HMAC_SHA512(std::string_view key, std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA512_DIGEST_SIZE);
    if (HMAC(EVP_sha512(),
             key.data(),
             key.size(),
             reinterpret_cast<const uint8_t*>(data.data()),
             data.size(),
             reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())),
             nullptr) == nullptr) {
        throw std::runtime_error("cb::crypto::HMAC(SHA512): HMAC failed");
    }
    return ret;
}

static std::string PBKDF2_HMAC_SHA1(std::string_view pass,
                                    std::string_view salt,
                                    unsigned int iterationCount) {
    std::string ret;
    ret.resize(cb::crypto::SHA1_DIGEST_SIZE);
    auto err = PKCS5_PBKDF2_HMAC(
            pass.data(),
            int(pass.size()),
            reinterpret_cast<const uint8_t*>(salt.data()),
            int(salt.size()),
            iterationCount,
            EVP_sha1(),
            cb::crypto::SHA1_DIGEST_SIZE,
            reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));

    if (err != 1) {
        throw std::runtime_error(
                "cb::crypto::PBKDF2_HMAC(SHA1): PKCS5_PBKDF2_HMAC_SHA1 "
                "failed: " +
                std::to_string(err));
    }

    return ret;
}

static std::string PBKDF2_HMAC_SHA256(std::string_view pass,
                                      std::string_view salt,
                                      unsigned int iterationCount) {
    std::string ret;
    ret.resize(cb::crypto::SHA256_DIGEST_SIZE);
    auto err = PKCS5_PBKDF2_HMAC(
            pass.data(),
            int(pass.size()),
            reinterpret_cast<const uint8_t*>(salt.data()),
            int(salt.size()),
            iterationCount,
            EVP_sha256(),
            cb::crypto::SHA256_DIGEST_SIZE,
            reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    if (err != 1) {
        throw std::runtime_error(
                "cb::crypto::PBKDF2_HMAC(SHA256): PKCS5_PBKDF2_HMAC failed" +
                std::to_string(err));
    }

    return ret;
}

static std::string PBKDF2_HMAC_SHA512(std::string_view pass,
                                      std::string_view salt,
                                      unsigned int iterationCount) {
    std::string ret;
    ret.resize(cb::crypto::SHA512_DIGEST_SIZE);
    auto err = PKCS5_PBKDF2_HMAC(
            pass.data(),
            int(pass.size()),
            reinterpret_cast<const uint8_t*>(salt.data()),
            int(salt.size()),
            iterationCount,
            EVP_sha512(),
            cb::crypto::SHA512_DIGEST_SIZE,
            reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    if (err != 1) {
        throw std::runtime_error(
                "cb::crypto::PBKDF2_HMAC(SHA512): PKCS5_PBKDF2_HMAC failed" +
                std::to_string(err));
    }

    return ret;
}

static std::string digest_sha1(std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA1_DIGEST_SIZE);
    SHA1(reinterpret_cast<const uint8_t*>(data.data()),
         data.size(),
         reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}

static std::string digest_sha256(std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA256_DIGEST_SIZE);
    SHA256(reinterpret_cast<const uint8_t*>(data.data()),
           data.size(),
           reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}

static std::string digest_sha512(std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA512_DIGEST_SIZE);
    SHA512(reinterpret_cast<const uint8_t*>(data.data()),
           data.size(),
           reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}
} // namespace internal

std::string cb::crypto::HMAC(const Algorithm algorithm,
                             std::string_view key,
                             std::string_view data) {
    TRACE_EVENT1("cbcrypto", "HMAC", "algorithm", int(algorithm));
    switch (algorithm) {
    case Algorithm::SHA1:
        return internal::HMAC_SHA1(key, data);
    case Algorithm::SHA256:
        return internal::HMAC_SHA256(key, data);
    case Algorithm::SHA512:
        return internal::HMAC_SHA512(key, data);
    }

    throw std::invalid_argument("cb::crypto::HMAC: Unknown Algorithm: " +
                                std::to_string((int)algorithm));
}

std::string cb::crypto::PBKDF2_HMAC(const Algorithm algorithm,
                                    std::string_view pass,
                                    std::string_view salt,
                                    unsigned int iterationCount) {
    TRACE_EVENT2("cbcrypto",
                 "PBKDF2_HMAC",
                 "algorithm",
                 int(algorithm),
                 "iteration",
                 iterationCount);
    switch (algorithm) {
    case Algorithm::SHA1:
        return internal::PBKDF2_HMAC_SHA1(pass, salt, iterationCount);
    case Algorithm::SHA256:
        return internal::PBKDF2_HMAC_SHA256(pass, salt, iterationCount);
    case Algorithm::SHA512:
        return internal::PBKDF2_HMAC_SHA512(pass, salt, iterationCount);
    }

    throw std::invalid_argument("cb::crypto::PBKDF2_HMAC: Unknown Algorithm: " +
                                std::to_string((int)algorithm));
}

std::string cb::crypto::digest(const Algorithm algorithm,
                               std::string_view data) {
    TRACE_EVENT1("cbcrypto", "digest", "algorithm", int(algorithm));
    switch (algorithm) {
    case Algorithm::SHA1:
        return internal::digest_sha1(data);
    case Algorithm::SHA256:
        return internal::digest_sha256(data);
    case Algorithm::SHA512:
        return internal::digest_sha512(data);
    }

    throw std::invalid_argument("cb::crypto::digest: Unknown Algorithm" +
                                std::to_string((int)algorithm));
}
