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

#ifdef _MSC_VER
#include <windows.h>
#include <bcrypt.h>
#elif defined(__APPLE__)
#include <CommonCrypto/CommonCryptor.h>
#include <CommonCrypto/CommonDigest.h>
#include <CommonCrypto/CommonHMAC.h>
#include <CommonCrypto/CommonKeyDerivation.h>
#else
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#endif

namespace internal {

#ifdef _MSC_VER

struct HeapAllocDeleter {
    void operator()(PBYTE bytes) {
        HeapFree(GetProcessHeap(), 0, bytes);
    }
};

using uniqueHeapPtr = std::unique_ptr<BYTE, HeapAllocDeleter>;

static inline std::string hash(std::string_view key,
                               std::string_view data,
                               LPCWSTR algorithm,
                               int flags) {
    BCRYPT_ALG_HANDLE hAlg;
    NTSTATUS status =
            BCryptOpenAlgorithmProvider(&hAlg, algorithm, nullptr, flags);
    if (status < 0) {
        throw std::runtime_error(
                "digest: BCryptOpenAlgorithmProvider return: " +
                std::to_string(status));
    }

    DWORD pcbResult = 0;
    DWORD cbHashObject = 0;

    // calculate the size of the buffer to hold the hash object
    status = BCryptGetProperty(hAlg,
                               BCRYPT_OBJECT_LENGTH,
                               (PBYTE)&cbHashObject,
                               sizeof(DWORD),
                               &pcbResult,
                               0);
    if (status < 0) {
        BCryptCloseAlgorithmProvider(hAlg, 0);
        throw std::runtime_error("digest: BCryptGetProperty return: " +
                                 std::to_string(status));
    }

    // calculate the length of the hash
    DWORD cbHash = 0;
    status = BCryptGetProperty(hAlg,
                               BCRYPT_HASH_LENGTH,
                               (PBYTE)&cbHash,
                               sizeof(DWORD),
                               &pcbResult,
                               0);
    if (status < 0) {
        BCryptCloseAlgorithmProvider(hAlg, 0);
        throw std::runtime_error("digest: BCryptGetProperty return: " +
                                 std::to_string(status));
    }

    // allocate the hash object on the heap
    uniqueHeapPtr pbHashObject(
            (PBYTE)HeapAlloc(GetProcessHeap(), 0, cbHashObject));
    if (!pbHashObject) {
        BCryptCloseAlgorithmProvider(hAlg, 0);
        throw std::bad_alloc();
    }

    std::string ret;
    ret.resize(cbHash);

    // create the hash
    BCRYPT_HASH_HANDLE hHash;
    status = BCryptCreateHash(hAlg,
                              &hHash,
                              pbHashObject.get(),
                              cbHashObject,
                              (PUCHAR)key.data(),
                              ULONG(key.size()),
                              0);
    if (status < 0) {
        BCryptCloseAlgorithmProvider(hAlg, 0);
        throw std::runtime_error("digest: BCryptCreateHash return: " +
                                 std::to_string(status));
    }

    status = BCryptHashData(hHash, (PBYTE)data.data(), ULONG(data.size()), 0);
    if (status < 0) {
        BCryptCloseAlgorithmProvider(hAlg, 0);
        BCryptDestroyHash(hHash);
        throw std::runtime_error("digest: BCryptHashData return: " +
                                 std::to_string(status));
    }

    status = BCryptFinishHash(
            hHash,
            reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())),
            cbHash,
            0);

    // Release resources
    BCryptCloseAlgorithmProvider(hAlg, 0);
    BCryptDestroyHash(hHash);

    if (status < 0) {
        throw std::runtime_error("digest: BCryptFinishHash return: " +
                                 std::to_string(status));
    }

    return ret;
}

static std::string HMAC_SHA1(std::string_view key, std::string_view data) {
    return hash(key, data, BCRYPT_SHA1_ALGORITHM, BCRYPT_ALG_HANDLE_HMAC_FLAG);
}

static std::string HMAC_SHA256(std::string_view key, std::string_view data) {
    return hash(
            key, data, BCRYPT_SHA256_ALGORITHM, BCRYPT_ALG_HANDLE_HMAC_FLAG);
}

static std::string HMAC_SHA512(std::string_view key, std::string_view data) {
    return hash(
            key, data, BCRYPT_SHA512_ALGORITHM, BCRYPT_ALG_HANDLE_HMAC_FLAG);
}

static inline std::string PBKDF2(std::string_view pass,
                                 std::string_view salt,
                                 unsigned int iterationCount,
                                 LPCWSTR algorithm) {
    // open an algorithm handle
    BCRYPT_ALG_HANDLE hAlg;
    NTSTATUS status;

    status = BCryptOpenAlgorithmProvider(
            &hAlg, algorithm, nullptr, BCRYPT_ALG_HANDLE_HMAC_FLAG);
    if (status < 0) {
        throw std::runtime_error(
                "digest: BCryptOpenAlgorithmProvider return: " +
                std::to_string(status));
    }

    DWORD pcbResult = 0;
    DWORD cbHashObject = 0;

    // calculate the length of the hash
    DWORD cbHash = 0;
    status = BCryptGetProperty(hAlg,
                               BCRYPT_HASH_LENGTH,
                               (PBYTE)&cbHash,
                               sizeof(DWORD),
                               &pcbResult,
                               0);
    if (status < 0) {
        BCryptCloseAlgorithmProvider(hAlg, 0);
        throw std::runtime_error("digest: BCryptGetProperty return: " +
                                 std::to_string(status));
    }

    std::string ret;
    ret.resize(cbHash);

    status = BCryptDeriveKeyPBKDF2(hAlg,
                                   (PUCHAR)pass.data(),
                                   ULONG(pass.size()),
                                   (PUCHAR)salt.data(),
                                   ULONG(salt.size()),
                                   iterationCount,
                                   (PUCHAR)ret.data(),
                                   ULONG(ret.size()),
                                   0);

    // Release resources
    BCryptCloseAlgorithmProvider(hAlg, 0);

    if (status < 0) {
        throw std::runtime_error("digest: BCryptDeriveKeyPBKDF2 return: " +
                                 std::to_string(status));
    }

    return ret;
}

static std::string PBKDF2_HMAC_SHA1(std::string_view pass,
                                    std::string_view salt,
                                    unsigned int iterationCount) {
    return PBKDF2(pass, salt, iterationCount, BCRYPT_SHA1_ALGORITHM);
}

static std::string PBKDF2_HMAC_SHA256(std::string_view pass,
                                      std::string_view salt,
                                      unsigned int iterationCount) {
    return PBKDF2(pass, salt, iterationCount, BCRYPT_SHA256_ALGORITHM);
}

static std::string PBKDF2_HMAC_SHA512(std::string_view pass,
                                      std::string_view salt,
                                      unsigned int iterationCount) {
    return PBKDF2(pass, salt, iterationCount, BCRYPT_SHA512_ALGORITHM);
}

static std::string digest_sha1(std::string_view data) {
    return hash({}, data, BCRYPT_SHA1_ALGORITHM, 0);
}

static std::string digest_sha256(std::string_view data) {
    return hash({}, data, BCRYPT_SHA256_ALGORITHM, 0);
}

static std::string digest_sha512(std::string_view data) {
    return hash({}, data, BCRYPT_SHA512_ALGORITHM, 0);
}

#elif defined(__APPLE__)

static std::string HMAC_SHA1(std::string_view key, std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA1_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA1,
           key.data(),
           key.size(),
           data.data(),
           data.size(),
           reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}

static std::string HMAC_SHA256(std::string_view key, std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA256_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA256,
           key.data(),
           key.size(),
           data.data(),
           data.size(),
           reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}

static std::string HMAC_SHA512(std::string_view key, std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA512_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA512,
           key.data(),
           key.size(),
           data.data(),
           data.size(),
           reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}

static std::string PBKDF2_HMAC_SHA1(std::string_view pass,
                                    std::string_view salt,
                                    unsigned int iterationCount) {
    std::string ret;
    ret.resize(cb::crypto::SHA1_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(
            kCCPBKDF2,
            pass.data(),
            pass.size(),
            reinterpret_cast<const uint8_t*>(salt.data()),
            salt.size(),
            kCCPRFHmacAlgSHA1,
            iterationCount,
            reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())),
            ret.size());

    if (err != 0) {
        throw std::runtime_error(
                "cb::crypto::PBKDF2_HMAC(SHA1): CCKeyDerivationPBKDF failed: " +
                std::to_string(err));
    }
    return ret;
}

static std::string PBKDF2_HMAC_SHA256(std::string_view pass,
                                      std::string_view salt,
                                      unsigned int iterationCount) {
    std::string ret;
    ret.resize(cb::crypto::SHA256_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(
            kCCPBKDF2,
            pass.data(),
            pass.size(),
            reinterpret_cast<const uint8_t*>(salt.data()),
            salt.size(),
            kCCPRFHmacAlgSHA256,
            iterationCount,
            reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())),
            ret.size());
    if (err != 0) {
        throw std::runtime_error(
                "cb::crypto::PBKDF2_HMAC(SHA256): CCKeyDerivationPBKDF "
                "failed: " +
                std::to_string(err));
    }
    return ret;
}

static std::string PBKDF2_HMAC_SHA512(std::string_view pass,
                                      std::string_view salt,
                                      unsigned int iterationCount) {
    std::string ret;
    ret.resize(cb::crypto::SHA512_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(
            kCCPBKDF2,
            pass.data(),
            pass.size(),
            reinterpret_cast<const uint8_t*>(salt.data()),
            salt.size(),
            kCCPRFHmacAlgSHA512,
            iterationCount,
            reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())),
            ret.size());
    if (err != 0) {
        throw std::runtime_error(
                "cb::crypto::PBKDF2_HMAC(SHA512): CCKeyDerivationPBKDF "
                "failed: " +
                std::to_string(err));
    }
    return ret;
}

static std::string digest_sha1(std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA1_DIGEST_SIZE);
    CC_SHA1(data.data(),
            data.size(),
            reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}

static std::string digest_sha256(std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA256_DIGEST_SIZE);
    CC_SHA256(data.data(),
              data.size(),
              reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}

static std::string digest_sha512(std::string_view data) {
    std::string ret;
    ret.resize(cb::crypto::SHA512_DIGEST_SIZE);
    CC_SHA512(data.data(),
              data.size(),
              reinterpret_cast<uint8_t*>(const_cast<char*>(ret.data())));
    return ret;
}

#else
// OpenSSL

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

#endif

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
