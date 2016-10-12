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
#include <cbcrypto/cbcrypto.h>

#include <iomanip>
#include <memory>
#include <openssl/evp.h>
#include <platform/base64.h>
#include <sstream>
#include <stdexcept>

#ifdef __APPLE__

#include <CommonCrypto/CommonDigest.h>
#include <CommonCrypto/CommonHMAC.h>
#include <CommonCrypto/CommonKeyDerivation.h>

#else

#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/md5.h>

#endif

#ifdef __APPLE__

static std::vector<uint8_t> HMAC_MD5(const std::vector<uint8_t>& key,
                                     const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::MD5_DIGEST_SIZE);
    CCHmac(kCCHmacAlgMD5, key.data(), key.size(), data.data(), data.size(),
           ret.data());
    return ret;
}

static std::vector<uint8_t> HMAC_SHA1(const std::vector<uint8_t>& key,
                                      const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA1_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA1, key.data(), key.size(), data.data(), data.size(),
           ret.data());
    return ret;
}

static std::vector<uint8_t> HMAC_SHA256(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA256_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA256, key.data(), key.size(), data.data(), data.size(),
           ret.data());
    return ret;
}

static std::vector<uint8_t> HMAC_SHA512(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA512_DIGEST_SIZE);
    CCHmac(kCCHmacAlgSHA512, key.data(), key.size(), data.data(), data.size(),
           ret.data());
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA1(const std::string& pass,
                                             const std::vector<uint8_t>& salt,
                                             unsigned int iterationCount) {
    std::vector<uint8_t> ret(cb::crypto::SHA1_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(kCCPBKDF2,
                                    pass.data(), pass.size(),
                                    salt.data(), salt.size(),
                                    kCCPRFHmacAlgSHA1, iterationCount,
                                    ret.data(), ret.size());

    if (err != 0) {
        throw std::runtime_error(
            "cb::crypto::PBKDF2_HMAC(SHA1): CCKeyDerivationPBKDF failed: " +
            std::to_string(err));
    }
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA256(const std::string& pass,
                                               const std::vector<uint8_t>& salt,
                                               unsigned int iterationCount) {
    std::vector<uint8_t> ret(cb::crypto::SHA256_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(kCCPBKDF2,
                                    pass.data(), pass.size(),
                                    salt.data(), salt.size(),
                                    kCCPRFHmacAlgSHA256, iterationCount,
                                    ret.data(), ret.size());
    if (err != 0) {
        throw std::runtime_error(
            "cb::crypto::PBKDF2_HMAC(SHA256): CCKeyDerivationPBKDF failed: " +
            std::to_string(err));
    }
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA512(const std::string& pass,
                                               const std::vector<uint8_t>& salt,
                                               unsigned int iterationCount) {
    std::vector<uint8_t> ret(cb::crypto::SHA512_DIGEST_SIZE);
    auto err = CCKeyDerivationPBKDF(kCCPBKDF2,
                                    pass.data(), pass.size(),
                                    salt.data(), salt.size(),
                                    kCCPRFHmacAlgSHA512, iterationCount,
                                    ret.data(), ret.size());
    if (err != 0) {
        throw std::runtime_error(
            "cb::crypto::PBKDF2_HMAC(SHA512): CCKeyDerivationPBKDF failed: " +
            std::to_string(err));
    }
    return ret;
}

static std::vector<uint8_t> digest_md5(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::MD5_DIGEST_SIZE);
    CC_MD5(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha1(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA1_DIGEST_SIZE);
    CC_SHA1(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha256(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA256_DIGEST_SIZE);
    CC_SHA256(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha512(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA512_DIGEST_SIZE);
    CC_SHA512(data.data(), data.size(), ret.data());
    return ret;
}

#else

// OpenSSL

static std::vector<uint8_t> HMAC_MD5(const std::vector<uint8_t>& key,
                                     const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::MD5_DIGEST_SIZE);
    if (HMAC(EVP_md5(), key.data(), key.size(), data.data(), data.size(),
             ret.data(), nullptr) == nullptr) {
        throw std::runtime_error("cb::crypto::HMAC(MD5): HMAC failed");
    }
    return ret;
}

static std::vector<uint8_t> HMAC_SHA1(const std::vector<uint8_t>& key,
                                      const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA1_DIGEST_SIZE);
    if (HMAC(EVP_sha1(), key.data(), key.size(), data.data(), data.size(),
             ret.data(), nullptr) == nullptr) {
        throw std::runtime_error("cb::crypto::HMAC(SHA1): HMAC failed");
    }
    return ret;
}

static std::vector<uint8_t> HMAC_SHA256(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA256_DIGEST_SIZE);
    if (HMAC(EVP_sha256(), key.data(), key.size(), data.data(), data.size(),
             ret.data(), nullptr) == nullptr) {
        throw std::runtime_error(
            "cb::crypto::HMAC(SHA256): HMAC failed");
    }
    return ret;
}

static std::vector<uint8_t> HMAC_SHA512(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA512_DIGEST_SIZE);
    if (HMAC(EVP_sha512(), key.data(), key.size(), data.data(), data.size(),
             ret.data(), nullptr) == nullptr) {
        throw std::runtime_error(
            "cb::crypto::HMAC(SHA512): HMAC failed");
    }
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA1(const std::string& pass,
                                             const std::vector<uint8_t>& salt,
                                             unsigned int iterationCount) {
    std::vector<uint8_t> ret(cb::crypto::SHA1_DIGEST_SIZE);
#if defined(HAVE_PKCS5_PBKDF2_HMAC)
    auto err = PKCS5_PBKDF2_HMAC(pass.data(), int(pass.size()),
                                 salt.data(), int(salt.size()),
                                 iterationCount,
                                 EVP_sha1(),
                                 cb::crypto::SHA1_DIGEST_SIZE,
                                 ret.data());

    if (err != 1) {
        throw std::runtime_error(
            "cb::crypto::PBKDF2_HMAC(SHA1): PKCS5_PBKDF2_HMAC_SHA1 failed: " +
            std::to_string(err));
    }
#elif defined(HAVE_PKCS5_PBKDF2_HMAC_SHA1)
    auto err = PKCS5_PBKDF2_HMAC_SHA1(pass.data(), int(pass.size()),
                                      salt.data(), int(salt.size()),
                                      iterationCount,
                                      cb::crypto::SHA1_DIGEST_SIZE,
                                      ret.data());
    if (err != 1) {
        throw std::runtime_error(
            "cb::crypto::PBKDF2_HMAC(SHA1): PKCS5_PBKDF2_HMAC_SHA1 failed" +
            std::to_string(err));
    }
#else
    throw std::runtime_error("cb::crypto::PBKDF2_HMAC(SHA1): Not supported");
#endif

    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA256(const std::string& pass,
                                               const std::vector<uint8_t>& salt,
                                               unsigned int iterationCount) {
    std::vector<uint8_t> ret(cb::crypto::SHA256_DIGEST_SIZE);
#if defined(HAVE_PKCS5_PBKDF2_HMAC)
    auto err = PKCS5_PBKDF2_HMAC(pass.data(), int(pass.size()),
                                 salt.data(), int(salt.size()),
                                 iterationCount,
                                 EVP_sha256(),
                                 cb::crypto::SHA256_DIGEST_SIZE,
                                 ret.data());
    if (err != 1) {
        throw std::runtime_error(
            "cb::crypto::PBKDF2_HMAC(SHA256): PKCS5_PBKDF2_HMAC failed" +
            std::to_string(err));
    }
#else
    throw std::runtime_error(
        "cb::crypto::PBKDF2_HMAC(SHA256): Not supported");
#endif
    return ret;
}

static std::vector<uint8_t> PBKDF2_HMAC_SHA512(const std::string& pass,
                                               const std::vector<uint8_t>& salt,
                                               unsigned int iterationCount) {
    std::vector<uint8_t> ret(cb::crypto::SHA512_DIGEST_SIZE);
#if defined(HAVE_PKCS5_PBKDF2_HMAC)
    auto err = PKCS5_PBKDF2_HMAC(pass.data(), int(pass.size()),
                                 salt.data(), int(salt.size()),
                                 iterationCount,
                                 EVP_sha512(),
                                 cb::crypto::SHA512_DIGEST_SIZE,
                                 ret.data());
     if (err != 1) {
        throw std::runtime_error(
            "cb::crypto::PBKDF2_HMAC(SHA512): PKCS5_PBKDF2_HMAC failed" +
            std::to_string(err));
    }
#else
    throw std::runtime_error(
        "cb::crypto::PBKDF2_HMAC(SHA512): Not supported");
#endif
    return ret;
}

static std::vector<uint8_t> digest_md5(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::MD5_DIGEST_SIZE);
    MD5(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha1(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA1_DIGEST_SIZE);
    SHA1(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha256(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA256_DIGEST_SIZE);
    SHA256(data.data(), data.size(), ret.data());
    return ret;
}

static std::vector<uint8_t> digest_sha512(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> ret(cb::crypto::SHA512_DIGEST_SIZE);
    SHA512(data.data(), data.size(), ret.data());
    return ret;
}

#endif

std::vector<uint8_t> cb::crypto::HMAC(const Algorithm algorithm,
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

    throw std::invalid_argument("cb::crypto::HMAC: Unknown Algorithm: " +
                                std::to_string((int)algorithm));
}

std::vector<uint8_t> cb::crypto::PBKDF2_HMAC(const Algorithm algorithm,
                                             const std::string& pass,
                                             const std::vector<uint8_t>& salt,
                                             unsigned int iterationCount) {
    switch (algorithm) {
    case Algorithm::MD5:
        throw std::invalid_argument(
            "cb::crypto::PBKDF2_HMAC: Can't use MD5");
    case Algorithm::SHA1:
        return PBKDF2_HMAC_SHA1(pass, salt, iterationCount);
    case Algorithm::SHA256:
        return PBKDF2_HMAC_SHA256(pass, salt, iterationCount);
    case Algorithm::SHA512:
        return PBKDF2_HMAC_SHA512(pass, salt, iterationCount);
    }

    throw std::invalid_argument(
        "cb::crypto::PBKDF2_HMAC: Unknown Algorithm: " +
        std::to_string((int)algorithm));
}

static inline void verifyLegalAlgorithm(const cb::crypto::Algorithm al) {
    switch (al) {
    case cb::crypto::Algorithm::MD5:
    case cb::crypto::Algorithm::SHA1:
    case cb::crypto::Algorithm::SHA256:
    case cb::crypto::Algorithm::SHA512:
        return;
    }
    throw std::invalid_argument(
        "verifyLegalAlgorithm: Unknown Algorithm: " + std::to_string((int)al));
}

bool cb::crypto::isSupported(const Algorithm algorithm) {
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

std::vector<uint8_t> cb::crypto::digest(const Algorithm algorithm,
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
        "cb::crypto::digest: Unknown Algorithm" +
        std::to_string((int)algorithm));
}

namespace cb {
    namespace crypto {
        static const EVP_CIPHER* getCipher(const Cipher& cipher,
                                           const std::vector<uint8_t>& key,
                                           const std::vector<uint8_t>& iv) {
            const EVP_CIPHER* cip = nullptr;

            switch (cipher) {
            case cb::crypto::Cipher::AES_256_cbc:
                cip = EVP_aes_256_cbc();
                break;
            }

            if (cip == nullptr) {
                throw std::invalid_argument(
                    "cb::crypto::getCipher: Unknown Cipher " +
                    std::to_string(int(cipher)));
            }

            if (int(key.size()) != EVP_CIPHER_key_length(cip)) {
                throw std::invalid_argument(
                    "cb::crypto::getCipher: Cipher requires a key "
                        "length of " +
                    std::to_string(EVP_CIPHER_key_length(cip)) +
                    " provided key with length " + std::to_string(key.size()));
            }

            if (int(iv.size()) != EVP_CIPHER_iv_length(cip)) {
                throw std::invalid_argument(
                    "cb::crypto::getCipher: Cipher requires a iv "
                        "length of " +
                    std::to_string(EVP_CIPHER_iv_length(cip)) +
                    " provided iv with length " + std::to_string(iv.size()));
            }

            return cip;
        }

        /**
         * decode the META information for the encryption bits.
         *
         * @param meta the input json data (not const to avoid all of the const
         *             casts, and this is a private helper function not visible
         *             outside this file
         * @param cipher the cipher to use (out)
         * @param key the key to use (out)
         * @param iv the iv to use (out)
         */
        static void decodeJsonMeta(cJSON* meta,
                                   Cipher& cipher,
                                   std::vector<uint8_t>& key,
                                   std::vector<uint8_t>& iv) {

            auto* obj = cJSON_GetObjectItem(meta, "cipher");
            if (obj == nullptr) {
                throw std::runtime_error(
                    "cb::crypto::decodeJsonMeta: cipher not specified");
            }

            cipher = crypto::to_cipher(obj->valuestring);
            obj = cJSON_GetObjectItem(meta, "key");
            if (obj == nullptr) {
                throw std::runtime_error(
                    "cb::crypto::decodeJsonMeta: key not specified");
            }

            auto str = Couchbase::Base64::decode(obj->valuestring);
            key.resize(str.size());
            memcpy(key.data(), str.data(), key.size());

            obj = cJSON_GetObjectItem(meta, "iv");
            if (obj == nullptr) {
                throw std::runtime_error(
                    "cb::crypto::decodeJsonMeta: iv not specified");
            }

            str = Couchbase::Base64::decode(obj->valuestring);
            iv.resize(str.size());
            memcpy(iv.data(), str.data(), iv.size());
        }
    }
}

// helper class for use with std::unique_ptr in managing cJSON* objects.
struct EVP_CIPHER_CTX_Deleter {
    void operator()(EVP_CIPHER_CTX* ctx) {
        if (ctx != nullptr) {
            EVP_CIPHER_CTX_free(ctx);
        }
    }
};

using unique_EVP_CIPHER_CTX_ptr = std::unique_ptr<EVP_CIPHER_CTX,
    EVP_CIPHER_CTX_Deleter>;


std::vector<uint8_t> cb::crypto::encrypt(const Cipher& cipher,
                                         const std::vector<uint8_t>& key,
                                         const std::vector<uint8_t>& iv,
                                         const uint8_t* data,
                                         size_t length) {
    unique_EVP_CIPHER_CTX_ptr ctx(EVP_CIPHER_CTX_new());

    auto* cip = getCipher(cipher, key, iv);
    if (EVP_EncryptInit_ex(ctx.get(), cip, nullptr, key.data(),
                           iv.data()) != 1) {
        throw std::runtime_error(
            "cb::crypto::encrypt: EVP_EncryptInit_ex failed");
    }

    std::vector<uint8_t> ret(length + EVP_CIPHER_block_size(cip));
    int len1 = int(ret.size());

    if (EVP_EncryptUpdate(ctx.get(), ret.data(), &len1, data,
                          int(length)) != 1) {
        throw std::runtime_error(
            "cb::crypto::encrypt: EVP_EncryptUpdate failed");
    }

    int len2 = int(ret.size()) - len1;
    if (EVP_EncryptFinal_ex(ctx.get(), ret.data() + len1, &len2) != 1) {
        throw std::runtime_error(
            "cb::crypto::encrypt: EVP_EncryptFinal_ex failed");
    }

    // Resize the destination to the sum of the two length fields
    ret.resize(size_t(len1) + size_t(len2));
    return ret;
}

std::vector<uint8_t> cb::crypto::encrypt(const Cipher& cipher,
                                         const std::vector<uint8_t>& key,
                                         const std::vector<uint8_t>& iv,
                                         const std::vector<uint8_t>& data) {
    return encrypt(cipher, key, iv, data.data(), data.size());
}

std::vector<uint8_t> cb::crypto::encrypt(const cJSON* json,
                                         const uint8_t* data,
                                         size_t length) {
    Cipher cipher;
    std::vector<uint8_t> key;
    std::vector<uint8_t> iv;

    decodeJsonMeta(const_cast<cJSON*>(json), cipher, key, iv);
    return encrypt(cipher, key, iv, data, length);
}

std::vector<uint8_t> cb::crypto::decrypt(const Cipher& cipher,
                                         const std::vector<uint8_t>& key,
                                         const std::vector<uint8_t>& iv,
                                         const uint8_t* data,
                                         size_t length) {
    unique_EVP_CIPHER_CTX_ptr ctx(EVP_CIPHER_CTX_new());
    auto* cip = getCipher(cipher, key, iv);

    if (EVP_DecryptInit_ex(ctx.get(), cip, nullptr, key.data(),
                           iv.data()) != 1) {
        throw std::runtime_error(
            "cb::crypto::decrypt: EVP_DecryptInit_ex failed");
    }

    std::vector<uint8_t> ret(length);
    int len1 = int(ret.size());

    if (EVP_DecryptUpdate(ctx.get(), ret.data(), &len1, data,
                          int(length)) != 1) {
        throw std::runtime_error(
            "cb::crypto::decrypt: EVP_DecryptUpdate failed");
    }

    int len2 = int(length) - len1;
    if (EVP_DecryptFinal_ex(ctx.get(), ret.data() + len1, &len2) != 1) {
        throw std::runtime_error(
            "cb::crypto::decrypt: EVP_DecryptFinal_ex failed");
    }

    // Resize the destination to the sum of the two length fields
    ret.resize(size_t(len1) + size_t(len2));
    return ret;
}

std::vector<uint8_t> cb::crypto::decrypt(const Cipher& cipher,
                                         const std::vector<uint8_t>& key,
                                         const std::vector<uint8_t>& iv,
                                         const std::vector<uint8_t>& data) {
    return decrypt(cipher, key, iv, data.data(), data.size());
}

cb::crypto::Cipher cb::crypto::to_cipher(const std::string& str) {

    if (str == "AES_256_cbc") {
        return Cipher::AES_256_cbc;
    }

    throw std::invalid_argument("to_cipher: Unknown cipher: " + str);
}

std::vector<uint8_t> cb::crypto::decrypt(const cJSON* json,
                                         const uint8_t* data,
                                         size_t length) {
    Cipher cipher;
    std::vector<uint8_t> key;
    std::vector<uint8_t> iv;

    decodeJsonMeta(const_cast<cJSON*>(json), cipher, key, iv);
    return decrypt(cipher, key, iv, data, length);
}

std::string cb::crypto::digest(const Algorithm algorithm,
                               const std::string& passwd) {
    std::vector<uint8_t> data(passwd.size());
    memcpy(data.data(), passwd.data(), passwd.size());
    auto digest = cb::crypto::digest(algorithm, data);
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (const auto& c : digest) {
        ss << std::setw(2) << uint32_t(c);
    }

    return ss.str();
}
