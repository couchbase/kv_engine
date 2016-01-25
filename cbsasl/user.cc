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
#include "user.h"
#include "cbsasl_internal.h"

#include <atomic>
#include <openssl/evp.h>
#include <platform/base64.h>
#include <platform/random.h>
#include <stdexcept>

// Reduce the iteration count to 10 while we're waiting for MB-17617
std::atomic<int> IterationCount(10);

/**
 * Generate a salt and store it base64 encoded into the salt
 */
void generateSalt(std::vector<uint8_t>& bytes, std::string& salt) {
    Couchbase::RandomGenerator randomGenerator(true);

    if (!randomGenerator.getBytes(bytes.data(), bytes.size())) {
        throw std::runtime_error("Failed to get random bytes");
    }

    using Couchbase::Base64::encode;

    salt = encode(std::string(reinterpret_cast<char*>(bytes.data()),
                              bytes.size()));
}

Couchbase::User::User(const std::string& unm, const std::string& passwd)
    : username(unm),
      plaintextPassword(passwd),
      iterationCount(IterationCount.load()),
      dummy(false) {

#ifdef HAVE_PKCS5_PBKDF2_HMAC_SHA1
    std::vector<uint8_t> salt(Sha1DigestSize);

    generateSalt(salt, sha1Salt);

    if (PKCS5_PBKDF2_HMAC_SHA1(passwd.data(), int(passwd.size()),
                               salt.data(), int(salt.size()),
                               iterationCount,
                               Sha1DigestSize,
                               saltedSha1Password.data()) != 1) {
        throw std::runtime_error("PKCS5_PBKDF2_HMAC_SHA1 failed");
    }
#endif

#ifdef HAVE_PKCS5_PBKDF2_HMAC
    // The version of OpenSSL shipped with MacOSX does not contain
    // PKCS5_PBKDF2_HMAC and I don't feel like creating an implementation
    // just for MacOSX (the customer base is too small and this is
    // a non-essential functionality)
    salt.resize(Sha256DigestSize);
    generateSalt(salt, sha256Salt);
    if (PKCS5_PBKDF2_HMAC(passwd.data(), int(passwd.size()),
                          salt.data(), int(salt.size()),
                          iterationCount,
                          EVP_sha256(),
                          Sha256DigestSize,
                          saltedSha256Password.data()) != 1) {
        throw std::runtime_error("PKCS5_PBKDF2_HMAC SHA256 failed");
    }

    salt.resize(Sha512DigestSize);
    generateSalt(salt, sha512Salt);
    if (PKCS5_PBKDF2_HMAC(passwd.data(), int(passwd.size()),
                          salt.data(), int(salt.size()),
                          iterationCount,
                          EVP_sha512(),
                          Sha512DigestSize,
                          saltedSha512Password.data()) != 1) {
        throw std::runtime_error("PKCS5_PBKDF2_HMAC SHA512 failed");
    }
#endif
}

void cbsasl_set_hmac_iteration_count(cbsasl_getopt_fn getopt_fn,
                                     void* context) {

    const char* result = nullptr;
    unsigned int result_len;

    if (getopt_fn(context, nullptr, "hmac iteration count", &result,
                  &result_len) == CBSASL_OK) {
        if (result != nullptr) {
            std::string val(result, result_len);
            try {
                IterationCount.store(std::stoi(val));
            } catch (...) {
                cbsasl_log(nullptr, cbsasl_loglevel_t::Error,
                           "Failed to update HMAC iteration count");
            }
        }
    }
}

void Couchbase::User::generateSecrets(const Mechanism& mech) {
    if (!dummy) {
        throw std::runtime_error("Couchbase::User::generateSecrets can't be "
                                     "used on a real user object");
    }

    std::vector<uint8_t> salt;
    std::string passwd;

    switch (mech) {
#ifdef HAVE_PKCS5_PBKDF2_HMAC
    case Mechanism::SCRAM_SHA512:
        salt.resize(Sha512DigestSize);
        generateSalt(salt, passwd);
        generateSalt(salt, sha512Salt);
        if (PKCS5_PBKDF2_HMAC(passwd.data(), int(passwd.size()),
                              salt.data(), int(salt.size()),
                              iterationCount,
                              EVP_sha512(),
                              Sha512DigestSize,
                              saltedSha512Password.data()) != 1) {
            throw std::runtime_error("PKCS5_PBKDF2_HMAC SHA512 failed");
        }
        break;
    case Mechanism::SCRAM_SHA256:
        salt.resize(Sha256DigestSize);
        generateSalt(salt, passwd);
        generateSalt(salt, sha256Salt);
        if (PKCS5_PBKDF2_HMAC(passwd.data(), int(passwd.size()),
                              salt.data(), int(salt.size()),
                              iterationCount,
                              EVP_sha256(),
                              Sha256DigestSize,
                              saltedSha256Password.data()) != 1) {
            throw std::runtime_error("PKCS5_PBKDF2_HMAC SHA256 failed");
        }
        break;
#endif
#ifdef HAVE_PKCS5_PBKDF2_HMAC_SHA1
        case Mechanism::SCRAM_SHA1:
        salt.resize(Sha1DigestSize);
        generateSalt(salt, passwd);
        generateSalt(salt, sha1Salt);
        if (PKCS5_PBKDF2_HMAC_SHA1(passwd.data(), int(passwd.size()),
                                   salt.data(), int(salt.size()),
                                   iterationCount,
                                   Sha1DigestSize,
                                   saltedSha1Password.data()) != 1) {
            throw std::runtime_error("PKCS5_PBKDF2_HMAC_SHA1 failed");
        }
        break;
#endif
    default:
        throw std::logic_error("Couchbase::User::generateSecrets invalid mech");
    }
}
