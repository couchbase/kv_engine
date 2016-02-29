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

std::atomic<int> IterationCount(4096);

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

    struct {
        Crypto::Algorithm algoritm;
        int digest_length;
        std::string& salt;
        std::vector<uint8_t>& salted_password;
    } algo_info[] = {
        {
            Crypto::Algorithm::SHA1,
            Crypto::SHA1_DIGEST_SIZE,
            sha1Salt,
            saltedSha1Password
        }, {
            Crypto::Algorithm::SHA256,
            Crypto::SHA256_DIGEST_SIZE,
            sha256Salt,
            saltedSha256Password
        }, {
            Crypto::Algorithm::SHA512,
            Crypto::SHA512_DIGEST_SIZE,
            sha512Salt,
            saltedSha512Password
        }
    };

    for (const auto& info : algo_info) {
        if (Crypto::isSupported(info.algoritm)) {
            std::vector<uint8_t> salt(info.digest_length);
            generateSalt(salt, info.salt);
            auto digest = Crypto::PBKDF2_HMAC(info.algoritm,
                                              passwd, salt,
                                              iterationCount);
            info.salted_password = digest;
        }
    }
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
    case Mechanism::SCRAM_SHA512:
        salt.resize(Crypto::SHA512_DIGEST_SIZE);
        generateSalt(salt, passwd);
        generateSalt(salt, sha512Salt);
        saltedSha256Password = Crypto::PBKDF2_HMAC(Crypto::Algorithm::SHA512,
                                                   passwd, salt,
                                                   iterationCount);
        break;
    case Mechanism::SCRAM_SHA256:
        salt.resize(Crypto::SHA256_DIGEST_SIZE);
        generateSalt(salt, passwd);
        generateSalt(salt, sha256Salt);
        saltedSha256Password = Crypto::PBKDF2_HMAC(Crypto::Algorithm::SHA256,
                                                   passwd, salt,
                                                   iterationCount);
        break;
    case Mechanism::SCRAM_SHA1:
        salt.resize(Crypto::SHA1_DIGEST_SIZE);
        generateSalt(salt, passwd);
        generateSalt(salt, sha1Salt);
        saltedSha1Password = Crypto::PBKDF2_HMAC(Crypto::Algorithm::SHA1,
                                                 passwd, salt, iterationCount);
        break;
    default:
        throw std::logic_error("Couchbase::User::generateSecrets invalid mech");
    }
}
