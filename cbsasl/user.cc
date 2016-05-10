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
#include <string>
#include <memory>

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
      dummy(false) {

    struct {
        Crypto::Algorithm algoritm;
        Mechanism mech;
    } algo_info[] = {
        {
            Crypto::Algorithm::SHA1,
            Mechanism::SCRAM_SHA1
        }, {
            Crypto::Algorithm::SHA256,
            Mechanism::SCRAM_SHA256
        }, {
            Crypto::Algorithm::SHA512,
            Mechanism::SCRAM_SHA512
        }
    };

    PasswordMetaData pd(passwd);
    password[Mechanism::PLAIN] = pd;

    for (const auto& info : algo_info) {
        if (Crypto::isSupported(info.algoritm)) {
            generateSecrets(info.mech, passwd);
        }
    }
}

Couchbase::User::User(cJSON* obj)
    : dummy(false) {
    if (obj == nullptr) {
        throw std::runtime_error("Couchbase::User::User: obj cannot be null");
    }

    if (obj->type != cJSON_Object) {
        throw std::runtime_error("Couchbase::User::User: Invalid object type");
    }

    for (auto* o = obj->child; o != nullptr; o = o->next) {
        std::string label(o->string);
        if (label == "n") {
            username.assign(o->valuestring);
        } else if (label == "sha512") {
            PasswordMetaData pd(o);
            password[Mechanism::SCRAM_SHA512] = pd;
        } else if (label == "sha256") {
            PasswordMetaData pd(o);
            password[Mechanism::SCRAM_SHA256] = pd;
        } else if (label == "sha1") {
            PasswordMetaData pd(o);
            password[Mechanism::SCRAM_SHA1] = pd;
        } else if (label == "plain") {
            PasswordMetaData pd(Couchbase::Base64::decode(o->valuestring));
            password[Mechanism::PLAIN] = pd;
        } else {
            throw std::runtime_error("Couchbase::User::User: Invalid "
                                         "label \"" + label + "\" specified");
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
        break;
    case Mechanism::SCRAM_SHA256:
        salt.resize(Crypto::SHA256_DIGEST_SIZE);
        break;
    case Mechanism::SCRAM_SHA1:
        salt.resize(Crypto::SHA1_DIGEST_SIZE);
        break;
    case Mechanism::PLAIN:
    case Mechanism::UNKNOWN:
        throw std::logic_error("Couchbase::User::generateSecrets invalid algorithm");
    }

    if (salt.empty()) {
        throw std::logic_error("Couchbase::User::generateSecrets invalid algorithm");
    }

    // Generate a random password
    generateSalt(salt, passwd);

    // Generate the secrets by using that random password
    generateSecrets(mech, passwd);
}

void Couchbase::User::generateSecrets(const Mechanism& mech,
                                      const std::string& passwd) {

    std::vector<uint8_t> salt;
    std::string encodedSalt;
    Crypto::Algorithm algorithm;

    switch (mech) {
    case Mechanism::SCRAM_SHA512:
        salt.resize(Crypto::SHA512_DIGEST_SIZE);
        algorithm = Crypto::Algorithm::SHA512;
        break;
    case Mechanism::SCRAM_SHA256:
        salt.resize(Crypto::SHA256_DIGEST_SIZE);
        algorithm = Crypto::Algorithm::SHA256;
        break;
    case Mechanism::SCRAM_SHA1:
        salt.resize(Crypto::SHA1_DIGEST_SIZE);
        algorithm = Crypto::Algorithm::SHA1;
        break;
    case Mechanism::PLAIN:
    case Mechanism::UNKNOWN:
        throw std::logic_error("Couchbase::User::generateSecrets invalid algorithm");
    }

    if (salt.empty()) {
        throw std::logic_error("Couchbase::User::generateSecrets invalid algorithm");
    }

    generateSalt(salt, encodedSalt);
    auto digest = Crypto::PBKDF2_HMAC(algorithm, passwd, salt, IterationCount);

    password[mech] =
        PasswordMetaData(std::string((const char*)digest.data(), digest.size()),
                         encodedSalt, IterationCount);
}

Couchbase::User::PasswordMetaData::PasswordMetaData(cJSON* obj) {
    if (obj->type != cJSON_Object) {
        throw std::runtime_error("Couchbase::User::PasswordMetaData: invalid"
                                     " object type");
    }

    auto* h = cJSON_GetObjectItem(obj, "h");
    auto* s = cJSON_GetObjectItem(obj, "s");
    auto* i = cJSON_GetObjectItem(obj, "i");

    if (h == nullptr || s == nullptr || i == nullptr) {
        throw std::runtime_error("Couchbase::User::PasswordMetaData: missing "
                                     "mandatory attributes");
    }

    if (h->type != cJSON_String) {
        throw std::runtime_error("Couchbase::User::PasswordMetaData: hash"
                                     " should be a string");
    }

    if (s->type != cJSON_String) {
        throw std::runtime_error("Couchbase::User::PasswordMetaData: salt"
                                     " should be a string");
    }

    if (i->type != cJSON_Number) {
        throw std::runtime_error("Couchbase::User::PasswordMetaData: iteration"
                                     " count should be a number");
    }

    if (cJSON_GetArraySize(obj) != 3) {
        throw std::runtime_error("Couchbase::User::PasswordMetaData: invalid "
                                     "number of labels specified");
    }

    salt.assign(s->valuestring);
    // validate that we may decode the salt
    Couchbase::Base64::decode(salt);
    password.assign(Couchbase::Base64::decode(h->valuestring));
    iteration_count = i->valueint;
    if (iteration_count < 0) {
        throw std::runtime_error("Couchbase::User::PasswordMetaData: iteration "
                                     "count must be positive");
    }
}

cJSON* Couchbase::User::PasswordMetaData::to_json() const {
    auto* ret = cJSON_CreateObject();
    std::string s((char*)password.data(), password.size());
    cJSON_AddStringToObject(ret, "h", Couchbase::Base64::encode(s).c_str());
    cJSON_AddStringToObject(ret, "s", salt.c_str());
    cJSON_AddNumberToObject(ret, "i", iteration_count);

    return ret;
}

unique_cJSON_ptr Couchbase::User::to_json() const {
    auto* ret = cJSON_CreateObject();

    cJSON_AddStringToObject(ret, "n", username.c_str());
    for (auto& e : password) {
        auto* obj = e.second.to_json();
        switch (e.first) {
        case Mechanism::PLAIN:
            cJSON_AddStringToObject(ret, "plain",
                                    cJSON_GetObjectItem(obj, "h")->valuestring);
            cJSON_Delete(obj);
            break;

        case Mechanism::SCRAM_SHA512:
            cJSON_AddItemToObject(ret, "sha512", obj);
            break;

        case Mechanism::SCRAM_SHA256:
            cJSON_AddItemToObject(ret, "sha256", obj);
            break;
        case Mechanism::SCRAM_SHA1:
            cJSON_AddItemToObject(ret, "sha1", obj);
            break;
        default:
            throw std::runtime_error(
                "Couchbase::User::toJSON(): Unsupported mech");
        }
    }

    return unique_cJSON_ptr(ret);
}

std::string Couchbase::User::to_string() const {
    auto json = to_json();
    char* ptr = cJSON_Print(json.get());
    std::string ret(ptr);
    cJSON_Free(ptr);
    return ret;
}

const Couchbase::User::PasswordMetaData& Couchbase::User::getPassword(
    const Mechanism& mech) const {

    const auto iter = password.find(mech);

    if (iter == password.end()) {
        throw std::invalid_argument("Couchbase::User::getPassword: requested "
                                        "mechanism not available");
    } else {
        return iter->second;
    }
}
