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
#include <cbsasl/user.h>
#include <folly/Synchronized.h>
#include <memcached/limits.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>
#include <platform/random.h>
#include <sodium.h>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>

using cb::crypto::Algorithm;

namespace cb::sasl::pwdb {

User::User(const nlohmann::json& json, UserData unm)
    : username(std::move(unm)) {
    const auto name = username.getRawValue();
    if (name.size() > cb::limits::MaxUsernameLength ||
        name.find_first_of(R"(()<>,;:\"/[]?={})") != std::string::npos ||
        name.find('@') != name.rfind('@')) {
        throw std::runtime_error("User(\"" + username.getSanitizedValue() +
                                 "\"): is not a valid username");
    }

    if (!json.is_object()) {
        throw std::runtime_error("User(\"" + username.getSanitizedValue() +
                                 "\"): provided json MUST be an object");
    }

    for (auto it = json.begin(); it != json.end(); ++it) {
        std::string label(it.key());
        if (label == "hash") {
            password_hash = User::PasswordMetaData(it.value());
        } else if (label == "scram-sha-512") {
            scram_sha_512 = ScramPasswordMetaData(it.value());
        } else if (label == "scram-sha-256") {
            scram_sha_256 = ScramPasswordMetaData(it.value());
        } else if (label == "scram-sha-1") {
            scram_sha_1 = ScramPasswordMetaData(it.value());
        } else {
            throw std::runtime_error("User(\"" + username.getSanitizedValue() +
                                     "\"): Invalid attribute \"" + label +
                                     "\" specified");
        }
    }

    if (!password_hash) {
        throw std::runtime_error("User(\"" + username.getSanitizedValue() +
                                 "\") must contain hash entry");
    }
}

std::atomic<int> IterationCount(4096);

class ScamShaFallbackSalt {
public:
    static ScamShaFallbackSalt& instance() {
        static ScamShaFallbackSalt _instance;
        return _instance;
    }

    void set(std::string_view salt) {
        auto decoded = cb::base64::decode(salt);
        fallback.swap(decoded);
    }

    std::string get() {
        return *fallback.rlock();
    }

protected:
    ScamShaFallbackSalt();
    folly::Synchronized<std::string> fallback;
};

/**
 * Generate a salt and store it base64 encoded into the salt
 */
static void generateSalt(std::vector<uint8_t>& bytes, std::string& salt) {
    cb::RandomGenerator randomGenerator;

    if (!randomGenerator.getBytes(bytes.data(), bytes.size())) {
        throw std::runtime_error("Failed to get random bytes");
    }

    salt = cb::base64::encode(std::string_view{
            reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

ScamShaFallbackSalt::ScamShaFallbackSalt() {
    std::vector<uint8_t> data(cb::crypto::SHA512_DIGEST_SIZE);
    std::string encoded;
    generateSalt(data, encoded);
    set(encoded);
}

User UserFactory::create(const std::string& unm,
                         const std::string& passwd,
                         std::function<bool(crypto::Algorithm)> callback,
                         std::string_view password_hash_type) {
    using namespace std::string_view_literals;
    User ret{unm, false};

    // The format of the plain password encoding is that we're appending the
    // generated hmac to the salt (which should be 16 bytes). This makes
    // our plain text password generation compatible with ns_server
    std::vector<uint8_t> salt(16);
    std::string saltstring;
    generateSalt(salt, saltstring);

    if (!callback) {
        callback = [](Algorithm) { return true; };
    }

    if (password_hash_type.empty()) {
        if (callback(Algorithm::DeprecatedPlain)) {
            ret.generatePasswordHash("SHA-1"sv, passwd);
        }

        if (callback(Algorithm::Argon2id13)) {
            ret.generatePasswordHash("argon2id"sv, passwd);
        }
    } else {
        ret.generatePasswordHash(password_hash_type, passwd);
    }

    for (const auto& alg : std::vector<Algorithm>{
                 {Algorithm::SHA1, Algorithm::SHA256, Algorithm::SHA512}}) {
        if (callback(alg)) {
            ret.generateSecrets(alg, passwd);
        }
    }
    return ret;
}

User UserFactory::createDummy(const std::string& unm, Algorithm algorithm) {
    User ret{unm};

    // Generate a random password
    std::vector<uint8_t> salt;
    std::string passwd;

    switch (algorithm) {
    case Algorithm::SHA1:
        salt.resize(cb::crypto::SHA1_DIGEST_SIZE);
        break;
    case Algorithm::SHA256:
        salt.resize(cb::crypto::SHA256_DIGEST_SIZE);
        break;
    case Algorithm::SHA512:
        salt.resize(cb::crypto::SHA512_DIGEST_SIZE);
        break;
    case Algorithm::Argon2id13:
    case Algorithm::DeprecatedPlain:
        throw std::invalid_argument(
                "cb::cbsasl::UserFactory::createDummy invalid algorithm");
        break;
    }

    if (salt.empty()) {
        throw std::logic_error(
                "cb::cbsasl::UserFactory::createDummy invalid algorithm");
    }

    generateSalt(salt, passwd);
    // Generate the secrets by using that random password
    ret.generateSecrets(algorithm, passwd);

    return ret;
}

void UserFactory::setDefaultHmacIterationCount(int count) {
    IterationCount.store(count);
}

void UserFactory::setScramshaFallbackSalt(const std::string& salt) {
    ScamShaFallbackSalt::instance().set(salt);
}

static ScramPasswordMetaData generateShaSecrets(Algorithm algorithm,
                                                std::string_view passwd,
                                                std::string_view unm,
                                                bool dummy) {
    std::vector<uint8_t> salt;
    std::string encodedSalt;

    if (dummy) {
        auto hs_salt = cb::crypto::HMAC(
                algorithm, unm, ScamShaFallbackSalt::instance().get());
        std::copy(hs_salt.begin(), hs_salt.end(), std::back_inserter(salt));
        encodedSalt = cb::base64::encode(std::string_view{
                reinterpret_cast<char*>(salt.data()), salt.size()});
    } else {
        switch (algorithm) {
        case Algorithm::SHA512:
            salt.resize(cb::crypto::SHA512_DIGEST_SIZE);
            break;
        case Algorithm::SHA256:
            salt.resize(cb::crypto::SHA256_DIGEST_SIZE);
            break;
        case Algorithm::SHA1:
            salt.resize(cb::crypto::SHA1_DIGEST_SIZE);
            break;
        case Algorithm::Argon2id13:
        case Algorithm::DeprecatedPlain:
            throw std::invalid_argument(
                    "generateShaSecrets(): Argon2id13 can't be reached here");
        }
        generateSalt(salt, encodedSalt);
    }

    auto digest = cb::crypto::pwhash(
            algorithm,
            passwd,
            {reinterpret_cast<const char*>(salt.data()), salt.size()},
            nlohmann::json{{"iterations", IterationCount.load()}});

    const auto server_key = cb::crypto::HMAC(algorithm, digest, "Server Key");
    const auto stored_key = cb::crypto::digest(
            algorithm, cb::crypto::HMAC(algorithm, digest, "Client Key"));

    return ScramPasswordMetaData(
            {{"server_key", cb::base64::encode(server_key)},
             {"stored_key", cb::base64::encode(stored_key)},
             {"salt", encodedSalt},
             {"iterations", IterationCount.load()}});
}

static User::PasswordMetaData generateArgon2id13Secret(
        std::string_view passwd) {
    std::string encodedSalt;
    std::vector<uint8_t> salt(crypto_pwhash_argon2id_saltbytes());
    generateSalt(salt, encodedSalt);

    const auto ops = getenv("MEMCACHED_UNIT_TESTS")
                             ? crypto_pwhash_opslimit_min()
                             : crypto_pwhash_opslimit_moderate();
    const auto mcost = getenv("MEMCACHED_UNIT_TESTS")
                               ? crypto_pwhash_memlimit_min()
                               : crypto_pwhash_argon2i_memlimit_moderate();

    auto generated = cb::crypto::pwhash(
            Algorithm::Argon2id13,
            passwd,
            {reinterpret_cast<const char*>(salt.data()), salt.size()},
            {{"time", ops}, {"memory", mcost}});

    return User::PasswordMetaData(
            nlohmann::json{{"algorithm", "argon2id"},
                           {"hash", cb::base64::encode(generated)},
                           {"salt", encodedSalt},
                           {"time", ops},
                           {"memory", mcost},
                           {"parallelism", 1}});
}

void User::generateSecrets(Algorithm algo, std::string_view passwd) {
    switch (algo) {
    case Algorithm::SHA1:
        scram_sha_1 = generateShaSecrets(
                algo, passwd, getUsername().getRawValue(), dummy);
        return;
    case Algorithm::SHA256:
        scram_sha_256 = generateShaSecrets(
                algo, passwd, getUsername().getRawValue(), dummy);
        return;
    case Algorithm::SHA512:
        scram_sha_512 = generateShaSecrets(
                algo, passwd, getUsername().getRawValue(), dummy);
        return;

    case Algorithm::Argon2id13:
    case Algorithm::DeprecatedPlain:
        break;
    }
    throw std::invalid_argument("User::generateSecrets(): Invalid algorithm");
}

void User::generatePasswordHash(std::string_view password_hash_type,
                                std::string_view passwd) {
    using namespace std::string_view_literals;
    if (password_hash_type == "pbkdf2-hmac-sha512"sv) {
        std::vector<uint8_t> salt(cb::crypto::SHA512_DIGEST_SIZE);
        std::string saltstring;
        generateSalt(salt, saltstring);
        const auto iterations = IterationCount.load();
        auto digest = cb::crypto::pwhash(
                Algorithm::SHA512,
                passwd,
                {reinterpret_cast<const char*>(salt.data()), salt.size()},
                nlohmann::json{{"iterations", iterations}});
        password_hash = User::PasswordMetaData{
                nlohmann::json{{"algorithm", "pbkdf2-hmac-sha512"},
                               {"hash", cb::base64::encode(digest)},
                               {"salt", saltstring},
                               {"iterations", iterations}}};
    } else if (password_hash_type == "argon2id"sv) {
        password_hash = generateArgon2id13Secret(passwd);
    } else if (password_hash_type == "SHA-1"sv) {
        std::vector<uint8_t> salt(16);
        std::string saltstring;
        generateSalt(salt, saltstring);
        auto mystr = cb::crypto::pwhash(
                Algorithm::DeprecatedPlain,
                passwd,
                {reinterpret_cast<const char*>(salt.data()), salt.size()});
        password_hash = User::PasswordMetaData{
                nlohmann::json{{"algorithm", "SHA-1"},
                               {"hash", cb::base64::encode(mystr)},
                               {"salt", saltstring}}};
    } else {
        throw std::invalid_argument("Unsupported password hash type");
    }
}

User::PasswordMetaData::PasswordMetaData(const nlohmann::json& obj) {
    if (!obj.is_object()) {
        throw std::invalid_argument("PasswordMetaData: invalid object type");
    }

    if (!obj.contains("algorithm")) {
        throw std::invalid_argument(
                "PasswordMetaData(): algorithm must be specified");
    }

    std::optional<size_t> m;
    std::optional<size_t> t;
    std::optional<size_t> p;
    std::optional<size_t> i;

    for (auto it = obj.begin(); it != obj.end(); ++it) {
        const std::string label = it.key();
        if (label == "hash") {
            if (!it->is_string()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): hash must be a string");
            }
            password = cb::base64::decode(it->get<std::string>());
        } else if (label == "salt") {
            if (!it->is_string()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): salt must be a string");
            }
            salt = cb::base64::decode(it->get<std::string>());
        } else if (label == "memory") {
            if (!it->is_number()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): memory must be a number");
            }
            m = it->get<std::size_t>();
            properties["memory"] = *m;
        } else if (label == "parallelism") {
            if (!it->is_number()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): parallelism must be a number");
            }
            p = it->get<std::size_t>();
            properties["parallelism"] = *p;
        } else if (label == "time") {
            if (!it->is_number()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): time must be a number");
            }
            t = it->get<std::size_t>();
            properties["time"] = *t;
        } else if (label == "algorithm") {
            if (!it->is_string()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): algorithm must be a string");
            }
            algorithm = it->get<std::string>();
            if (algorithm != "argon2id" && algorithm != "SHA-1" &&
                algorithm != "pbkdf2-hmac-sha512") {
                throw std::invalid_argument(
                        R"(PasswordMetaData(): algorithm must be set to "argon2id", "pbkdf2-hmac-sha512" or "SHA-1")");
            }
        } else if (label == "iterations") {
            if (!it->is_number()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): iterations must be a number");
            }
            i = it->get<std::size_t>();
            properties["iterations"] = *i;
        } else {
            throw std::invalid_argument(
                    "PasswordMetaData(): Invalid attribute: \"" + label + "\"");
        }
    }

    if (algorithm == "argon2id") {
        if (!m) {
            throw std::invalid_argument(
                    "PasswordMetaData(): argon2id requires memory to be set");
        }
        if (!p) {
            throw std::invalid_argument(
                    "PasswordMetaData(): argon2id requires parallelism to be "
                    "set");
        }
        if (*p != 1) {
            throw std::invalid_argument(
                    "PasswordMetaData(): parallelism must be set to 1");
        }
        if (!t) {
            throw std::invalid_argument(
                    "PasswordMetaData(): argon2id requires time to be set");
        }
        if (i) {
            throw std::invalid_argument(
                    "PasswordMetaData(): argon2id should not contain "
                    "iterations");
        }
    } else {
        if (m) {
            throw std::invalid_argument(
                    "PasswordMetaData(): memory can't be set with SHA-1 or "
                    "pbkdf2-hmac-sha512");
        }
        if (p) {
            throw std::invalid_argument(
                    "PasswordMetaData(): parallelism can't be set with SHA-1 "
                    "or pbkdf2-hmac-sha512");
        }
        if (t) {
            throw std::invalid_argument(
                    "PasswordMetaData(): time can't be set with SHA-1 or "
                    "pbkdf2-hmac-sha512");
        }
        if (algorithm == "pbkdf2-hmac-sha512") {
            if (!i) {
                throw std::invalid_argument(
                        "PasswordMetaData(): pbkdf2-hmac-sha512 requires "
                        "iterations to be set");
            }
        } else {
            if (i) {
                throw std::invalid_argument(
                        "PasswordMetaData(): iterations can't be set with "
                        "SHA-1");
            }
        }
    }

    if (salt.empty()) {
        throw std::invalid_argument(
                "PasswordMetaData(): salt must be specified");
    }

    if (password.empty()) {
        throw std::invalid_argument(
                "PasswordMetaData(): hash must be specified");
    }
}

nlohmann::json User::PasswordMetaData::to_json() const {
    auto ret = properties;
    ret["algorithm"] = algorithm;
    ret["hash"] = cb::base64::encode(password);
    ret["salt"] = cb::base64::encode(salt);
    return ret;
}

nlohmann::json User::to_json() const {
    nlohmann::json ret;
    if (password_hash) {
        ret["hash"] = password_hash->to_json();
    }

    if (scram_sha_512) {
        ret["scram-sha-512"] = scram_sha_512->to_json();
    }

    if (scram_sha_256) {
        ret["scram-sha-256"] = scram_sha_256->to_json();
    }

    if (scram_sha_1) {
        ret["scram-sha-1"] = scram_sha_1->to_json();
    }

    return ret;
}

bool User::isPasswordHashAvailable(Algorithm algorithm) const {
    switch (algorithm) {
    case Algorithm::SHA1:
        return scram_sha_1.has_value();
    case Algorithm::SHA256:
        return scram_sha_256.has_value();
    case Algorithm::SHA512:
        return scram_sha_512.has_value();
    case Algorithm::Argon2id13:
        return password_hash && password_hash->getAlgorithm() == "argon2id";
    case Algorithm::DeprecatedPlain:
        return password_hash && password_hash->getAlgorithm() == "SHA-1";
    }
    throw std::invalid_argument(
            "User::isPasswordHashAvailable: Invalid algorithm");
}

const ScramPasswordMetaData& User::getScramMetaData(
        cb::crypto::Algorithm algorithm) const {
    switch (algorithm) {
    case Algorithm::SHA1:
        if (scram_sha_1) {
            return *scram_sha_1;
        }
        break;
    case Algorithm::SHA256:
        if (scram_sha_256) {
            return *scram_sha_256;
        }
        break;
    case Algorithm::SHA512:
        if (scram_sha_512) {
            return *scram_sha_512;
        }
        break;
    case Algorithm::Argon2id13:
    case Algorithm::DeprecatedPlain:
        break;
    }
    throw std::invalid_argument(
            "getScramMetaData(): the requested algorithm can't be used with "
            "SCRAM");
}

void to_json(nlohmann::json& json, const User& user) {
    json = user.to_json();
}

} // namespace cb::sasl::pwdb
