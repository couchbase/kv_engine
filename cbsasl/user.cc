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
#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <platform/base64.h>
#include <platform/random.h>

#ifdef HAVE_LIBSODIUM
#include <sodium.h>
#endif

#include <atomic>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

using cb::crypto::Algorithm;

namespace cb::sasl::pwdb {

static std::string getUsernameField(const nlohmann::json& json) {
    if (!json.is_object()) {
        throw std::runtime_error(
                "cb::sasl::pwdb::getUsernameField(): provided json MUST be an "
                "object");
    }
    auto n = json.find("n");
    if (n == json.end()) {
        throw std::runtime_error(
                "cb::sasl::pwdb::getUsernameField(): missing mandatory label "
                "'n'");
    }
    if (!n->is_string()) {
        throw std::runtime_error(
                "cb::sasl::pwdb::getUsernameField(): 'n' must be a string");
    }
    return n->get<std::string>();
}

User::User(const nlohmann::json& json) : username(getUsernameField(json)) {
    // getUsernameField validated that json is in fact an object.
    // Iterate over the rest of the fields and pick out the values

    for (auto it = json.begin(); it != json.end(); ++it) {
        std::string label(it.key());
        if (label == "n") {
            // skip. we've already processed this
        } else if (label == "hash") {
            if (password_hash) {
                throw std::runtime_error("cb::sasl::pwdb::User::User(" +
                                         username.getSanitizedValue() +
                                         ") contains hash and plain entry");
            }
            password_hash = User::PasswordMetaData(it.value(), true);
        } else if (label == "sha512") {
            scram_sha_512 =
                    ScramPasswordMetaData(it.value(), Algorithm::SHA512);
        } else if (label == "sha256") {
            scram_sha_256 =
                    ScramPasswordMetaData(it.value(), Algorithm::SHA256);
        } else if (label == "sha1") {
            scram_sha_1 = ScramPasswordMetaData(it.value(), Algorithm::SHA1);
        } else if (label == "plain") {
            if (password_hash) {
                throw std::runtime_error("cb::sasl::pwdb::User::User(" +
                                         username.getSanitizedValue() +
                                         ") contains hash and plain entry");
            }
            if (it.value().is_object()) {
                User::PasswordMetaData pd(it.value(), true);
                password_hash = pd;
            } else {
                const auto decoded = Couchbase::Base64::decode(
                        it.value().get<std::string>());
                // the salt is the first 16 bytes, the rest is the password
                std::string salt =
                        Couchbase::Base64::encode(decoded.substr(0, 16));
                std::string hash =
                        Couchbase::Base64::encode(decoded.substr(16));
                User::PasswordMetaData pd(
                        nlohmann::json{{"h", hash}, {"s", salt}}, true);
                password_hash = pd;
            }
        } else {
            throw std::runtime_error(
                    "cb::sasl::pwdb::User::User(): Invalid label \"" + label +
                    "\" specified");
        }
    }

    if (!password_hash) {
        throw std::runtime_error("cb::sasl::pwdb::User::User(" +
                                 username.getSanitizedValue() +
                                 ") must contain either hash or plain entry");
    }
}

User::User(const nlohmann::json& json, UserData unm)
    : username(std::move(unm)) {
    if (!json.is_object()) {
        throw std::runtime_error("User(\"" + username.getSanitizedValue() +
                                 "\"): provided json MUST be an object");
    }

    for (auto it = json.begin(); it != json.end(); ++it) {
        std::string label(it.key());
        if (label == "hash") {
            password_hash = User::PasswordMetaData(it.value());
        } else if (label == "scram-sha-512") {
            if (it.value().value("server_key", "").empty()) {
                scram_sha_512 =
                        ScramPasswordMetaData(it.value(), Algorithm::SHA512);
            } else {
                scram_sha_512 = ScramPasswordMetaData(it.value());
            }
        } else if (label == "scram-sha-256") {
            if (it.value().value("server_key", "").empty()) {
                scram_sha_256 =
                        ScramPasswordMetaData(it.value(), Algorithm::SHA256);
            } else {
                scram_sha_256 = ScramPasswordMetaData(it.value());
            }
        } else if (label == "scram-sha-1") {
            if (it.value().value("server_key", "").empty()) {
                scram_sha_1 =
                        ScramPasswordMetaData(it.value(), Algorithm::SHA1);
            } else {
                scram_sha_1 = ScramPasswordMetaData(it.value());
            }
        } else {
            throw std::runtime_error("User(\"" + username.getSanitizedValue() +
                                     "\"): Invalid attribute \"" + label +
                                     "\" specified");
        }
    }

    if (!isPasswordHashAvailable(Algorithm::Argon2id13) &&
        !isPasswordHashAvailable(Algorithm::DeprecatedPlain)) {
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

    void set(const std::string& salt) {
        auto decoded = Couchbase::Base64::decode(salt);
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

    salt = Couchbase::Base64::encode(std::string{
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
                         std::function<bool(crypto::Algorithm)> callback) {
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

    if (callback(Algorithm::DeprecatedPlain)) {
        auto mystr = cb::crypto::pwhash(
                Algorithm::DeprecatedPlain,
                passwd,
                {reinterpret_cast<const char*>(salt.data()), salt.size()});

        ret.password_hash = User::PasswordMetaData{
                nlohmann::json{{"algorithm", "SHA-1"},
                               {"hash", Couchbase::Base64::encode(mystr)},
                               {"salt", saltstring}}};
    }

    for (const auto& alg : std::vector<Algorithm>{{Algorithm::SHA1,
                                                   Algorithm::SHA256,
                                                   Algorithm::SHA512,
#ifdef HAVE_LIBSODIUM
                                                   Algorithm::Argon2id13
#endif
         }}) {
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
        encodedSalt = Couchbase::Base64::encode(
                std::string{reinterpret_cast<char*>(salt.data()), salt.size()});
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
            {{"server_key", Couchbase::Base64::encode(server_key)},
             {"stored_key", Couchbase::Base64::encode(stored_key)},
             {"salt", encodedSalt},
             {"iterations", IterationCount.load()}});
}

static User::PasswordMetaData generateArgon2id13Secret(
        std::string_view passwd) {
#ifdef HAVE_LIBSODIUM
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
                           {"hash", Couchbase::Base64::encode(generated)},
                           {"salt", encodedSalt},
                           {"time", ops},
                           {"memory", mcost},
                           {"parallelism", 1}});
#else
    throw std::runtime_error("built without support for Argon2id");
#endif
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
        password_hash = generateArgon2id13Secret(passwd);
        return;
    case Algorithm::DeprecatedPlain:
        break;
    }
    throw std::invalid_argument("User::generateSecrets(): Invalid algorithm");
}

static nlohmann::json rewrite_password_metadata_obj(const nlohmann::json& obj) {
    nlohmann::json ret = obj;
    if (ret.contains("h")) {
        ret["hash"] = ret["h"];
        ret.erase("h");
    }
    if (ret.contains("s")) {
        ret["salt"] = ret["s"];
        ret.erase("s");
    }
    if (ret.contains("i")) {
        ret["iterations"] = ret["i"];
        ret.erase("i");
    } else {
        ret["algorithm"] = "SHA-1";
    }
    return ret;
}

User::PasswordMetaData::PasswordMetaData(const nlohmann::json& obj, bool)
    : PasswordMetaData(rewrite_password_metadata_obj(obj)) {
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

    for (auto it = obj.begin(); it != obj.end(); ++it) {
        const std::string label = it.key();
        if (label == "hash") {
            if (!it->is_string()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): hash must be a string");
            }
            password = Couchbase::Base64::decode(it->get<std::string>());
        } else if (label == "salt") {
            if (!it->is_string()) {
                throw std::invalid_argument(
                        "PasswordMetaData(): salt must be a string");
            }
            salt = Couchbase::Base64::decode(it->get<std::string>());
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
            if (algorithm != "argon2id" && algorithm != "SHA-1") {
                throw std::invalid_argument(
                        R"(PasswordMetaData(): algorithm must be set to "argon2id" or "SHA-1")");
            }
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
    } else if (algorithm == "SHA-1") {
        if (m) {
            throw std::invalid_argument(
                    "PasswordMetaData(): memory can't be set with SHA-1");
        }
        if (p) {
            throw std::invalid_argument(
                    "PasswordMetaData(): parallelism can't be set with SHA-1");
        }
        if (t) {
            throw std::invalid_argument(
                    "PasswordMetaData(): time can't be set with SHA-1");
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
    ret["hash"] = Couchbase::Base64::encode(password);
    ret["salt"] = Couchbase::Base64::encode(salt);
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
