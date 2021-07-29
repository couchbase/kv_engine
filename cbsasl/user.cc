/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/user.h>
#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <platform/base64.h>
#include <platform/random.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

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

User::User(const nlohmann::json& json)
    : username(getUsernameField(json)), dummy(false) {
    // getUsernameField validated that json is in fact an object.
    // Iterate over the rest of the fields and pick out the values

    for (auto it = json.begin(); it != json.end(); ++it) {
        std::string label(it.key());
        if (label == "n") {
            // skip. we've already processed this
        } else if (label == "sha512") {
            User::PasswordMetaData pd(it.value());
            password[Mechanism::SCRAM_SHA512] = pd;
        } else if (label == "sha256") {
            User::PasswordMetaData pd(it.value());
            password[Mechanism::SCRAM_SHA256] = pd;
        } else if (label == "sha1") {
            User::PasswordMetaData pd(it.value());
            password[Mechanism::SCRAM_SHA1] = pd;
        } else if (label == "plain") {
            User::PasswordMetaData pd(Couchbase::Base64::decode(it.value()));
            password[Mechanism::PLAIN] = pd;
        } else if (label == "uuid") {
            uuid = cb::uuid::from_string(it.value().get<std::string>());
        } else if (label == "limits") {
            user::from_json(it.value(), limits);
        } else {
            throw std::runtime_error(
                    "cb::sasl::pwdb::User::User(): Invalid "
                    "label \"" +
                    label + "\" specified");
        }
    }
}

std::atomic<int> IterationCount(4096);

class ScamShaFallbackSalt {
public:
    ScamShaFallbackSalt();

    void set(const std::string& salt) {
        std::lock_guard<std::mutex> guard(mutex);
        data = cb::base64::decode(salt);
    }

    std::vector<uint8_t> get() {
        std::lock_guard<std::mutex> guard(mutex);
        return std::vector<uint8_t>{data};
    }

protected:
    mutable std::mutex mutex;
    std::vector<uint8_t> data;
} scramsha_fallback_salt;

/**
 * Generate a salt and store it base64 encoded into the salt
 */
static void generateSalt(std::vector<uint8_t>& bytes, std::string& salt) {
    cb::RandomGenerator randomGenerator;

    if (!randomGenerator.getBytes(bytes.data(), bytes.size())) {
        throw std::runtime_error("Failed to get random bytes");
    }

    using Couchbase::Base64::encode;

    salt = encode(
            std::string(reinterpret_cast<char*>(bytes.data()), bytes.size()));
}

ScamShaFallbackSalt::ScamShaFallbackSalt()
    : data(cb::crypto::SHA512_DIGEST_SIZE) {
    std::string ignore;
    generateSalt(data, ignore);
}

User UserFactory::create(const std::string& unm, const std::string& passwd) {
    User ret{unm, false};

    struct {
        cb::crypto::Algorithm algoritm;
        Mechanism mech;
    } algo_info[] = {{cb::crypto::Algorithm::SHA1, Mechanism::SCRAM_SHA1},
                     {cb::crypto::Algorithm::SHA256, Mechanism::SCRAM_SHA256},
                     {cb::crypto::Algorithm::SHA512, Mechanism::SCRAM_SHA512}};

    // The format of the plain password encoding is that we're appending the
    // generated hmac to the salt (which should be 16 bytes). This makes
    // our plain text password generation compatible with ns_server
    std::vector<uint8_t> pwentry(16);
    std::string saltstring;
    generateSalt(pwentry, saltstring);
    std::vector<uint8_t> pw;
    std::copy(passwd.begin(), passwd.end(), std::back_inserter(pw));

    const auto hmac = cb::crypto::HMAC(
            cb::crypto::Algorithm::SHA1,
            {reinterpret_cast<const char*>(pwentry.data()), pwentry.size()},
            {reinterpret_cast<const char*>(pw.data()), pw.size()});
    std::copy(hmac.begin(), hmac.end(), std::back_inserter(pwentry));
    std::string hash{(const char*)pwentry.data(), pwentry.size()};

    ret.password[Mechanism::PLAIN] = User::PasswordMetaData{hash};

    for (const auto& info : algo_info) {
        if (cb::crypto::isSupported(info.algoritm)) {
            ret.generateSecrets(info.mech, passwd);
        }
    }

    return ret;
}

User UserFactory::createDummy(const std::string& unm, const Mechanism& mech) {
    User ret{unm};

    // Generate a random password
    std::vector<uint8_t> salt;
    std::string passwd;

    switch (mech) {
    case Mechanism::SCRAM_SHA512:
        salt.resize(cb::crypto::SHA512_DIGEST_SIZE);
        break;
    case Mechanism::SCRAM_SHA256:
        salt.resize(cb::crypto::SHA256_DIGEST_SIZE);
        break;
    case Mechanism::SCRAM_SHA1:
        salt.resize(cb::crypto::SHA1_DIGEST_SIZE);
        break;
    case Mechanism::PLAIN:
        throw std::logic_error(
                "cb::cbsasl::UserFactory::createDummy invalid algorithm");
    }

    if (salt.empty()) {
        throw std::logic_error(
                "cb::cbsasl::UserFactory::createDummy invalid algorithm");
    }

    generateSalt(salt, passwd);

    // Generate the secrets by using that random password
    ret.generateSecrets(mech, passwd);

    return ret;
}

void UserFactory::setDefaultHmacIterationCount(int count) {
    IterationCount.store(count);
}

void UserFactory::setScramshaFallbackSalt(const std::string& salt) {
    scramsha_fallback_salt.set(salt);
}

void User::generateSecrets(Mechanism mech, std::string_view passwd) {
    std::vector<uint8_t> salt;
    std::string encodedSalt;
    cb::crypto::Algorithm algorithm = cb::crypto::Algorithm::MD5;

    switch (mech) {
    case Mechanism::SCRAM_SHA512:
        if (dummy) {
            auto fallback = scramsha_fallback_salt.get();
            auto hs_salt =
                    cb::crypto::HMAC(cb::crypto::Algorithm::SHA512,
                                     getUsername().getRawValue(),
                                     {reinterpret_cast<char*>(fallback.data()),
                                      fallback.size()});
            std::copy(hs_salt.begin(), hs_salt.end(), std::back_inserter(salt));
        } else {
            salt.resize(cb::crypto::SHA512_DIGEST_SIZE);
        }
        algorithm = cb::crypto::Algorithm::SHA512;
        break;
    case Mechanism::SCRAM_SHA256:
        if (dummy) {
            auto fallback = scramsha_fallback_salt.get();
            auto hs_salt =
                    cb::crypto::HMAC(cb::crypto::Algorithm::SHA256,
                                     getUsername().getRawValue(),
                                     {reinterpret_cast<char*>(fallback.data()),
                                      fallback.size()});
            std::copy(hs_salt.begin(), hs_salt.end(), std::back_inserter(salt));
        } else {
            salt.resize(cb::crypto::SHA256_DIGEST_SIZE);
        }
        algorithm = cb::crypto::Algorithm::SHA256;
        break;
    case Mechanism::SCRAM_SHA1:
        if (dummy) {
            auto fallback = scramsha_fallback_salt.get();
            auto hs_salt =
                    cb::crypto::HMAC(cb::crypto::Algorithm::SHA1,
                                     getUsername().getRawValue(),
                                     {reinterpret_cast<char*>(fallback.data()),
                                      fallback.size()});
            std::copy(hs_salt.begin(), hs_salt.end(), std::back_inserter(salt));
        } else {
            salt.resize(cb::crypto::SHA1_DIGEST_SIZE);
        }
        algorithm = cb::crypto::Algorithm::SHA1;
        break;
    case Mechanism::PLAIN:
        throw std::logic_error(
                "cb::cbsasl::User::generateSecrets invalid algorithm");
    }

    if (algorithm == cb::crypto::Algorithm::MD5) {
        // gcc7 complains that algorithm may have been uninitialized when we
        // used it below. This would happen if the user provided a mech
        // which isn't handled above. If that happens we should just
        // throw an exception.
        throw std::invalid_argument(
                "cb::sasl::User::generateSecrets: invalid mechanism provided");
    }

    if (salt.empty()) {
        throw std::logic_error(
                "cb::cbsasl::User::generateSecrets invalid algorithm");
    }

    if (dummy) {
        using Couchbase::Base64::encode;
        encodedSalt = encode(
                std::string{reinterpret_cast<char*>(salt.data()), salt.size()});
    } else {
        generateSalt(salt, encodedSalt);
    }

    auto digest = cb::crypto::PBKDF2_HMAC(
            algorithm,
            passwd,
            {reinterpret_cast<const char*>(salt.data()), salt.size()},
            IterationCount);

    password[mech] = PasswordMetaData(digest, encodedSalt, IterationCount);
}

User::PasswordMetaData::PasswordMetaData(const nlohmann::json& obj) {
    if (!obj.is_object()) {
        throw std::runtime_error(
                "cb::cbsasl::User::PasswordMetaData: invalid object type");
    }

    auto h = obj.find("h");
    auto s = obj.find("s");
    auto i = obj.find("i");

    if (h == obj.end() || s == obj.end() || i == obj.end()) {
        throw std::runtime_error(
                "cb::cbsasl::User::PasswordMetaData: missing mandatory "
                "attributes");
    }

    if (!h->is_string()) {
        throw std::runtime_error(
                "cb::cbsasl::User::PasswordMetaData: hash"
                " should be a string");
    }

    if (!s->is_string()) {
        throw std::runtime_error(
                "cb::cbsasl::User::PasswordMetaData: salt"
                " should be a string");
    }

    if (!i->is_number()) {
        throw std::runtime_error(
                "cb::cbsasl::User::PasswordMetaData: iteration"
                " count should be a number");
    }

    if (obj.size() != 3) {
        throw std::runtime_error(
                "cb::cbsasl::User::PasswordMetaData: invalid "
                "number of labels specified");
    }

    salt = s->get<std::string>();
    Couchbase::Base64::decode(salt);
    password.assign(Couchbase::Base64::decode(*h));
    iteration_count = gsl::narrow<int>(*i);
    if (iteration_count < 0) {
        throw std::runtime_error(
                "cb::cbsasl::User::PasswordMetaData: iteration "
                "count must be positive");
    }
}

nlohmann::json User::PasswordMetaData::to_json() const {
    nlohmann::json ret;
    std::string s((char*)password.data(), password.size());
    ret["h"] = Couchbase::Base64::encode(s);
    ret["s"] = salt;
    ret["i"] = iteration_count;

    return ret;
}

nlohmann::json User::to_json() const {
    nlohmann::json ret;

    ret["n"] = username.getRawValue();
    for (auto& e : password) {
        auto obj = e.second.to_json();
        switch (e.first) {
        case Mechanism::PLAIN:
            ret["plain"] = obj["h"];
            break;
        case Mechanism::SCRAM_SHA512:
            ret["sha512"] = obj;
            break;
        case Mechanism::SCRAM_SHA256:
            ret["sha256"] = obj;
            break;
        case Mechanism::SCRAM_SHA1:
            ret["sha1"] = obj;
            break;
        default:
            throw std::runtime_error(
                    "cb::cbsasl::User::toJSON(): Unsupported mech");
        }
    }
    ret["uuid"] = ::to_string(uuid);
    nlohmann::json json = limits;
    if (!json.empty()) {
        ret["limits"] = limits;
    }

    return ret;
}

const User::PasswordMetaData& User::getPassword(const Mechanism& mech) const {
    const auto iter = password.find(mech);

    if (iter == password.end()) {
        throw std::invalid_argument(
                "cb::cbsasl::User::getPassword: requested "
                "mechanism not available");
    } else {
        return iter->second;
    }
}

namespace user {
void to_json(nlohmann::json& json, const Limits& limits) {
    json = {{"ingress_mib_per_min", limits.ingress_mib_per_min},
            {"egress_mib_per_min", limits.egress_mib_per_min},
            {"num_connections", limits.num_connections},
            {"num_ops_per_min", limits.num_ops_per_min}};
}

void from_json(const nlohmann::json& json, Limits& limits) {
    limits = {};
    for (auto it = json.begin(); it != json.end(); ++it) {
        const std::string label(it.key());
        if (!it.value().is_number()) {
            throw std::runtime_error(
                    "cb::sasl::pwdb::user::from_json: All limits must be "
                    "numeric values: " +
                    label);
        }
        if (label == "ingress_mib_per_min") {
            limits.ingress_mib_per_min = it.value().get<uint64_t>();
        } else if (label == "egress_mib_per_min") {
            limits.egress_mib_per_min = it.value().get<uint64_t>();
        } else if (label == "num_connections") {
            limits.num_connections = it.value().get<uint64_t>();
        } else if (label == "num_ops_per_min") {
            limits.num_ops_per_min = it.value().get<uint64_t>();

        } else {
            throw std::runtime_error(
                    "cb::sasl::pwdb::user::from_json: Invalid "
                    "label \"" +
                    label + "\" specified");
        }
    }
}
} // namespace user
} // namespace cb::sasl::pwdb
