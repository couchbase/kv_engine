/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cbsasl/mechanism.h>
#include <cbsasl/scram_password_meta_data.h>
#include <nlohmann/json.hpp>
#include <utilities/logtags.h>
#include <cstdint>
#include <optional>
#include <string>

namespace cb::crypto {
enum class Algorithm;
}

namespace cb::sasl::pwdb {

class UserFactory;

class User {
public:
    /**
     * To allow multiple authentication schemes we need to store
     * some metadata for each of the different scheme.
     */
    class PasswordMetaData {
    public:
        /// Create a new instance of the PasswordMetaData
        PasswordMetaData() = default;

        /**
         * Create a new instance of the PasswordMetaData with
         * the specified attributes
         *
         * @param h the password
         * @param s the salt used (Base64 encoded)
         * @param json additional properties
         */
        explicit PasswordMetaData(std::string h,
                                  std::string s,
                                  nlohmann::json json = {})
            : salt(std::move(s)),
              password(std::move(h)),
              properties(std::move(json)) {
        }

        /**
         * Create a new instance of the PasswordMetaData and
         * initialize it from the specified JSON.
         *
         * @param obj reference to the json structure
         */
        explicit PasswordMetaData(const nlohmann::json& obj);

        /**
         * This is a helper function used from the unit tests
         * to generate a JSON representation of a user
         */
        nlohmann::json to_json() const;

        const std::string& getSalt() const {
            return salt;
        }

        const std::string& getPassword() const {
            return password;
        }

        const std::string& getAlgorithm() const {
            return algorithm;
        }

        const nlohmann::json& getProperties() const {
            return properties;
        }

        nlohmann::json& getProperties() {
            return properties;
        }

    protected:
        /// The salt in raw format
        std::string salt;

        /// The password hash in raw format
        std::string password;

        /// The algorithm in play (not used by SCRAM)
        std::string algorithm;

        /// Various per hash type properties
        nlohmann::json properties;
    };

    /**
     * Create an empty user object
     *
     * A dummy user object is used by SCRAM sasl authentication
     * so that the "attacking" client would need to perform the
     * full authentication step in order to authenticate towards
     * the server instead of returning "no such user" if we failed
     * to look up the user (to respond with the SALT and iteration
     * count).
     *
     * @param unm the username for the object (potentially empty)
     * @param dmy should we create a dummy object or not
     */
    explicit User(std::string unm = "", const bool dmy = true)
        : username(std::move(unm)), dummy(dmy) {
    }

    /**
     * Create a user entry and initialize it from the JSON structure
     *
     * @param obj the object containing the JSON description
     * @param username the name of the user
     * @throws std::runtime_error if there is a syntax error
     */
    User(const nlohmann::json& json, UserData username);

    /**
     * Get the username for this entry
     */
    const UserData getUsername() const {
        return username;
    }

    /**
     * Is this a dummy object or not?
     */
    bool isDummy() const {
        return dummy;
    }

    bool isPasswordHashAvailable(cb::crypto::Algorithm algorithm) const;

    /// Get the password metadata used for SCRAM authentication
    const ScramPasswordMetaData& getScramMetaData(
            cb::crypto::Algorithm algorithm) const;
    const PasswordMetaData& getPaswordHash() const {
        return password_hash.value();
    };

    /**
     * Generate a JSON encoding of this object (it is primarily used
     * from the test suite to generate a user database)
     */
    nlohmann::json to_json() const;

protected:
    friend class UserFactory;

    void generateSecrets(crypto::Algorithm algo, std::string_view passwd);

    std::optional<ScramPasswordMetaData> scram_sha_512;
    std::optional<ScramPasswordMetaData> scram_sha_256;
    std::optional<ScramPasswordMetaData> scram_sha_1;
    std::optional<PasswordMetaData> password_hash;

    UserData username;

    /// To avoid leaking if a user exists or not we want to be able to
    /// complete a full SASL authentication cycle (and not fail in the
    /// initial SASL roundtrip for SCRAM). This flag indicates that this
    /// is such a dummy object that we want the authentcation to fail
    /// anyway (even if the user ended up using the same password as
    /// we randomly generated).
    bool dummy = false;
};

void to_json(nlohmann::json& json, const User& user);

/**
 * The UserFactory class is used to generate a User Object from a
 * username and plain text password.
 */
class UserFactory {
public:
    /**
     * Construct a new user object
     *
     * @param name username
     * @param pw password
     * @param callback callback to return true/false if the provided
     *                 algorithm should be created
     *
     * @return a newly created dummy object
     */
    static User create(const std::string& name,
                       const std::string& pw,
                       std::function<bool(crypto::Algorithm)> callback = {});

    /**
     * Construct a dummy user object that may be used in authentication
     *
     * @param name username
     * @param mech generate just the password for this mechanism
     *
     * @return a newly created dummy object
     */
    static User createDummy(const std::string& name,
                            cb::crypto::Algorithm algorithm);

    /**
     * Set the default iteration count to use (may be overridden by
     * the cbsasl property function.
     *
     * @param count the new iteration count to use
     */
    static void setDefaultHmacIterationCount(int count);

    static void setScramshaFallbackSalt(const std::string& salt);
};
} // namespace cb::sasl::pwdb
