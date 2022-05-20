/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <cbcrypto/cbcrypto.h>
#include <cbsasl/mechanism.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/uuid.h>
#include <utilities/logtags.h>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

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
         * @param i iteration count
         */
        explicit PasswordMetaData(std::string h, std::string s = "", int i = 0)
            : salt(std::move(s)), password(std::move(h)), iteration_count(i) {
        }

        /**
         * Create a new instance of the PasswordMetaData and
         * initialize it from the specified JSON. The JSON *MUST*
         * be of the following syntax:
         *
         *     {
         *         "h" : "base64 encoded password",
         *         "s" : "base64 encoded salt",
         *         "i" : "iteration count"
         *     }
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

        int getIterationCount() const {
            return iteration_count;
        }

    protected:
        /// Base 64 encoded version of the salt
        std::string salt;

        /// The actual password used
        std::string password;

        /// The iteration count used for generating the password
        int iteration_count = 0;
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
     * Create a user entry and initialize it from the supplied
     * JSON structure:
     *
     *       {
     *            "limits" : {
     *                "egress_mib_per_min": 1,
     *                "ingress_mib_per_min": 1,
     *                "num_ops_per_min": 1,
     *                "num_connections": 1
     *            },
     *            "n" : "username",
     *            "sha512" : {
     *                "h" : "base64 encoded sha512 hash of the password",
     *                "s" : "base64 encoded salt",
     *                "i" : iteration-count
     *            },
     *            "sha256" : {
     *                "h" : "base64 encoded sha256 hash of the password",
     *                "s" : "base64 encoded salt",
     *                "i" : iteration-count
     *            },
     *            "sha1" : {
     *                "h" : "base64 encoded sha1 hash of the password",
     *                "s" : "base64 encoded salt",
     *                "i" : iteration-count
     *            },
     *            "plain" : "base64 encoded hex version of sha1 hash
     *                       of plain text password",
     *            "uuid" : "00000000-0000-0000-0000-000000000000"
     *       }
     *
     * @param obj the object containing the JSON description
     * @throws std::runtime_error if there is a syntax error
     */
    explicit User(const nlohmann::json& json);

    /**
     * Get the username for this entry
     */
    const UserData getUsername() const {
        return username;
    }

    /// Get the users UUID
    cb::uuid::uuid_t getUuid() const {
        return uuid;
    }

    /**
     * Is this a dummy object or not?
     */
    bool isDummy() const {
        return dummy;
    }

    /**
     * Get the password metadata used for the requested mechanism
     *
     * @param mech the mechanism to retrieve the metadata for
     * @return the passwod metadata
     * @throws std::illegal_arguement if the mechanism isn't supported
     */
    const PasswordMetaData& getPassword(const Mechanism& mech) const;

    /**
     * Generate a JSON encoding of this object (it is primarily used
     * from the test suite to generate a user database)
     */
    nlohmann::json to_json() const;

protected:
    friend class UserFactory;

    /**
     * Generate (and insert) the "secrets" entry for the provided mechanism
     * and password.
     */
    void generateSecrets(Mechanism mech, std::string_view passwd);

    std::map<Mechanism, PasswordMetaData> password;

    UserData username;

    cb::uuid::uuid_t uuid{};

    /// To avoid leaking if a user exists or not we want to be able to
    /// complete a full SASL authentication cycle (and not fail in the
    /// initial SASL roundtrip for SCRAM). This flag indicates that this
    /// is such a dummy object that we want the authentcation to fail
    /// anyway (even if the user ended up using the same password as
    /// we randomly generated).
    bool dummy;
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
     *
     * @return a newly created dummy object
     */
    static User create(const std::string& name, const std::string& pw);

    /**
     * Construct a dummy user object that may be used in authentication
     *
     * @param name username
     * @param mech generate just the password for this mechanism
     *
     * @return a newly created dummy object
     */
    static User createDummy(const std::string& name, const Mechanism& mech);

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
