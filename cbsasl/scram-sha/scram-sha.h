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

/**
 * This file contains the interface to the SCRAM-SHA1, SCRAM-SHA256 and
 * SCRAM-512 support.
 *
 * SCRAM is defined in https://www.ietf.org/rfc/rfc5802.txt
 *
 * The current implementation does not support channel binding (so we
 * don't advertise the -PLUS)
 */

#include "../cbcrypto.h"
#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <cbsasl/user.h>
#include <array>
#include <iostream>
#include <vector>

namespace cb::sasl::mechanism::scram {

class ScramShaBackend {
protected:
    ScramShaBackend(const Mechanism& mech, const cb::crypto::Algorithm algo)
        : mechanism(mech), algorithm(algo) {
    }

    /**
     * Add a property to the message list according to
     * https://www.ietf.org/rfc/rfc5802.txt section 5.1
     *
     * The purpose of these conversion functions is that we want to
     * make sure that we enforce the right format on the various attributes
     * and that we detect illegal keys.
     *
     * @param out the destination stream
     * @param key the key to add
     * @param value the string representation of the attribute to add
     * @param more set to true if we should add a trailing comma (more data
     *             follows)
     */
    static void addAttribute(std::ostream& out,
                             char key,
                             const std::string& value,
                             bool more);

    using AttributeMap = std::map<char, std::string>;

    /**
     * Decode the attribute list into a set. The attribute list looks like:
     * "k=value,y=value" etc
     *
     * @param list the list to parse
     * @param attributes where to store the attributes
     * @return true if success, false otherwise
     */
    static bool decodeAttributeList(Context& context,
                                    std::string_view list,
                                    AttributeMap& attributes);

    /**
     * Add a property to the message list according to
     * https://www.ietf.org/rfc/rfc5802.txt section 5.1
     *
     * The purpose of these conversion functions is that we want to
     * make sure that we enforce the right format on the various attributes
     * and that we detect illegal keys.
     *
     * @param out the destination stream
     * @param key the key to add
     * @param value the integer value of the attribute to add
     * @param more set to true if we should add a trailing comma (more data
     *             follows)
     */
    static void addAttribute(std::ostream& out, char key, int value, bool more);

    /**
     * Generate the Server Signature. It is computed as:
     *
     * SaltedPassword  := Hi(Normalize(password), salt, i)
     * ServerKey       := HMAC(SaltedPassword, "Server Key")
     * ServerSignature := HMAC(ServerKey, AuthMessage)
     *
     * @param server_key the server key to use
     */
    std::string getServerSignature(std::string_view server_key);

    /**
     * Generate the ClientSignature. It is computed as:
     *
     * AuthMessage     := client-first-message-bare + "," +
     *                    server-first-message + "," +
     *                    client-final-message-without-proof
     * ClientSignature := HMAC(StoredKey, AuthMessage)
     *
     * @param stored_key the stored key to use
     */
    std::string getClientSignature(std::string_view stored_key);

    /**
     * Get the AUTH message (as specified in the RFC)
     */
    std::string getAuthMessage();

    std::string client_first_message;
    std::string client_first_message_bare;
    std::string client_final_message;
    std::string client_final_message_without_proof;
    std::string server_first_message;
    std::string server_final_message;

    std::string clientNonce;
    std::string serverNonce;
    std::string nonce;

    Mechanism mechanism;
    const cb::crypto::Algorithm algorithm;
};

class ServerBackend : public cb::sasl::server::MechanismBackend,
                      public ScramShaBackend {
public:
    ServerBackend(server::ServerContext& ctx,
                  Mechanism mechanism,
                  cb::crypto::Algorithm algo,
                  std::function<std::string()> generateNonceFunction);

    std::pair<Error, std::string_view> start(std::string_view input) override;

    std::pair<Error, std::string_view> step(std::string_view input) override;

    std::string getName() const final {
        return ::to_string(mechanism);
    }

protected:
    pwdb::User user;
};

class Sha512ServerBackend : public ServerBackend {
public:
    explicit Sha512ServerBackend(
            server::ServerContext& ctx,
            std::function<std::string()> generateNonceFunction = {})
        : ServerBackend(ctx,
                        Mechanism::SCRAM_SHA512,
                        cb::crypto::Algorithm::SHA512,
                        std::move(generateNonceFunction)) {
    }
};

class Sha256ServerBackend : public ServerBackend {
public:
    explicit Sha256ServerBackend(
            server::ServerContext& ctx,
            std::function<std::string()> generateNonceFunction = {})
        : ServerBackend(ctx,
                        Mechanism::SCRAM_SHA256,
                        cb::crypto::Algorithm::SHA256,
                        std::move(generateNonceFunction)) {
    }
};

class Sha1ServerBackend : public ServerBackend {
public:
    explicit Sha1ServerBackend(
            server::ServerContext& ctx,
            std::function<std::string()> generateNonceFunction = {})
        : ServerBackend(ctx,
                        Mechanism::SCRAM_SHA1,
                        cb::crypto::Algorithm::SHA1,
                        std::move(generateNonceFunction)) {
    }
};

/**
 * Implementation of the class that provides the client side implementation
 * of the SCRAM-SHA[1,256,512]
 */
class ClientBackend : public cb::sasl::client::MechanismBackend,
                      public ScramShaBackend {
public:
    ClientBackend(
            client::GetUsernameCallback& user_cb,
            client::GetPasswordCallback& password_cb,
            client::ClientContext& ctx,
            Mechanism mechanism,
            cb::crypto::Algorithm algo,
            const std::function<std::string()>& generateNonceFunction,
            std::function<void(char, const std::string&)> property_listener);

    std::pair<Error, std::string_view> start() override;
    std::pair<Error, std::string_view> step(std::string_view input) override;

    std::string getName() const final {
        return ::to_string(mechanism);
    }

protected:
    std::string getClientKey();
    std::string getClientProof();

    bool generateSaltedPassword(const std::string& secret);

    std::string getSaltedPassword() {
        if (saltedPassword.empty()) {
            throw std::logic_error(
                    "getSaltedPassword called before salted "
                    "password is initialized");
        }
        return saltedPassword;
    }

    std::string saltedPassword;
    std::string salt;
    std::string errorMessage;
    std::function<void(char, const std::string&)> propertyListener;
    unsigned int iterationCount = 4096;
};

class Sha512ClientBackend : public ClientBackend {
public:
    Sha512ClientBackend(
            client::GetUsernameCallback& user_cb,
            client::GetPasswordCallback& password_cb,
            client::ClientContext& ctx,
            std::function<std::string()> generateNonceFunction = {},
            std::function<void(char, const std::string&)> propertyListener = {})
        : ClientBackend(user_cb,
                        password_cb,
                        ctx,
                        Mechanism::SCRAM_SHA512,
                        cb::crypto::Algorithm::SHA512,
                        generateNonceFunction,
                        std::move(propertyListener)) {
    }
};

class Sha256ClientBackend : public ClientBackend {
public:
    Sha256ClientBackend(
            client::GetUsernameCallback& user_cb,
            client::GetPasswordCallback& password_cb,
            client::ClientContext& ctx,
            std::function<std::string()> generateNonceFunction = {},
            std::function<void(char, const std::string&)> propertyListener = {})
        : ClientBackend(user_cb,
                        password_cb,
                        ctx,
                        Mechanism::SCRAM_SHA256,
                        cb::crypto::Algorithm::SHA256,
                        generateNonceFunction,
                        std::move(propertyListener)) {
    }
};

class Sha1ClientBackend : public ClientBackend {
public:
    Sha1ClientBackend(
            client::GetUsernameCallback& user_cb,
            client::GetPasswordCallback& password_cb,
            client::ClientContext& ctx,
            std::function<std::string()> generateNonceFunction = {},
            std::function<void(char, const std::string&)> propertyListener = {})
        : ClientBackend(user_cb,
                        password_cb,
                        ctx,
                        Mechanism::SCRAM_SHA1,
                        cb::crypto::Algorithm::SHA1,
                        generateNonceFunction,
                        std::move(propertyListener)) {
    }
};

} // namespace cb::sasl::mechanism::scram
